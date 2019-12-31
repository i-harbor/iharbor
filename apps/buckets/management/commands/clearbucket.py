import threading
from datetime import timedelta

from django.utils import timezone
from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q
from django.db.utils import ProgrammingError

from buckets.utils import BucketFileManagement, delete_table_for_model_class
from buckets.models import Archive
from utils.oss import HarborObject


class Command(BaseCommand):
    '''
    清理bucket命令，清理满足彻底删除条件的对象和目录
    '''
    pool_sem = threading.Semaphore(1000)  # 定义最多同时启用多少个线程

    help = 'Really delete objects and directories that have been deleted from a bucket'

    def add_arguments(self, parser):
        parser.add_argument(
            '--bucket-name', default=None, dest='bucketname',
            help='Name of bucket have been deleted will be clearing,',
        )

        parser.add_argument(
            '--daysago', default='30', dest='daysago', type=int,
            help='Clear objects and directories that have been deleted more than days ago.',
        )
        parser.add_argument(
            '--all-deleted', default=None, nargs='?', dest='all_deleted', const=True, # 当命令行有此参数时取值const, 否则取值default
            help='All buckets that have been deleted will be clearing.',
        )

    def handle(self, *args, **options):
        daysago = options.get('daysago', 30)
        try:
            daysago = int(daysago)
            if daysago < 0:
                daysago = 0
        except:
            raise CommandError("Clearing buckets cancelled.")

        self._clear_datetime = timezone.now() - timedelta(days=daysago)

        buckets = self.get_buckets(**options)

        if input('Are you sure you want to do this?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("Clearing buckets cancelled.")

        self.clear_buckets(buckets)

    def get_buckets(self, **options):
        '''
        获取给定的bucket或所有bucket
        :param options:
        :return:
        '''
        bucketname = options['bucketname']
        all_deleted = options['all_deleted']

        # 指定名字的桶
        if bucketname:
            self.stdout.write(self.style.NOTICE('Will clear all buckets named {0}'.format(bucketname)))
            return Archive.objects.filter(name=bucketname, archive_time__lt=self._clear_datetime).all()

        # 全部已删除归档的桶
        if all_deleted:
            self.stdout.write(self.style.NOTICE('Will clear all buckets that have been softly deleted '))
            return Archive.objects.filter(archive_time__lt=self._clear_datetime).all()

        # 未给出参数
        if not bucketname:
            bucketname = input('Please input a bucket name:')

        self.stdout.write(self.style.NOTICE('Will clear all buckets named {0}'.format(bucketname)))
        return Archive.objects.filter(name=bucketname).all()

    def get_objs_and_dirs(self, modelclass, num=1000):
        '''
        获取对象和目录,默认最多返回1000条

        :param modelclass: 对象和目录的模型类
        :param num: 获取数量
        :param by_filters: 是否按条件过滤，当整个bucket是删除的状态就不需要过滤
        :return:
        '''
        try:
            objs = modelclass.objects.all()[:num]
        except Exception as e:
            self.stdout.write(self.style.ERROR('Error when clearing bucket table named {0},'.format(
                modelclass.Meta.db_table) + str(e)))
            return None
        return objs

    def is_meet_delete_time(self, bucket):
        '''
        归档的桶是否满足删除时间要求，即是否可以清理

        :param bucket: Archive()
        :return:
            True    # 满足
            False   # 不满足
        '''
        archive_time = bucket.archive_time.replace(tzinfo=None)

        if timezone.is_aware(self._clear_datetime):
            if not timezone.is_aware(archive_time):
                archive_time = timezone.make_aware(archive_time)
        else:
            if not timezone.is_naive(archive_time):
                archive_time = timezone.make_naive(archive_time)

        if archive_time < self._clear_datetime:
            return True

        return False

    def clear_one_bucket(self, bucket):
        '''
        清除一个bucket中满足删除条件的对象和目录

        :param bucket: Archive()
        :return:
        '''
        self.stdout.write('Now clearing bucket named {0}'.format(bucket.name))
        table_name = bucket.get_bucket_table_name()
        modelclass = BucketFileManagement(collection_name=table_name).get_obj_model_class()

        # 已删除归档的桶不满足删除时间条件，直接返回不清理
        if not self.is_meet_delete_time(bucket):
            return

        ho = HarborObject(obj_id='')
        try:
            while True:
                objs = self.get_objs_and_dirs(modelclass=modelclass)
                if objs is None or len(objs) <= 0:
                    break

                for obj in objs:
                    if obj.is_file():
                        obj_key = obj.get_obj_key(bucket.id)
                        ho.reset_obj_id_and_size(obj_id=obj_key, obj_size=obj.si)
                        ok, err = ho.delete(obj_size=obj.si)
                        if ok:
                            obj.delete()
                        else:
                            self.stdout.write(self.style.WARNING(
                                f"Failed to deleted a object from ceph:"+ err))
                    else:
                        obj.delete()

                self.stdout.write(self.style.WARNING(f"Success deleted {objs.count()} objects from bucket {bucket.name}."))

            # 如果bucket对应表已空，删除bucket和表
            if modelclass.objects.count() == 0:
                if delete_table_for_model_class(modelclass):
                    bucket.delete()
                    self.stdout.write(self.style.WARNING(f"deleted bucket and it's table:{bucket.name}"))
                else:
                    self.stdout.write(self.style.ERROR(f'deleted bucket table error:{bucket.name}'))

            self.stdout.write('Clearing bucket named {0} is completed'.format(bucket.name))
        except (ProgrammingError, Exception) as e:
            if e.args[0] == 1146: # table not exists
                bucket.delete()
                self.stdout.write(self.style.WARNING(f"only deleted bucket({bucket.name}),{e}"))
            else:
                self.stdout.write(self.style.ERROR(f'deleted bucket({bucket.name}) table error: {e}' ))

        self.pool_sem.release() # 可用线程数+1

    def clear_buckets(self, buckets):
        '''
        多线程清理bucket
        :param buckets:
        :return: None
        '''
        for bucket in buckets:
            if self.pool_sem.acquire(): # 可用线程数-1，控制线程数量，当前正在运行线程数量达到上限会阻塞等待
                worker = threading.Thread(target=self.clear_one_bucket, kwargs={'bucket': bucket})
                worker.start()

        # 等待所有线程结束
        while True:
            c = threading.active_count()
            if c <= 1:
                break

        self.stdout.write(self.style.SUCCESS('Successfully clear {0} buckets'.format(buckets.count())))
