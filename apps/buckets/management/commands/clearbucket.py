import threading
from datetime import timedelta

from django.utils import timezone
from django.core.management.base import BaseCommand, CommandError
from django.db.utils import ProgrammingError

from buckets.utils import BucketFileManagement, delete_table_for_model_class
from buckets.models import Archive
from utils.oss import HarborObject


class Command(BaseCommand):
    """
    清理bucket命令，清理满足彻底删除条件的对象和目录
    """
    pool_sem = threading.Semaphore(1000)  # 定义最多同时启用多少个线程

    help = 'Really delete objects and directories that have been deleted from a bucket'
    _clear_datetime = None

    def add_arguments(self, parser):
        parser.add_argument(
            '--bucket-name', default=None, dest='bucket-name',
            help='Name of bucket have been deleted will be clearing,',
        )

        parser.add_argument(
            '--days-ago', default='30', dest='days_ago', type=int,
            help='Clear objects and directories that have been deleted more than days ago.',
        )
        parser.add_argument(
            # 当命令行有此参数时取值const, 否则取值default
            '--all-deleted', default=None, nargs='?', dest='all_deleted', const=True,
            help='All buckets that have been deleted will be clearing.',
        )

        parser.add_argument(
            '--multithreading', default=None, nargs='?', dest='multithreading', const=True,
            help='use multithreading mode.',
        )

    def handle(self, *args, **options):
        multithreading = options.get('multithreading')
        if multithreading:
            self.stdout.write(self.style.NOTICE('work mode in multithreading'))
        else:
            self.stdout.write(self.style.NOTICE('work mode in one thread'))

        days_ago = options.get('days_ago', 30)
        try:
            days_ago = int(days_ago)
            if days_ago < 0:
                days_ago = 0
        except Exception as e:
            raise CommandError(f"Clearing buckets cancelled. invalid 'days-ago', {str(e)}")

        self._clear_datetime = timezone.now() - timedelta(days=days_ago)

        buckets = self.get_buckets(**options)

        if input('Are you sure you want to do this?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("Clearing buckets cancelled.")

        multithreading = options.get('multithreading')
        if multithreading:
            self.clear_buckets_multithreading(buckets)
        else:
            self.clear_buckets(buckets)

    def get_buckets(self, **options):
        """
        获取给定的bucket或所有bucket
        :param options:
        :return:
        """
        bucket_name = options['bucket-name']
        all_deleted = options['all_deleted']

        # 指定名字的桶
        if bucket_name:
            self.stdout.write(self.style.NOTICE('Will clear all buckets named {0}'.format(bucket_name)))
            return Archive.objects.filter(name=bucket_name, type=Archive.TYPE_COMMON,
                                          archive_time__lt=self._clear_datetime).all()

        # 全部已删除归档的桶
        if all_deleted:
            self.stdout.write(self.style.NOTICE('Will clear all buckets that have been softly deleted '))
            return Archive.objects.filter(type=Archive.TYPE_COMMON, archive_time__lt=self._clear_datetime).all()

        # 未给出参数
        if not bucket_name:
            bucket_name = input('Please input a bucket name:')

        self.stdout.write(self.style.NOTICE('Will clear all buckets named {0}'.format(bucket_name)))
        return Archive.objects.filter(name=bucket_name, type=Archive.TYPE_COMMON).all()

    def get_objs_and_dirs(self, model_class, num=100):
        """
        获取对象,默认最多返回1000条

        :param model_class: 对象和目录的模型类
        :param num: 获取数量
        :return:
        """
        try:
            objs = model_class.objects.filter(fod=True).all()[:num]
        except Exception as e:
            self.stdout.write(self.style.ERROR('Error when clearing bucket table named {0},'.format(
                model_class.Meta.db_table) + str(e)))
            return None

        return objs

    def is_meet_delete_time(self, bucket):
        """
        归档的桶是否满足删除时间要求，即是否可以清理

        :param bucket: Archive()
        :return:
            True    # 满足
            False   # 不满足
        """
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

    def clear_one_bucket(self, bucket, in_thread: bool = True):
        """
        清除一个bucket中满足删除条件的对象和目录

        :param bucket: Archive()
        :param in_thread: 是否是线程
        :return:
        """
        self.stdout.write('Now clearing bucket named {0}'.format(bucket.name))
        table_name = bucket.get_bucket_table_name()
        model_class = BucketFileManagement(collection_name=table_name).get_obj_model_class()

        # 已删除归档的桶不满足删除时间条件，直接返回不清理
        if not self.is_meet_delete_time(bucket):
            return

        pool_name = bucket.get_pool_name()
        try:
            while True:
                ho = HarborObject(pool_name=pool_name, obj_id='')
                objs = self.get_objs_and_dirs(model_class=model_class)
                self.stdout.write('new loop, get objs ok')
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
                                f"Failed to deleted a object from ceph:" + err))
                    else:
                        obj.delete()

                self.stdout.write(self.style.WARNING(
                    f"Success deleted {objs.count()} objects from bucket {bucket.name}."))

            # 如果bucket对应表没有对象了，删除bucket和表
            if model_class.objects.filter(fod=True).count() == 0:
                if delete_table_for_model_class(model_class):
                    bucket.delete()
                    self.stdout.write(self.style.WARNING(f"deleted bucket and it's table:{bucket.name}"))
                else:
                    self.stdout.write(self.style.ERROR(f'deleted bucket table error:{bucket.name}'))

            self.stdout.write(self.style.SUCCESS('Clearing bucket named {0} is completed'.format(bucket.name)))
        except (ProgrammingError, Exception) as e:
            if e.args[0] == 1146:   # table not exists
                bucket.delete()
                self.stdout.write(self.style.WARNING(f"only deleted bucket({bucket.name}),{e}"))
            else:
                self.stdout.write(self.style.ERROR(f'deleted bucket({bucket.name}) table error: {e}'))

        if in_thread:
            self.pool_sem.release()     # 可用线程数+1

    def clear_buckets_multithreading(self, buckets):
        """
        多线程清理bucket
        :param buckets:
        :return: None
        """
        for bucket in buckets:
            if self.pool_sem.acquire():     # 可用线程数-1，控制线程数量，当前正在运行线程数量达到上限会阻塞等待
                worker = threading.Thread(target=self.clear_one_bucket, kwargs={'bucket': bucket})
                worker.start()

        # 等待所有线程结束
        while True:
            c = threading.active_count()
            if c <= 1:
                break

        self.stdout.write(self.style.SUCCESS('Successfully clear {0} buckets'.format(buckets.count())))

    def clear_buckets(self, buckets):
        for bucket in buckets:
            try:
                self.clear_one_bucket(bucket, in_thread=False)
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'deleted bucket({bucket.name}) table error: {e}'))

        self.stdout.write(self.style.SUCCESS('Successfully clear {0} buckets'.format(buckets.count())))
