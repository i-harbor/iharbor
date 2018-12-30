from datetime import datetime, timedelta

from django.core.management.base import BaseCommand, CommandError
from mongoengine.queryset.visitor import Q as mQ

from buckets.utils import BucketFileManagement
from buckets.models import Bucket
from utils.oss.rados_interfaces import CephRadosObject

class Command(BaseCommand):
    '''
    清理bucket命令，清理满足彻底删除条件的对象和目录
    '''

    help = 'Really delete objects and directories that have been softly deleted from a bucket'

    def add_arguments(self, parser):
        parser.add_argument(
            '--bucket-name', default=None, dest='bucketname',
            help='Name of bucket will be clearing,',
        )
        parser.add_argument(
            '--all', default=None, nargs='?', dest='all', const=True, # 当命令行有此参数时取值const, 否则取值default
            help='All buckets will be clearing',
        )
        parser.add_argument(
            '--daysago', default='30', dest='daysago', type=int,
            help='Clear objects and directories that have been softly deleted more than days ago.',
        )

    def handle(self, *args, **options):
        daysago = options.get('daysago', 30)
        try:
            daysago = int(daysago)
            if daysago < 0:
                daysago = 0
        except:
            raise CommandError("Clearing buckets cancelled.")

        self._clear_datetime = datetime.utcnow() - timedelta(days=daysago)

        buckets = self.get_buckets(**options)

        if input('Are you sure you want to do this?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("Clearing buckets cancelled.")

        for bucket in buckets:
            self.clear_one_bucket(bucket)

        self.stdout.write(self.style.SUCCESS('Successfully clear {0} buckets'.format(buckets.count())))

    def get_buckets(self, **options):
        '''
        获取给定的bucket或所有bucket
        :param options:
        :return:
        '''
        bucketname = options['bucketname']
        all = options['all']

        if all is not None:
            self.stdout.write(self.style.NOTICE(
                'Will clear objs or dirs that have been softly deleted before {0} from all buckets.'.format(self._clear_datetime)))
            return Bucket.objects.all()

        if not bucketname:
            bucketname = input('Please input a bucket name:')

        self.stdout.write(self.style.NOTICE('Will clear all buckets named {0}'.format(bucketname)))
        return Bucket.objects.filter(name=bucketname).all()

    def get_objs_and_dirs(self, modelclass, num=1000, by_filters=True):
        '''
        获取满足要被删除条件的对象和目录,默认最多返回1000条

        :param modelclass: 对象和目录的模型类
        :param num: 获取数量
        :param by_filters: 是否按条件过滤，当整个bucket是删除的状态就不需要过滤
        :return:
        '''
        if not by_filters:
            return modelclass.objects().limit(num)

        return modelclass.objects(mQ(sds=True) & mQ(upt__lt=self._clear_datetime)).limit(num)

    def clear_one_bucket(self, bucket):
        '''
        清除一个bucket中满足删除条件的对象和目录

        :param bucket: Bucket obj
        :return:
        '''
        self.stdout.write(self.style.NOTICE('Now clearing bucket named {0}'.format(bucket.name)))

        collection_name = bucket.get_bucket_mongo_collection_name()
        modelclass = BucketFileManagement(collection_name=collection_name).get_bucket_file_class()

        # 如果bucket是已删除的, 并且满足删除条件
        by_filters = True
        modified_time = bucket.modified_time.replace(tzinfo=None)
        if bucket.is_soft_deleted() and  modified_time < self._clear_datetime:
            by_filters = False

        cro = CephRadosObject(obj_id='')
        while True:
            objs = self.get_objs_and_dirs(modelclass=modelclass, by_filters=by_filters)
            if objs.count() <= 0:
                break

            for obj in objs:
                if obj.is_file():
                    cro.reset_obj_id(str(obj.id))
                    if cro.delete():
                        obj.delete()
                else:
                    obj.delete()

        # 如果bucket对应集合已空，删除bucket和collection
        if by_filters == False:
            if modelclass.objects().count() == 0:
                modelclass.drop_collection()
                bucket.delete()

        self.stdout.write(self.style.NOTICE('Clearing bucket named {0} is completed'.format(bucket.name)))

