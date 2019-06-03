from django.core.management.base import BaseCommand, CommandError
from buckets.utils import BucketFileManagement, delete_table_for_model_class
from buckets.models import Bucket

class Command(BaseCommand):
    '''
    向存储桶插入一些对象元数据，用于测试数据
    '''

    help = 'Insert some test data, object metadata, into a bucket'

    def add_arguments(self, parser):
        parser.add_argument(
            '--bucket-name', default=None, dest='bucket_name',
            help='Name of bucket will be clearing,',
        )
        parser.add_argument(
            '--count', default='100', dest='count', type=int,
            help='How many test data you want insert into a bucket.',
        )
        parser.add_argument(
            '--base-name', default=None, dest='base_name',
            help='Base name for object metadata.',
        )

    def handle(self, *args, **options):
        bucket_name = options.get('bucket_name')
        # 未给出参数
        if not bucket_name:
            bucket_name = input('Please input a bucket name:')

        count = options.get('count')
        base_name = options.get('base_name')
        if not base_name:
            base_name = ''

        self.insert_test_data(bucket_name=bucket_name, count=count, base_name=base_name)

    def insert_test_data(self, bucket_name, count, base_name):
        bucket = Bucket.get_bucket_by_name(bucket_name)
        if not bucket:
            raise CommandError("Bucket is not existing")

        self.stdout.write(self.style.NOTICE(f'Will insert {count} test data into bucket named {bucket_name}'))
        table_name = bucket.get_bucket_table_name()
        bfm = BucketFileManagement(collection_name=table_name)
        ModelClass = bfm.get_obj_model_class()

        log_times = int(count / 10)
        for i in range(count):
            obj_name = f'{base_name}{i}'
            obj = ModelClass(na=obj_name,  # 全路径文件名
                             name=obj_name,  # 文件名
                             fod=True,  # 文件
                             si=0,  # 文件大小
                             did=0)
            obj.do_save()

            if (i > log_times) and (i % log_times) == 0:
                self.stdout.write(self.style.NOTICE(f'Already insert {i} test data into bucket named {bucket_name}'))

        self.stdout.write(self.style.NOTICE(f'Insert {count} test data into bucket named {bucket_name} is completed'))

