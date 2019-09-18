import threading
from datetime import timedelta

from django.utils import timezone
from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q
from django.db.utils import ProgrammingError

from buckets.utils import BucketFileManagement, delete_table_for_model_class
from buckets.models import Bucket
from utils.oss import HarborObject


class Command(BaseCommand):
    '''
    为bucket对应的表，清理满足彻底删除条件的对象和目录
    '''
    pool_sem = threading.Semaphore(1000)  # 定义最多同时启用多少个线程

    help = """** manage.py buckettable --all --sql="sql template" **    
           **  manage.py buckettable --bucket-name=xxx --sql="sql template" **  
           ** sql template example:**  
           ** ALTER TABLE {table_name} ADD md5 CHAR(32) NOT NULL DEFAULT '' COMMENT 'MD5' **  
           ** ALTER TABLE {table_name} MODIFY COLUMN md5 VARCHAR(200) NOT NULL DEFAULT 'abcd' **"""

    def add_arguments(self, parser):
        parser.add_argument(
            '--bucket-name', default=None, dest='bucketname',
            help='Name of bucket will',
        )
        parser.add_argument(
            '--all', default=None, nargs='?', dest='all', const=True, # 当命令行有此参数时取值const, 否则取值default
            help='exec sql for all buckets',
        )
        parser.add_argument(
            '--sql', default='', dest='sql', type=str,
            help='sql will be exec.',
        )

    def handle(self, *args, **options):

        tpl_sql = options['sql']   # sql模板
        if not tpl_sql:
            raise CommandError("cancelled.")

        self.tpl_sql = tpl_sql
        buckets = self.get_buckets(**options)

        if input('Are you sure you want to do this?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("cancelled.")

        self.modify_buckets(buckets)

    def get_buckets(self, **options):
        '''
        获取给定的bucket或所有bucket
        :param options:
        :return:
        '''
        bucketname = options['bucketname']
        all = options['all']

        # 指定名字的桶
        if bucketname:
            self.stdout.write(self.style.NOTICE('Will clear all buckets named {0}'.format(bucketname)))
            return Bucket.objects.filter(name=bucketname).all()

        # 全部的桶
        if all is not None:
            self.stdout.write(self.style.NOTICE( 'Will exec sql for all buckets.'))
            return Bucket.objects.all()

        # 未给出参数
        if not bucketname:
            bucketname = input('Please input a bucket name:')

        self.stdout.write(self.style.NOTICE('Will exec sql for bucket named {0}'.format(bucketname)))
        return Bucket.objects.filter(name=bucketname).all()

    def modufy_one_bucket(self, bucket):
        '''
        为一个bucket执行sql

        :param bucket: Bucket obj
        :return:
        '''
        table_name = bucket.get_bucket_table_name()
        table_name = f'`{table_name}`'
        try:
            sql = self.tpl_sql.format(table_name=table_name)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"error when format sql for bucket '{bucket.name}': {str(e)}"))
        else:
            bfm = BucketFileManagement(collection_name=table_name)
            model_class = bfm.get_obj_model_class()
            try:
                a = model_class.objects.raw(raw_query=sql)
                b = a.query._execute_query()
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"error when execute sql for bucket '{bucket.name}': {str(e)}"))
            else:
                self.stdout.write(self.style.SUCCESS(f"success to execute sql for bucket '{bucket.name}'"))

        self.pool_sem.release() # 可用线程数+1

    def modify_buckets(self, buckets):
        '''
        多线程为bucket执行sql
        :param buckets:
        :return: None
        '''
        for bucket in buckets:
            if self.pool_sem.acquire(): # 可用线程数-1，控制线程数量，当前正在运行线程数量达到上限会阻塞等待
                worker = threading.Thread(target=self.modufy_one_bucket, kwargs={'bucket': bucket})
                worker.start()

        # 等待所有线程结束
        while True:
            c = threading.active_count()
            if c <= 1:
                break

        self.stdout.write(self.style.SUCCESS('Successfully exec sql for {0} buckets'.format(buckets.count())))
