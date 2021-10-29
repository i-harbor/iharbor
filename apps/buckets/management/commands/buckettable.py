import threading

from django.core.management.base import BaseCommand, CommandError

from buckets.utils import BucketFileManagement
from buckets.models import Bucket


class Command(BaseCommand):
    '''
    为bucket table执行sql
    '''
    pool_sem = threading.Semaphore(100)  # 定义最多同时启用多少个线程
    error_buckets = []

    help = """** manage.py buckettable --all --sql="sql template" **    
           **  manage.py buckettable --bucket-name=xxx --sql="sql template" **  
           ** sql template example:**  
           ** ALTER TABLE {table_name} ADD md5 CHAR(32) NOT NULL DEFAULT '' COMMENT 'MD5' **  
           ** ALTER TABLE {table_name} MODIFY COLUMN md5 VARCHAR(200) NOT NULL DEFAULT 'abcd' **
           ** ALTER TABLE {table_name} ADD COLUMN \`async1\` datetime(6) NULL; **
           """

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
        parser.add_argument(
            '--id-gt', default=0, dest='id-gt', type=int,
            help='All buckets with ID greater than "id-gt".',
        )

        parser.add_argument(
            '--multithreading', default=None, nargs='?', dest='multithreading', const=True,
            help='use multithreading mode.',
        )

    def handle(self, *args, **options):
        print(options)

        multithreading = options.get('multithreading')
        if multithreading:
            self.stdout.write(self.style.NOTICE('work mode in multithreading'))
        else:
            self.stdout.write(self.style.NOTICE('work mode in one thread'))

        tpl_sql = options['sql']   # sql模板
        if not tpl_sql:
            raise CommandError("cancelled.")

        self.tpl_sql = tpl_sql
        buckets = self.get_buckets(**options)

        if input('Are you sure you want to do this?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("cancelled.")

        if multithreading:
            self.modify_buckets_multithreading(buckets)
        else:
            self.modify_buckets(buckets)

    def _get_buckets(self, **options):
        '''
        获取给定的bucket或所有bucket
        :param options:
        :return:
        '''
        bucketname = options['bucketname']
        all = options['all']
        id_gt = options['id-gt']

        # 指定名字的桶
        if bucketname:
            self.stdout.write(self.style.NOTICE('Will clear all buckets named {0}'.format(bucketname)))
            return Bucket.objects.filter(name=bucketname).all()

        # 全部的桶
        if all is not None:
            self.stdout.write(self.style.NOTICE( 'Will exec sql for all buckets.'))
            qs = Bucket.objects.all()
            if id_gt > 0:
                qs = qs.filter(id__gt=id_gt)

            return qs

        # 未给出参数
        if not bucketname:
            bucketname = input('Please input a bucket name:')

        self.stdout.write(self.style.NOTICE('Will exec sql for bucket named {0}'.format(bucketname)))
        return Bucket.objects.filter(name=bucketname).all()

    def get_buckets(self, **options):
        buckets = self._get_buckets(**options)
        buckets = buckets.order_by('id')
        return buckets

    def modify_one_bucket(self, bucket, in_thread: bool = True):
        '''
        为一个bucket执行sql

        :param bucket: Bucket obj
        :param in_thread: 是否是线程
        :return:
        '''
        ret = True
        table_name = bucket.get_bucket_table_name()
        table_name = f'`{table_name}`'
        try:
            sql = self.tpl_sql.format(table_name=table_name)
        except Exception as e:
            ret = False
            self.stdout.write(self.style.ERROR(f"error when format sql for bucket '{bucket.name}': {str(e)}"))
        else:
            bfm = BucketFileManagement(collection_name=table_name)
            model_class = bfm.get_obj_model_class()
            try:
                a = model_class.objects.raw(raw_query=sql)
                b = a.query._execute_query()
            except Exception as e:
                ret = False
                self.error_buckets.append(bucket.name)
                self.stdout.write(self.style.ERROR(
                    f"error when execute sql for bucket '{bucket.name}':{type(e)} {str(e)}"))
            else:
                self.stdout.write(self.style.SUCCESS(f"success to execute sql for bucket '{bucket.name}'"))

        if in_thread:
            self.pool_sem.release() # 可用线程数+1

        return ret

    def modify_buckets_multithreading(self, buckets):
        '''
        多线程为bucket执行sql
        :param buckets:
        :return: None
        '''
        for bucket in buckets:
            if self.pool_sem.acquire(): # 可用线程数-1，控制线程数量，当前正在运行线程数量达到上限会阻塞等待
                worker = threading.Thread(target=self.modify_one_bucket, kwargs={'bucket': bucket})
                worker.start()

        # 等待所有线程结束
        while True:
            c = threading.active_count()
            if c <= 1:
                break

        self.stdout.write(self.style.SUCCESS(
            'Successfully exec sql for {0} buckets'.format(buckets.count() - len(self.error_buckets))))
        self.stdout.write(self.style.ERROR(
            f"error when execute sql for buckets:{self.error_buckets}"))

    def modify_buckets(self, buckets):
        '''
        单线程为bucket执行sql
        :param buckets:
        :return: None
        '''
        count = 0
        for bucket in buckets:
            ok = self.modify_one_bucket(bucket, in_thread=False)
            if ok:
                count += 1
            else:
                self.stdout.write(self.style.ERROR(
                    f"error when execute sql for bucket: name={bucket.name}, id={bucket.id}"))
                break

        self.stdout.write(
            self.style.SUCCESS(
                'Successfully exec sql for {0} buckets'.format(count)
            )
        )

