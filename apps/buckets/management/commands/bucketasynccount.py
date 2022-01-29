from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q, F

from buckets.utils import BucketFileManagement
from buckets.models import Bucket, BackupBucket


class Command(BaseCommand):
    help = """
    在清空桶对象同步备份点标记async1或async2后，统计有多少对象同步备份点标记未清空：
    [manage.py bucketasynccount --all --count-not-null --async="async1"];
    [manage.py bucketasynccount --sql --async="async1"]；
    
    统计桶对象同步完成数：
    [manage.py bucketasynccount --all --count --async="async1"];
    """

    ASYNC1 = 'async1'
    ASYNC2 = 'async2'
    ASYNC_CHOICE = [ASYNC1, ASYNC2]

    def add_arguments(self, parser):
        parser.add_argument(
            '--bucket-name', default=None, dest='bucket-name',
            help='Name of bucket have been deleted will be clearing,',
        )

        parser.add_argument(
            # 当命令行有此参数时取值const, 否则取值default
            '--all', default=None, nargs='?', dest='all', const=True,
            help='All buckets will be action.',
        )

        parser.add_argument(
            '--async', default='', dest='async',
            help=f'对象同步时间字段设置为null， {self.ASYNC_CHOICE}',
        )

        parser.add_argument(
            '--count-not-null', default=None, nargs='?', dest='count-not-null', const=True,
            help='how many objects that "async" not null per bucket.',
        )

        parser.add_argument(
            '--count', default=None, nargs='?', dest='count', const=True,
            help='how many objects already async ok per bucket.',
        )

        parser.add_argument(
            '--sql', default=None, nargs='?', dest='sql', const=True,
            help='Print Sql.',
        )

    def handle(self, *args, **options):
        action = ''
        count_not_null = options['count-not-null']
        async_name = options['async']
        print_sql = options['sql']
        count = options['count']
        if async_name and async_name not in self.ASYNC_CHOICE:
            raise CommandError(f"--async must in {self.ASYNC_CHOICE}")

        if count_not_null:
            if not async_name:
                raise CommandError(f"required --async must in {self.ASYNC_CHOICE}")

            action = f'count objects {async_name} is not null'

        if count:
            if not async_name:
                raise CommandError(f"required --async must in {self.ASYNC_CHOICE}")

            action = f'Count how many objects already async ok about {async_name}'

        if print_sql:
            action = f'Print SQL'

        buckets = self.get_buckets(**options)

        if input(f"Are you sure you want to {action}?\n\n Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("action buckets cancelled.")

        if print_sql:
            self.do_print_sql(buckets=buckets, async_name=async_name)
            return

        if count_not_null:
            self.count_not_null_objects_of_buckets(buckets, async_name=async_name)
            return

        if count:
            self.count_async_objects_of_buckets(buckets=buckets, async_name=async_name)
            return

    def get_buckets(self, **options):
        """
        获取给定的bucket或所有bucket
        :param options:
        :return:
        """
        bucket_name = options['bucket-name']
        all_bucket = options['all']
        async_name = options['async']
        if async_name == self.ASYNC1:
            backup_num = 1
        else:
            backup_num = 2

        qs = Bucket.objects.filter(
            backup_buckets__status=BackupBucket.Status.START,
            backup_buckets__backup_num=backup_num,
        ).all().order_by('id')

        # 指定名字的桶
        if bucket_name:
            self.stdout.write(self.style.WARNING('Will action all buckets named {0}'.format(bucket_name)))
            return qs.filter(name=bucket_name).all()

        if all_bucket:
            self.stdout.write(self.style.WARNING('Will action all buckets'))
            return qs

        # 未给出参数
        if not bucket_name:
            bucket_name = input('Please input a bucket name:')

        self.stdout.write(self.style.WARNING('Will action all buckets named {0}'.format(bucket_name)))
        return qs.filter(name=bucket_name).all()

    def count_not_null_objects_of_buckets(self, buckets, async_name):
        buckets_count = len(buckets)
        self.stdout.write(self.style.WARNING(f'All buckets: {buckets_count}'))

        all_null_buckets = {}
        not_all_null_buckets = {}
        for bucket in buckets:
            count = self.count_not_null_bucket_objects(bucket=bucket, async_name=async_name)
            if count > 0:
                not_all_null_buckets[bucket.name] = bucket
                self.stdout.write(self.style.ERROR(
                    f'Count {count} objects {async_name} is not null of bucket {bucket.name}'))
            else:
                all_null_buckets[bucket.name] = bucket
                self.stdout.write(self.style.SUCCESS(
                    f'Count {count} objects {async_name} is not null of bucket {bucket.name}'))

        self.stdout.write(self.style.SUCCESS(
            f'{len(all_null_buckets)} bucket, all objects {async_name} is null'))

        self.stdout.write(self.style.WARNING(
            f'{len(not_all_null_buckets)} bucket, not all objects {async_name} is null'))

        if len(not_all_null_buckets) > 0:
            self.stdout.write(self.style.WARNING(
                f'The buckets that not all objects {async_name} is null, {not_all_null_buckets.keys()}'))

    def do_print_sql(self, buckets, async_name):
        bucket = buckets[0]
        qs = self.get_count_not_null_queryset(bucket=bucket, async_name=async_name)
        self.stdout.write(self.style.SUCCESS(
            f'The SQL: {qs.query}'))

    def get_queryset(self, bucket):
        table_name = bucket.get_bucket_table_name()
        model_class = BucketFileManagement(collection_name=table_name).get_obj_model_class()
        qs = model_class.objects.all()
        return qs

    def get_count_not_null_queryset(self, bucket, async_name):
        qs = self.get_queryset(bucket=bucket)
        if async_name == 'async1':
            qs = qs.filter(~Q(async1__isnull=True))
        elif async_name == 'async2':
            qs = qs.filter(~Q(async2__isnull=True))
        else:
            raise CommandError(f'Invalid async field name "{async_name}"')

        return qs

    def count_not_null_bucket_objects(self, bucket, async_name):
        qs = self.get_count_not_null_queryset(bucket=bucket, async_name=async_name)
        return qs.count()

    def get_all_objects(self, bucket):
        qs = self.get_queryset(bucket)
        return qs.filter(fod=True)

    def get_need_async_objects(self, bucket, async_name):
        qs = self.get_all_objects(bucket)
        if async_name == self.ASYNC1:
            qs = qs.filter(Q(async1__isnull=True) | Q(upt__gt=F('async1')))
        else:
            qs = qs.filter(Q(async2__isnull=True) | Q(upt__gt=F('async2')))

        return qs

    def count_async_objects_of_buckets(self, buckets, async_name):
        buckets_count = len(buckets)
        self.stdout.write(self.style.WARNING(f'All buckets: {buckets_count}'))

        for bucket in buckets:
            all_count = self.get_all_objects(bucket).count()
            need_count = self.get_need_async_objects(bucket=bucket, async_name=async_name).count()
            async_count = all_count - need_count
            show = f'Bucket {bucket.name} in {async_name}: already async {async_count}, ' \
                   f'still need async {need_count}, all objects {all_count}'
            if need_count > 0:
                self.stdout.write(self.style.WARNING(show))
            else:
                self.stdout.write(self.style.SUCCESS(show))
