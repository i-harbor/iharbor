from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q

from buckets.utils import BucketFileManagement
from buckets.models import Bucket, BackupBucket


class Command(BaseCommand):
    help = """
    [manage.py bucketasynccount --all --count  --async="async1"]
    [manage.py bucketasynccount --sql  --async="async1"]
    """

    def add_arguments(self, parser):
        parser.add_argument(
            '--bucket-name', default=None, dest='bucket-name',
            help='Name of bucket have been deleted will be clearing,',
        )

        parser.add_argument(
            # 当命令行有此参数时取值const, 否则取值default
            '--all', default=None, nargs='?', dest='all_deleted', const=True,
            help='All buckets will be action.',
        )

        parser.add_argument(
            '--async', default='', dest='async',
            help='对象同步时间字段设置为null， async1, async2',
        )

        parser.add_argument(
            '--count', default=None, nargs='?', dest='count', const=True,
            help='how many objects that "async" not null per bucket.',
        )

        parser.add_argument(
            '--sql', default=None, nargs='?', dest='sql', const=True,
            help='Print Sql.',
        )

    def handle(self, *args, **options):
        action = ''
        count = options['count']
        async_name = options['async']
        print_sql = options['sql']
        if async_name and async_name not in ['async1', 'async2']:
            raise CommandError("async must in ['async1', 'async2']")

        if count:
            action = f'count objects {async_name} is not null'

        if print_sql:
            action = f'Print SQL'

        buckets = self.get_buckets(**options)

        if input(f"Are you sure you want to {action}?\n\n Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("action buckets cancelled.")

        if print_sql:
            self.do_print_sql(buckets=buckets, async_name=async_name)
            return

        if count:
            self.count_not_null_objects_of_buckets(buckets, async_name=async_name)
            return

    def get_buckets(self, **options):
        """
        获取给定的bucket或所有bucket
        :param options:
        :return:
        """
        bucket_name = options['bucket-name']
        all_deleted = options['all_deleted']

        qs = Bucket.objects.filter(
            backup_buckets__status=BackupBucket.Status.START
        ).all().order_by('id')

        # 指定名字的桶
        if bucket_name:
            self.stdout.write(self.style.NOTICE('Will action all buckets named {0}'.format(bucket_name)))
            return qs.filter(name=bucket_name).all()

        # 全部已删除归档的桶
        if all_deleted:
            self.stdout.write(self.style.NOTICE('Will action all buckets'))
            return qs

        # 未给出参数
        if not bucket_name:
            bucket_name = input('Please input a bucket name:')

        self.stdout.write(self.style.NOTICE('Will action all buckets named {0}'.format(bucket_name)))
        return qs.filter(name=bucket_name).all()

    def count_not_null_objects_of_buckets(self, buckets, async_name):
        buckets_count = len(buckets)
        self.stdout.write(self.style.NOTICE(f'All buckets: {buckets_count}'))

        all_null_buckets = {}
        not_all_null_buckets = {}
        for bucket in buckets:
            count = self.count_bucket_objects(bucket=bucket, async_name=async_name)
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

        self.stdout.write(self.style.NOTICE(
            f'{len(not_all_null_buckets)} bucket, not all objects {async_name} is null'))

        if len(not_all_null_buckets) > 0:
            self.stdout.write(self.style.NOTICE(
                f'The buckets that not all objects {async_name} is null, {not_all_null_buckets.keys()}'))

    def do_print_sql(self, buckets, async_name):
        bucket = buckets[0]
        qs = self.get_count_queryset(bucket=bucket, async_name=async_name)
        self.stdout.write(self.style.SUCCESS(
            f'The SQL: {qs.query}'))

    def get_queryset(self, bucket):
        table_name = bucket.get_bucket_table_name()
        model_class = BucketFileManagement(collection_name=table_name).get_obj_model_class()
        qs = model_class.objects.all()
        return qs

    def get_count_queryset(self, bucket, async_name):
        lookups = {
            f'{async_name}__isnull': True
        }
        qs = self.get_queryset(bucket=bucket)
        # return qs.exclude(**lookups)
        if async_name == 'async1':
            qs = qs.filter(~Q(async1__isnull=True))
        elif async_name == 'async2':
            qs = qs.filter(~Q(async2__isnull=True))
        else:
            raise CommandError(f'Invalid async field name "{async_name}"')

        return qs

    def count_bucket_objects(self, bucket, async_name):
        qs = self.get_count_queryset(bucket=bucket, async_name=async_name)
        return qs.count()
