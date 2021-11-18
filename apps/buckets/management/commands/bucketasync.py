from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q

from buckets.utils import BucketFileManagement
from buckets.models import Bucket, BackupBucket


class Command(BaseCommand):
    help = 'manage.py statsbucket --all --name-contains str1 str2 str3 --id-gt=0 --limit=100  --async="async1"'

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
            '--all', default=None, nargs='?', dest='all_deleted', const=True,
            help='All buckets will be action.',
        )
        parser.add_argument(
            '--id-gt', default=0, dest='id-gt', type=int,
            help='All buckets with ID greater than "id-gt".',
        )
        parser.add_argument(
            '--name-contains', default=None, dest='name-contains', nargs='*',
            help='对象路径中包含此指定内容的所有对象, 可以多个值 关系为or',
        )
        parser.add_argument(
            '--async', default='', dest='async',
            help='对象同步时间字段设置为null， async1, async2',
        )

        parser.add_argument(
            '--limit', default=100, dest='limit', type=int,
            help='how many buckets per times.',
        )

    def handle(self, *args, **options):
        limit = options['limit']
        buckets = self.get_buckets(**options)

        if input('Are you sure you want to do this?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("action buckets cancelled.")

        self.action_buckets(buckets=buckets[0: limit], options=options)

    def get_buckets(self, **options):
        """
        获取给定的bucket或所有bucket
        :param options:
        :return:
        """
        bucket_name = options['bucket-name']
        all_deleted = options['all_deleted']
        id_gt = options['id-gt']

        qs = Bucket.objects.filter(
            backup_buckets__status=BackupBucket.Status.START
        ).all().order_by('id')
        if id_gt > 0:
            qs = qs.filter(id__gt=id_gt)

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

    def action_buckets(self, buckets, options):
        name_contains = options['name-contains']        # None or [str, ]
        async = options['async']
        if async and async not in ['async1', 'async1']:
            raise CommandError("async must in ['async1', 'async1']")

        no_empty_bucket = 0
        buckets_count = len(buckets)
        last_id = 0
        for bucket in buckets:
            try:
                count = self.count_bucket_objects(bucket, name_contains)
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'count objects of bucket({bucket.name}, id={bucket.id}) error: {e}'))
                break

            if count > 0:
                no_empty_bucket += 1
                if async:
                    updates = {async: None}
                    qs = self.get_queryset(bucket=bucket, name_contains=name_contains)
                    try:
                        rows = qs.update(**updates)
                        self.stdout.write(self.style.SUCCESS(
                            f'[Action] {rows} objects na contains "{name_contains}" update {async} to null, bucket named {bucket.name}'))
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(
                            f'update object {async} to null, bucket({bucket.name}, id={bucket.id}) error: {e}'))

            last_id = bucket.id

        self.stdout.write(self.style.SUCCESS(f'Successfully action {buckets_count} buckets, '
                                             f'match objects {no_empty_bucket} buckets, last bucket id={last_id}'))

    def get_queryset(self, bucket, name_contains):
        lookups = None
        if name_contains:
            lookups = Q(na__contains=name_contains[0])
            for i in name_contains[1:]:
                lookups = lookups | Q(na__contains=i)

        table_name = bucket.get_bucket_table_name()
        model_class = BucketFileManagement(collection_name=table_name).get_obj_model_class()
        qs = model_class.objects.filter(fod=True)
        if lookups is not None:
            qs = qs.filter(lookups)

        return qs

    def count_bucket_objects(self, bucket, name_contains):
        """

        :param bucket: Bucket()
        :return:
        """
        qs = self.get_queryset(bucket=bucket, name_contains=name_contains)
        count = qs.count()
        self.stdout.write(self.style.SUCCESS(f'[Count] {count} objects na contains "{name_contains}" bucket named {bucket.name}'))
        return count
