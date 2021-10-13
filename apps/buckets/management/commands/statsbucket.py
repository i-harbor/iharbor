from django.core.management.base import BaseCommand, CommandError

from buckets.models import Bucket


class Command(BaseCommand):
    """
    清理bucket命令，清理满足彻底删除条件的对象和目录
    """
    help = """
    stats bucket
    [manage.py statsbucket --bucket-names name1 name2 name3]
    [manage.py statsbucket --all]
    """
    _clear_datetime = None

    def add_arguments(self, parser):
        parser.add_argument(
            '--bucket-names', default=None, dest='bucket-names', nargs='*',
            help='Names of bucket will be stats,',
        )

        parser.add_argument(
            # 当命令行有此参数时取值const, 否则取值default
            '--all', default=None, nargs='?', dest='all_bucket', const=True,
            help='All buckets will be stats.',
        )

    def handle(self, *args, **options):
        buckets = self.get_buckets(**options)

        if input('Are you sure you want to do this?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("Stats buckets cancelled.")

        self.stats_buckets(buckets)

    def get_buckets(self, **options):
        """
        获取给定的bucket或所有bucket
        :param options:
        :return:
        """
        bucket_names = options['bucket-names']
        all_bucket = options['all_bucket']

        # 指定名字的桶
        if bucket_names:
            self.stdout.write(self.style.NOTICE(f'Will stats all buckets named {bucket_names}'))
            return Bucket.objects.filter(name__in=bucket_names).all()

        # 全部已删除归档的桶
        if all_bucket:
            self.stdout.write(self.style.NOTICE('Will stats all buckets'))
            return Bucket.objects.all()

        # 未给出参数
        bucket_name = input('Please input a bucket name:')
        self.stdout.write(self.style.NOTICE(f'Will stats all buckets named {bucket_name}'))
        return Bucket.objects.filter(name=bucket_name).all()

    def stats_buckets(self, buckets):
        num = 0
        for bucket in buckets:
            num += 1
            try:
                bucket.update_stats()
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'stats bucket({bucket.name}) error: {e}'))

            self.stdout.write(self.style.SUCCESS(f'{num}# stats bucket <{bucket.name}> ok.'))

        self.stdout.write(self.style.SUCCESS('Successfully stats {0} buckets'.format(buckets.count())))
