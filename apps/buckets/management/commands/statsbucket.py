import os

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

        parser.add_argument(
            '--output', default=None, nargs='?', dest='output', type=str,
            help='output bucket stats to txt file.',
        )

    def handle(self, *args, **options):
        output = options.get('output', '')
        if output:
            self.stdout.write(self.style.WARNING(f'Output bucket stats to file {output}'))
            if '/' in output:
                dir_path, name = output.rsplit('/', maxsplit=1)
                if not name:
                    raise CommandError(f"invalid path, {dir_path}.")
                if dir_path and not os.path.isdir(dir_path):
                    raise CommandError(f"path not exist, {dir_path}.")

            if os.path.isdir(output):
                raise CommandError(f"dir already exist, {output}.")

        buckets = self.get_buckets(**options)

        if input('Are you sure you want to do this?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("Stats buckets cancelled.")

        self.stats_buckets(buckets, output)

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
            self.stdout.write(self.style.WARNING(f'Will stats all buckets named {bucket_names}'))
            return Bucket.objects.filter(name__in=bucket_names).all()

        # 全部已删除归档的桶
        if all_bucket:
            self.stdout.write(self.style.WARNING('Will stats all buckets'))
            return Bucket.objects.all()

        # 未给出参数
        bucket_name = input('Please input a bucket name:')
        self.stdout.write(self.style.WARNING(f'Will stats all buckets named {bucket_name}'))
        return Bucket.objects.filter(name=bucket_name).all()

    def stats_buckets(self, buckets, output: str):
        if output:
            with open(output, 'w+') as f:
                lines = self.stats_buckets_lines(buckets=buckets, output=output)
                f.writelines(lines)
            self.stdout.write(self.style.SUCCESS(f'output to file {output}'))
        else:
            self.stats_buckets_lines(buckets=buckets, output=output)

    def stats_buckets_lines(self, buckets, output: str):
        num = 0
        lines = []
        if output:
            lines.append(f'id;name;objects;size\n')

        for bucket in buckets:
            num += 1
            try:
                bucket.update_stats()
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'stats bucket({bucket.name}) error: {e}'))

            self.stdout.write(self.style.SUCCESS(f'{num}# stats bucket <{bucket.name}> ok.'))
            if output:
                lines.append(f'{bucket.id};{bucket.name};{bucket.objs_count};{bucket.size}\n')

        self.stdout.write(self.style.SUCCESS('Successfully stats {0} buckets'.format(buckets.count())))
        return lines
