from django.core.management.base import BaseCommand, CommandError

from buckets.models import Bucket, Archive
from api.validators import DNSStringValidator, ValidationError
from api.serializers import BucketCreateSerializer


class Command(BaseCommand):
    help = 'manage.py resumebucket --archive-id=1 --bucket-name="test"'

    def add_arguments(self, parser):
        parser.add_argument(
            '--archive-id', default=None, dest='archive-id', type=int,
            help='Id of archive bucket',
        )

        parser.add_argument(
            '--bucket-name', default=None, dest='bucket-name', type=str,
            help='Name of archive bucket',
        )

        parser.add_argument(
            # 当命令行有此参数时取值const, 否则取值default
            '--test', default=None, nargs='?', dest='test', const=True,
            help='test.',
        )

    def handle(self, *args, **options):
        test = options['test']
        archive_id = options['archive-id']
        bucket_name = options['bucket-name']
        if not bucket_name or not archive_id:
            self.stdout.write(self.style.ERROR(f'Invalid archive-id({archive_id} or archive bucket-name({bucket_name})'))
            raise CommandError(f'Invalid archive-id({archive_id} or archive bbucket-name({bucket_name})')

        archive = Archive.objects.filter(id=archive_id, name=bucket_name).first()
        if archive is None:
            self.stdout.write(self.style.ERROR(f'Not found archive bucket'))
            return

        self.stdout.write(self.style.SUCCESS(f'Get archive bucket(id={archive.original_id}, name={archive.name}, '
                                             f'user={archive.user.username}), test={test}'))

        if input('Are you sure you want to do this?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("Cancelled.")

        self.resume_bucket(archive=archive, test=test)

    def resume_bucket(self, archive, test):
        b = Bucket.objects.filter(id=archive.original_id).first()
        if b is not None:
            self.stdout.write(self.style.ERROR(
                f'Already Exist bucket（id={b.id}, name={b.name}）, can not resume archive(id={archive.id}, '
                f'bucket id={archive.original_id}，bucket name={archive.name}).'))
            return

        bucket_name = archive.name
        b = Bucket.objects.filter(name=archive.name).first()
        if b is not None:
            self.stdout.write(self.style.ERROR(
                f'Already Exist same name bucket（id={b.id}, name={b.name}）, can not resume archive(id={archive.id}, '
                f'bucket id={archive.original_id}，bucket name={archive.name}).'))

            if input(f'是否把（{archive.name}）换一个新的桶名?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
                raise CommandError("Cancelled.")

            bucket_name = input(f'请输入3-63个字符长度的存储桶名称，可输入小写英文字母、数字或者-（不允许在开头和结尾）：')

            try:
                bucket_name = BucketCreateSerializer.validate_bucket_name(bucket_name)
            except ValidationError as e:
                raise CommandError(str(e))

        if test is True:
            self.stdout.write(self.style.SUCCESS('Test resume'))
        else:
            bucket = archive.resume_archive(bucket_name=bucket_name)
            self.stdout.write(self.style.SUCCESS(f'Successfully resume archive（id={archive.id}） to '
                                                 f'bucket(id={bucket.id}, name={bucket.name})'))
