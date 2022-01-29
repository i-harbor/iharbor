from django.core.management.base import BaseCommand, CommandError

from buckets.utils import create_bucket
from buckets.models import Bucket
from users.models import UserProfile


class Command(BaseCommand):
    error_buckets = []

    help = """
    create bucket: 
    manage.py --bucket-name="xxx" [--id=x] --username="xxx"
    """

    def add_arguments(self, parser):
        parser.add_argument(
            '--bucket-name', default=None, dest='bucketname', type=str,
            help='Name of bucket will',
        )
        parser.add_argument(
            '--id', default=None, dest='id', type=int, # 当命令行有此参数时取值const, 否则取值default
            help='id for bucket',
        )
        parser.add_argument(
            '--username', default='', dest='username', type=str,
            help='username.',
        )

    def handle(self, *args, **options):
        bucket_name = options['bucketname']
        if not bucket_name:
            raise CommandError("bucket_name required.")

        bucket_id = options['id']
        if not bucket_id:
            raise CommandError("bucket_id required.")

        username = options['username']
        if not username:
            raise CommandError("username required.")

        self.stdout.write(self.style.NOTICE(f'Will create bucket(id={bucket_id}, name={bucket_name}, username={username})'))
        if input('Are you sure you want to do this?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("cancelled.")

        self.create_bucket(bucket_id=bucket_id, bucket_name=bucket_name, username=username)

    def create_bucket(self, bucket_id, bucket_name, username):
        user = UserProfile.objects.filter(username=username).first()
        if user is None:
            raise CommandError(f"user({username}).")

        bucket = Bucket.objects.filter(name=bucket_name).first()
        if bucket is not None:
            raise CommandError(f"bucket name ({bucket_name}) already exists.")

        bucket = Bucket.objects.filter(id=bucket_id).first()
        if bucket is not None:
            raise CommandError(f"bucket id ({bucket_id}) already exists.")

        # 创建bucket,创建bucket的对象元数据表
        try:
            bucket = create_bucket(_id=bucket_id, name=bucket_name, user=user)
        except Exception as e:
            raise CommandError(f"create bucket failed, {str(e)}.")

        self.stdout.write(self.style.SUCCESS(
            f'Ok create bucket (id={bucket.id}, name={bucket.name}, '
            f'ceph_using={bucket.ceph_using}, poolname={bucket.pool_name})'))
