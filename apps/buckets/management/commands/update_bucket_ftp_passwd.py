import requests

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from buckets.models import Bucket, BucketToken, rand_hex_string
from users.models import UserProfile


class Command(BaseCommand):
    error_buckets = []

    help = """[manage.py update_bucket_ftp_passwd --all --id-gt=0]"""

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
            '--id-gt', default=0, dest='id-gt', type=int,
            help='All buckets with ID greater than "id-gt".',
        )

    def handle(self, *args, **options):
        buckets = self.get_buckets(**options)
        self.stdout.write(self.style.NOTICE(f'Count of buckets: {len(buckets)}'))
        if input('Are you sure you want to do this?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("cancelled.")

        self.update_buckets_ftp_password(buckets)

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
            self.stdout.write(self.style.NOTICE('Will all buckets named {0}'.format(bucketname)))
            return Bucket.objects.filter(name=bucketname).all()

        # 全部的桶
        if all is not None:
            self.stdout.write(self.style.NOTICE( 'Will for all buckets.'))
            qs = Bucket.objects.select_related('user').all()
            if id_gt > 0:
                qs = qs.filter(id__gt=id_gt)

            return qs

        raise CommandError("Don't know which buckets to do, give bucket name or all.")

    def get_buckets(self, **options):
        buckets = self._get_buckets(**options)
        buckets = buckets.order_by('id')
        return buckets

    def update_buckets_ftp_password(self, buckets):
        count = 0
        for bucket in buckets:
            ok = self.update_ftp_password(bucket)
            if ok:
                count += 1
            else:
                break

        self.stdout.write(
            self.style.SUCCESS(
                'Successfully {0} buckets'.format(count)
            )
        )

    def update_ftp_password(self, bucket: Bucket):
        rw_password = rand_hex_string()
        ro_password = rand_hex_string()
        bucket.set_ftp_password(password=rw_password)
        bucket.set_ftp_ro_password(password=ro_password)
        try:
            bucket.save(update_fields=['ftp_password', 'ftp_ro_password'])
            return True
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error, bucket name={bucket.name}, id={bucket.id}, {e}'))
            return False