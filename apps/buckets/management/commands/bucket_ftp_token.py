import requests

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from buckets.models import Bucket, BucketToken
from users.models import UserProfile


class Command(BaseCommand):
    '''
    为bucket table执行sql
    '''
    error_buckets = []

    help = """** manage.py buckettable --all --sql="sql template" **
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
            '--token', default='', dest='token', type=str,
            help='auth token.',
        )
        parser.add_argument(
            '--host', default='', dest='host', type=str,
            help='target iharbor service url, http://hostname/',
        )
        parser.add_argument(
            '--id-gt', default=0, dest='id-gt', type=int,
            help='All buckets with ID greater than "id-gt".',
        )

        parser.add_argument(
            '--ftp-update', default=None, nargs='?', dest='ftp-update', const=True,
            # 当命令行有此参数时取值const, 否则取值default
            help='upate bucket ftp status、password to target host bucket',
        )

        parser.add_argument(
            '--export-buckets', default=None, nargs='?', dest='export-buckets', const=True,  # 当命令行有此参数时取值const, 否则取值default
            help='export all buckets to file',
        )

        parser.add_argument(
            '--permission', default=None, nargs='?', dest='permission', const=True,
            # 当命令行有此参数时取值const, 否则取值default
            help='set bucket public/private permission',
        )

        parser.add_argument(
            '--to-user', default=None, nargs='?', dest='to-user', const=True,
            # 当命令行有此参数时取值const, 否则取值default
            help='set bucket to user',
        )

        parser.add_argument(
            '--export-bucket-token', default=None, nargs='?', dest='export-bucket-token', const=True,
            # 当命令行有此参数时取值const, 否则取值default
            help='export all bucket token to file',
        )

        parser.add_argument(
            '--import-bucket-token', default=None, nargs='?', dest='import-bucket-token', const=True,
            # 当命令行有此参数时取值const, 否则取值default
            help='import all bucket token from file',
        )

    def handle(self, *args, **options):

        export_bucket_token = options['export-bucket-token']
        if export_bucket_token:
            if input('Are you sure you want to export bucket token?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
                raise CommandError("cancelled.")

            self.export_bucket_token()
            return

        import_bucket_token = options['import-bucket-token']
        if import_bucket_token:
            if input('Are you sure you want to import bucket token?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
                raise CommandError("cancelled.")

            self.import_bucket_token()
            return

        ftp_update = options['ftp-update']
        permission = options['permission']
        if ftp_update or permission:
            token = options['token']
            host = options['host']
            self.token = token
            self.host = host.rstrip('/')
            if not host.startswith('http'):
                raise CommandError(f"invalid input host（{host}）.")

            if not token:
                raise CommandError("token is required.")

        buckets = self.get_buckets(**options)
        self.stdout.write(self.style.NOTICE(f'Count of buckets: {len(buckets)}'))
        if input('Are you sure you want to do this?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("cancelled.")

        if ftp_update:
            self.ftp_buckets(buckets)
            return

        export_buckets = options['export-buckets']
        if export_buckets:
            self.export_buckets(buckets)
            return

        if permission:
            self.permission_buckets(buckets)
            return

        to_user = options['to-user']
        if to_user:
            self.bucket_to_user(buckets)
            return

        self.stdout.write(self.style.NOTICE('Nothing do.'))
        raise CommandError("cancelled.")

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

    def ftp_buckets(self, buckets):
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

    def update_ftp_password(self, bucket):
        api = f'{self.host}/api/v1/ftp/{bucket.name}/'
        params = {}
        if bucket.ftp_enable:
            params['enable'] = 'true'
        else:
            params['enable'] = 'false'

        params['password'] = bucket.raw_ftp_password
        params['ro_password'] = bucket.raw_ftp_ro_password

        r = requests.patch(api, params=params, headers={'Authorization': f'Token {self.token}'})
        if r.status_code == 200:
            self.stdout.write(self.style.SUCCESS(f'Successfully ftp for bucket name={bucket.name}, id={bucket.id}'))
            return True
        elif r.status_code == 404:
            self.stdout.write(
                self.style.ERROR(f'Target not found, bucket name={bucket.name}, id={bucket.id}, {r.text}'))
        else:
            self.stdout.write(self.style.ERROR(f'Error, bucket name={bucket.name}, id={bucket.id}, {r.text}'))

        return False

    def permission_buckets(self, buckets):
        count = 0
        for bucket in buckets:
            ok = self.update_access_permission(bucket)
            if ok:
                count += 1
            else:
                break

        self.stdout.write(
            self.style.SUCCESS(
                'Successfully {0} buckets'.format(count)
            )
        )

    def update_access_permission(self, bucket):
        api = f'{self.host}/api/v1/buckets/{bucket.name}/'
        params = {'by-name': 'true'}
        if bucket.access_permission == Bucket.PUBLIC:
            params['public'] = 1
        else:
            params['public'] = 2

        r = requests.patch(api, params=params, headers={'Authorization': f'Token {self.token}'})
        if r.status_code == 200:
            self.stdout.write(self.style.SUCCESS(
                f'Successfully set permission for bucket name={bucket.name}, id={bucket.id}'))
            return True
        elif r.status_code == 404:
            self.stdout.write(
                self.style.ERROR(f'Target not found, bucket name={bucket.name}, id={bucket.id}, {r.text}'))
        else:
            self.stdout.write(self.style.ERROR(f'Error, bucket name={bucket.name}, id={bucket.id}, {r.text}'))

        return False

    def export_buckets(self, buckets):
        filename = '/home/export-buckets.txt'
        with open(filename, 'w+') as f:
            for b in buckets:
                line = f"{b.id} {b.name} {b.user.id} {b.user.username} \n"
                f.write(line)

        self.stdout.write(self.style.SUCCESS(f'Successfully export {len(buckets)} buckets to file: {filename}'))


    def get_buckets_map(self, filename='/home/export-buckets.txt'):
        buckets_map = {}
        with open(filename, 'r') as f:
            while True:
                line = f.readline()
                line = line.strip('\n').strip(' ')
                if not line:
                    break

                items = line.split(' ')
                b_id, b_name, user_id, username = items
                buckets_map[b_name] = {'id': int(b_id), 'name': b_name, 'user_id': int(user_id), 'username': username}

        return buckets_map

    def bucket_to_user(self, buckets):
        buckets_map = self.get_buckets_map()
        for b in buckets:
            if b.name not in buckets_map:
                self.stdout.write(self.style.ERROR(f'Error, bucket name={b.name}, id={b.id} not in map file'))
                break

            mb = buckets_map[b.name]
            user = UserProfile.objects.filter(username=mb['username']).first()
            if user is None:
                self.stdout.write(self.style.ERROR(
                    f'Error, bucket name={b.name}, id={b.id}, user {mb["username"]} not exists'))
                break

            if user.id != mb['user_id']:
                self.stdout.write(self.style.NOTICE(
                    f"bucket name={b.name}, id={b.id}, user(id={user.id}) != map user id {mb['user_id']}"))

            if b.user_id == user.id:
                self.stdout.write(self.style.SUCCESS(
                    f'OK bucket name={b.name}, id={b.id} already belong to user {user.username}, {mb}'))
                continue

            b.user_id = user.id
            b.save(update_fields=['user_id'])
            self.stdout.write(self.style.SUCCESS(f'OK bucket name={b.name}, id={b.id} to user {user.username}, {mb}'))

    def export_bucket_token(self):
        btokens = BucketToken.objects.select_related('bucket').all()
        count = len(btokens)
        filename = '/home/export-bucket-token.txt'
        with open(filename, 'w+') as f:
            for bt in btokens:
                line = f"{bt.key} {bt.bucket.name} {bt.permission} {bt.created.timestamp()} \n"
                f.write(line)

        self.stdout.write(self.style.SUCCESS(f'Successfully export {count} bucket token to file: {filename}'))

    def get_bucket_token_map(self, filename='/home/export-bucket-token.txt'):
        buckets_map = {}
        with open(filename, 'r') as f:
            while True:
                line = f.readline()
                line = line.strip('\n').strip(' ')
                if not line:
                    break

                items = line.split(' ')
                key, b_name, permission, created = items
                buckets_map[key] = {'key': key, 'name': b_name, 'permission': permission,
                                    'created': float(created)}

        return buckets_map

    def import_bucket_token(self):
        token_map = self.get_bucket_token_map()
        self.stdout.write(self.style.SUCCESS(f'bucket token count: {len(token_map)}'))
        for key, token in token_map.items():
            bucket_name = token['name']
            t = BucketToken.objects.select_related('bucket').filter(key=key, bucket__name=bucket_name).first()
            if t is not None:
                self.stdout.write(self.style.SUCCESS(f'bucket token={key}, already exists'))
                continue

            bucket = Bucket.objects.filter(name=bucket_name).first()
            if bucket is None:
                self.stdout.write(self.style.SUCCESS(f'Error, bucket token{key}, bucket(name={bucket_name}) not exists'))
                continue

            nt = BucketToken(
                key=key, bucket_id=bucket.id,
                permission=token['permission'],
                created=timezone.now()
            )
            nt.save()
            self.stdout.write(self.style.SUCCESS(f'Ok, bucket token{key}, bucket(name={bucket_name}) created'))
