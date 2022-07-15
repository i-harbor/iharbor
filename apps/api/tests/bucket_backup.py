from buckets.models import Bucket, BackupBucket
from django.urls import reverse
from . import tests
from .tests import get_or_create_user, set_token_auth_header, token_auth_response


class BackupBucketAPITests(tests.MyAPITransactionTestCase):
    databases = {'default', 'metadata'}

    def setUp(self):
        # 创建test用户
        self.user_password = 'password'
        self.user_username = 'test'
        self.user = get_or_create_user(username=self.user_username, password=self.user_password)
        set_token_auth_header(self, username=self.user.username, password=self.user_password)

        # 创建test用户 桶test
        self.bucket_name = 'test'
        Bucket(name=self.bucket_name, user_id=self.user.id).save()
        self.bucket = Bucket.objects.get(name=self.bucket_name)

        # 创建 test1 用户
        self.user_password1 = 'password1'
        self.user_username1 = 'test1'
        self.user1 = get_or_create_user(username=self.user_username1, password=self.user_password1)
        set_token_auth_header(self, username=self.user1.username, password=self.user_password1)

        # 创建test1用户 桶 test1
        self.bucket_name1 = 'test1'
        Bucket(name=self.bucket_name1, user_id=self.user1.id).save()
        self.bucket1 = Bucket.objects.get(name=self.bucket_name1)

    def test_list_backup_bucket(self):
        
        backup_test_id = 0  # 记录桶备份的id
        backup_test1_id = 0
        backup_unknown_id = 100  # 未知id

        # 用户 test 获取备份桶中的信息（没有添加数据的时候）
        set_token_auth_header(self, username=self.user.username, password=self.user_password)
        url = reverse('api:backup_bucket-list')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.data['code'], 'Notfound')

        # 用户 test1 获取备份桶中的信息（没有添加数据的时候）
        set_token_auth_header(self, username=self.user1.username, password=self.user_password1)
        url = reverse('api:backup_bucket-list')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.data['code'], 'Notfound')

        # 添加数据
        token = token_auth_response(username=self.user.username, password=self.user_password)
        self.add_data(BackupBucket, self.bucket.id, self.bucket.name, token.data['token']['key'], 1)

        token = token_auth_response(username=self.user1.username, password=self.user_password1)
        self.add_data(BackupBucket, self.bucket1.id, self.bucket1.name, token.data['token']['key'], 1)

        # test 和 test1 用户分别访问自己的数据桶的备份信息
        # test 用户
        set_token_auth_header(self, username=self.user.username, password=self.user_password)
        url = reverse('api:backup_bucket-list')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['count', 'next', 'previous', 'results'], response.data)
        self.assertEqual(response.data['count'], 1)
        self.assertKeysIn(['endpoint_url', 'bucket_name', 'bucket_token', 'backup_num', 'remarks', 'id',
                           'created_time', 'modified_time', 'status', 'error', 'bucket'], response.data['results'][0])
        self.assertKeysIn(['id', 'name'], response.data['results'][0]['bucket'])
        backup_test_id = response.data['results'][0]['id']

        # test1 用户
        set_token_auth_header(self, username=self.user1.username, password=self.user_password1)
        url = reverse('api:backup_bucket-list')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['count', 'next', 'previous', 'results'], response.data)
        self.assertEqual(response.data['count'], 1)
        self.assertKeysIn(['endpoint_url', 'bucket_name', 'bucket_token', 'backup_num', 'remarks', 'id',
                           'created_time', 'modified_time', 'status', 'error', 'bucket'], response.data['results'][0])
        self.assertKeysIn(['id', 'name'], response.data['results'][0]['bucket'])
        backup_test1_id = response.data['results'][0]['id']

        # test 和 test1 相互访问对方的桶的备份信息
        # test1 访问 test 的数据
        url = reverse('api:backup_bucket-detail', kwargs={'id': backup_test_id})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.data['code'], 'AccessDenied')

        # test 访问 test1 的数据
        set_token_auth_header(self, username=self.user.username, password=self.user_password)
        url = reverse('api:backup_bucket-detail', kwargs={'id': backup_test1_id})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.data['code'], 'AccessDenied')

        # 未知 id 是否正确校验
        url = reverse('api:backup_bucket-detail', kwargs={'id': backup_unknown_id})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.data['code'], 'Notfound')

        # 通过桶名称查看备份信息
        url = reverse('api:backup_bucket-list-bucketp-backups', kwargs={'bucket_name': self.bucket_name})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['count', 'next', 'previous', 'results'], response.data)
        self.assertEqual(response.data['count'], 1)
        self.assertKeysIn(['endpoint_url', 'bucket_name', 'bucket_token', 'backup_num', 'remarks', 'id',
                           'created_time', 'modified_time', 'status', 'error', 'bucket'], response.data['results'][0])
        self.assertKeysIn(['id', 'name'], response.data['results'][0]['bucket'])

        # 查看其他用户下的桶的备份信息
        url = reverse('api:backup_bucket-list-bucketp-backups', kwargs={'bucket_name': self.bucket_name1})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.data['code'], 'AccessDenied')

    @staticmethod
    def add_data(BackupModel, bucket_id, bucket_name, bucket_token, backup_num):
        backup = BackupModel.objects.create(
            endpoint_url='https://xxx.cn/',
            bucket_name=bucket_name,
            bucket_token=bucket_token,
            backup_num=backup_num,
            remarks='',
            created_time='2022-03-04 09:51:32.254758',
            modified_time='2022-03-04 09:51:32.254758',
            status='start',
            bucket_id=bucket_id,
            error='',
        )
        backup.save()
