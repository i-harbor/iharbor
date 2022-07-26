from urllib import parse

from django.conf import settings
from django.urls import reverse

from buckets.models import Bucket, BackupBucket
from buckets.utils import create_bucket, delete_table_for_model_class, BucketFileManagement
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
        self.bucket = Bucket(name=self.bucket_name, user_id=self.user.id)
        self.bucket.save()

        # 创建 user_san 用户
        self.user_san_password = 'password_san'
        self.user_san_username = 'test_san'
        self.user_san = get_or_create_user(username=self.user_san_username, password=self.user_san_password)
        set_token_auth_header(self, username=self.user_san.username, password=self.user_san_password)

        # 创建user_san用户 桶 test_san
        self.bucket_san_name = 'test_san'
        self.bucket_san = Bucket(name=self.bucket_san_name, user_id=self.user_san.id)
        self.bucket_san.save()

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
        set_token_auth_header(self, username=self.user_san.username, password=self.user_san_password)
        url = reverse('api:backup_bucket-list')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.data['code'], 'Notfound')

        # 添加数据
        token = token_auth_response(username=self.user.username, password=self.user_password)
        self.add_data(BackupBucket, self.bucket.id, self.bucket.name, token.data['token']['key'], 1)

        token = token_auth_response(username=self.user_san.username, password=self.user_san_password)
        self.add_data(BackupBucket, self.bucket_san.id, self.bucket_san.name, token.data['token']['key'], 1)

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
        set_token_auth_header(self, username=self.user_san.username, password=self.user_san_password)
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
        url = reverse('api:backup_bucket-list-bucketp-backups', kwargs={'bucket_name': self.bucket_san})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.data['code'], 'AccessDenied')

    def test_create_backup_bucket(self):
        # 创建 user_wang 用户
        self.user_wang_password = 'password_wang'
        self.user_wang_username = 'test_wang'
        self.user_wang = get_or_create_user(username=self.user_wang_username, password=self.user_wang_password)
        set_token_auth_header(self, username=self.user_wang.username, password=self.user_wang_password)

        # 创建user_wang用户 桶 test_wang
        self.bucket_wang_name = 'test_wang'
        self.bucket_wang = create_bucket(name=self.bucket_wang_name, user=self.user_wang)

        test_settings_case = getattr(settings, 'TEST_CASE', None)
        if test_settings_case is None:
            raise Exception("Please configure the test_settings.py file")
        test_settings_securty = getattr(settings, 'TEST_CASE_SECURITY', None)
        if test_settings_securty is None:
            raise Exception("Please configure the securty_settings.py file")

        endpoint_url = test_settings_case['BACKUP_SERVER']['PROVIDER']['endpoint_url']
        readwritetoken = test_settings_securty['BACKUP_BUCKET_TOKEN']['bucket_token_write']
        bucket_name = test_settings_case['BACKUP_SERVER']['PROVIDER']['bucket_name']
        bucket_id = self.bucket_wang.id
        backup_num = 1
        remarks = ''
        status = 'start'  # start stop deleted
        url = reverse('api:backup_bucket-list')
        # 数据正确 创建成功
        response = self.create_bucket_backup(url=url, endpoint_url=endpoint_url, bucket_name=bucket_name,
                                             bucket_token=readwritetoken, backup_num=backup_num, status=status,
                                             bucket_id=bucket_id, remarks=remarks)
        self.assertEqual(response.status_code, 201)
        self.assertKeysIn(['endpoint_url', 'bucket_name', 'bucket_token', 'backup_num', 'remarks', 'id', 'created_time',
                           'modified_time', 'status', 'error', 'bucket'], response.data)
        self.assertKeysIn(['id', 'name'], response.data['bucket'])
        response_id = response.data['id']

        # 修改备份数据
        url = reverse('api:backup_bucket-detail', kwargs={'id': response_id})
        response = self.client.patch(url, data={'bucket_token': readwritetoken})
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['endpoint_url', 'bucket_name', 'bucket_token', 'backup_num', 'remarks', 'id', 'created_time',
                           'modified_time', 'status', 'error', 'bucket'], response.data)
        self.assertKeysIn(['id', 'name'], response.data['bucket'])

        # 修改备份数据 bucket_token 只读权限
        bucket_token_read = test_settings_securty['BACKUP_BUCKET_TOKEN']['bucket_token_read']
        url = reverse('api:backup_bucket-detail', kwargs={'id': response_id})
        response = self.client.patch(url, data={'bucket_token': bucket_token_read})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.data['code'], 'BadRequest')
        # 修改备份数据 bucket_token 随机
        bucket_token_read = '4a0bc1fe3fb34e631ddd6fsdfwefvdefw684c'
        url = reverse('api:backup_bucket-detail', kwargs={'id': response_id})
        response = self.client.patch(url, data={'bucket_token': bucket_token_read})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.data['code'], 'BadRequest')

        # 删除备份数据
        url = reverse('api:backup_bucket-detail', kwargs={'id': response_id})
        response = self.client.delete(url)
        self.assertEqual(response.status_code, 204)

        url = reverse('api:backup_bucket-list')
        # 服务地址填写错误
        response = self.create_bucket_backup(url=url, endpoint_url='http://sdjfnjs.com', bucket_name=bucket_name,
                                             bucket_token=readwritetoken, backup_num=backup_num, status=status,
                                             bucket_id=bucket_id, remarks=remarks)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.data['code'], 'BadRequest')
        # bucket_name 名称错误
        response = self.create_bucket_backup(url=url, endpoint_url=endpoint_url, bucket_name='wet',
                                             bucket_token=readwritetoken, backup_num=backup_num, status=status,
                                             bucket_id=bucket_id, remarks=remarks)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.data['code'], 'BadRequest')
        # bucket_token 权限问题
        response = self.create_bucket_backup(url=url, endpoint_url=endpoint_url, bucket_name=bucket_name,
                                             bucket_token='sdfjsdifewbvuder', backup_num=backup_num, status=status,
                                             bucket_id=bucket_id, remarks=remarks)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.data['code'], 'BadRequest')
        # # backup_num 不在范围内
        response = self.create_bucket_backup(url=url, endpoint_url=endpoint_url, bucket_name=bucket_name,
                                             bucket_token=readwritetoken, backup_num=6, status=status,
                                             bucket_id=bucket_id, remarks=remarks)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.data['code'], 'BadRequest')
        # # status 不是规定的内容
        response = self.create_bucket_backup(url=url, endpoint_url=endpoint_url, bucket_name=bucket_name,
                                             bucket_token=readwritetoken, backup_num=backup_num, status='starts',
                                             bucket_id=bucket_id, remarks=remarks)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.data['code'], 'BadRequest')
        # # bucket_id 不是本用户的桶
        response = self.create_bucket_backup(url=url, endpoint_url=endpoint_url, bucket_name=bucket_name,
                                             bucket_token=readwritetoken, backup_num=backup_num, status=status,
                                             bucket_id=self.bucket.id, remarks=remarks)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.data['code'], 'AccessDenied')

        bfm = BucketFileManagement(collection_name=self.bucket_wang.get_bucket_table_name())
        model_cls = bfm.get_obj_model_class()
        delete_table_for_model_class(model_cls)

    def create_bucket_backup(self, url, endpoint_url, bucket_name, bucket_token, backup_num, status, bucket_id, remarks):
        data = {
            'endpoint_url': endpoint_url,
            'bucket_name': bucket_name,
            'bucket_token': bucket_token,
            'backup_num': backup_num,
            'remarks': remarks,
            'status': status,
            'bucket_id': bucket_id
        }
        response = self.client.post(url, data=data)
        return response

    def create_bucket_token(self, bucket_id_name, perms, by_name=False):
        url_detail = reverse('api:buckets-token-create', kwargs={'id_or_name': bucket_id_name})
        query_params = {'permission': perms}
        if by_name:
            query_params['by-name'] = True

        query = parse.urlencode(query_params)
        response = self.client.post(f'{url_detail}?{query}')
        return response

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
