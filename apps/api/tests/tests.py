import os
import io
import random
import hashlib
import collections
from datetime import datetime, timedelta
from urllib import parse
from string import printable

from django.urls import reverse
from django.contrib.auth import get_user_model
from django.test.client import Client
from django.conf import settings
from django.utils import timezone
from rest_framework.test import APITestCase, APITransactionTestCase
from rest_framework.test import APIClient

from buckets.models import BucketToken, BucketFileBase, Bucket, Archive
from buckets.management.commands.clearbucket import Command as ClearBucketCommand
from buckets.utils import BucketFileManagement
from ceph.models import CephCluster
from users.models import UserProfile
from . import config_ceph_clustar_settings, ensure_s3_multipart_table_exists


User = get_user_model()
EMPTY_HEX_MD5 = 'd41d8cd98f00b204e9800998ecf8427e'


def get_or_create_user(username='test', password='password'):
    user = User.objects.filter(username=username).first()
    if user:
        if not user.check_password(password):
            user.set_password(password)
            user.save()
        return user

    user = User(username=username, is_active=True)
    user.set_password(password)
    user.save()
    return user


def force_login(test_case: APITestCase, password: str = None):
    password = password if password else 'password'
    user = get_or_create_user(password=password)
    test_case.client.force_login(user=user)
    test_case.user = user


def jwt_auth_response(username, password):
    url = reverse('api:jwt-token')
    response = APIClient().post(url, data={'username': username, 'password': password})
    return response


def set_jwt_auth_header(test_case, username, password):
    r = jwt_auth_response(username, password)
    access_token = r.data['access']
    test_case.client.credentials(HTTP_AUTHORIZATION='Bearer ' + access_token)


def token_auth_response(username, password, new_token=False):
    url = reverse('api:auth-token')
    if new_token:
        query = parse.urlencode({'new': 'true'})
        url = f'{url}?{query}'

    response = APIClient().post(url, data={'username': username, 'password': password})
    return response


def set_token_auth_header(test_case, username, password):
    r = token_auth_response(username, password)
    token = r.data['token']['key']
    test_case.client.credentials(HTTP_AUTHORIZATION='Token ' + token)


def random_string(length: int = 10):
    return random.choices(printable, k=length)


def random_bytes_io(mb_num: int):
    bio = io.BytesIO()
    if mb_num <= 0:
        return bio

    for i in range(1024):           # MB
        s = ''.join(random_string(mb_num))
        b = s.encode() * 1024         # KB
        bio.write(b)

    bio.seek(0)
    return bio


def generate_file(filename, mb_num):
    per_mb = 1
    data = random_bytes_io(per_mb)
    with open(filename, 'wb+') as f:
        s = per_mb * 1024 ** 2 + 1
        d = data.read(s)
        for i in range(mb_num):
            f.write(d)


def remove_file(filename):
    os.remove(filename)


def calculate_file_md5(filename):
    with open(filename, 'rb') as f:
        return calculate_md5(f)


def calculate_md5(file):
    if hasattr(file, 'seek'):
        file.seek(0)

    md5obj = hashlib.md5()
    if isinstance(file, collections.Iterable):
        for data in file:
            md5obj.update(data)
    else:
        for data in chunks(file):
            md5obj.update(data)

    _hash = md5obj.hexdigest()
    return _hash


def chunks(f, chunk_size=2*2**20):
    """
    Read the file and yield chunks of ``chunk_size`` bytes (defaults to
    ``File.DEFAULT_CHUNK_SIZE``).
    """
    try:
        f.seek(0)
    except AttributeError:
        pass

    while True:
        data = f.read(chunk_size)
        if not data:
            break
        yield data


def assert_keys_in(testcase, keys: list, container):
    for k in keys:
        testcase.assertIn(k, container)


def assert_dict_is_subdict(testcase, d: dict, sub: dict):
    for k, v in sub.items():
        if k in d and v == d[k]:
            continue
        else:
            testcase.fail(f'{sub} is not sub dict of {d}')

    return True

    # sub_set = set(sub.items())
    # d_set = set(d.items())
    # if not sub_set.issubset(d_set):
    #     testcase.fail(f'{sub} is not sub dict of {d}')


def assert_error_response(testcase, status_code: int, code: str, response, message_key='message'):
    testcase.assertEqual(response.status_code, status_code)
    testcase.assertKeysIn(['code', message_key], response.data)
    testcase.assertEqual(response.data['code'], code)


class MyAPITestCase(APITestCase):
    def assertKeysIn(self, keys: list, container):
        assert_keys_in(self, keys, container)

    def assertDictIsSubDict(self, d: dict, sub: dict):
        assert_dict_is_subdict(self, d, sub)

    def assertErrorResponse(self, status_code: int, code: str, response):
        assert_error_response(self, status_code, code, response)

    def assertOldErrorResponse(self, status_code: int, code: str, response):
        assert_error_response(self, status_code, code, response, message_key='code_text')


class MyAPITransactionTestCase(APITransactionTestCase):
    def assertKeysIn(self, keys: list, container):
        assert_keys_in(self, keys, container)

    def assertDictIsSubDict(self, d: dict, sub: dict):
        assert_dict_is_subdict(self, d, sub)

    def assertErrorResponse(self, status_code: int, code: str, response):
        assert_error_response(self, status_code, code, response)

    def assertOldErrorResponse(self, status_code: int, code: str, response):
        assert_error_response(self, status_code, code, response, message_key='code_text')


class JwtAPITests(MyAPITransactionTestCase):
    def setUp(self):
        self.user_password = 'password'
        user = get_or_create_user(password=self.user_password)
        # self.client.force_login(user=user)
        self.user = user

    def test_jwt(self):
        username = self.user.username
        response = jwt_auth_response(username=username, password='test')
        self.assertEqual(response.status_code, 401)

        response = jwt_auth_response(username=username, password=self.user_password)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['access', 'refresh'], response.data)

        access_token = response.data['access']
        refresh_token = response.data['refresh']

        url = reverse('api:jwt-verify')
        response = Client().post(url, data={'token': access_token})
        self.assertEqual(response.status_code, 200)
        response = Client().post(url, data={'token': refresh_token})
        self.assertEqual(response.status_code, 200)
        response = Client().post(url, data={'token': 'test'})
        self.assertEqual(response.status_code, 401)

        url = reverse('api:jwt-refresh')
        response = Client().post(url, data={'refresh': access_token})
        self.assertEqual(response.status_code, 401)

        response = Client().post(url, data={'refresh': refresh_token})
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['access'], response.data)

        # request api auth by jwt
        access_token = response.data['access']
        self.client.credentials(HTTP_AUTHORIZATION='Bearer ' + 'test')
        url = reverse('api:auth-token')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 401)

        self.client.credentials(HTTP_AUTHORIZATION='Bearer ' + access_token)
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)


class TokenAPITests(MyAPITransactionTestCase):
    def setUp(self):
        self.user_password = 'password'
        user = get_or_create_user(password=self.user_password)
        # self.client.force_login(user=user)
        self.user = user

    def list_token_auth_by_token(self, token: str):
        url = reverse('api:auth-token')
        self.client.credentials(HTTP_AUTHORIZATION='Token ' + token)
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['token'], response.data)
        self.assertKeysIn(['key', 'user', 'created'], response.data['token'])
        self.assertEqual(token, response.data['token']['key'])

    def test_token(self):
        """POST auth api"""
        username = self.user.username
        url = reverse('api:auth-token')
        response = token_auth_response(username=username, password='test')
        self.assertEqual(response.status_code, 400)

        response = token_auth_response(username=username, password=self.user_password)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['token'], response.data)
        self.assertKeysIn(['key', 'user', 'created'], response.data['token'])

        old_token = response.data['token']
        response = token_auth_response(username=username, password=self.user_password)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['token'], old_token)
        self.assertEqual(response.data['token']['key'], old_token['key'])

        # create new token
        response = token_auth_response(username=username, password=self.user_password, new_token=True)
        self.assertEqual(response.status_code, 200)
        self.assertNotEqual(response.data['token'], old_token)
        token = response.data['token']['key']
        self.assertNotEqual(token, old_token['key'])

        """Get and Update token api"""
        self.list_token_auth_by_token(token)

        # update token
        response = self.client.put(url)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['token'], response.data)
        self.assertKeysIn(['key', 'user', 'created'], response.data['token'])
        new_token = response.data['token']['key']
        self.assertNotEqual(token, new_token)

        # old token auth failed
        response = self.client.get(url)
        self.assertEqual(response.status_code, 401)

        # auth use new token
        self.list_token_auth_by_token(new_token)


class AuthKeyAPITests(MyAPITransactionTestCase):
    def setUp(self):
        self.user_password = 'password'
        user = get_or_create_user(password=self.user_password)
        # self.client.force_login(user=user)
        self.user = user

    @staticmethod
    def create_user_auth_key(client, username, password):
        """
        201
        """
        url = reverse('api:auth-key-list')
        return client.post(url, data={'username': username, 'password': password})

    def test_keys(self):
        username = self.user.username

        # create key
        response = self.create_user_auth_key(self.client, username, self.user_password)
        self.assertEqual(response.status_code, 201)
        self.assertKeysIn(['key'], response.data)
        self.assertKeysIn(['access_key', 'secret_key', 'user', 'create_time',
                           'state', 'permission'], response.data['key'])
        self.assertEqual(response.data['key']['user'], username)
        self.assertEqual(response.data['key']['state'], True)
        access_key = response.data['key']['access_key']

        # active key
        url_detail = reverse('api:auth-key-detail', kwargs={'access_key': access_key})
        query = parse.urlencode({'active': False})
        url = f'{url_detail}?{query}'
        response = self.client.patch(url)
        self.assertEqual(response.status_code, 401)

        # 认证header
        set_token_auth_header(self, username=self.user.username, password=self.user_password)
        response = self.client.patch(url)
        self.assertEqual(response.status_code, 200)

        # list keys
        url = reverse('api:auth-key-list')
        self.client.force_login(self.user)
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['keys'], response.data)
        self.assertIsInstance(response.data['keys'], list)
        self.assertEqual(len(response.data['keys']), 1)
        self.assertKeysIn(['access_key', 'secret_key', 'user', 'create_time',
                           'state', 'permission'], response.data['keys'][0])
        self.assertEqual(response.data['keys'][0]['state'], False)

        # create key
        self.client.logout()
        response = self.create_user_auth_key(self.client, username, self.user_password)
        self.assertEqual(response.status_code, 201)

        # 认证header
        set_token_auth_header(self, username=self.user.username, password=self.user_password)

        # list keys
        url = reverse('api:auth-key-list')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['keys'], response.data)
        self.assertEqual(len(response.data['keys']), 2)

        # delete key
        url_detail = reverse('api:auth-key-detail', kwargs={'access_key': access_key})
        response = self.client.delete(url_detail)
        self.assertEqual(response.status_code, 204)

        # list keys
        url = reverse('api:auth-key-list')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['keys'], response.data)
        self.assertEqual(len(response.data['keys']), 1)


class BucketsAPITests(MyAPITransactionTestCase):
    databases = {'default', 'metadata'}

    def setUp(self):
        settings.BUCKET_LIMIT_DEFAULT = 2
        config_ceph_clustar_settings()
        self.user_password = 'password'
        user = get_or_create_user(password=self.user_password)
        # self.client.force_login(user=user)
        self.user = user

    @staticmethod
    def create_bucket(client, name):
        url = reverse('api:buckets-list')
        response = client.post(url, data={'name': name})
        if response.status_code not in [200, 201]:
            print(f'create bucket error: {response.data}')

        return response

    @staticmethod
    def delete_bucket(client, name):
        url_detail = reverse('api:buckets-detail', kwargs={'id_or_name': name})
        query = parse.urlencode({'by-name': True})
        response = client.delete(f'{url_detail}?{query}')
        return response

    def bucket_detail_response(self, id_or_name, by_name=False):
        url_detail = reverse('api:buckets-detail', kwargs={'id_or_name': id_or_name})
        if by_name:
            query = parse.urlencode({'by-name': True})
            url_detail = f'{url_detail}?{query}'

        return self.client.get(url_detail)

    def test_create_delete_list_detail_bucket(self):
        bucket_name = 'test'
        set_jwt_auth_header(self, username=self.user.username, password=self.user_password)

        response = self.create_bucket(self.client, 'ss')
        self.assertEqual(response.status_code, 400)
        response = self.create_bucket(self.client, '-tss')
        self.assertEqual(response.status_code, 400)
        response = self.create_bucket(self.client, 'ss-')
        self.assertEqual(response.status_code, 400)

        response = self.create_bucket(self.client, bucket_name)
        self.assertEqual(response.status_code, 201, 'create bucket failed')
        self.assertKeysIn(['bucket', 'data'], response.data)
        self.assertKeysIn(['id', 'name', 'user', 'created_time',
                           'access_permission', 'ftp_enable', 'ftp_password',
                           'ftp_ro_password', 'remarks'], response.data['bucket'])

        bucket_id = response.data['bucket']['id']

        response = self.create_bucket(self.client, bucket_name)
        self.assertEqual(response.status_code, 409)
        self.assertIn(response.data['code'], ['BucketAlreadyExists', 'BucketAlreadyOwnedByYou'])

        # bucket detail
        response = self.bucket_detail_response(bucket_id)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['id', 'name', 'user', 'created_time',
                           'access_permission', 'ftp_enable', 'ftp_password',
                           'ftp_ro_password', 'remarks'], response.data['bucket'])
        bucket_detail = response.data['bucket']

        response = self.bucket_detail_response(bucket_name, by_name=True)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['bucket'], bucket_detail)

        # list bucket
        url_list = reverse('api:buckets-list')
        response = self.client.get(url_list)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['count', 'next', 'page', 'previous',
                           'buckets'], response.data)
        self.assertEqual(response.data['count'], 1)
        self.assertIsInstance(response.data['buckets'], list)
        self.assertKeysIn(['id', 'name', 'user', 'created_time',
                           'access_permission', 'ftp_enable', 'ftp_password',
                           'ftp_ro_password', 'remarks'], response.data['buckets'][0])

        # delete bucket
        response = self.delete_bucket(self.client, bucket_name)
        self.assertEqual(response.status_code, 204)
        self.clear_bucket_archive(bucket_name)

        response = self.bucket_detail_response(bucket_name, by_name=True)
        self.assertEqual(response.status_code, 404)

    def test_remark_permission_bucket(self):
        bucket_name = 'test2'
        set_jwt_auth_header(self, username=self.user.username, password=self.user_password)

        # create bucket
        response = self.create_bucket(self.client, bucket_name)
        self.assertEqual(response.status_code, 201)
        bucket_id = response.data['bucket']['id']

        # remarks
        url = reverse('api:buckets-remark', kwargs={'id_or_name': bucket_name})
        query = parse.urlencode({'by-name': 'true'})
        response = self.client.patch(f'{url}?{query}')
        self.assertEqual(response.status_code, 400)

        remarks_test = 'test remarks'
        query = parse.urlencode({'by-name': 'true', 'remarks': remarks_test})
        response = self.client.patch(f'{url}?{query}')
        self.assertEqual(response.status_code, 200)

        response = self.bucket_detail_response(bucket_id)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['bucket']['remarks'], remarks_test)

        # access permission
        url_detail = reverse('api:buckets-detail', kwargs={'id_or_name': bucket_id})
        # 400
        query = parse.urlencode({'public': 6})
        response = self.client.patch(f'{url_detail}?{query}')
        self.assertEqual(response.status_code, 400)

        # public
        query = parse.urlencode({'public': 1})
        response = self.client.patch(f'{url_detail}?{query}')
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['public', 'share'], response.data)
        self.assertIsInstance(response.data['share'], list)
        self.assertEqual(response.data['public'], 1)

        # private
        query = parse.urlencode({'public': 2})
        response = self.client.patch(f'{url_detail}?{query}')
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['public', 'share'], response.data)
        self.assertIsInstance(response.data['share'], list)
        self.assertEqual(response.data['public'], 2)

        # read and write
        query = parse.urlencode({'public': 3})
        response = self.client.patch(f'{url_detail}?{query}')
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['public', 'share'], response.data)
        self.assertIsInstance(response.data['share'], list)
        self.assertEqual(response.data['public'], 3)

        response = self.bucket_detail_response(bucket_id)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['bucket']['access_permission'], '公有（可读写）')

        # delete bucket
        response = self.delete_bucket(self.client, bucket_name)
        self.assertEqual(response.status_code, 204)
        self.clear_bucket_archive(bucket_name)

    def create_bucket_token(self, bucket_id_name, perms, by_name=False):
        url_detail = reverse('api:buckets-token-create', kwargs={'id_or_name': bucket_id_name})
        query_params = {'permission': perms}
        if by_name:
            query_params['by-name'] = True

        query = parse.urlencode(query_params)
        response = self.client.post(f'{url_detail}?{query}')
        return response

    def test_bucket_token(self):
        bucket_name = 'test2'
        set_jwt_auth_header(self, username=self.user.username, password=self.user_password)

        # create bucket
        response = self.create_bucket(self.client, bucket_name)
        self.assertEqual(response.status_code, 201, 'create bucket failed')
        bucket_id = response.data['bucket']['id']

        # create failed bucket token
        response = self.create_bucket_token(bucket_id_name=bucket_id, perms='test')
        self.assertEqual(response.status_code, 400)

        # create readonly bucket token
        response = self.create_bucket_token(bucket_id_name=bucket_id,
                                            perms=BucketToken.PERMISSION_READONLY)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['key', 'bucket', 'permission', 'created'], response.data)
        self.assertKeysIn(BucketToken.PERMISSION_READONLY, response.data['permission'])
        bucket_token = response.data['key']

        # ftp enable api auth by readonly bucket token
        self.client.credentials(HTTP_AUTHORIZATION='BucketToken ' + bucket_token)
        ftp_url = reverse('api:ftp-detail', kwargs={'bucket_name': bucket_name})
        query = parse.urlencode({'enable': True})
        response = self.client.patch(f'{ftp_url}?{query}')
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.data['code'], 'AccessDenied')

        # create readwrite bucket token
        self.client.logout()
        set_jwt_auth_header(self, username=self.user.username, password=self.user_password)
        response = self.create_bucket_token(bucket_id_name=bucket_id,
                                            perms=BucketToken.PERMISSION_READWRITE)
        bucket_token = response.data['key']

        # ftp enable api auth by readwrite bucket token
        self.client.logout()
        self.client.credentials(HTTP_AUTHORIZATION='BucketToken ' + bucket_token)
        ftp_url = reverse('api:ftp-detail', kwargs={'bucket_name': bucket_name})
        query = parse.urlencode({'enable': True, 'password': 'readwrite',
                                 'ro_password': 'readonly'})
        response = self.client.patch(f'{ftp_url}?{query}')
        self.assertEqual(response.status_code, 200)

        response = self.bucket_detail_response(bucket_name, by_name=True)
        self.assertEqual(response.data['bucket']['ftp_enable'], True)
        self.assertEqual(response.data['bucket']['ftp_password'], 'readwrite')
        self.assertEqual(response.data['bucket']['ftp_ro_password'], 'readonly')

        set_jwt_auth_header(self, username=self.user.username, password=self.user_password)
        # list bucket token
        url = reverse('api:buckets-token-list', kwargs={'id_or_name': bucket_id})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['count', 'tokens'], response.data)
        self.assertEqual(response.data['count'], 2)
        self.assertKeysIn(['key', 'bucket', 'permission', 'created'], response.data['tokens'][0])

        # bucket token detail
        url = reverse('api:bucket-token-detail', kwargs={'token': bucket_token})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['key', 'bucket', 'permission', 'created'], response.data)

        # delete bucket token
        url = reverse('api:bucket-token-detail', kwargs={'token': bucket_token})
        response = self.client.delete(url)
        self.assertEqual(response.status_code, 204)

        url = reverse('api:bucket-token-detail', kwargs={'token': bucket_token})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.data['code'], 'NoSuchToken')

        response = self.delete_bucket(self.client, bucket_name)
        self.assertEqual(response.status_code, 204)
        self.clear_bucket_archive(bucket_name)

    @staticmethod
    def clear_bucket_archive(bucketname):
        buckets = Archive.objects.filter(name=bucketname, type=Archive.TYPE_COMMON).all()
        for b in buckets:
            cmd = ClearBucketCommand()
            cmd._clear_datetime = datetime.utcnow()
            cmd.clear_one_bucket(b)


class DirAPITests(MyAPITransactionTestCase):
    databases = {'default', 'metadata'}

    def setUp(self):
        settings.BUCKET_LIMIT_DEFAULT = 2
        config_ceph_clustar_settings()
        self.user_password = 'password'
        user = get_or_create_user(password=self.user_password)
        # self.client.force_login(user=user)
        self.user = user
        set_token_auth_header(self, username=self.user.username, password=self.user_password)

        self.bucket_name = 'test'
        response = BucketsAPITests.create_bucket(self.client, self.bucket_name)
        self.assertEqual(response.status_code, 201, 'create bucket failed')

    @staticmethod
    def create_dir_response(client, bucket_name: str, dirpath: str):
        """201 ok"""
        url = reverse('api:dir-detail', kwargs={'bucket_name': bucket_name, 'dirpath': dirpath})
        return client.post(url)

    def test_dir(self):
        bucket_name = self.bucket_name

        # create dir
        parent_dir = '父目录名'
        sub_dir = '子目录名'
        sub_dir_path = f'{parent_dir}/{sub_dir}'
        response = self.create_dir_response(self.client, bucket_name, dirpath=parent_dir)
        self.assertEqual(response.status_code, 201)
        self.assertKeysIn(['code', 'data', 'dir'], response.data)
        self.assertKeysIn(['na', 'name', 'fod', 'did', 'si', 'ult',
                           'upt', 'dlc', 'download_url', 'access_permission'], response.data['dir'])
        self.assertDictIsSubDict(response.data['dir'], {
            'na': parent_dir, 'name': parent_dir, 'fod': False, 'si': 0
        })

        response = self.create_dir_response(self.client, bucket_name, dirpath=sub_dir_path)
        self.assertEqual(response.status_code, 201)

        # list dir
        url = reverse('api:dir-list', kwargs={'bucket_name': bucket_name})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['bucket_name', 'dir_path', 'files', 'count',
                           'next', 'previous', 'page'], response.data)
        self.assertEqual(response.data['count'], 1)
        self.assertKeysIn(['na', 'name', 'fod', 'did', 'si', 'ult',
                           'upt', 'dlc', 'download_url',
                           'access_permission'], response.data['files'][0])
        self.assertDictIsSubDict(response.data['files'][0], {
            'na': parent_dir, 'name': parent_dir, 'fod': False, 'si': 0
        })

        url = reverse('api:dir-detail', kwargs={'bucket_name': bucket_name, 'dirpath': parent_dir})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['count'], 1)
        self.assertDictIsSubDict(response.data['files'][0], {
            'na': sub_dir_path, 'name': sub_dir, 'fod': False, 'si': 0
        })

        # set permission
        share_password = 'testcode'
        url = reverse('api:dir-detail', kwargs={'bucket_name': bucket_name, 'dirpath': sub_dir_path})
        query = parse.urlencode({'share': BucketFileBase.SHARE_ACCESS_READONLY, 'days': 6, 'password': share_password})
        response = self.client.patch(f'{url}?{query}')
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['share', 'share_code', 'code', 'code_text'], response.data)
        self.assertEqual(response.data['share_code'], share_password)
        share_url = reverse('share:share-view', kwargs={'share_base': f'{bucket_name}/{sub_dir_path}'})
        self.assertIn(share_url, response.data['share'])

        # metadata api
        response = MetadataAPITests().get_metadata(self.client, bucket_name=bucket_name, path=sub_dir_path)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['bucket_name', 'obj', 'dir_path', 'info'], response.data)
        self.assertDictIsSubDict(response.data['obj'], {
            'na': sub_dir_path, 'name': sub_dir, 'fod': False, 'si': 0,
            'access_code': BucketFileBase.SHARE_ACCESS_READONLY
        })

        # get share url
        url = reverse('api:share-detail', kwargs={'bucket_name': bucket_name, 'path': sub_dir_path})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['is_obj'], False)
        self.assertEqual(response.data['share_code'], share_password)
        share_url = reverse('share:share-view', kwargs={'share_base': f'{bucket_name}/{sub_dir_path}'})
        self.assertIn(share_url, response.data['share_uri'])

        # delete dir
        url = reverse('api:dir-detail', kwargs={'bucket_name': bucket_name, 'dirpath': parent_dir})
        response = self.client.delete(url)
        self.assertOldErrorResponse(409, 'NoEmptyDir', response)

        url = reverse('api:dir-detail', kwargs={'bucket_name': bucket_name, 'dirpath': sub_dir_path})
        response = self.client.delete(url)
        self.assertEqual(response.status_code, 204)

        response = self.client.delete(url)
        self.assertOldErrorResponse(404, 'NoSuchKey', response)

    def tearDown(self):
        # delete bucket
        response = BucketsAPITests.delete_bucket(self.client, self.bucket_name)
        self.assertEqual(response.status_code, 204)
        BucketsAPITests.clear_bucket_archive(self.bucket_name)


class ObjectsAPITests(MyAPITransactionTestCase):
    databases = {'default', 'metadata'}

    def setUp(self):
        settings.BUCKET_LIMIT_DEFAULT = 2
        config_ceph_clustar_settings()
        ensure_s3_multipart_table_exists()
        self.user_password = 'password'
        self.user = get_or_create_user(password=self.user_password)
        set_token_auth_header(self, username=self.user.username, password=self.user_password)
        self.bucket_name = 'test'
        r = BucketsAPITests().create_bucket(self.client, self.bucket_name)
        self.assertEqual(r.status_code, 201)
        self.bucket = Bucket.objects.get(name=self.bucket_name)

    @staticmethod
    def put_object_response(client, bucket_name: str, key: str, file):
        """200 ok"""
        url = reverse('api:obj-detail', kwargs={'bucket_name': bucket_name, 'objpath': key})
        file_md5 = calculate_md5(file)
        headers = {'HTTP_Content_MD5': file_md5}
        file.seek(0)
        return client.put(url, data={'file': file}, **headers)

    @staticmethod
    def put_object_v2_response(client, bucket_name: str, key: str, file):
        """200 ok"""
        url = reverse('api:v2-obj-detail', kwargs={'bucket_name': bucket_name, 'objpath': key})
        file_md5 = calculate_md5(file)
        headers = {'HTTP_Content_MD5': file_md5}
        file.seek(0)
        data = file.read()
        headers.update({'HTTP_Content_Length': str(len(data))})
        return client.put(url, data=data,
                          content_type='application/octet-stream', **headers)

    def download_object_response(self, bucket_name: str, key: str,
                                 offset: int = None, size: int = None):
        url = reverse('api:obj-detail', kwargs={'bucket_name': bucket_name, 'objpath': key})
        if offset is not None and size is not None:
            query = parse.urlencode({'offset': offset, 'size': size})
            url = f'{url}?{query}'

        return self.client.get(url)

    @staticmethod
    def delete_object_response(client, bucket_name: str, key: str):
        url = reverse('api:obj-detail', kwargs={'bucket_name': bucket_name, 'objpath': key})
        return client.delete(url)

    @staticmethod
    def upload_one_chunk(client, bucket_name: str, key: str, offset: int, chunk):
        """
        上传一个分片

        :param client:
        :param bucket_name:
        :param key: object key
        :param offset: 分片偏移量
        :param chunk: 分片
        :return:
            Response
        """
        url = reverse('api:obj-detail', kwargs={'bucket_name': bucket_name, 'objpath': key})
        chunk_file = io.BytesIO(chunk)
        return client.post(url, data={
            "chunk_offset": offset, "chunk_size": len(chunk), 'chunk': chunk_file})

    @staticmethod
    def upload_one_chunk_v2(client, bucket_name: str, key: str, offset: int, chunk: bytes):
        """
        上传一个分片

        :param client:
        :param bucket_name:
        :param key: object key
        :param offset: 分片偏移量
        :param chunk: 分片
        :return:
            Response
        """
        url = reverse('api:v2-obj-detail', kwargs={'bucket_name': bucket_name, 'objpath': key})
        query = parse.urlencode({'offset': offset})
        file_md5 = calculate_md5(io.BytesIO(chunk))
        headers = {'HTTP_Content_MD5': file_md5}
        return client.post(f'{url}?{query}', data=chunk,
                           content_type='application/octet-stream', **headers)

    def multipart_upload_object(self, client, bucket_name: str, key: str, file,
                                part_size: int = 5*1024**2, api_version: str = 'v1'):
        offset = 0
        for chunk in chunks(file, chunk_size=part_size):
            if not chunk:
                return True

            if api_version == 'v1':
                response = self.upload_one_chunk(client, bucket_name, key, offset, chunk)
                self.assertEqual(response.status_code, 200, 'upload_one_chunk failed')
            elif api_version == 'v2':
                response = self.upload_one_chunk_v2(client, bucket_name, key, offset, chunk)
                self.assertEqual(response.status_code, 200, 'upload_one_chunk_v2 failed')
            else:
                raise Exception(f'invalid param api_version value, {api_version}')

            offset += len(chunk)

        return True

    def test_upload_download_delete(self):
        file = random_bytes_io(mb_num=6)
        file_md5 = calculate_md5(file)
        key = 'test.pdf'
        key2 = 'a/b/c/D/大家/test.txt'
        key3 = 'test3.pdf'
        key4 = 'test4.pdf'
        ceph1 = CephCluster.objects.filter(id=3).first()
        ceph2 = CephCluster.objects.filter(id=4).first()
        response = self.put_object_response(self.client, bucket_name=self.bucket_name, key=key, file=file)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['created'], True)
        # 查询对象
        collection_name = self.bucket.get_bucket_table_name()
        bfm = BucketFileManagement(path="", collection_name=collection_name)
        obj = bfm.get_obj(path=key)
        obj_id = obj.get_pool_id()
        self.assertEqual(ceph1.id, obj_id)

        # 优先值修改
        ceph1.priority_stored_value = 3
        ceph1.save(update_fields=['priority_stored_value'])
        ceph1.refresh_from_db()

        response = self.put_object_response(self.client, bucket_name=self.bucket_name, key=key3, file=file)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['created'], True)
        # 查询对象
        collection_name = self.bucket.get_bucket_table_name()
        bfm = BucketFileManagement(path="", collection_name=collection_name)
        obj = bfm.get_obj(path=key3)
        obj_id = obj.get_pool_id()
        self.assertEqual(ceph2.id, obj_id)

        # 将获取到的ceph 优先值存储 禁用
        ceph2.disable_choice = True
        ceph2.save(update_fields=['disable_choice'])
        ceph2.refresh_from_db()
        response = self.put_object_response(self.client, bucket_name=self.bucket_name, key=key4, file=file)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['created'], True)

        collection_name = self.bucket.get_bucket_table_name()
        bfm = BucketFileManagement(path="", collection_name=collection_name)
        obj = bfm.get_obj(path=key4)
        obj_id = obj.get_pool_id()
        self.assertEqual(ceph1.id, obj_id)

        response = self.download_object_response(bucket_name=self.bucket_name, key=key,
                                                 offset=0, size=10)
        self.assertEqual(response.status_code, 200)
        for data in response:
            self.assertEqual(len(data), 10)
            break
        else:
            raise self.failureException('read object failed')

        response = self.download_object_response(bucket_name=self.bucket_name, key=key)
        self.assertEqual(response.status_code, 200)
        download_md5 = calculate_md5(response)
        self.assertEqual(download_md5, file_md5, msg='Compare the MD5 of upload file and download file')

        response = self.put_object_response(self.client, bucket_name=self.bucket_name, key=key2, file=file)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['created'], True)

        response = self.download_object_response(bucket_name=self.bucket_name, key=key2)
        self.assertEqual(response.status_code, 200)
        download_md5 = calculate_md5(response)
        self.assertEqual(download_md5, file_md5, msg='Compare the MD5 of upload file and download file')

        # delete object
        response = self.delete_object_response(self.client, bucket_name=self.bucket_name, key=key2)
        self.assertEqual(response.status_code, 204)

        response = self.delete_object_response(self.client, bucket_name=self.bucket_name, key=key)
        self.assertEqual(response.status_code, 204)

        response = self.delete_object_response(self.client, bucket_name=self.bucket_name, key=key)
        self.assertEqual(response.status_code, 404)

        response = self.delete_object_response(self.client, bucket_name=self.bucket_name, key=key3)
        self.assertEqual(response.status_code, 204)

        response = self.delete_object_response(self.client, bucket_name=self.bucket_name, key=key4)
        self.assertEqual(response.status_code, 204)

    def test_multipart_upload_download_delete(self):
        file = random_bytes_io(mb_num=16)
        file_md5 = calculate_md5(file)
        key = 'a/的/test.pdf'
        ok = self.multipart_upload_object(client=self.client, bucket_name=self.bucket_name, key=key, file=file)
        self.assertTrue(ok, 'multipart_upload_object failed')

        response = self.download_object_response(bucket_name=self.bucket_name, key=key)
        self.assertEqual(response.status_code, 200)
        download_md5 = calculate_md5(response)
        self.assertEqual(download_md5, file_md5, msg='Compare the MD5 of multipart upload file and download file')

        # delete object
        response = self.delete_object_response(self.client, bucket_name=self.bucket_name, key=key)
        self.assertEqual(response.status_code, 204)

    def test_share_object(self):
        file = random_bytes_io(mb_num=6)
        dir_path = 'aa'
        key = f'{dir_path}/test.pdf'

        response = DirAPITests.create_dir_response(self.client, bucket_name=self.bucket_name, dirpath=dir_path)
        self.assertEqual(response.status_code, 201)

        response = self.put_object_response(self.client, bucket_name=self.bucket_name, key=key, file=file)
        self.assertEqual(response.status_code, 200)

        # get share url
        url = reverse('api:share-detail', kwargs={'bucket_name': self.bucket_name, 'path': key})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.data['code'], 'NotShared')

        # metadata api
        response = MetadataAPITests.get_metadata(self.client, bucket_name=self.bucket_name, path=key)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['obj']['access_code'], BucketFileBase.SHARE_ACCESS_NO)

        # share object
        share_password = 'test'
        url = reverse('api:obj-detail', kwargs={'bucket_name': self.bucket_name, 'objpath': key})
        query = parse.urlencode({'share': BucketFileBase.SHARE_ACCESS_READONLY,
                                'days': 66, 'password': share_password})
        response = self.client.patch(f'{url}?{query}')
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['share', 'days', 'share_uri', 'access_code'], response.data)
        self.assertEqual(response.data['access_code'], BucketFileBase.SHARE_ACCESS_READONLY)
        share_url = reverse('share:obs-detail', kwargs={'objpath': f'{self.bucket_name}/{key}'})
        share_uri = f"{share_url}?p={share_password}"
        self.assertIn(share_uri, response.data['share_uri'])

        # metadata api
        response = MetadataAPITests.get_metadata(self.client, bucket_name=self.bucket_name, path=key)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['obj']['access_code'], BucketFileBase.SHARE_ACCESS_READONLY)

        # get share url
        url = reverse('api:share-detail', kwargs={'bucket_name': self.bucket_name, 'path': key})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['is_obj'], True)
        share_url = reverse('share:obs-detail', kwargs={'objpath': f'{self.bucket_name}/{key}'})
        share_uri = f"{share_url}?p={share_password}"
        self.assertIn(share_uri, response.data['share_uri'])

        # delete object
        response = self.delete_object_response(self.client, bucket_name=self.bucket_name, key=key)
        self.assertEqual(response.status_code, 204)

    def test_obj_rados_move(self):
        file = random_bytes_io(mb_num=6)
        object_size = 6 * 1024**2
        dir_path = 'aa'
        key = 'test.pdf'
        rename = 'rename.pdf'
        move_key = f'{dir_path}/{rename}'

        response = DirAPITests.create_dir_response(self.client, bucket_name=self.bucket_name, dirpath=dir_path)
        self.assertEqual(response.status_code, 201)

        response = self.put_object_response(self.client, bucket_name=self.bucket_name, key=key, file=file)
        self.assertEqual(response.status_code, 200)

        collection_name = self.bucket.get_bucket_table_name()
        bfm = BucketFileManagement(path="", collection_name=collection_name)
        obj = bfm.get_obj(path=key)
        pool_id = obj.get_pool_id()
        url = reverse('api:obj-rados-detail', kwargs={
            'bucket_name': self.bucket_name, 'objpath': key})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['rados', 'chunk_size', 'size', 'filename'], response.data['info'])
        self.assertIn(f"iharbor:{settings.CEPH_RADOS[str(pool_id)]['CLUSTER_NAME']}"
                      f"/{settings.CEPH_RADOS[str(pool_id)]['POOL_NAME'][0]}/{self.bucket.id}_",
                      response.data['info']['rados'][0])
        info: dict = response.data['info']
        info.pop('rados', None)
        self.assertDictIsSubDict(info, {
            'chunk_size': 2 * 1024**3, 'size': object_size, 'filename': key
        })

        # move object
        url = reverse('api:move-detail', kwargs={
            'bucket_name': self.bucket_name, 'objpath': key})
        query = parse.urlencode({'move_to': dir_path, 'rename': rename})
        response = self.client.post(f'{url}?{query}')
        self.assertEqual(response.status_code, 201)
        self.assertDictIsSubDict(response.data, {'bucket_name': self.bucket_name, 'dir_path': dir_path})
        self.assertDictIsSubDict(response.data['obj'], {
            'na': move_key, 'name': rename, 'fod': True, 'si': object_size, 'dlc': 0,
            'access_code': BucketFileBase.SHARE_ACCESS_NO
        })

        # delete object
        response = self.delete_object_response(self.client, bucket_name=self.bucket_name, key=move_key)
        self.assertEqual(response.status_code, 204)

    def test_v2_put_object(self):
        file = random_bytes_io(mb_num=16)
        file_md5 = calculate_md5(file)
        empty_file = random_bytes_io(mb_num=0)
        empty_file_md5 = calculate_md5(empty_file)
        key = 'a/b/c/v2test.pdf'

        response = self.put_object_v2_response(self.client, bucket_name=self.bucket_name, key=key, file=file)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['created'], True)

        response = self.download_object_response(bucket_name=self.bucket_name, key=key)
        self.assertEqual(response.status_code, 200)
        download_md5 = calculate_md5(response)
        self.assertEqual(download_md5, file_md5, msg='Compare the MD5 of upload file and download file')

        response = self.put_object_v2_response(self.client, bucket_name=self.bucket_name, key=key, file=empty_file)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['created'], False)

        response = self.download_object_response(bucket_name=self.bucket_name, key=key)
        self.assertEqual(response.status_code, 200)
        download_md5 = calculate_md5(response)
        self.assertEqual(download_md5, empty_file_md5, msg='Compare the MD5 of upload empty file and download file')

        # delete object
        response = self.delete_object_response(self.client, bucket_name=self.bucket_name, key=key)
        self.assertEqual(response.status_code, 204)

        # test empty object
        empty_key = 'a/empty.txt'
        response = self.put_object_v2_response(self.client, bucket_name=self.bucket_name, key=empty_key, file=empty_file)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['created'], True)

        # delete empty object
        response = self.delete_object_response(self.client, bucket_name=self.bucket_name, key=empty_key)
        self.assertEqual(response.status_code, 204)

    def test_v2_multipart_upload_download_delete(self):
        file = random_bytes_io(mb_num=16)
        file_md5 = calculate_md5(file)
        key = 'a/b/v2test.pdf'
        ok = self.multipart_upload_object(client=self.client, bucket_name=self.bucket_name, key=key,
                                          file=file, api_version='v2')
        self.assertTrue(ok, 'multipart_upload_object v2 failed')

        response = self.download_object_response(bucket_name=self.bucket_name, key=key)
        self.assertEqual(response.status_code, 200)
        download_md5 = calculate_md5(response)
        self.assertEqual(download_md5, file_md5, msg='Compare the MD5 of multipart upload file and download file')

        # delete object
        response = self.delete_object_response(self.client, bucket_name=self.bucket_name, key=key)
        self.assertEqual(response.status_code, 204)

    def tearDown(self):
        # delete bucket
        response = BucketsAPITests.delete_bucket(self.client, self.bucket_name)
        self.assertEqual(response.status_code, 204)
        BucketsAPITests.clear_bucket_archive(self.bucket_name)


class MetadataAPITests(MyAPITransactionTestCase):
    databases = {'default', 'metadata'}

    def setUp(self):
        settings.BUCKET_LIMIT_DEFAULT = 2
        config_ceph_clustar_settings()
        self.user_password = 'password'
        user = get_or_create_user(password=self.user_password)
        # self.client.force_login(user=user)
        self.user = user
        set_token_auth_header(self, username=self.user.username, password=self.user_password)
        self.bucket_name = 'test'
        r = BucketsAPITests.create_bucket(self.client, self.bucket_name)
        self.assertEqual(r.status_code, 201)

    @staticmethod
    def get_metadata(client, bucket_name: str, path: str):
        # metadata api
        url = reverse('api:metadata-detail', kwargs={'bucket_name': bucket_name, 'path': path})
        response = client.get(url)
        return response

    @staticmethod
    def create_empty_object_metadata(testcase, bucket_name: str, key: str, check_response=False):
        # create empty object
        filename = key.rsplit('/', maxsplit=1)[-1]
        url = reverse('api:metadata-detail', kwargs={'bucket_name': bucket_name, 'path': key})
        response = testcase.client.post(url)
        if check_response:
            testcase.assertEqual(response.status_code, 200)
            testcase.assertKeysIn(['obj', 'info'], response.data)
            testcase.assertKeysIn(['rados', 'size', 'filename'], response.data['info'])
            testcase.assertDictIsSubDict(response.data['obj'], {
                'na': key, 'name': filename, 'fod': True, 'si': 0,
            })

        return response

    def test_refresh_and_metadata(self):
        file = random_bytes_io(mb_num=6)
        dir_path = 'bb'
        obj_name = 'object.key'
        key = f'{dir_path}/{obj_name}'

        response = DirAPITests.create_dir_response(self.client, bucket_name=self.bucket_name, dirpath=dir_path)
        self.assertEqual(response.status_code, 201)

        response = ObjectsAPITests.put_object_response(self.client, bucket_name=self.bucket_name, key=key, file=file)
        self.assertEqual(response.status_code, 200)

        # refresh metadata
        url = reverse('api:refresh-meta-detail', kwargs={'bucket_name': self.bucket_name, 'path': dir_path})
        response = self.client.post(url)
        self.assertEqual(response.status_code, 404)

        url = reverse('api:refresh-meta-detail', kwargs={'bucket_name': self.bucket_name, 'path': key})
        response = self.client.post(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['info']['size'], 6*1024**2)

        # get metadata
        response = self.get_metadata(self.client, bucket_name=self.bucket_name, path=key)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['bucket_name', 'obj', 'dir_path', 'info'], response.data)
        self.assertDictIsSubDict(response.data['obj'], {
            'na': key, 'name': obj_name, 'fod': True, 'si': 6*1024**2,
            'access_code': BucketFileBase.SHARE_ACCESS_NO
        })
        self.assertKeysIn(['rados', 'chunk_size', 'size', 'filename'], response.data['info'])
        self.assertDictIsSubDict(response.data['info'], {
            'chunk_size': 2*1024**3, 'size': 6*1024**2, 'filename': obj_name
        })

        # create empty object
        empty_key = 'object.empty'
        self.create_empty_object_metadata(
            self, bucket_name=self.bucket_name, key=empty_key, check_response=True
        )

    def tearDown(self):
        r = BucketsAPITests.delete_bucket(self.client, self.bucket_name)
        self.assertEqual(r.status_code, 204)
        BucketsAPITests.clear_bucket_archive(self.bucket_name)


class CephAPITests(MyAPITransactionTestCase):
    def setUp(self):
        self.user_password = 'password'
        user: UserProfile = get_or_create_user(password=self.user_password)
        self.user = user
        set_token_auth_header(self, username=self.user.username, password=self.user_password)

    def test_perf(self):
        url = reverse('api:ceph_performance-list')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 403)

        self.user.is_superuser = True
        self.user.save()

        # response = self.client.get(url)
        # self.assertEqual(response.status_code, 200)
        # self.assertKeysIn(['bw_rd', 'bw_wr', 'bw', 'op_rd', 'op_wr', 'op'], response.data)


class SearchBucketAPITests(MyAPITransactionTestCase):
    databases = {'default', 'metadata'}

    def setUp(self):
        settings.BUCKET_LIMIT_DEFAULT = 2
        config_ceph_clustar_settings()
        self.user_password = 'password'
        user: UserProfile = get_or_create_user(password=self.user_password)
        self.user = user
        set_token_auth_header(self, username=self.user.username, password=self.user_password)

        self.bucket_name = 'test'
        r = BucketsAPITests.create_bucket(self.client, self.bucket_name)
        self.assertEqual(r.status_code, 201)

    def test_search_objects_in_bucket(self):
        key1 = 'ab.txt'
        key2 = 'abCD.txt'
        MetadataAPITests.create_empty_object_metadata(
            self, bucket_name=self.bucket_name, key=key1, check_response=True
        )
        MetadataAPITests.create_empty_object_metadata(
            self, bucket_name=self.bucket_name, key=key2, check_response=True
        )

        # search
        url = reverse('api:search-object-list')
        query = parse.urlencode({'bucket': self.bucket_name, 'search': 'abC'})
        response = self.client.get(f'{url}?{query}')
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['count', 'next', 'previous', 'page', 'files', 'bucket'], response.data)
        self.assertEqual(response.data['count'], 1)
        self.assertKeysIn(['na', 'name', 'fod', 'did', 'si', 'ult', 'upt', 'dlc',
                           'download_url', 'access_permission', 'access_code', 'md5'], response.data['files'][0])
        self.assertDictIsSubDict(response.data['files'][0], {
            'na': key2, 'name': key2, 'fod': True, 'did': 0, 'si': 0
        })

        query = parse.urlencode({'bucket': self.bucket_name, 'search': 'ab'})
        response = self.client.get(f'{url}?{query}')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['count'], 2)

        query = parse.urlencode({'bucket': self.bucket_name, 'search': 'abc'})
        response = self.client.get(f'{url}?{query}')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['count'], 0)

    def tearDown(self):
        r = BucketsAPITests.delete_bucket(self.client, self.bucket_name)
        self.assertEqual(r.status_code, 204)
        BucketsAPITests.clear_bucket_archive(self.bucket_name)


class StatsAPITests(MyAPITransactionTestCase):
    databases = {'default', 'metadata'}

    def setUp(self):
        settings.BUCKET_LIMIT_DEFAULT = 2
        config_ceph_clustar_settings()
        self.user_password = 'password'
        user: UserProfile = get_or_create_user(password=self.user_password)
        self.user = user
        set_token_auth_header(self, username=self.user.username, password=self.user_password)

        self.bucket_name = 'test'
        r = BucketsAPITests.create_bucket(self.client, self.bucket_name)
        self.assertEqual(r.status_code, 201)
        self.bucket = Bucket.objects.filter(id=r.data['bucket']['id']).first()

        file = random_bytes_io(mb_num=6)
        self.key = 'object.key'
        response = ObjectsAPITests.put_object_response(self.client, bucket_name=self.bucket_name, key=self.key,
                                                       file=file)
        self.assertEqual(response.status_code, 200)

    def set_bucket_stat_time(self):
        self.bucket.stats_time = timezone.now() - timedelta(days=1)
        self.bucket.save(update_fields=['stats_time'])

    def test_stats_ceph(self):
        self.user.is_superuser = True
        self.user.save()
        url = reverse('api:stats_ceph-list')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['kb', 'kb_used', 'kb_avail', 'num_objects'], response.data['stats'])

    def test_stats_user(self):
        MetadataAPITests.create_empty_object_metadata(
            self, bucket_name=self.bucket_name, key='test', check_response=True
        )

        self.set_bucket_stat_time()
        # 获取当前用户的统计信息
        url = reverse('api:stats_user-list')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['space', 'count', 'buckets'], response.data)
        self.assertEqual(response.data['space'], 6*1024**2)
        self.assertEqual(response.data['count'], 2)
        self.assertKeysIn(['stats', 'stats_time', 'bucket_name'], response.data['buckets'][0])
        bucket_stats = response.data['buckets'][0]
        self.assertEqual(bucket_stats['stats']['space'], 6 * 1024 ** 2)
        self.assertEqual(bucket_stats['stats']['count'], 2)
        self.assertEqual(bucket_stats['bucket_name'], self.bucket_name)

        # 超级用户获取指定用户的统计信息
        url = reverse('api:stats_user-detail', kwargs={'username': self.user.username})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 403)

        # 超级用户权限设置
        self.user.is_superuser = True
        self.user.save()

        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['space', 'count', 'buckets'], response.data)
        self.assertEqual(response.data['space'], 6 * 1024 ** 2)
        self.assertEqual(response.data['count'], 2)
        self.assertKeysIn(['stats', 'stats_time', 'bucket_name'], response.data['buckets'][0])
        bucket_stats = response.data['buckets'][0]
        self.assertEqual(bucket_stats['stats']['space'], 6 * 1024 ** 2)
        self.assertEqual(bucket_stats['stats']['count'], 2)
        self.assertEqual(bucket_stats['bucket_name'], self.bucket_name)

    def test_stats_bucket(self):
        url = reverse('api:stats_bucket-detail', kwargs={'bucket_name': 'not-found'})
        response = self.client.get(url)
        self.assertOldErrorResponse(status_code=404, code='NoSuchBucket', response=response)

        self.set_bucket_stat_time()
        url = reverse('api:stats_bucket-detail', kwargs={'bucket_name': self.bucket_name})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['stats', 'stats_time', 'bucket_name', 'user_id', 'username'], response.data)
        self.assertEqual(response.data['stats']['space'], 6 * 1024 ** 2)
        self.assertEqual(response.data['stats']['count'], 1)
        self.assertEqual(response.data['bucket_name'], self.bucket_name)
        self.assertEqual(response.data['user_id'], self.user.id)
        self.assertEqual(response.data['username'], self.user.username)

    def tearDown(self):
        response = ObjectsAPITests.delete_object_response(self.client, bucket_name=self.bucket_name, key=self.key)
        self.assertEqual(response.status_code, 204)

        r = BucketsAPITests.delete_bucket(self.client, self.bucket_name)
        self.assertEqual(r.status_code, 204)
        BucketsAPITests.clear_bucket_archive(self.bucket_name)


class UserAPITests(MyAPITransactionTestCase):
    def setUp(self):
        self.user_password = 'password'
        user = get_or_create_user(password=self.user_password)
        self.user = user
        set_token_auth_header(self, username=self.user.username, password=self.user_password)
        self.target_user = get_or_create_user(username='target_user@test.com', password=self.user_password)

    def test_user_count(self):
        # 超级用户权限设置
        self.user.is_superuser = True
        self.user.save()

        url = reverse('api:usercount-list')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['count'], 2)

    def test_list_user(self):
        url = reverse('api:user-list')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 403)

        # 超级用户权限设置
        self.user.is_superuser = True
        self.user.save()

        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['count', 'next', 'previous', 'results'], response.data)
        self.assertIsInstance(response.data['results'], list)
        self.assertEqual(response.data['count'], 2)
        self.assertKeysIn(['id', 'username', 'email', 'date_joined',
                           'last_login', 'first_name', 'last_name', 'is_active',
                           'telephone', 'company'], response.data['results'][0])

    def test_user_detail(self):
        url = reverse('api:user-detail', kwargs={'username': self.target_user.username})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 403)

        # 超级用户权限设置
        self.user.is_superuser = True
        self.user.is_staff = True
        self.user.save()

        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['id', 'username', 'email', 'date_joined',
                           'last_login', 'first_name', 'last_name', 'is_active',
                           'telephone', 'company'], response.data)

    def test_user_modify(self):
        url = reverse('api:user-detail', kwargs={'username': self.target_user.username})
        data = {
            "is_active": True,
            "password": "new_password",
            "first_name": "tom",
            "last_name": "jerry",
            "telephone": "110",
            "company": "cnic"
        }
        response = self.client.patch(url, data=data)
        self.assertEqual(response.status_code, 403)

        # 超级用户权限设置
        self.user.is_superuser = True
        self.user.is_staff = True
        self.user.save()

        response = self.client.patch(url, data=data)
        self.assertEqual(response.status_code, 200)

        url = reverse('api:user-detail', kwargs={'username': self.target_user.username})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['id', 'username', 'email', 'date_joined',
                           'last_login', 'first_name', 'last_name', 'is_active',
                           'telephone', 'company'], response.data)
        self.assertDictIsSubDict(response.data, {
            'username': self.target_user.username,
            "first_name": "tom",
            "last_name": "jerry",
            "telephone": "110",
            "company": "cnic",
            "is_active": True
        })
        self.target_user.refresh_from_db()
        self.assertEqual(self.target_user.check_password('new_password'), True)

    def test_add_user(self):
        url = reverse('api:user-list')
        data = {
            "username": "user@example.com",
            "password": "new-string",
            "last_name": "string",
            "first_name": "string",
            "telephone": "string",
            "company": "string"
        }
        response = self.client.post(url, data=data)
        self.assertEqual(response.status_code, 201)

    def test_delete_user(self):
        url = reverse('api:user-detail', kwargs={'username': self.target_user.username})
        response = self.client.delete(url)
        self.assertEqual(response.status_code, 403)

        # 超级用户权限设置
        self.user.is_superuser = True
        self.user.is_staff = True
        self.user.save()

        response = self.client.delete(url)
        self.assertEqual(response.status_code, 204)
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['is_active'], False)

    def test_user_security(self):
        url = reverse('api:security-detail', kwargs={'username': self.target_user.username})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 403)

        # 超级用户权限设置
        self.user.is_superuser = True
        self.user.save()

        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['user', 'token', 'jwt'], response.data)
        self.assertNotIn('keys', response.data)

        # keys
        response = self.client.get(f'{url}?key')
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['user', 'token', 'jwt', 'keys'], response.data)
        self.assertEqual(0, len(response.data['keys']))

        self.client.logout()
        r = AuthKeyAPITests.create_user_auth_key(self.client, self.target_user.username, self.user_password)
        self.assertEqual(r.status_code, 201)

        set_token_auth_header(self, username=self.user.username, password=self.user_password)
        response = self.client.get(f'{url}?key')
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['user', 'token', 'jwt', 'keys'], response.data)
        self.assertEqual(1, len(response.data['keys']))
        self.assertKeysIn(['user', 'access_key', 'secret_key', 'create_time',
                           'state', 'permission'], response.data['keys'][0])


class ListBucketObjectsAPITests(MyAPITransactionTestCase):
    databases = {'default', 'metadata'}

    def setUp(self):
        settings.BUCKET_LIMIT_DEFAULT = 2
        config_ceph_clustar_settings()
        self.user_password = 'password'
        user: UserProfile = get_or_create_user(password=self.user_password)
        self.user = user
        set_token_auth_header(self, username=self.user.username, password=self.user_password)

        self.bucket_name = 'test'
        r = BucketsAPITests.create_bucket(self.client, self.bucket_name)
        self.assertEqual(r.status_code, 201)

    def test_search_objects_in_bucket(self):
        prefix = 'gg'
        key1 = f'{prefix}/ab.txt'
        key2 = 'ABCD.txt'
        file = random_bytes_io(mb_num=1)
        r = DirAPITests.create_dir_response(self.client, bucket_name=self.bucket_name, dirpath=prefix)
        self.assertEqual(r.status_code, 201)

        MetadataAPITests.create_empty_object_metadata(
            self, bucket_name=self.bucket_name, key=key1, check_response=True
        )

        response = ObjectsAPITests.put_object_response(self.client, bucket_name=self.bucket_name, key=key2, file=file)
        self.assertEqual(response.status_code, 200)

        # list bucket all
        base_url = reverse('api:list-bucket-detail', kwargs={'bucket_name': self.bucket_name})
        response = self.client.get(base_url)
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['Name', 'Prefix', 'IsTruncated', 'MaxKeys', 'KeyCount',
                           'Next', 'Previous', 'Contents'], response.data)
        self.assertDictIsSubDict(response.data, {
            'Name': self.bucket_name, 'Prefix': '', 'IsTruncated': False,
            'MaxKeys': 1000, 'KeyCount': 3
        })
        self.assertKeysIn(['Key', 'LastModified', 'ETag', 'Size', 'IsObject'],
                          response.data['Contents'][0])

        # list bucket all ?prefix
        query = parse.urlencode({'prefix': prefix})
        response = self.client.get(f'{base_url}?{query}')
        self.assertEqual(response.status_code, 200)
        self.assertDictIsSubDict(response.data, {
            'Name': self.bucket_name, 'Prefix': prefix, 'IsTruncated': False,
            'MaxKeys': 1000, 'KeyCount': 2
        })
        for o in response.data['Contents']:
            self.assertIn(o['Key'], [prefix, key1])

        # list bucket base dir
        query = parse.urlencode({'prefix': '', 'delimiter': '/'})
        response = self.client.get(f'{base_url}?{query}')
        self.assertEqual(response.status_code, 200)
        self.assertDictIsSubDict(response.data, {
            'Name': self.bucket_name, 'Prefix': '', 'IsTruncated': False,
            'MaxKeys': 1000, 'KeyCount': 2, 'Delimiter': '/'
        })
        obj1 = response.data['Contents'][0]
        if obj1['Key'] == prefix:
            self.assertKeysIn(['Key', 'LastModified', 'ETag', 'Size', 'IsObject'], obj1)
            self.assertDictIsSubDict(obj1, {
                'Key': prefix, 'ETag': EMPTY_HEX_MD5, 'Size': 0,
                'IsObject': False
            })
        elif obj1['Key'] == key2:
            self.assertKeysIn(['Key', 'LastModified', 'ETag', 'Size', 'IsObject'], obj1)
            self.assertDictIsSubDict(obj1, {
                'Key': key2, 'Size': 1024*1024, 'IsObject': True
            })

        # list bucket base dir ?max-keys & continuation-token
        query = parse.urlencode({'prefix': '', 'delimiter': '/', 'max-keys': 1})
        response = self.client.get(f'{base_url}?{query}')
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['Name', 'Prefix', 'IsTruncated', 'MaxKeys', 'KeyCount',
                           'Next', 'Previous', 'Contents', 'NextContinuationToken'], response.data)
        self.assertDictIsSubDict(response.data, {
            'Name': self.bucket_name, 'Prefix': '', 'IsTruncated': True,
            'MaxKeys': 1, 'KeyCount': 1, 'Delimiter': '/'
        })
        obj1 = response.data['Contents'][0]
        next_token = response.data['NextContinuationToken']
        query = parse.urlencode({'prefis': '', 'delimiter': '/', 'max-keys': 1,
                                 'continuation-token': next_token})
        response = self.client.get(f'{base_url}?{query}')
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['Name', 'Prefix', 'IsTruncated', 'MaxKeys', 'KeyCount',
                           'Next', 'Previous', 'Contents', 'ContinuationToken'], response.data)
        self.assertDictIsSubDict(response.data, {
            'Name': self.bucket_name, 'Prefix': '', 'IsTruncated': False,
            'MaxKeys': 1, 'KeyCount': 1, 'Delimiter': '/', 'ContinuationToken': next_token
        })
        obj2 = response.data['Contents'][0]
        if obj1['Key'] == prefix:
            self.assertEqual(obj2['Key'], key2)
        elif obj1['Key'] == key2:
            self.assertEqual(obj2['Key'], prefix)
        else:
            self.fail('Failed to list bucket base dir ?max-keys & continuation-token')

        # list bucket sub dir
        query = parse.urlencode({'prefix': prefix, 'delimiter': '/'})
        response = self.client.get(f'{base_url}?{query}')
        self.assertEqual(response.status_code, 200)
        self.assertDictIsSubDict(response.data, {
            'Name': self.bucket_name, 'Prefix': prefix, 'IsTruncated': False,
            'MaxKeys': 1000, 'KeyCount': 1, 'Delimiter': '/'
        })
        self.assertDictIsSubDict(response.data['Contents'][0], {
            'Key': key1, 'ETag': EMPTY_HEX_MD5, 'Size': 0,
            'IsObject': True
        })

        # list bucket not exists dir
        query = parse.urlencode({'prefix': 'test', 'delimiter': '/'})
        response = self.client.get(f'{base_url}?{query}')
        self.assertEqual(response.status_code, 404)
        self.assertErrorResponse(status_code=404, code='NoSuchKey', response=response)

    def tearDown(self):
        r = BucketsAPITests.delete_bucket(self.client, self.bucket_name)
        self.assertEqual(r.status_code, 204)
        BucketsAPITests.clear_bucket_archive(self.bucket_name)


class ShareAPITests(MyAPITransactionTestCase):
    databases = {'default', 'metadata'}

    def setUp(self):
        settings.BUCKET_LIMIT_DEFAULT = 2
        config_ceph_clustar_settings()
        self.user_password = 'password'
        user = get_or_create_user(password=self.user_password)
        self.user = user
        set_token_auth_header(self, username=self.user.username, password=self.user_password)

        self.bucket_name = 'test'
        response = BucketsAPITests.create_bucket(self.client, self.bucket_name)
        self.assertEqual(response.status_code, 201, 'create bucket failed')

    def test_list_share_dir(self):
        bucket_name = self.bucket_name
        file = random_bytes_io(mb_num=6)
        file_md5 = calculate_md5(file)
        parent_dir = '父目录名'
        sub_dir = '子目录名'
        sub_dir_path = f'{parent_dir}/{sub_dir}'
        object_name = 'test.pdf'
        key = f'{sub_dir_path}/{object_name}'

        set_token_auth_header(self, username=self.user.username, password=self.user_password)
        response = DirAPITests.create_dir_response(self.client, bucket_name, dirpath=parent_dir)
        self.assertEqual(response.status_code, 201)

        response = DirAPITests.create_dir_response(self.client, bucket_name, dirpath=sub_dir_path)
        self.assertEqual(response.status_code, 201)

        response = ObjectsAPITests.put_object_response(self.client, bucket_name=self.bucket_name, key=key, file=file)
        self.assertEqual(response.status_code, 200)

        # list share dir
        self.client.logout()
        list_base_url = reverse('share:list-detail', kwargs={'share_base': f'{bucket_name}/{parent_dir}'})
        response = self.client.get(list_base_url)
        self.assertEqual(response.status_code, 403)

        # set permission
        set_token_auth_header(self, username=self.user.username, password=self.user_password)
        share_password = 'password'
        url = reverse('api:dir-detail', kwargs={'bucket_name': bucket_name, 'dirpath': parent_dir})
        query = parse.urlencode({'share': BucketFileBase.SHARE_ACCESS_READONLY, 'days': 6, 'password': share_password})
        response = self.client.patch(f'{url}?{query}')
        self.assertEqual(response.status_code, 200)

        self.client.logout()
        # list share dir
        list_base_url = reverse('share:list-detail', kwargs={'share_base': f'{bucket_name}/{parent_dir}'})
        query = parse.urlencode({'p': share_password})
        response = self.client.get(f'{list_base_url}?{query}')
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['bucket_name', 'subpath', 'share_base', 'files', 'count',
                           'next', 'previous', 'page'], response.data)
        self.assertDictIsSubDict(response.data, {
            'bucket_name': self.bucket_name, 'subpath': '',
            'share_base': f'{self.bucket_name}/{parent_dir}',
            'count': 1
        })
        self.assertDictIsSubDict(response.data['files'][0], {
            'na': sub_dir_path, 'name': sub_dir, 'fod': False, 'si': 0
        })

        query = parse.urlencode({'subpath': sub_dir, 'p': share_password})
        response = self.client.get(f'{list_base_url}?{query}')
        self.assertEqual(response.status_code, 200)
        self.assertKeysIn(['bucket_name', 'subpath', 'share_base', 'files', 'count',
                           'next', 'previous', 'page'], response.data)
        self.assertDictIsSubDict(response.data, {
            'bucket_name': self.bucket_name, 'subpath': sub_dir,
            'share_base': f'{self.bucket_name}/{parent_dir}',
            'count': 1
        })
        self.assertDictIsSubDict(response.data['files'][0], {
            'na': key, 'name': object_name, 'fod': True, 'si': 6*1024*1024
        })

        # download share
        url = reverse('share:download-detail', kwargs={'share_base': f'{bucket_name}/{parent_dir}'})
        query = parse.urlencode({'subpath': f'{sub_dir}/{object_name}', 'p': share_password})
        response = self.client.get(f'{url}?{query}')
        self.assertEqual(response.status_code, 200)
        download_md5 = calculate_md5(response)
        self.assertEqual(download_md5, file_md5, msg='Compare the MD5 of upload file and download file')

        # download share range
        url = reverse('share:download-detail', kwargs={'share_base': f'{bucket_name}/{parent_dir}'})
        query = parse.urlencode({'subpath': f'{sub_dir}/{object_name}', 'p': share_password})
        response = self.client.get(f'{url}?{query}', HTTP_RANGE='bytes=1-10')
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.headers.get('content-length'), '10')

    def tearDown(self):
        set_token_auth_header(self, username=self.user.username, password=self.user_password)
        # delete bucket
        response = BucketsAPITests.delete_bucket(self.client, self.bucket_name)
        self.assertEqual(response.status_code, 204)
        BucketsAPITests.clear_bucket_archive(self.bucket_name)
