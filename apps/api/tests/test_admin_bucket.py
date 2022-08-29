
from urllib import parse

from django.conf import settings
from django.urls import reverse

from buckets.models import Bucket, BackupBucket
from buckets.utils import create_bucket, delete_table_for_model_class, BucketFileManagement
from users.models import UserProfile
from .tests import (
    get_or_create_user, set_token_auth_header, MyAPITransactionTestCase
)
from . import tests


class BackupBucketAPITests(MyAPITransactionTestCase):
    databases = {'default', 'metadata'}

    def setUp(self):
        # 创建test用户
        self.user_password = 'password'
        self.user_username = 'test'
        self.user = get_or_create_user(username=self.user_username, password=self.user_password)
        set_token_auth_header(self, username=self.user.username, password=self.user_password)

    def test_admin_create_bucket(self):
        url = reverse('api:admin-bucket-list')

        # AccessDenied
        r = self.client.post(url, data={'name': '', 'username': 'user1'})
        self.assertErrorResponse(status_code=403, code='AccessDenied', response=r)
        self.user.is_superuser = True
        self.user.save(update_fields=['is_superuser'])
        r = self.client.post(url, data={'name': '', 'username': 'user1'})
        self.assertErrorResponse(status_code=403, code='AccessDenied', response=r)

        self.user.is_superuser = False
        self.user.role = UserProfile.ROLE_APP_SUPPER_USER
        self.user.save(update_fields=['is_superuser', 'role'])
        r = self.client.post(url, data={'name': '', 'username': 'user1'})
        self.assertErrorResponse(status_code=403, code='AccessDenied', response=r)

        # invalid bucket name
        self.user.is_superuser = True
        self.user.role = UserProfile.ROLE_APP_SUPPER_USER
        self.user.save(update_fields=['is_superuser', 'role'])

        r = self.client.post(url, data={'name': '', 'username': 'test'})
        self.assertErrorResponse(status_code=400, code='InvalidBucketName', response=r)
        r = self.client.post(url, data={'name': 'aa', 'username': 'test'})
        self.assertErrorResponse(status_code=400, code='InvalidBucketName', response=r)
        r = self.client.post(url, data={'name': 'abc-', 'username': 'test'})
        self.assertErrorResponse(status_code=400, code='InvalidBucketName', response=r)
        r = self.client.post(url, data={'name': '-abc', 'username': 'test'})
        self.assertErrorResponse(status_code=400, code='InvalidBucketName', response=r)
        r = self.client.post(url, data={'name': 'a1_bc', 'username': 'test'})
        self.assertErrorResponse(status_code=400, code='InvalidBucketName', response=r)

        # invalid username
        r = self.client.post(url, data={'name': 'abc1', 'username': ''})
        self.assertErrorResponse(status_code=400, code='InvalidUsername', response=r)

        # ok
        username = 'user1'
        bucket_name = 'abc1'
        u1 = UserProfile.objects.filter(username=username).first()
        self.assertIsNone(u1)
        r = self.client.post(url, data={'name': bucket_name, 'username': username})
        self.assertEqual(r.status_code, 200)
        u1 = UserProfile.objects.filter(username=username).first()
        self.assertIsNotNone(u1)
        bucket1 = Bucket.objects.filter(name=bucket_name).first()
        self.assertEqual(bucket1.name, bucket_name)

        # Bucket Already Exists
        r = self.client.post(url, data={'name': bucket_name, 'username': username})
        self.assertErrorResponse(status_code=409, code='BucketAlreadyExists', response=r)

        # clear bucket
        bucket1.delete_and_archive()
        tests.BucketsAPITests.clear_bucket_archive(bucket_name)
