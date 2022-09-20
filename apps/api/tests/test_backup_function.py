import requests

from django.urls import reverse
from django.conf import settings
from django.utils import timezone

from buckets.models import Bucket, BackupBucket
from buckets.utils import BucketFileManagement
from api.backup import AsyncBucketManager
from . import tests, config_ceph_clustar_settings
from .tests import (
    random_bytes_io, calculate_md5
)


class BackupFunctionTests(tests.MyAPITransactionTestCase):
    databases = {'default', 'metadata'}

    def setUp(self):
        settings.BUCKET_LIMIT_DEFAULT = 2
        config_ceph_clustar_settings()
        self.user_password = 'password'
        user = tests.get_or_create_user(password=self.user_password)
        self.user = user
        tests.set_token_auth_header(self, username=self.user.username, password=self.user_password)

        self.bucket_name = 'test'
        response = tests.BucketsAPITests.create_bucket(self.client, self.bucket_name)
        self.assertEqual(response.status_code, 201, 'create bucket failed')
        self.bucket = Bucket.objects.get(id=response.data['bucket']['id'])

    def test_get_need_async_bucket_queryset(self):
        bucket1 = Bucket(name='test1')
        bucket1.save()
        bucket2 = Bucket(name='test2')
        bucket2.save()
        bucket3 = Bucket(name='test3')
        bucket3.save()
        BackupBucket(bucket=bucket1, endpoint_url='https://exemple.com',
                     bucket_token='token', status=BackupBucket.Status.START,
                     bucket_name='backup1', backup_num=1).save()

        BackupBucket(bucket=bucket2, endpoint_url='https://exemple.com',
                     bucket_token='token', status=BackupBucket.Status.STOP,
                     bucket_name='backup2', backup_num=1).save()
        BackupBucket(bucket=bucket3, endpoint_url='https://exemple.com',
                     bucket_token='token', status=BackupBucket.Status.START,
                     bucket_name='backup1', backup_num=1).save()

        abm = AsyncBucketManager()
        qs = abm.get_need_async_bucket_queryset()
        self.assertEqual(len(qs), 2)

        BackupBucket(bucket=bucket2, endpoint_url='https://exemple.com',
                     bucket_token='token', status=BackupBucket.Status.START,
                     bucket_name='backup2', backup_num=2).save()
        qs = abm.get_need_async_bucket_queryset()
        self.assertEqual(len(qs), 3)

        qs = abm.get_need_async_bucket_queryset(limit=1)
        self.assertEqual(len(qs), 1)
        self.assertEqual(qs[0].id, bucket1.id)
        qs = abm.get_need_async_bucket_queryset(id_gt=qs[0].id, limit=1)
        self.assertEqual(len(qs), 1)
        self.assertEqual(qs[0].id, bucket2.id)
        qs = abm.get_need_async_bucket_queryset(id_gt=qs[0].id, limit=1)
        self.assertEqual(len(qs), 1)
        self.assertEqual(qs[0].id, bucket3.id)

    @staticmethod
    def create_object(bucket, key: str, size: int = 0, is_dir=False):
        table_name = bucket.get_bucket_table_name()
        bfm = BucketFileManagement(collection_name=table_name)
        object_class = bfm.get_obj_model_class()
        name = key.split('/', maxsplit=1)[-1]
        obj = object_class(na=key, name=name, fod=not is_dir, si=size)
        obj.save()
        return obj

    def test_get_need_async_objects_queryset(self):
        bucket = self.bucket
        obj1 = self.create_object(bucket=bucket, key='obj1')
        obj2 = self.create_object(bucket=bucket, key='obj2')
        self.create_object(bucket=bucket, key='dir1', is_dir=True)

        abm = AsyncBucketManager()
        meet_time = timezone.now()
        qs = abm.get_need_async_objects_queryset(bucket=bucket, meet_time=meet_time, id_gt=0)
        self.assertEqual(len(qs), 0)

        backup1 = BackupBucket(
            bucket=bucket, endpoint_url='https://exemple.com',
            bucket_token='token', status=BackupBucket.Status.START,
            bucket_name='backup1', backup_num=1)
        backup1.save()
        qs = abm.get_need_async_objects_queryset(bucket=bucket, meet_time=meet_time, id_gt=0)
        self.assertEqual(len(qs), 2)

        # test now - obj.upt > timedelta(minutes=meet_async_timedelta_minutes)
        qs = abm.get_need_async_objects_queryset(bucket=bucket, id_gt=0)
        self.assertEqual(len(qs), 0)

        # test param id_mod_div, id_mod_equal
        qs = abm.get_need_async_objects_queryset(bucket=bucket, limit=2, meet_time=meet_time, id_gt=0)
        self.assertEqual(len(qs), 2)
        id_mod_div = 1
        id_mod_equal = 0
        qs = abm.get_need_async_objects_queryset(bucket=bucket, limit=2, meet_time=meet_time, id_gt=0,
                                                 id_mod_div=id_mod_div, id_mod_equal=id_mod_equal)
        self.assertEqual(len(qs), 2)

        id_mod_div = 2
        qs = abm.get_need_async_objects_queryset(bucket=bucket, limit=2, meet_time=meet_time, id_gt=0,
                                                 id_mod_div=id_mod_div, id_mod_equal=id_mod_equal)
        self.assertEqual(len(qs), 1)
        for o in qs:
            self.assertEqual(o.id % id_mod_div, id_mod_equal)

        # test param backup_num
        qs = abm.get_need_async_objects_queryset(bucket=bucket, limit=2, meet_time=meet_time, backup_num=1, id_gt=0)
        self.assertEqual(len(qs), 2)
        qs = abm.get_need_async_objects_queryset(bucket=bucket, limit=2, meet_time=meet_time, backup_num=2, id_gt=0)
        self.assertEqual(len(qs), 2)

        # test param in_gt
        qs = abm.get_need_async_objects_queryset(bucket=bucket, limit=1, meet_time=meet_time, id_gt=0)
        self.assertEqual(len(qs), 1)
        self.assertEqual(qs[0].id, obj1.id)
        qs = abm.get_need_async_objects_queryset(bucket=bucket, id_gt=qs[0].id, limit=1, meet_time=meet_time)
        self.assertEqual(len(qs), 1)
        self.assertEqual(qs[0].id, obj2.id)

        obj1.async1 = timezone.now()
        obj1.save(update_fields=['async1'])
        qs = abm.get_need_async_objects_queryset(bucket=bucket, meet_time=meet_time, id_gt=0)
        self.assertEqual(len(qs), 1)
        self.assertEqual(qs[0].id, obj2.id)

        # test param backup_num
        qs = abm.get_need_async_objects_queryset(bucket=bucket, limit=2, meet_time=meet_time, backup_num=1, id_gt=0)
        self.assertEqual(len(qs), 1)
        qs = abm.get_need_async_objects_queryset(bucket=bucket, limit=2, meet_time=meet_time, backup_num=2, id_gt=0)
        self.assertEqual(len(qs), 2)

        # obj1.upt = timezone.now()
        obj1.save(update_fields=['upt'])    # upt会自动更新到now time
        meet_time = timezone.now()
        qs = abm.get_need_async_objects_queryset(bucket=bucket, meet_time=meet_time, id_gt=0)
        self.assertEqual(len(qs), 2)

        backup1.status = backup1.Status.STOP
        backup1.save(update_fields=['status'])
        qs = abm.get_need_async_objects_queryset(bucket=bucket, meet_time=meet_time, id_gt=0)
        self.assertEqual(len(qs), 0)

        backup2 = BackupBucket(bucket=bucket, endpoint_url='https://exemple.com',
                               bucket_token='token', status=BackupBucket.Status.START,
                               bucket_name='backup2', backup_num=2)
        backup2.save()
        qs = abm.get_need_async_objects_queryset(bucket=bucket, meet_time=meet_time, id_gt=0)
        self.assertEqual(len(qs), 2)

        # only obj2 need to backup2
        backup1.status = backup1.Status.START
        backup1.save(update_fields=['status'])
        obj1.async1 = timezone.now()
        obj1.save(update_fields=['async1'])
        obj2.async1 = timezone.now()
        obj2.save(update_fields=['async1'])

        obj1.async2 = timezone.now()
        obj1.save(update_fields=['async2'])
        qs = abm.get_need_async_objects_queryset(bucket=bucket, meet_time=meet_time, id_gt=0)
        self.assertEqual(len(qs), 1)
        self.assertEqual(qs[0].id, obj2.id)

        # obj1 need to backup1, obj2 need to backup2
        # obj1.upt = timezone.now()           # 不需要
        obj1.save(update_fields=['upt'])    # upt会自动更新到now time
        meet_time = timezone.now()
        qs = abm.get_need_async_objects_queryset(bucket=bucket, meet_time=meet_time, id_gt=0)
        self.assertEqual(len(qs), 2)

    def test_other(self):
        bucket = self.bucket
        obj1 = self.create_object(bucket=bucket, key='obj1', size=6)

        abm = AsyncBucketManager()
        b = abm.get_bucket_by_id(bucket_id=bucket.id)
        self.assertEqual(b.id, bucket.id)
        o = abm.get_object_by_id(bucket=b, object_id=obj1.id)
        self.assertEqual(o.id, obj1.id)
        ho = abm.get_object_ceph_rados(bucket=b, obj=o)
        ho.read(offset=0, size=2)

    @staticmethod
    def create_usefull_backup(bucket):
        backup_settings = settings.TEST_CASE['BACKUP_BUCKET']
        endpoint_url = backup_settings['endpoint_url']
        bucket_token = backup_settings['bucket_token']
        bucket_name = backup_settings['bucket_name']
        backup = BackupBucket(
            bucket=bucket, endpoint_url=endpoint_url,
            bucket_token=bucket_token, status=BackupBucket.Status.START,
            bucket_name=bucket_name, backup_num=1)

        try:
            backup.save()
        except Exception as e:
            raise e

        return backup

    def test_async_object(self):
        bucket = self.bucket
        backup = self.create_usefull_backup(bucket=bucket)

        file = random_bytes_io(mb_num=33)
        file_md5 = calculate_md5(file)
        print(f'file md5: {file_md5}')
        key = 'a/b/c/te st.pdf#'
        key2 = 'a/b/c/分片/#tes t.txt#'

        response = tests.ObjectsAPITests().put_object_response(
            self.client, bucket_name=self.bucket_name, key=key, file=file)
        self.assertEqual(response.status_code, 200)

        ok = tests.ObjectsAPITests().multipart_upload_object(
            client=self.client, bucket_name=self.bucket_name, key=key2, file=file)
        self.assertTrue(ok, 'multipart_upload_object failed')

        abm = AsyncBucketManager()

        """同步两个对象 test"""
        abm.MEET_ASYNC_TIMEDELTA_MINUTES = 0
        qs = abm.get_need_async_objects_queryset(bucket=bucket, id_gt=0)
        self.assertEqual(len(qs), 2)
        obj_key1 = None
        for obj in qs:
            abm.async_object(bucket_id=bucket.id, bucket_name=bucket.name, object_id=obj.id, object_key=obj.na)
            if obj.na == key:
                obj_key1 = obj

        url = reverse('api:obj-detail', kwargs={'bucket_name': backup.bucket_name, 'objpath': key})
        api = f'{backup.endpoint_url.rstrip("/")}/{url.lstrip("/")}'
        response = requests.get(api, headers={'Authorization': f'BucketToken {backup.bucket_token}'})
        download_md5 = calculate_md5(response)
        self.assertEqual(download_md5, file_md5, msg='Compare the MD5 of async object and download object')

        url = reverse('api:obj-detail', kwargs={'bucket_name': backup.bucket_name, 'objpath': key2})
        api = f'{backup.endpoint_url.rstrip("/")}/{url.lstrip("/")}'
        response = requests.get(api, headers={'Authorization': f'BucketToken {backup.bucket_token}'})
        download_md5 = calculate_md5(response)
        self.assertEqual(download_md5, file_md5, msg='Compare the MD5 of async object and download object')

        """test md5 invalid"""
        # from target delete object
        url = reverse('api:obj-detail', kwargs={'bucket_name': backup.bucket_name, 'objpath': key})
        api = f'{backup.endpoint_url.rstrip("/")}/{url.lstrip("/")}'
        response = requests.delete(api, headers={'Authorization': f'BucketToken {backup.bucket_token}'})
        self.assertEqual(response.status_code, 204)

        # set invalid md5 for source object
        obj_key1.async1 = None
        obj_key1.md5 = 'md5test'
        obj_key1.save(update_fields=['async1', 'md5'])
        # PutObject失败后，同步会尝试分片上传
        abm.async_object(bucket_id=bucket.id, bucket_name=bucket.name, object_id=obj_key1.id, object_key=obj_key1.na)
        url = reverse('api:obj-detail', kwargs={'bucket_name': backup.bucket_name, 'objpath': key})
        api = f'{backup.endpoint_url.rstrip("/")}/{url.lstrip("/")}'
        response = requests.get(api, headers={'Authorization': f'BucketToken {backup.bucket_token}'})
        download_md5 = calculate_md5(response)
        self.assertEqual(download_md5, file_md5, msg='Compare the MD5 of async object and download object')

        """async when object2 update time modified"""
        # 对象刚修改未超过MEET_ASYNC_TIMEDELTA_MINUTES时间，不会同步
        file = random_bytes_io(mb_num=34)
        file_md5 = calculate_md5(file)
        print(f'file md5: {file_md5}')
        ok = tests.ObjectsAPITests().multipart_upload_object(
            client=self.client, bucket_name=self.bucket_name, key=key2, file=file)
        self.assertTrue(ok, 'multipart_upload_object failed')

        abm.MEET_ASYNC_TIMEDELTA_MINUTES = 0    # 保证查到对象
        qs2 = abm.get_need_async_objects_queryset(bucket=bucket, id_gt=0)
        self.assertEqual(len(qs2), 1)
        obj2 = qs2[0]
        self.assertEqual(obj2.na, key2)
        abm.MEET_ASYNC_TIMEDELTA_MINUTES = 60   # 超过MEET_ASYNC_TIMEDELTA_MINUTES时间才会同步
        abm.async_object(bucket_id=bucket.id, bucket_name=bucket.name, object_id=obj2.id, object_key=obj2.na)

        url = reverse('api:obj-detail', kwargs={'bucket_name': backup.bucket_name, 'objpath': key2})
        api = f'{backup.endpoint_url.rstrip("/")}/{url.lstrip("/")}'
        response = requests.get(api, headers={'Authorization': f'BucketToken {backup.bucket_token}'})
        download_md5 = calculate_md5(response)
        self.assertNotEqual(download_md5, file_md5, msg='Compare the MD5 of async object and download object')

        # 对象刚修改，超过MEET_ASYNC_TIMEDELTA_MINUTES = 0 时间，会同步
        abm.MEET_ASYNC_TIMEDELTA_MINUTES = 0
        abm.async_object(bucket_id=bucket.id, bucket_name=bucket.name, object_id=obj2.id, object_key=obj2.na)
        url = reverse('api:obj-detail', kwargs={'bucket_name': backup.bucket_name, 'objpath': key2})
        api = f'{backup.endpoint_url.rstrip("/")}/{url.lstrip("/")}'
        response = requests.get(api, headers={'Authorization': f'BucketToken {backup.bucket_token}'})
        download_md5 = calculate_md5(response)
        self.assertEqual(download_md5, file_md5, msg='Compare the MD5 of async object and download object')

        """test delete async"""
        r = tests.ObjectsAPITests().delete_object_response(client=self.client, bucket_name=bucket.name, key=key)
        self.assertEqual(r.status_code, 204)
        for obj in qs:
            abm.async_delete_object(bucket_id=bucket.id, bucket_name=bucket.name, object_key=obj.na)

        url = reverse('api:obj-detail', kwargs={'bucket_name': backup.bucket_name, 'objpath': key})
        api = f'{backup.endpoint_url.rstrip("/")}/{url.lstrip("/")}'
        response = requests.get(api, headers={'Authorization': f'BucketToken {backup.bucket_token}'})
        self.assertEqual(response.status_code, 404)

        url = reverse('api:obj-detail', kwargs={'bucket_name': backup.bucket_name, 'objpath': key2})
        api = f'{backup.endpoint_url.rstrip("/")}/{url.lstrip("/")}'
        response = requests.get(api, headers={'Authorization': f'BucketToken {backup.bucket_token}'})
        self.assertEqual(response.status_code, 200)

    def test_async_empty_object(self):
        # test empty object
        bucket = self.bucket
        backup = self.create_usefull_backup(bucket=bucket)
        key = 'a/b/empty.txt'
        tests.MetadataAPITests.create_empty_object_metadata(
            testcase=self, bucket_name=bucket.name, key=key, check_response=True)

        url = reverse('api:obj-detail', kwargs={'bucket_name': backup.bucket_name, 'objpath': key})
        api = f'{backup.endpoint_url.rstrip("/")}/{url.lstrip("/")}'
        response = requests.delete(api, headers={'Authorization': f'BucketToken {backup.bucket_token}'})
        self.assertIn(response.status_code, [204, 404])

        # get metadata 404 before
        url = reverse('api:metadata-detail', kwargs={'bucket_name': backup.bucket_name, 'path': key})
        api = f'{backup.endpoint_url.rstrip("/")}/{url.lstrip("/")}'
        response = requests.get(api, headers={'Authorization': f'BucketToken {backup.bucket_token}'})
        self.assertEqual(response.status_code, 404)

        abm = AsyncBucketManager()
        abm.MEET_ASYNC_TIMEDELTA_MINUTES = 0
        qs = abm.get_need_async_objects_queryset(bucket=bucket, id_gt=0)
        obj = qs[0]
        self.assertEqual(obj.na, key)
        abm.async_object(bucket_id=bucket.id, bucket_name=bucket.name, object_id=obj.id, object_key=obj.na)

        # get metadata 200 after
        url = reverse('api:metadata-detail', kwargs={'bucket_name': backup.bucket_name, 'path': key})
        api = f'{backup.endpoint_url.rstrip("/")}/{url.lstrip("/")}'
        response = requests.get(api, headers={'Authorization': f'BucketToken {backup.bucket_token}'})
        self.assertEqual(response.status_code, 200)

        r = tests.ObjectsAPITests().delete_object_response(client=self.client, bucket_name=bucket.name, key=key)
        self.assertEqual(r.status_code, 204)
        abm.async_delete_object(bucket_id=bucket.id, bucket_name=bucket.name, object_key=obj.na)
        # get metadata 404 after async delete
        url = reverse('api:metadata-detail', kwargs={'bucket_name': backup.bucket_name, 'path': key})
        api = f'{backup.endpoint_url.rstrip("/")}/{url.lstrip("/")}'
        response = requests.get(api, headers={'Authorization': f'BucketToken {backup.bucket_token}'})
        self.assertEqual(response.status_code, 404)

    def tearDown(self):
        # delete bucket
        response = tests.BucketsAPITests.delete_bucket(self.client, self.bucket_name)
        self.assertEqual(response.status_code, 204)
        tests.BucketsAPITests.clear_bucket_archive(self.bucket_name)
