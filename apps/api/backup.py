from urllib import parse
from datetime import timedelta
import requests
from django.db import close_old_connections
from django.db.models import Q, F
from django.db.models.functions import Mod
from django.utils import timezone
from django.conf import settings

from buckets.models import Bucket, BackupBucket
from buckets.utils import BucketFileManagement
from utils.oss.pyrados import build_harbor_object, FileWrapper, HarborObject
from utils.md5 import FileMD5Handler, EMPTY_HEX_MD5


backup_setting = getattr(settings, 'BACKUP_BUCKET_SETTINGS', {})
meet_async_timedelta_minutes = backup_setting.get('meet_async_timedelta_minutes', 60)


def async_close_old_connections(func):
    def wrapper(*args, **kwargs):
        close_old_connections()
        return func(*args, **kwargs)

    return wrapper


class AsyncError(Exception):
    def __init__(self, message: str, code: str):
        self.message = message
        self.code = code

    def __str__(self):
        return f'code={self.code}, message={self.message}'


class IterFileWraper(FileWrapper):
    @property
    def len(self):
        return self.size


class IterCephRaods:
    def __init__(self, ho: HarborObject, offset: int = 0, size: int = None, block_size: int = 16*1024**2):
        self.ho = ho
        self.offset = offset
        self.size = size if size else self.ho.get_obj_size()
        self.block_size = block_size

    @property
    def len(self):
        return self.size

    def __iter__(self):
        return self.ho.read_obj_generator(offset=self.offset, end=self.size-1, block_size=self.block_size)


class AsyncBucketManager:
    AsyncError = AsyncError
    MEET_ASYNC_TIMEDELTA_MINUTES = meet_async_timedelta_minutes

    @staticmethod
    def _get_bucket_by_id(bucket_id):
        """
        :return:
            Bucket()    # exist
            None        # not exist
        """
        return Bucket.objects.filter(id=bucket_id).first()

    @staticmethod
    def _get_object_by_id(bucket, object_id):
        """
        查村对象

        :param bucket: bucket instance
        :param object_id: object id
        :return:
            BucketFileBase()    # exist
            None                # not exist
        """
        table_name = bucket.get_bucket_table_name()
        bfm = BucketFileManagement(collection_name=table_name)
        object_class = bfm.get_obj_model_class()
        obj = object_class.objects.filter(id=object_id).first()
        if obj is None:
            return None

        if obj.is_file():
            return obj

        return None

    @staticmethod
    def _get_object_by_key(bucket, object_key):
        """
        查村对象

        :param bucket: bucket instance
        :param object_key: object full path name
        :return:
            BucketFileBase()    # exist
            None                # not exist
        """
        table_name = bucket.get_bucket_table_name()
        bfm = BucketFileManagement(collection_name=table_name)
        obj = bfm.get_obj(object_key)
        if obj is None:
            return None

        if obj.is_file():
            return obj

        return None

    def _get_bucket(self, bucket_id, bucket_name: str):
        """
        查询bucket

        :return:
            Bucket()

        :raises: AsyncError
        """
        bucket = self._get_bucket_by_id(bucket_id)
        if bucket is None:
            raise AsyncError(
                message=f'The bucket with id "{bucket_id}" not exists',
                code='BucketNotExists'
            )

        if bucket.name != bucket_name:
            raise AsyncError(
                message=f'The bucket name with ID {bucket_id} is inconsistent with '
                        f'the given bucket name "{bucket_name}".',
                code='BucketInconsistent'
            )

        return bucket

    def _get_object(self, bucket, object_id, object_key: str):
        """
        查询object

        :return:
            object

        :raises: AsyncError
        """
        obj = self._get_object_by_id(bucket=bucket, object_id=object_id)
        if obj is None:
            raise AsyncError(
                message=f'The object with id "{object_id}" not exists',
                code='ObjectNotExists'
            )

        if obj.na != object_key:
            raise AsyncError(
                message=f'The object name with ID {object_id} is inconsistent with '
                        f'the given object key "{object_key}".',
                code='ObjectInconsistent'
            )

        return obj

    @staticmethod
    def _build_object_metadata_base_url(backup: BackupBucket, object_key: str):
        endpoint_url = backup.endpoint_url
        endpoint_url = endpoint_url.rstrip('/')
        object_key = object_key.lstrip('/')
        object_key = parse.quote(object_key, safe='/')
        url = f'{endpoint_url}/api/v1/metadata/{backup.bucket_name}/{object_key}/'
        return url

    @staticmethod
    def _build_object_base_url(backup: BackupBucket, object_key: str, api_version='v2'):
        endpoint_url = backup.endpoint_url
        endpoint_url = endpoint_url.rstrip('/')
        object_key = object_key.lstrip('/')
        object_key = parse.quote(object_key, safe='/')
        url = f'{endpoint_url}/api/{api_version}/obj/{backup.bucket_name}/{object_key}'
        if api_version == 'v2':
            return url

        return url+'/'

    def _build_post_chunk_url(self, backup: BackupBucket, object_key: str, offset: int, reset=None):
        querys = {
            'offset': offset
        }
        if reset:
            querys['reset'] = True

        query_str = parse.urlencode(query=querys)
        base_url = self._build_object_base_url(backup=backup, object_key=object_key)
        return f'{base_url}?{query_str}'

    @staticmethod
    @async_close_old_connections
    def _update_object_async_time(obj, async_time, backup_num):
        if backup_num == BackupBucket.BackupNum.ONE:
            obj.async1 = async_time
            obj.save(update_fields=['async1'])
        else:
            obj.async2 = async_time
            obj.save(update_fields=['async2'])

    def _get_meet_time(self):
        return timezone.now() - timedelta(minutes=self.MEET_ASYNC_TIMEDELTA_MINUTES)

    def _need_async_backup_map(self, bucket, obj, backup_num: int = None):
        """
        需要同步的备份点

        :param bucket:
        :param obj: 对象实例；删除对象时输入None
        :return: dict
            {backup_num: backup instance}
        """
        # 对象是否需要同步
        need_async_nums = []
        backup_map = {}

        # 对象修改时间超过now足够时间段后才允许同步, 尽量保证对象上传完成后再同步
        neet_time = self._get_meet_time()

        if obj is None:
            need_async_nums = BackupBucket.BackupNum.values
        else:
            if obj.async1 is None or (obj.upt >= obj.async1 and obj.upt < neet_time):
                need_async_nums.append(BackupBucket.BackupNum.ONE)
            if obj.async2 is None or (obj.upt >= obj.async2 and obj.upt < neet_time):
                need_async_nums.append(BackupBucket.BackupNum.TWO)

        if not need_async_nums:
            return backup_map

        lookups = {}
        if backup_num is not None:
            lookups['backup_num'] = backup_num

        backups = bucket.backup_buckets.filter(**lookups).select_related('bucket').all()
        for b in backups:
            if b.is_start_async():
                if b.backup_num in need_async_nums:
                    backup_map[b.backup_num] = b

        return backup_map

    @staticmethod
    def _do_request(method: str, url: str, data, headders):
        """
        :raises: Exception, requests.exceptions.RequestException
        """
        method = method.lower()
        func = getattr(requests, method, None)
        if func is None:
            raise Exception(f'package requests no method {method}')

        return func(url=url, data=data, headers=headders)

    @async_close_old_connections
    def get_bucket_by_id(self, bucket_id):
        return self._get_bucket_by_id(bucket_id=bucket_id)

    @async_close_old_connections
    def get_object_by_id(self, bucket, object_id):
        return self._get_object_by_id(bucket=bucket, object_id=object_id)

    @staticmethod
    def get_object_ceph_rados(bucket, obj):
        """
        获取对象对应ceph读写接口

        :param bucket: bucket instance
        :param obj: object instance
        :return:
            HarborObject()
        """
        obj_key = obj.get_obj_key(bucket.id)
        pool_name = bucket.get_pool_name()
        return build_harbor_object(using=bucket.ceph_using, pool_name=pool_name,
                                   obj_id=obj_key, obj_size=obj.obj_size)

    @async_close_old_connections
    def get_need_async_bucket_queryset(self, id_gt: int = 0, limit: int = 1000, names: list = None):
        """
        获取设置了备份点并开启了备份的所有桶, id正序排序

        :param id_gt: 查询id大于id_gt的数据，实现分页续读
        :param limit: 获取数据的数量
        :param names: bucket name list
        :return:
            QuerySet
        """
        qs = Bucket.objects.filter(
            id__gt=id_gt,
            backup_buckets__status=BackupBucket.Status.START
        )

        if names:
            qs = qs.filter(name__in=names)

        return qs.order_by('id')[0:limit]

    @async_close_old_connections
    def get_need_async_objects_queryset(self, bucket, id_gt: int = 0, limit: int = 1000, meet_time=None,
                                        id_mod_div: int = None, id_mod_equal: int = None, backup_num: int = None):
        """
        获取需要同步的对象的查询集, id正序排序

        :param bucket: bucket instance
        :param id_gt: 查询id大于id_gt的数据，实现分页续读
        :param limit: 获取数据的数量
        :param meet_time: 查询upt大于此时间的对象
        :param id_mod_div: object id求余的除数，和参数id_mod_equal一起使用
        :param id_mod_equal: object id求余的余数相等的筛选条件，仅在参数id_mod有效时有效
        :param backup_num: 筛选条件，只查询指定备份点编号需要同步的对象，默认查询所有备份点需要同步的对象
        :return:
            QuerySet
        """
        table_name = bucket.get_bucket_table_name()
        bfm = BucketFileManagement(collection_name=table_name)
        object_class = bfm.get_obj_model_class()

        backup_nums = []
        if backup_num is not None:
            backup_nums.append(backup_num)
        else:
            for backup in bucket.backup_buckets.all():
                if backup.status == BackupBucket.Status.START:
                    if backup.backup_num in backup.BackupNum.values:
                        backup_nums.append(backup.backup_num)

        if not backup_nums:
            return object_class.objects.none()

        queryset = object_class.objects.filter(fod=True, id__gt=id_gt).all()

        if meet_time is None:
            meet_time = self._get_meet_time()

        queryset = queryset.filter(Q(upt__lt=meet_time) | Q(upt__isnull=True))

        if backup_nums == [BackupBucket.BackupNum.ONE, ]:
            queryset = queryset.filter(
                Q(async1__isnull=True) | Q(upt__gt=F('async1'))
            ).order_by('id')
        elif backup_nums == [BackupBucket.BackupNum.TWO, ]:
            queryset = queryset.filter(
                Q(async2__isnull=True) | Q(upt__gt=F('async2'))
            ).order_by('id')
        else:
            queryset = queryset.filter(
                Q(async1__isnull=True) | Q(upt__gt=F('async1')) | Q(async2__isnull=True) | Q(upt__gt=F('async2'))
            ).order_by('id')

        if id_mod_div is not None and id_mod_equal is not None:
            if id_mod_div >= 1 and (0 <= id_mod_equal < id_mod_div):
                queryset = queryset.annotate(id_mod=Mod('id', id_mod_div)).filter(id_mod=id_mod_equal)

        return queryset[0:limit]

    def async_object_to_backup_bucket(self, bucket, obj, backup):
        """
        :return:
            True        # 同步成功

        """
        ho = self.get_object_ceph_rados(bucket=bucket, obj=obj)
        obj_size = obj.obj_size
        hex_md5 = obj.hex_md5
        if obj_size == 0:
            self.put_one_object(obj=obj, ho=None, backup=backup, object_md5=EMPTY_HEX_MD5)
            return True

        if obj_size <= 256 * 1024**2 and hex_md5:       # 256MB
            r = self.put_one_object(obj=obj, ho=ho, backup=backup, object_md5=hex_md5)
            if r is True:
                return True

        self.post_object_by_chunk(obj=obj, ho=ho, backup=backup)
        return True

    def create_object_metadata(self, obj, backup: BackupBucket):
        async_time = timezone.now()
        url = self._build_object_metadata_base_url(backup=backup, object_key=obj.na)
        headers = {
            'Authorization': f'BucketToken {backup.bucket_token}'
        }
        try:
            r = self._do_request(method='post', url=url, data=None, headders=headers)
        except requests.exceptions.RequestException as e:
            raise AsyncError(message=f'Failed async object({obj.na}), {backup}, put empty object, {str(e)}',
                             code='FailedAsyncObject')

        if r.status_code == 200:
            self._update_object_async_time(obj=obj, async_time=async_time, backup_num=backup.backup_num)
            return

        raise AsyncError(message=f'Failed async object({obj.na}), {backup}, put empty object, {r.text}',
                         code='FailedAsyncObject')

    def put_one_object(self, obj, ho, backup: BackupBucket, object_md5: str):
        """
        上传一个对象

        :return:
            True:               success
            raise AsyncError:   failed
            None:               md5 invalid, try async by chunk
        :raises: AsyncError
        """
        async_time = timezone.now()
        api = self._build_object_base_url(backup=backup, object_key=obj.na)
        headers = {
            'Authorization': f'BucketToken {backup.bucket_token}',
            'Content-MD5': object_md5
        }
        if ho is None or obj.obj_size == 0:
            data = None
            headers['Content-Length'] = '0'
        else:
            data = IterCephRaods(ho=ho)

        try:
            r = self._do_request(method='put', url=api, data=data, headders=headers)
        except requests.exceptions.RequestException as e:
            raise AsyncError(message=f'Failed async object({obj.na}), {backup}, put object, {str(e)}',
                             code='FailedAsyncObject')

        if r.status_code == 200:
            self._update_object_async_time(obj=obj, async_time=async_time, backup_num=backup.backup_num)
            return True

        if r.status_code == 400:
            data = r.json()
            code = data.get('code', '')
            if code in ['BadDigest', 'InvalidDigest']:  # md5和数据不一致
                return None

        raise AsyncError(message=f'Failed async object({obj.na}), {backup}, put object, {r.text}',
                         code='FailedAsyncObject')

    def post_object_by_chunk(self, obj, ho, backup: BackupBucket, per_size: int = 32*1024**2):
        """
        分片上传一个对象

        :raises: AsyncError
        """
        async_time = timezone.now()
        file = FileWrapper(ho=ho).open()
        while True:
            offset = file.offset
            reset = None
            if offset == 0:
                reset = True

            api = self._build_post_chunk_url(backup=backup, object_key=obj.na, offset=offset, reset=reset)
            data = file.read(per_size)
            if not data:
                if offset >= file.size:
                    break
                raise AsyncError(f'Failed async object({obj.na}), {backup}, post by chunk, read empty bytes from ceph, 对象同步可能不完整',
                                 code='FailedAsyncObject')

            md5_handler = FileMD5Handler()
            md5_handler.update(offset=0, data=data)
            hex_md5 = md5_handler.hex_md5
            headers = {
                'Authorization': f'BucketToken {backup.bucket_token}',
                'Content-MD5': hex_md5
            }
            try:
                r = self._do_request(method='post', url=api, data=data, headders=headers)
                if r.status_code == 200:
                    continue
            except requests.exceptions.RequestException as e:
                pass

            try:
                r = self._do_request(method='post', url=api, data=data, headders=headers)
            except requests.exceptions.RequestException as e:
                raise AsyncError(f'Failed async object({obj.na}), {backup}, post by chunk, {str(e)}',
                                 code='FailedAsyncObject')

            if r.status_code == 200:
                continue

            raise AsyncError(f'Failed async object({obj.na}), {backup}, post by chunk, {r.text}',
                             code='FailedAsyncObject')

        file.close()
        self._update_object_async_time(obj=obj, async_time=async_time, backup_num=backup.backup_num)

    @async_close_old_connections
    def async_object(self, bucket_id, object_id, bucket_name: str, object_key: str):
        """
        同步一个对象

        :raises: AsyncError
        """
        bucket = self._get_bucket(
            bucket_id=bucket_id, bucket_name=bucket_name
        )

        obj = self._get_object(
            bucket=bucket, object_id=object_id, object_key=object_key
        )

        r = self.async_bucket_object(bucket=bucket, obj=obj)
        for num, val in r.items():
            ok, err = val
            if not ok:
                raise err

    @async_close_old_connections
    def async_bucket_object(self, bucket, obj, backup_num: int = None):
        """
        :return:
            {
                backup_num:             # key是备份点编号1或2，可能都不存在；1存在表示备份点1进行了同步工作
                (bool, AsyncError)      # value[0]指示同步是否成功，value[1]为None或者AsyncError
            }
        """
        # 对象是否需要同步
        backup_map = self._need_async_backup_map(bucket=bucket, obj=obj)
        if not backup_map:
            return {}

        # async
        ret = {}
        for num, backup in backup_map.items():
            try:
                self.async_object_to_backup_bucket(bucket=bucket, obj=obj, backup=backup)
                ret[num] = (True, None)
            except AsyncError as e:
                ret[num] = (False, e)
                backup.error = str(e)[:254]
                backup.save(update_fields=['error'])

        return ret

    def async_delete_object_to_backup_bucket(self, object_key, backup):
        url = self._build_object_base_url(backup=backup, object_key=object_key, api_version='v1')
        try:
            response = requests.delete(url=url, headers={'Authorization': f'BucketToken {backup.bucket_token}'})
        except requests.exceptions.RequestException as e:
            raise AsyncError(f'Failed async delete object({object_key}), {backup}, {str(e)}',
                             code='FailedAsyncDeleteObject')

        if response.status_code in [204, 404]:
            return

        raise AsyncError(f'Failed async delete object({object_key}), {backup}, {response.text}',
                         code='FailedAsyncDeleteObject')

    @async_close_old_connections
    def async_delete_object(self, bucket_id, bucket_name: str, object_key: str):
        """
        删除一个对象同步

        :raises: AsyncError
        """
        bucket = self._get_bucket(
            bucket_id=bucket_id, bucket_name=bucket_name
        )

        obj = self._get_object_by_key(
            bucket=bucket, object_key=object_key
        )
        if obj is not None:     # 对象存在（可能删除后上传同名对象）不需要删除
            return

        # 对象是否需要同步
        backup_map = self._need_async_backup_map(bucket=bucket, obj=None)
        if not backup_map:
            return

        # async
        err = None
        for num, backup in backup_map.items():
            try:
                self.async_delete_object_to_backup_bucket(object_key=object_key, backup=backup)
            except AsyncError as e:
                err = e

        if err is not None:
            raise err
