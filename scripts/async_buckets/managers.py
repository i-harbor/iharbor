import json
from urllib import parse
from datetime import datetime
import requests

from utils.oss.pyrados import FileWrapper, HarborObject
from utils.md5 import FileMD5Handler, EMPTY_HEX_MD5

from .databases import django_settings
from .querys import QueryHandler, BackupNum


def get_ceph_conf():
    data_conf = QueryHandler().get_ceph_conf_sql()
    ceph_cluster = {}
    for ceph_conf in data_conf:
        ceph_pool_conf = {
            'CLUSTER_NAME': ceph_conf['cluster_name'],
            'USER_NAME': ceph_conf['user_name'],
            'DISABLE_CHOICE': True if ceph_conf['disable_choice'] else False,
            'CONF_FILE_PATH': ceph_conf['config_file'],
            'KEYRING_FILE_PATH': ceph_conf['keyring_file'],
            'POOL_NAME': tuple(json.loads(ceph_conf['pool_names'])),
            'PORIORITY_STORED_VALUE': ceph_conf['priority_stored_value'],
        }
        ceph_cluster[str(ceph_conf['id'])] = ceph_pool_conf

    return ceph_cluster


django_settings.CEPH_RADOS = get_ceph_conf()


def build_harbor_object(using: str, obj_id: str, obj_size: int = 0):
    """
    构建iharbor对象对应的ceph读写接口

    :param using: ceph集群配置别名，对应对象数据所在ceph集群
    :param obj_id: 对象在ceph存储池中对应的rados名称
    :param obj_size: 对象的大小
    """
    cephs = django_settings.CEPH_RADOS
    if using not in cephs:
        raise AsyncError(message=f'别名为"{using}"的CEPH集群信息未配置，请确认配置文件中的“CEPH_RADOS”配置内容',
                         code='CephSetting')

    ceph = cephs[using]
    cluster_name = ceph['CLUSTER_NAME']
    user_name = ceph['USER_NAME']
    conf_file = ceph['CONF_FILE_PATH']
    keyring_file = ceph['KEYRING_FILE_PATH']
    pool_name = ceph['POOL_NAME'][0]
    return HarborObject(pool_name=pool_name, obj_id=obj_id, obj_size=obj_size, cluster_name=cluster_name,
                        user_name=user_name, conf_file=conf_file, keyring_file=keyring_file, alise_cluster=using)


def get_utcnow():
    return datetime.utcnow()


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


class IharborBucketClient:
    @staticmethod
    def _build_object_metadata_base_url(endpoint_url: str, bucket_name: str, object_key: str):
        endpoint_url = endpoint_url.rstrip('/')
        object_key = object_key.lstrip('/')
        object_key = parse.quote(object_key, safe='/')
        url = f'{endpoint_url}/api/v1/metadata/{bucket_name}/{object_key}/'
        return url

    @staticmethod
    def _build_object_base_url(endpoint_url: str, bucket_name: str, object_key: str, api_version='v2'):
        endpoint_url = endpoint_url.rstrip('/')
        object_key = object_key.lstrip('/')
        object_key = parse.quote(object_key, safe='/')
        url = f'{endpoint_url}/api/{api_version}/obj/{bucket_name}/{object_key}'
        if api_version == 'v2':
            return url

        return url + '/'

    def _build_post_chunk_url(self, endpoint_url: str, bucket_name: str, object_key: str, offset: int, reset=None):
        querys = {
            'offset': offset
        }
        if reset:
            querys['reset'] = True

        query_str = parse.urlencode(query=querys)
        base_url = self._build_object_base_url(
            endpoint_url=endpoint_url, bucket_name=bucket_name, object_key=object_key)
        return f'{base_url}?{query_str}'

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

    def create_object_metadata(self, endpoint_url: str, bucket_name: str, object_key: str, bucket_token: str):
        """
        :return:
            True:               success
        """
        backup_str = f"endpoint_url={endpoint_url}, bucket name={bucket_name}, token={bucket_token}"
        url = self._build_object_metadata_base_url(
            endpoint_url=endpoint_url, bucket_name=bucket_name, object_key=object_key)
        headers = {
            'Authorization': f'BucketToken {bucket_token}'
        }
        try:
            r = self._do_request(method='post', url=url, data=None, headders=headers)
        except requests.exceptions.RequestException as e:
            raise AsyncError(message=f'Failed async object({object_key}), to backup({backup_str}), put empty object, {str(e)}',
                             code='FailedAsyncObject')

        if r.status_code == 200:
            return True

        raise AsyncError(message=f'Failed async object({object_key}), to backup({backup_str}), put empty object, {r.text}',
                         code='FailedAsyncObject')

    def put_one_object(self, ho, endpoint_url: str, bucket_name: str, object_key: str,
                       bucket_token: str, object_md5: str):
        """
        上传一个对象

        :return:
            True:               success
            raise AsyncError:   failed
            None:               md5 invalid, try async by chunk
        :raises: AsyncError
        """
        backup_str = f"endpoint_url={endpoint_url}, bucket name={bucket_name}, token={bucket_token}"
        api = self._build_object_base_url(endpoint_url=endpoint_url, bucket_name=bucket_name, object_key=object_key)
        headers = {
            'Authorization': f'BucketToken {bucket_token}',
            'Content-MD5': object_md5
        }
        if ho is None:
            data = None
            headers['Content-Length'] = '0'
        else:
            data = IterCephRaods(ho=ho)

        try:
            r = self._do_request(method='put', url=api, data=data, headders=headers)
        except requests.exceptions.RequestException as e:
            raise AsyncError(message=f'Failed async object({object_key}), to backup({backup_str}), put object, {str(e)}',
                             code='FailedAsyncObject')

        if r.status_code == 200:
            return True

        if r.status_code == 400:
            data = r.json()
            code = data.get('code', '')
            if code in ['BadDigest', 'InvalidDigest']:  # md5和数据不一致
                return None

        raise AsyncError(message=f'Failed async object({object_key}), to backup({backup_str}), put object, {r.text}',
                         code='FailedAsyncObject')

    def get_backup_address_object_size(self, endpoint_url: str, bucket_name: str, object_key: str, bucket_token: str):
        backup_str = f"endpoint_url={endpoint_url}, bucket name={bucket_name}, token={bucket_token}"
        url = self._build_object_metadata_base_url(endpoint_url=endpoint_url, bucket_name=bucket_name,
                                                   object_key=object_key)
        headers = {
            'Authorization': f'BucketToken {bucket_token}'
        }
        try:
            r = self._do_request(
                method="GET", headders=headers, url=url, data=None
            )
        except requests.exceptions.RequestException as e:
            raise AsyncError(
                message=f'Failed get object ({object_key}) size from backup point address ({backup_str}),'
                        f' break point resume object, {str(e)}', code='FailedAsyncObject')

        if r.status_code == 200:
            return r.json().get('obj')['si']

        else:
            data = r.json()
            raise AsyncError(
                message=f'Failed get object ({object_key}) size from backup point address ({backup_str}),'
                        f' break point resume object, {str(data)}', code='FailedAsyncObject')

    def post_object_by_chunk(self, ho, endpoint_url: str, bucket_name: str, object_key: str,
                             bucket_token: str, per_size: int = 32 * 1024 ** 2, breakpoint_resume=None):
        """
        分片上传一个对象
        :raises: AsyncError
        """
        backup_str = f"endpoint_url={endpoint_url}, bucket name={bucket_name}, token={bucket_token}"
        file = FileWrapper(ho=ho).open()
        if breakpoint_resume:
            # 续传 获取 已同步文件的大小
            incomplete_obj_size = self.get_backup_address_object_size(endpoint_url=endpoint_url, object_key=object_key,
                                                                      bucket_name=bucket_name, bucket_token=bucket_token)
            file.seek(incomplete_obj_size)

        while True:
            offset = file.offset
            reset = None
            if offset == 0:
                reset = True

            api = self._build_post_chunk_url(endpoint_url=endpoint_url, bucket_name=bucket_name,
                                             object_key=object_key, offset=offset, reset=reset)
            data = file.read(per_size)
            if not data:
                if offset >= file.size:
                    break
                raise AsyncError(f'Failed async object({object_key}), to backup({backup_str}), post by chunk, '
                                 f'read empty bytes from ceph, 对象同步可能不完整', code='FailedAsyncObject')

            md5_handler = FileMD5Handler()
            md5_handler.update(offset=0, data=data)
            hex_md5 = md5_handler.hex_md5
            headers = {
                'Authorization': f'BucketToken {bucket_token}',
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
                raise AsyncError(f'Failed async object({object_key}), to backup({backup_str}), post by chunk, {str(e)}',
                                 code='FailedAsyncObject')

            if r.status_code == 200:
                continue

            raise AsyncError(f'Failed async object({object_key}), to backup({backup_str}), post by chunk, {r.text}',
                             code='FailedAsyncObject')

        file.close()
        return True


class AsyncBucketManager:
    AsyncError = AsyncError

    @staticmethod
    def get_object_ceph_rados(bucket: dict, obj: dict):
        """
        获取对象对应ceph读写接口

        :param bucket: bucket
        :param obj: object
        :return:
            HarborObject()
        """
        obj_key = f"{str(bucket['id'])}_{str(obj['id'])}"
        return build_harbor_object(using=str(obj['pool_id']), obj_id=obj_key, obj_size=obj['si'])

    def async_object_to_backup_bucket(self, bucket: dict, obj: dict, backup: dict, breakpoint_resume=None):
        """
        :return:
            True        # 同步成功

        """
        client = IharborBucketClient()
        object_key = obj['na']
        obj_size = obj['si']
        hex_md5 = obj['md5']
        endpoint_url = backup['endpoint_url']
        bucket_name = backup['bucket_name']
        bucket_token = backup['bucket_token']

        if obj_size == 0:
            client.put_one_object(ho=None, endpoint_url=endpoint_url, bucket_name=bucket_name,
                                  object_key=object_key, bucket_token=bucket_token, object_md5=EMPTY_HEX_MD5)
            return True

        ho = self.get_object_ceph_rados(bucket=bucket, obj=obj)
        if obj_size <= 256 * 1024**2 and hex_md5:       # 256MB
            r = client.put_one_object(ho=ho, endpoint_url=endpoint_url, bucket_name=bucket_name,
                                      object_key=object_key, bucket_token=bucket_token, object_md5=hex_md5)
            if r is True:
                return True

        client.post_object_by_chunk(ho=ho, endpoint_url=endpoint_url, bucket_name=bucket_name,
                                    object_key=object_key, bucket_token=bucket_token,
                                    breakpoint_resume=breakpoint_resume)
        return True

    def async_bucket_object(self, bucket, obj, backup, logger):
        """
        breakpoint_resume: 标记 是否断点续传
        :return:
            backup_num

        :raises: AsyncError, CanNotConnection
        """
        backup_num = backup['backup_num']
        async_time = get_utcnow()
        breakpoint_resume = self.async_bucket_object_breakpoint_resume(obj=obj, backup_num=backup_num)
        if breakpoint_resume is False:  # 不是断点续传
            try:
                # 不是续传，需要更改 sync_start sync_end 字段
                QueryHandler().update_sync_start_and_end_before_upload_obj(bucket_id=bucket['id'], obj_id=obj['id'],
                                                                           async_time=async_time, backup_num=backup_num)
            except Exception as e:
                raise AsyncError(message=f'update sync_start sync_end failed in backup: {backup_num}',
                                 code='UpdateSyncEndFailed')
        if breakpoint_resume is None:
            logger.debug(f"不同步的文件：backup num {backup_num}, [bucket(id={bucket['id']}, name={bucket['name']})], "
                         f"[object(id={obj['id']}, key={obj['na']}, size={obj['si']})]")
            return None

        try:

            ok = self.async_object_to_backup_bucket(bucket=bucket, obj=obj, backup=backup,
                                                    breakpoint_resume=breakpoint_resume)
            async_time = get_utcnow()
            if ok:
                try:
                    r = QueryHandler().update_object_sync_end_time(
                        bucket_id=bucket['id'], obj_id=obj['id'], async_time=async_time, backup_num=backup_num)
                except Exception as e:
                    r = QueryHandler().update_object_sync_end_time(
                        bucket_id=bucket['id'], obj_id=obj['id'], async_time=async_time, backup_num=backup_num)

                if not r:
                    raise AsyncError(message=f'update sync_end failed in backup: {backup_num}',
                                     code='UpdateSyncEndFailed')
        except AsyncError as e:
            raise e
        except Exception as e:
            raise e

        return backup

    def async_bucket_object_breakpoint_resume(self, obj, backup_num):
        """
        判断是否是对象是否断点续传
        :return:
        """

        if not obj['upt']:
            return False

        if backup_num == BackupNum.ONE:
            if not obj['sync_start1']:
                breakpoint_resume = False
            elif obj['upt'] > obj['sync_start1']:
                breakpoint_resume = False
            elif not obj['sync_end1']:
                breakpoint_resume = True  # 续传
            else:
                breakpoint_resume = None
        else:
            if not obj['sync_start2']:
                breakpoint_resume = False
            elif obj['upt'] > obj['sync_start2']:
                breakpoint_resume = False
            elif not obj['sync_end2']:
                breakpoint_resume = True  # 续传
            else:
                breakpoint_resume = None

        return breakpoint_resume



