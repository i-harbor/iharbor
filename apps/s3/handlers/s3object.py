from django.conf import settings
from django.utils.translation import gettext as _

from utils.time import datetime_from_gmt
from utils.oss.pyrados import build_harbor_object
from buckets.models import BucketFileBase
from s3.harbor import HarborManager
from s3 import exceptions


MULTIPART_UPLOAD_MAX_SIZE = getattr(settings, 'S3_MULTIPART_UPLOAD_MAX_SIZE', 2 * 1024 ** 3)        # default 2GB
MULTIPART_UPLOAD_MIN_SIZE = getattr(settings, 'S3_MULTIPART_UPLOAD_MIN_SIZE', 5 * 1024 ** 2)        # default 5MB


def has_object_access_permission(request, bucket, obj):
    """
    当前已认证用户或未认证用户是否有访问对象的权限

    :param request: 请求体对象
    :param bucket: 存储桶对象
    :param obj: 文件对象
    :return:
        True(可访问)
        raise S3AccessDenied  # 不可访问

    :raises: S3AccessDenied
    """
    # 存储桶是否是公有权限
    if bucket.is_public_permission():
        return True

    # 存储桶是否属于当前用户
    if bucket.check_user_own_bucket(request.user):
        return True

    # 对象是否共享的，并且在有效共享事件内
    if not obj.is_shared_and_in_shared_time():
        raise exceptions.S3AccessDenied(message=_('您没有访问权限'))

    # 是否设置了分享密码
    if obj.has_share_password():
        p = request.query_params.get('p', None)
        if p is None:
            raise exceptions.S3AccessDenied(message=_('资源设有共享密码访问权限'))
        if not obj.check_share_password(password=p):
            raise exceptions.S3AccessDenied(message=_('共享密码无效'))

    return True


def compare_since(t, since):
    """
    :param t:
    :param since:
    :return:
        True    # t >= since
        False   # t < since
    """
    t_ts = t.timestamp()
    dt_ts = since.timestamp()
    if t_ts >= dt_ts:  # 指定时间以来有改动
        return True

    return False


def check_precondition_if_headers(headers: dict, last_modified, etag: str, key_match: str, key_none_match: str,
                                  key_modified_since: str, key_unmodified_since: str):
    """
    标头if条件检查

    :param headers:
    :param last_modified: 对象最后修改时间, datetime
    :param etag: 对象etag
    :param key_match: header name, like 'If-Match', 'x-amz-copy-source-if-match'
    :param key_none_match:  header name, like 'If-None-Match', 'x-amz-copy-source-if-none-match'
    :param key_modified_since: header name, like 'If-Modified-Since', 'x-amz-copy-source-if-modified-since'
    :param key_unmodified_since: header name, like 'If-Unmodified-Since', 'x-amz-copy-source-if-unmodified-since'
    :return: None
    :raises: S3Error
    """
    match = headers.get(key_match, None)
    none_match = headers.get(key_none_match, None)
    modified_since = headers.get(key_modified_since, None)
    unmodified_since = headers.get(key_unmodified_since, None)

    if modified_since:
        modified_since = datetime_from_gmt(modified_since)
        if modified_since is None:
            raise exceptions.S3InvalidRequest(extend_msg=f'Invalid value of header "{key_modified_since}".')

    if unmodified_since:
        unmodified_since = datetime_from_gmt(unmodified_since)
        if unmodified_since is None:
            raise exceptions.S3InvalidRequest(extend_msg=f'Invalid value of header "{key_unmodified_since}".')

    if (match is not None or none_match is not None) and not etag:
        raise exceptions.S3PreconditionFailed(
            extend_msg=f'ETag of the object is empty, Cannot support "{key_match}" and "{key_none_match}".')

    if match is not None and unmodified_since is not None:
        if match != etag:       # If-Match: False
            raise exceptions.S3PreconditionFailed()
        else:
            if compare_since(t=last_modified, since=unmodified_since):  # 指定时间以来改动; If-Unmodified-Since: False
                pass
    elif match is not None:
        if match != etag:       # If-Match: False
            raise exceptions.S3PreconditionFailed()
    elif unmodified_since is not None:
        if compare_since(t=last_modified, since=unmodified_since):   # 指定时间以来有改动；If-Unmodified-Since: False
            raise exceptions.S3PreconditionFailed()

    if none_match is not None and modified_since is not None:
        if none_match == etag:  # If-None-Match: False
            raise exceptions.S3NotModified()
        elif not compare_since(t=last_modified, since=modified_since):   # 指定时间以来无改动; If-modified-Since: False
            raise exceptions.S3NotModified()
    elif none_match is not None:
        if none_match == etag:  # If-None-Match: False
            raise exceptions.S3NotModified()
    elif modified_since is not None:
        if not compare_since(t=last_modified, since=modified_since):  # 指定时间以来无改动; If-modified-Since: False
            raise exceptions.S3NotModified()


def create_object_metadata(user, bucket_or_name, obj_key: str, x_amz_acl: str):
    """
    :param user:
    :param bucket_or_name: bucket name or bucket instance
    :param obj_key: object key
    :param x_amz_acl: 访问权限
    :return: (
        bucket,         # bucket instance
        obj,            # object instance
        rados,          # ceph rados of object
        created         # True: new created; False: not new
    )
    :raises: S3Error
    """
    # 访问权限
    acl_choices = {'private': BucketFileBase.SHARE_ACCESS_NO,
                   'public-read': BucketFileBase.SHARE_ACCESS_READONLY,
                   'public-read-write': BucketFileBase.SHARE_ACCESS_READWRITE}

    if x_amz_acl not in acl_choices:
        raise exceptions.S3InvalidRequest(f'The value {x_amz_acl} of header "x-amz-acl" is not supported.')

    h_manager = HarborManager()
    if isinstance(bucket_or_name, str):
        bucket, obj, created = h_manager.create_empty_obj(
            bucket_name=bucket_or_name, obj_path=obj_key, user=user)
    else:
        bucket = bucket_or_name
        collection_name = bucket.get_bucket_table_name()
        obj, created = h_manager.get_or_create_obj(collection_name, obj_key)

    if x_amz_acl != 'private':
        share_code = acl_choices[x_amz_acl]
        obj.set_shared(share=share_code)

    rados = build_object_rados(bucket=bucket, obj=obj)
    if created is False:  # 对象已存在，不是新建的
        try:
            h_manager._pre_reset_upload(bucket=bucket, obj=obj, rados=rados)  # 重置对象大小
        except Exception as exc:
            raise exceptions.S3InvalidRequest(f'reset object error, {str(exc)}')

    return bucket, obj, rados, created


def build_object_rados(bucket, obj):
    pool_name = bucket.get_pool_name()
    obj_ceph_key = obj.get_obj_key(bucket.id)
    return build_harbor_object(using=bucket.ceph_using, pool_name=pool_name, obj_id=obj_ceph_key, obj_size=obj.si)
