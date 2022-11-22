from urllib import parse

from django.utils import timezone
from rest_framework.response import Response

from buckets.models import BucketFileBase
from utils.storagers import FileMD5Handler
from utils.oss.pyrados import build_harbor_object
from s3.viewsets import S3CustomGenericViewSet
from s3 import exceptions
from s3.harbor import HarborManager
from s3 import renders
from . import s3object


class CopyObjectHandler:
    def copy_object(self, request, view: S3CustomGenericViewSet):
        bucket_name = view.get_bucket_name(request)
        obj_path_name = view.get_obj_path_name(request)
        x_amz_copy_source = request.headers.get('x-amz-copy-source', '')
        if x_amz_copy_source.startswith('arm:'):
            return view.exception_response(request, exceptions.S3NotImplemented(
                message='CopyObject unsupported access points, header "x-amz-copy-source" unsupported "ARN" format'
            ))

        try:
            source_bucket_name, source_key, version_id = self.parse_x_amz_copy_source(x_amz_copy_source)
        except exceptions.S3Error as exc:
            return view.exception_response(request, exc)

        if obj_path_name == source_key:
            return view.exception_response(
                request, exc=exceptions.S3InvalidRequest(message=f'对象的key和复制的源对象key不能相同。'))

        if version_id:
            return view.exception_response(request, exceptions.S3NotImplemented(
                message='CopyObject unsupported copy the specified version of the object '
                        'by versionId in header "x-amz-copy-source"'
            ))

        try:
            source_bucket, source_object = self.get_source_bucket_object(
                request=request, bucket_name=source_bucket_name, obj_key=source_key)
        except exceptions.S3Error as exc:
            return view.exception_response(request, exc)

        try:
            self.check_precondition(request=request, obj_upt=source_object.upt, etag=source_object.md5)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        if source_bucket_name == bucket_name and source_key == obj_path_name:
            if not source_bucket.check_user_own_bucket(request.user):
                return view.exception_response(request, exceptions.S3AccessDenied(
                    message=f'no permission to access bucket "{bucket_name}"'))

            return self.handle_updete_metadata(view=view, request=request, obj=source_object)

        if source_object.obj_size > s3object.MULTIPART_UPLOAD_MAX_SIZE:
            return view.exception_response(request, exceptions.S3NotImplemented(
                message=f'The size of source object is too large'))

        return self.handle_copy_object(view=view, request=request, bucket_name=bucket_name, obj_key=obj_path_name,
                                       source_bucket=source_bucket, source_object=source_object)

    @staticmethod
    def get_source_bucket_object(request, bucket_name: str, obj_key: str):
        """
        源对象包括公开权限的对象
        :return:
            bucket, object

        :raises: S3Error
        """
        hm = HarborManager()
        try:
            bucket, obj = hm.get_bucket_and_obj_or_dir(
                bucket_name=bucket_name, path=obj_key, user=request.user, all_public=True)
        except exceptions.S3Error as e:
            raise e

        if obj is None:
            raise exceptions.S3NoSuchKey()

        if not obj.is_file():
            raise exceptions.S3NoSuchKey()

        return bucket, obj

    @staticmethod
    def handle_updete_metadata(view, request, obj):
        """
        :return: response
        """
        now_time = timezone.now()
        try:
            obj = HarborManager().update_obj_metadata_time(obj=obj, create_time=now_time, modified_time=now_time)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        data = {
            'ETag': obj.md5,
            'LastModified': obj.upt
        }
        view.set_renderer(request, renders.CusXMLRenderer(root_tag_name='CopyObjectResult'))
        return Response(data=data, status=200)

    @staticmethod
    def parse_x_amz_copy_source(x_amz_copy_source: str):
        """
        :retrun: tuple
            (
                bucket: str,
                key: str,
                versionId: str      # str or None
            )
        """
        (scheme, netloc, path, query, fragment) = parse.urlsplit(x_amz_copy_source)
        copy_source_path = parse.unquote(path)
        copy_source_path = copy_source_path.lstrip('/')
        source_bucket_key = copy_source_path.split('/', maxsplit=1)
        if len(source_bucket_key) != 2:
            raise exceptions.S3InvalidRequest(
                extend_msg='invalid value of header "x-amz-copy-source"')

        source_bucket, source_key = source_bucket_key
        try:
            query_dict = parse.parse_qs(query, keep_blank_values=True)
        except Exception as e:
            raise exceptions.S3InvalidRequest(
                extend_msg='invalid value of header "x-amz-copy-source"')

        version_id = query_dict.get('versionId')
        if version_id and isinstance(version_id, list):
            version_id = version_id[0]

        return source_bucket, source_key, version_id

    @staticmethod
    def check_precondition(request, obj_upt, etag):
        """
         标头if条件检查

        :param request:
        :param obj_upt: 对象最后修改时间
        :param etag: 对象etag
        :return: None
        :raises: S3Error
        """
        try:
            s3object.check_precondition_if_headers(
                headers=request.headers, last_modified=obj_upt, etag=etag,
                key_match='x-amz-copy-source-if-match',
                key_none_match='x-amz-copy-source-if-none-match',
                key_modified_since='x-amz-copy-source-if-modified-since',
                key_unmodified_since='x-amz-copy-source-if-unmodified-since'
            )
        except exceptions.S3NotModified as e:
            raise exceptions.S3PreconditionFailed(extend_msg=str(e))

    def handle_copy_object(self, view, request, bucket_name: str, obj_key: str, source_bucket, source_object):
        """
        :return: response
        """
        if bucket_name == source_bucket.name:
            bucket, obj, obj_rados, created = self.create_object_metadata(
                request=request, bucket_or_name=source_bucket, obj_key=obj_key)
        else:
            bucket, obj, obj_rados, created = self.create_object_metadata(
                request=request, bucket_or_name=bucket_name, obj_key=obj_key)

        source_rados = self.build_object_rados(bucket=source_bucket, obj=source_object)
        try:
            write_size, md5 = self.copy_object_rados(obj_rados=obj_rados, source_rados=source_rados)
            if write_size != source_object.obj_size:
                raise exceptions.S3InternalError(message='raods data copy is interrupted or incomplete')
        except exceptions.S3Error as e:
            obj.do_delete()
            obj_rados.delete()
            return view.exception_response(request=request, exc=exceptions.S3InternalError(
                message=f"copy object rados failed, {str(e)}"))

        # update metadata
        obj.md5 = md5
        obj.si = write_size
        obj.upt = timezone.now()
        obj.stl = False  # 没有共享时间限制
        try:
            obj.save(update_fields=['si', 'md5', 'upt', 'stl'])
        except Exception as e:
            obj.do_delete()
            obj_rados.delete()
            return view.exception_response(request=request, exc=exceptions.S3InternalError(
                message=f"copy object rados failed, {str(e)}"))

        data = {
            'ETag': obj.md5,
            'LastModified': obj.upt
        }
        view.set_renderer(request, renders.CusXMLRenderer(root_tag_name='CopyObjectResult'))
        return Response(data=data, status=200)

    @staticmethod
    def copy_object_rados(obj_rados, source_rados):
        """
        :return: (
            len: int         # length of copy bytes
            md5: str         # md5 of copy bytes
        )
        :raises: S3Error
        """
        md5_handler = FileMD5Handler()
        offset = 0
        source_generator = source_rados.read_obj_generator()
        for data in source_generator:
            if not data:
                break

            ok, msg = obj_rados.write(offset=offset, data_block=data)
            if not ok:
                ok, msg = obj_rados.write(offset=offset, data_block=data)

            if not ok:
                raise exceptions.S3InternalError(extend_msg=msg)

            md5_handler.update(offset=offset, data=data)
            offset = offset + len(data)

        return offset, md5_handler.hex_md5

    def create_object_metadata(self, request, bucket_or_name, obj_key: str):
        """
        :param request:
        :param bucket_or_name: bucket name or bucket instance
        :param obj_key: object key
        :return: (
            bucket,         # bucket instance
            obj,            # object instance
            rados,          # ceph rados of object
            created         # True: new created; False: not new
        )
        :raises: S3Error
        """
        h_manager = HarborManager()
        if isinstance(bucket_or_name, str):
            bucket, obj, created = h_manager.create_empty_obj(
                bucket_name=bucket_or_name, obj_path=obj_key, user=request.user)
        else:
            bucket = bucket_or_name
            collection_name = bucket.get_bucket_table_name()
            obj, created = h_manager.get_or_create_obj(collection_name, obj_key)

        # 访问权限
        acl_choices = {'private': BucketFileBase.SHARE_ACCESS_NO,
                       'public-read': BucketFileBase.SHARE_ACCESS_READONLY,
                       'public-read-write': BucketFileBase.SHARE_ACCESS_READWRITE}
        x_amz_acl = request.headers.get('X-Amz-Acl', 'private').lower()
        if x_amz_acl not in acl_choices:
            raise exceptions.S3InvalidRequest(f'The value {x_amz_acl} of header "x-amz-acl" is not supported.')

        if x_amz_acl != 'private':
            share_code = acl_choices[x_amz_acl]
            obj.set_shared(share=share_code)

        rados = self.build_object_rados(bucket=bucket, obj=obj)
        if created is False:  # 对象已存在，不是新建的
            try:
                h_manager._pre_reset_upload(bucket=bucket, obj=obj, rados=rados)  # 重置对象大小
            except Exception as exc:
                raise exceptions.S3InvalidRequest(f'reset object error, {str(exc)}')

        return bucket, obj, rados, created

    @staticmethod
    def build_object_rados(bucket, obj):
        pool_name = bucket.get_pool_name()
        obj_key = obj.get_obj_key(bucket.id)
        return build_harbor_object(using=bucket.ceph_using, pool_name=pool_name, obj_id=obj_key, obj_size=obj.si)
