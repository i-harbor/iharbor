import base64

from rest_framework import status
from rest_framework.response import Response
from rest_framework.exceptions import UnsupportedMediaType

from utils.oss.pyrados import RadosError
from utils.md5 import EMPTY_HEX_MD5, EMPTY_BYTES_MD5
from utils.storagers import FileUploadToCephHandler, try_close_file
from s3.viewsets import S3CustomGenericViewSet
from s3 import exceptions
from s3.harbor import HarborManager
from . import s3object


class PutObjectHandler:
    @staticmethod
    def create_dir(request, view: S3CustomGenericViewSet):
        bucket_name = view.get_bucket_name(request)
        dir_path_name = view.get_obj_path_name(request)

        if not dir_path_name:
            return view.exception_response(request, exceptions.S3InvalidSuchKey())

        content_length = request.headers.get('Content-Length', None)
        if content_length is None:
            return view.exception_response(request, exceptions.S3MissingContentLength())

        try:
            content_length = int(content_length)
        except Exception:
            return view.exception_response(request, exceptions.S3InvalidContentLength())

        if content_length != 0:
            return view.exception_response(request, exceptions.S3InvalidContentLength())

        hm = HarborManager()
        bucket = hm.get_user_own_bucket(name=bucket_name, user=request.user)
        if not bucket:
            return view.exception_response(request, exceptions.S3NoSuchBucket())

        table_name = bucket.get_bucket_table_name()
        try:
            hm.create_path(table_name=table_name, path=dir_path_name)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        return Response(status=status.HTTP_200_OK, headers={'ETag': EMPTY_HEX_MD5})

    @staticmethod
    def delete_dir(request, view: S3CustomGenericViewSet):
        bucket_name = view.get_bucket_name(request)
        dir_path_name = view.get_obj_path_name(request)
        if not dir_path_name:
            return view.exception_response(request, exceptions.S3InvalidSuchKey())

        try:
            HarborManager().rmdir(bucket_name=bucket_name, dirpath=dir_path_name, user=request.user)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        return Response(status=status.HTTP_204_NO_CONTENT)

    def put_object(self, request, view: S3CustomGenericViewSet):
        try:
            bucket, obj, rados, created = s3object.create_object_metadata(request=request, view=view)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        return self.put_object_handle(request=request, view=view, bucket=bucket, obj=obj, rados=rados, created=created)

    def put_object_handle(self, request, view: S3CustomGenericViewSet, bucket, obj, rados, created):
        pool_name = bucket.get_pool_name()
        obj_key = obj.get_obj_key(bucket.id)
        uploader = FileUploadToCephHandler(using=bucket.ceph_using, request=request,
                                           pool_name=pool_name, obj_key=obj_key)
        request.upload_handlers = [uploader]

        def clean_put(_uploader, _obj, _created):
            # 删除数据和元数据
            f = getattr(_uploader, 'file', None)
            if f is not None:
                try_close_file(f)

            s = f.size if f else 0
            try:
                rados.delete(obj_size=s)
            except Exception:
                pass
            if _created:
                _obj.do_delete()

        try:
            view.kwargs['filename'] = 'filename'
            put_data = request.data
        except UnsupportedMediaType:
            clean_put(uploader, obj, created)
            return view.exception_response(request, exceptions.S3UnsupportedMediaType())
        except RadosError as e:
            clean_put(uploader, obj, created)
            return view.exception_response(request, exceptions.S3InternalError(extend_msg=str(e)))
        except Exception as exc:
            clean_put(uploader, obj, created)
            return view.exception_response(request, exceptions.S3InvalidRequest(extend_msg=str(exc)))

        file = put_data.get('file')
        if not file:
            content_length = view.request.headers.get('Content-Length', None)
            try:
                content_length = int(content_length)
            except Exception:
                clean_put(uploader, obj, created)
                return view.exception_response(request, exceptions.S3MissingContentLength())

            # 是否是空对象
            if content_length != 0:
                clean_put(uploader, obj, created)
                return view.exception_response(request, exceptions.S3InvalidRequest('Request body is empty.'))

            bytes_md5 = EMPTY_BYTES_MD5
            obj_md5 = EMPTY_HEX_MD5
            obj_size = 0
        else:
            bytes_md5 = file.md5_handler.digest()
            obj_md5 = file.file_md5
            obj_size = file.size

        content_b64_md5 = view.request.headers.get('Content-MD5', '')
        if content_b64_md5:
            base64_md5 = base64.b64encode(bytes_md5).decode('ascii')
            if content_b64_md5 != base64_md5:
                # 删除数据和元数据
                clean_put(uploader, obj, created)
                return view.exception_response(request, exceptions.S3BadDigest())

        try:
            obj.si = obj_size
            obj.md5 = obj_md5
            obj.save(update_fields=['si', 'md5'])
        except Exception as e:
            # 删除数据和元数据
            clean_put(uploader, obj, created)
            return view.exception_response(request, exceptions.S3InternalError('更新对象元数据错误'))

        try_close_file(file)

        headers = {'ETag': obj_md5}
        x_amz_acl = request.headers.get('x-amz-acl', None)
        if x_amz_acl:
            headers['X-Amz-Acl'] = x_amz_acl
        return Response(status=status.HTTP_200_OK, headers=headers)
