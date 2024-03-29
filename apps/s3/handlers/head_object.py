from django.utils.translation import gettext as _
from rest_framework.response import Response
from rest_framework import status

from utils.md5 import EMPTY_HEX_MD5
from s3.viewsets import S3CustomGenericViewSet
from s3 import exceptions
from s3.harbor import HarborManager, MultipartUploadManager
from s3 import serializers
from .s3object import has_object_access_permission, check_precondition_if_headers
from .get_object import GetObjectHandler


class HeadObjectHandler:
    def head_object(self, request, view: S3CustomGenericViewSet):
        bucket_name = view.get_bucket_name(request)
        s3_obj_key = view.get_s3_obj_key(request)
        obj_path_name = s3_obj_key.strip('/')
        part_number = request.query_params.get('partNumber', None)
        ranges = request.headers.get('range', None)

        if ranges is not None and part_number is not None:
            return view.exception_response(request, exceptions.S3InvalidRequest())

        # 存储桶验证和获取桶对象
        hm = HarborManager()
        try:
            bucket, fileobj = hm.get_bucket_and_obj_or_dir(
                bucket_name=bucket_name, path=obj_path_name, user=request.user, all_public=True
            )
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        # 桶锁操作检查
        if not bucket.lock_readable():
            return view.exception_response(request, exc=exceptions.S3BucketLockRead())

        if fileobj is None:
            return view.exception_response(request, exceptions.S3NoSuchKey())

        if s3_obj_key.endswith('/'):  # dir
            if fileobj.is_file():
                return view.exception_response(request, exceptions.S3NoSuchKey())

            is_dir = True
        else:                          # object
            if not fileobj.is_file():
                return view.exception_response(request, exceptions.S3NoSuchKey())

            is_dir = False

        # 是否有文件对象的访问权限
        try:
            has_object_access_permission(request=request, bucket=bucket, obj=fileobj)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        if is_dir:
            if part_number is not None and part_number != '1':
                return view.exception_response(
                    request, exceptions.S3InvalidArgument(message=_('无效的参数partNumber.')))

            ranges = request.headers.get('range', None)
            if ranges is not None:
                return view.exception_response(request, exceptions.S3InvalidRange())

            response = self.s3_head_object_dir(fileobj)
        elif part_number is not None or ranges is not None:
            if part_number:
                try:
                    part_number = int(part_number)
                except ValueError:
                    return view.exception_response(
                        request, exceptions.S3InvalidArgument(message=_('无效的参数partNumber.')))

            try:
                response = self.head_object_part_or_range_response(
                    bucket=bucket, obj=fileobj, part_number=part_number, header_range=ranges)
            except exceptions.S3Error as e:
                return view.exception_response(request, e)
        else:
            try:
                response = self.head_object_common_response(bucket=bucket, obj=fileobj)
            except exceptions.S3Error as e:
                return view.exception_response(request, e)

        upt = fileobj.upt if fileobj.upt else fileobj.ult
        etag = response['ETag']
        try:
            HeadObjectHandler.head_object_precondition_if_headers(request, obj_upt=upt, etag=etag)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        # 防止标头Content-Type被渲染器覆盖
        response.content_type = response['Content-Type'] if response.has_header('Content-Type') else None
        return response

    @staticmethod
    def head_object_precondition_if_headers(request, obj_upt, etag: str):
        """
        标头if条件检查

        :param request:
        :param obj_upt: 对象最后修改时间
        :param etag: 对象etag
        :return: None
        :raises: S3Error
        """
        check_precondition_if_headers(
            headers=request.headers, last_modified=obj_upt, etag=etag,
            key_match='If-Match',
            key_none_match='If-None-Match',
            key_modified_since='If-Modified-Since',
            key_unmodified_since='If-Unmodified-Since'
        )

    def head_object_no_multipart_response(self, obj, status_code: int = 200, headers=None):
        """
        非多部分对象head响应
        """
        h = self.head_object_common_headers(obj=obj)
        if headers:
            h.update(headers)

        return Response(status=status_code, headers=h)

    def head_object_common_response(self, bucket, obj):
        """
        对象head响应，会检测对象是否是多部分对象
        :raises: S3Error
        """
        # multipart object check
        parts_qs = MultipartUploadManager().get_multipart_upload_by_bucket_obj(bucket=bucket, obj=obj)
        if parts_qs:
            headers = self.head_object_common_headers(obj=obj, part=parts_qs)
        else:
            headers = self.head_object_common_headers(obj=obj, part=None)

        return Response(status=200, headers=headers)

    @staticmethod
    def head_object_common_headers(obj, part=None):
        last_modified = obj.upt if obj.upt else obj.ult
        headers = {
            'Content-Length': obj.si,
            'Last-Modified': serializers.time_to_gmt(last_modified),
            'Accept-Ranges': 'bytes',  # 接受类型，支持断点续传
            'Content-Type': 'binary/octet-stream'
        }

        if part:
            headers['ETag'] = part.obj_etag
            headers['x-amz-mp-parts-count'] = part.parts_count
        else:
            headers['ETag'] = obj.md5

        return headers

    def head_object_part_or_range_response(self, bucket, obj, part_number: int, header_range: str):
        """
        head对象指定部分编号或byte范围

        :param bucket: 桶实例
        :param obj: 对象元数据实例
        :param part_number: int or None
        :param header_range: str or None
        :return:

        :raises: S3Error
        """
        obj_size = obj.si
        response = Response(status=status.HTTP_206_PARTIAL_CONTENT)

        if header_range:
            offset, end = GetObjectHandler.get_object_offset_and_end(header_range, filesize=obj_size)

            # multipart object check
            upload = MultipartUploadManager.get_multipart_upload_by_bucket_obj(bucket=bucket, obj=obj)
            if upload:
                response['ETag'] = upload.obj_etag
                response['x-amz-mp-parts-count'] = upload.parts_count
            else:
                response['ETag'] = obj.md5

        elif part_number:
            upload = MultipartUploadManager.get_multipart_upload_by_bucket_obj(bucket=bucket, obj=obj)
            if upload:
                part, index = upload.get_part_by_number(number=part_number)
            else:
                part = None

            if not part:
                content_range = f'bytes 0-{obj_size-1}/{obj_size}'
                return self.head_object_no_multipart_response(obj, status_code=status.HTTP_206_PARTIAL_CONTENT,
                                                              headers={'Content-Range': content_range})
            response['ETag'] = upload.obj_etag
            response['x-amz-mp-parts-count'] = upload.parts_count
            offset = upload.get_part_offset(part_number=part_number)
            size = part['Size']
            end = offset + size - 1
        else:
            raise exceptions.S3InvalidRequest()

        last_modified = obj.upt if obj.upt else obj.ult
        response['Content-Range'] = f'bytes {offset}-{end}/{obj_size}'
        response['Content-Length'] = end - offset + 1
        response['Last-Modified'] = serializers.time_to_gmt(last_modified)
        response['Accept-Ranges'] = 'bytes'  # 接受类型，支持断点续传
        response['Content-Type'] = 'binary/octet-stream'  # 注意格式
        return response

    @staticmethod
    def s3_head_object_dir(obj):
        """
        head的是一个目录
        :return:
            Response()
        """
        last_modified = obj.upt if obj.upt else obj.ult
        response = Response(status=status.HTTP_200_OK)
        response['Content-Length'] = 0
        response['ETag'] = f'"{EMPTY_HEX_MD5}"'
        response['Last-Modified'] = serializers.time_to_gmt(last_modified)
        response['Accept-Ranges'] = 'bytes'  # 接受类型，支持断点续传
        # response['Content-Type'] = 'application/x-directory; charset=UTF-8'  # 注意格式, dir
        response['Content-Type'] = 'binary/octet-stream'
        return response
