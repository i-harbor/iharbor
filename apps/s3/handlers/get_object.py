import json
import re

from django.http import FileResponse
from django.utils.http import urlquote
from django.utils.translation import gettext as _
from rest_framework import status

from utils.md5 import EMPTY_HEX_MD5
from s3.viewsets import S3CustomGenericViewSet
from s3 import exceptions
from s3.harbor import HarborManager, MultipartUploadManager
from s3 import serializers
from .s3object import has_object_access_permission, check_precondition_if_headers


class GetObjectHandler:
    def s3_get_object(self, request, view: S3CustomGenericViewSet):
        bucket_name = view.get_bucket_name(request)
        s3_obj_key = view.get_s3_obj_key(request)
        obj_path_name = s3_obj_key.strip('/')

        part_number = request.query_params.get('partNumber', None)
        header_range = request.headers.get('range', None)
        if part_number is not None and header_range is not None:
            return view.exception_response(request, exceptions.S3InvalidRequest())

        # 存储桶验证和获取桶对象
        hm = HarborManager()
        try:
            bucket, fileobj = hm.get_bucket_and_obj_or_dir(bucket_name=bucket_name, path=obj_path_name,
                                                           user=request.user, all_public=True)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

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

            response = GetObjectHandler.s3_get_object_dir(fileobj)
        elif part_number is not None:
            # return view.exception_response(request, exceptions.S3NotImplemented(
            #     message='GetObject not implemented param "partNumber"'))
            try:
                part_number = int(part_number)
                response = self.s3_get_object_part_response(
                    bucket=bucket, obj=fileobj, part_number=part_number)
            # except ValueError:
            #     return view.exception_response(request, exceptions.S3InvalidArgument(message=_('无效的参数partNumber.')))
            except exceptions.S3Error as e:
                return view.exception_response(request, e)
        else:
            try:
                response = self.s3_get_object_range_or_whole_response(
                    request=request, bucket=bucket, obj=fileobj)
            except exceptions.S3Error as e:
                return view.exception_response(request, e)

        upt = fileobj.upt if fileobj.upt else fileobj.ult
        etag = response['ETag']
        try:
            GetObjectHandler.head_object_precondition_if_headers(request, obj_upt=upt, etag=etag)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        # 用户设置的参数覆盖
        response_content_disposition = request.query_params.get('response-content-disposition', None)
        response_content_type = request.query_params.get('response-content-type', None)
        response_content_encoding = request.query_params.get('response-content-encoding', None)
        response_content_language = request.query_params.get('response-content-language', None)
        if response_content_disposition:
            response['Content-Disposition'] = response_content_disposition
        if response_content_encoding:
            response['Content-Encoding'] = response_content_encoding
        if response_content_language:
            response['Content-Language'] = response_content_language
        if response_content_type:
            response['Content-Type'] = response_content_type

        response['x-amz-storage-class'] = 'STANDARD'

        return response

    @staticmethod
    def s3_get_object_dir(obj):
        """
        获取的是一个目录
        :return:
            Response()
        """
        last_modified = obj.upt if obj.upt else obj.ult
        response = FileResponse(b'')
        response['Content-Length'] = 0
        response['ETag'] = f'"{EMPTY_HEX_MD5}"'
        response['Last-Modified'] = serializers.time_to_gmt(last_modified)
        response['Accept-Ranges'] = 'bytes'  # 接受类型，支持断点续传
        response['Content-Type'] = 'application/x-directory; charset=UTF-8'  # 注意格式, dir
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

    @staticmethod
    def get_object_part(bucket, obj_id: int, part_number: int):  # 将 obj_id 修改成 obj实例
        """
        获取对象一个part元数据

        :return:
            part
            None    # part_number == 1时，非多部分对象

        :raises: S3Error
        """
        if not (1 <= part_number <= 10000):
            raise exceptions.S3InvalidPartNumber()

        return None

        # if part_number == 1:
        #     return None
        #
        # raise exceptions.S3InvalidPartNumber()

    def s3_get_object_part_response(self, bucket, obj, part_number: int):
        """
        读取对象一个part的响应

        :return:
            Response()

        :raises: S3Error
        """
        obj_size = obj.si
        # 读取的对象是否具有多部分上传记录
        part = self.is_s3_multipart_object(bucket=bucket, obj=obj)
        if part is None:
            # 对象没有多部分上传，就不存在part_number
            raise exceptions.S3InvalidRequest()

        offset = (part_number - 1) * part.chunk_size
        size = 0
        end = 0

        if part.part_num != part_number:
            # 不是最后一块
            size = part.chunk_size
            end = offset + size - 1
        else:
            part_info = json.loads(part.part_json)['Parts'][-1]
            size = part_info['Size']
            end = offset + size - 1
        generator = HarborManager()._get_obj_generator(bucket=bucket, obj=obj, offset=offset, end=end)
        response = FileResponse(generator, status=status.HTTP_206_PARTIAL_CONTENT)
        response['Content-Length'] = end - offset + 1
        response['ETag'] = part.obj_etag
        response['x-amz-mp-parts-count'] = part.part_num
        response['Content-Range'] = f'bytes {offset}-{end}/{obj_size}'

        # part = self.get_object_part(bucket=bucket, obj_id=obj.id, part_number=part_number)
        # if part:
        #
        #     offset = part.obj_offset
        #     size = part.size
        #     end = offset + size - 1
        #     generator = HarborManager()._get_obj_generator(bucket=bucket, obj=obj, offset=offset, end=end)
        #     response = FileResponse(generator, status=status.HTTP_206_PARTIAL_CONTENT)
        #     response['Content-Length'] = end - offset + 1
        #     response['ETag'] = part.obj_etag
        #     response['x-amz-mp-parts-count'] = part.parts_count
        #     response['Content-Range'] = f'bytes {offset}-{end}/{obj_size}'
        # else:   # 非多部分对象
        #     generator = HarborManager()._get_obj_generator(bucket=bucket, obj=obj)
        #     response = FileResponse(generator)
        #     response['Content-Length'] = obj_size
        #     response['ETag'] = obj.md5
        #     if obj_size > 0:
        #         end = max(obj_size - 1, 0)
        #         response['Content-Range'] = f'bytes {0}-{end}/{obj_size}'

        last_modified = obj.upt if obj.upt else obj.ult
        filename = urlquote(obj.name)  # 中文文件名需要
        response['Last-Modified'] = serializers.time_to_gmt(last_modified)
        response['Accept-Ranges'] = 'bytes'  # 接受类型，支持断点续传
        response['Content-Type'] = 'binary/octet-stream'  # 注意格式
        response['Content-Disposition'] = f"attachment;filename*=utf-8''{filename}"  # 注意filename 这个是下载后的名字

        return response

    def s3_get_object_range_or_whole_response(self, request, bucket, obj):
        """
        读取对象指定范围或整个对象

        :return:
            Response()

        :raises: S3Error
        """
        obj_size = obj.si
        filename = obj.name
        hm = HarborManager()
        ranges = request.headers.get('range', None)
        if ranges is not None:  # 是否是断点续传部分读取
            offset, end = self.get_object_offset_and_end(ranges, filesize=obj_size)

            generator = hm._get_obj_generator(bucket=bucket, obj=obj, offset=offset, end=end)
            response = FileResponse(generator, status=status.HTTP_206_PARTIAL_CONTENT)
            response['Content-Range'] = f'bytes {offset}-{end}/{obj_size}'
            response['Content-Length'] = end - offset + 1
        else:
            generator = hm._get_obj_generator(bucket=bucket, obj=obj)
            response = FileResponse(generator)
            response['Content-Length'] = obj_size

            # 增加一次下载次数
            obj.download_cound_increase()

        # multipart object check
        part = self.is_s3_multipart_object(bucket=bucket, obj=obj)

        if part:        #
            response['ETag'] = part.obj_etag
            response['x-amz-mp-parts-count'] = part.part_num
        else:
            response['ETag'] = obj.md5
        # response['ETag'] = obj.md5

        last_modified = obj.upt if obj.upt else obj.ult
        filename = urlquote(filename)  # 中文文件名需要

        response['Last-Modified'] = serializers.time_to_gmt(last_modified)
        response['Accept-Ranges'] = 'bytes'  # 接受类型，支持断点续传
        response['Content-Type'] = 'binary/octet-stream'  # 注意格式
        response['Content-Disposition'] = f"attachment;filename*=utf-8''{filename}"  # 注意filename 这个是下载后的名字
        return response

    @staticmethod
    def get_object_offset_and_end(h_range: str, filesize: int):
        """
        获取读取开始偏移量和结束偏移量

        :param h_range: range Header
        :param filesize: 对象大小
        :return:
            (offset:int, end:int)

        :raise S3Error
        """
        start, end = GetObjectHandler.parse_header_range(h_range)
        if start is None and end is None:
            raise exceptions.S3InvalidRange()

        if isinstance(start, int):
            if start >= filesize or start < 0:
                raise exceptions.S3InvalidRange()

        end_max = filesize - 1
        # 读最后end个字节
        if (start is None) and isinstance(end, int):
            offset = max(filesize - end, 0)
            end = end_max
        else:
            offset = start
            if isinstance(end, int):
                end = min(end, end_max)
            else:
                end = end_max

        return offset, end

    @staticmethod
    def parse_header_range(h_range: str):
        """
        parse Range header string

        :param h_range: 'bytes={start}-{end}'  下载第M－N字节范围的内容
        :return: (M, N)
            start: int or None
            end: int or None
        """
        m = re.match(r'bytes=(\d*)-(\d*)', h_range)
        if not m:
            return None, None
        items = m.groups()

        start = int(items[0]) if items[0] else None
        end = int(items[1]) if items[1] else None
        if isinstance(start, int) and isinstance(end, int) and start > end:
            return None, None
        return start, end

    def is_s3_multipart_object(self, bucket, obj):
        """
        对象是否存在多部分上传数据
        :param bucket:
        :param obj:
        :return:
        """
        upload = MultipartUploadManager().get_multipart_upload_by__bucket_obj(bucket=bucket, obj=obj)
        if upload:
            return upload
        else:
            return None


