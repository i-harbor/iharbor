import json
import time
from datetime import datetime
from pytz import utc

from django.utils import timezone
from django.conf import settings
from django.utils.translation import gettext
from django.db import DatabaseError, transaction
from buckets.models import BucketFileBase
from s3.harbor import HarborManager, MultipartUploadManager
from s3.viewsets import S3CustomGenericViewSet
from s3 import exceptions, renders, paginations, serializers
from s3.multiparts import MultipartPartsManager
from s3.models import MultipartUpload
from utils import storagers

from rest_framework.response import Response
from rest_framework import status

from utils.storagers import try_close_file

GMT_FORMAT = '%a, %d %b %Y %H:%M:%S GMT'


def datetime_from_gmt(value):
    """
    :param value: gmt格式时间字符串
    :return:
        datetime() or None
    """
    try:
        t = datetime.strptime(value, GMT_FORMAT)
        return t.replace(tzinfo=utc)
    except Exception as e:
        return None


class MultipartUploadHandler:

    def create_multipart_upload(self, request, view: S3CustomGenericViewSet):
        bucket_name = view.get_bucket_name(request)
        key = view.get_s3_obj_key(request)
        expires = request.headers.get('Expires', None)
        # 访问权限  'public-read': BucketFileBase.SHARE_ACCESS_READONLY 公有读不赋予上传权限
        acl_choices = {'private': BucketFileBase.SHARE_ACCESS_NO,
                       'public-read-write': BucketFileBase.SHARE_ACCESS_READWRITE,
                       'public-read': BucketFileBase.SHARE_ACCESS_READONLY
                       }
        x_amz_acl = request.headers.get('X-Amz-Acl', 'private').lower()

        if x_amz_acl not in acl_choices:
            return view.exception_response(
                request, exc=exceptions.S3InvalidRequest(message=gettext(f'The value {x_amz_acl} '
                                                                         f'of header "x-amz-acl" is not supported.')))

        obj_perms_code = acl_choices[x_amz_acl]

        if obj_perms_code != BucketFileBase.SHARE_ACCESS_NO:
            return view.exception_response(
                request, exc=exceptions.S3InvalidRequest(message=gettext(f'The value {x_amz_acl} '
                                                                         f'of header "x-amz-acl" is no permission.')))

        hm = HarborManager()
        # 查看桶和对象是否存在
        try:
            bucket, obj = hm.get_bucket_and_obj_or_dir(bucket_name=bucket_name, path=key)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        bucket_table = bucket.get_bucket_table_name()
        # 多部分上传表的查询：
        upload_data = MultipartUpload.objects.filter(bucket_name=bucket.name, bucket_id=bucket.id, obj_key=key).first()

        if upload_data:
            self.rest_or_create_object(bucket=bucket, obj=obj, hm=hm, key=key, bucket_table=bucket_table)
            # 删除重新创建
            upload_data.delete()
            upload_data = hm.create_multipart_data(bucket_id=bucket.id, bucket_name=bucket.name, obj=obj,
                                                   key=key, obj_perms_code=obj_perms_code)
            return self.create_multipart_upload_response_handler(request=request, view=view,
                                                                 bucket_name=bucket.name,
                                                                 key=key, upload_id=upload_data.id)
        # upload_data is None
        obj = self.rest_or_create_object(bucket=bucket, obj=obj, hm=hm, key=key, bucket_table=bucket_table)
        upload_data = hm.create_multipart_data(bucket_id=bucket.id, bucket_name=bucket.name, obj=obj,
                                               key=key, obj_perms_code=obj_perms_code)

        return self.create_multipart_upload_response_handler(request=request, view=view,
                                                             bucket_name=bucket.name,
                                                             key=key, upload_id=upload_data.id)

    def create_multipart_upload_response_handler(self, request, view, bucket_name, key, upload_id):
        view.set_renderer(request, renders.CusXMLRenderer(root_tag_name='InitiateMultipartUploadResult'))
        data = {
            'Bucket': bucket_name,
            'Key': key,
            'UploadId': upload_id
        }
        return Response(data=data, status=status.HTTP_200_OK)

    def upload_part(self, request, view: S3CustomGenericViewSet):
        # 先上传第一块， 合并检查块大小，创建多部md5, 合并时 计算块数量，
        bucket_name = view.get_bucket_name(request)
        content_length = request.headers.get('Content-Length', 0)
        part_num = request.query_params.get('partNumber', None)
        upload_id = request.query_params.get('uploadId', None)
        obj_key = view.get_s3_obj_key(request)

        # int
        content_length, part_num = self.upload_part_handler_header(content_length=content_length,
                                                                   part_num=part_num, view=view, request=request)

        try:
            # 如果终止上传 报错
            upload, bucket = self.get_upload_and_bucket(request=request, upload_id=upload_id,
                                                        bucket_name=bucket_name, view=view)
        except exceptions.S3Error as e:
            return view.exception_response(request, exc=e)

        # 检查上传状态
        if upload.status != MultipartUpload.UploadStatus.UPLOADING.value:
            return view.exception_response(request, exc=exceptions.S3CompleteMultipartAlreadyInProgress())

        # 检查是否上传第一块
        if not upload.check_first_part(num=part_num):
            return view.exception_response(request, exc=exceptions.S3InvalidRequest(
                                                        message=gettext('Upload the first block first')))

        hm = HarborManager()
        offset = 0

        if part_num != 1:
            offset = (part_num - 1) * upload.chunk_size
            # 如果重传的大小与原先的不一致， 必须先修改第一块,重传文件
            parts = upload.parts_object()  #list
            part_info, index = MultipartPartsManager().query_part_info(num=part_num, parts=parts)
            if part_info and content_length != part_info['Size']:
                return view.exception_response(request, exc=exceptions.S3InvalidPart(
                    message=gettext('The block information is different from the original block size. '
                                    'Please upload the block again or retransmit the file')))

        obj = hm.get_object(bucket_name=bucket_name, path_name=obj_key, user=request.user)
        ceph_obj_key = obj.get_obj_key(bucket.id)
        uploader = storagers.PartUploadToCephHandler(request=request, using=bucket.ceph_using,
                                                     pool_name=bucket.pool_name, obj_key=ceph_obj_key, offset=offset)
        request.upload_handlers = [uploader]
        view.kwargs['filename'] = 'filename'
        put_data = request.data
        file = put_data.get('file')
        part_md5 = file.file_md5
        part_size = file.size
        part_etag = self.upload_part_handler(request=request, view=view, upload=upload, part_num=part_num,
                                             bucket=bucket, obj=obj, uploader=uploader, hm=hm, part_md5=part_md5,
                                             part_size=part_size)
        data = {'ETag': part_etag}
        return Response(headers=data, status=status.HTTP_200_OK)

    def upload_part_handler(self, request, view, upload, part_num, bucket, obj, uploader, hm, part_md5, part_size):

        def clean_put(_uploader):
            # 删除数据
            f = getattr(_uploader, 'file', None)
            if f is not None:
                try_close_file(f)
                try:
                    f.delete()
                except Exception:
                    pass

        part = {'PartNumber': part_num, 'lastModified': timezone.now(), 'ETag': part_md5, 'Size': part_size}

        try:
            with transaction.atomic(using='metadata'):
                upload = MultipartUpload.objects.select_for_update().get(id=upload.id)
                flag = upload.insert_part(part_num, part, part_size)
                if flag:
                    obj.si += part_size
                hm._update_obj_metadata(obj=obj, size=obj.si)
        except DatabaseError as e:
            clean_put(uploader)
            return view.exception_response(request, e)

        return part_md5

    def complete_multipart_upload(self, request, view: S3CustomGenericViewSet, upload_id):
        """
        完成多部分上传处理
        """
        bucket_name = view.get_bucket_name(request)
        obj_key = view.get_s3_obj_key(request)
        complete_parts_list = self.handler_complete_multipart_handers(request=request, view=view)

        try:
            upload, bucket = self.get_upload_and_bucket(request=request, upload_id=upload_id, bucket_name=bucket_name,
                                                        view=view)
        except exceptions.S3Error as e:
            return view.exception_response(request, exc=e)

        if upload.status == MultipartUpload.UploadStatus.COMPLETED.value:
            return view.exception_response(request, exc=exceptions.S3NoSuchUpload())

        # 查看文件是否存在
        hm = HarborManager()

        try:
            obj = hm.get_object(bucket_name=bucket_name, path_name=obj_key, user=request.user)
        except exceptions.S3Error as e:
            # 文件不存在
            return view.exception_response(request, exc=exceptions.S3NotFound())

        # 组合状态
        if upload:
            upload.status = MultipartUpload.UploadStatus.COMPOSING.value
            upload.save(update_fields=['status'])

        try:
            mmanger = MultipartUploadManager()
            complete_parts_dict, complete_part_numbers = mmanger.handle_validate_complete_parts(complete_parts_list)
            # 获取需要组合的所有part元数据和对象ETag，和没有用到的part元数据列表
            obj_etag = mmanger.get_upload_parts_and_validate(
                bucket=bucket, upload=upload, complete_parts=complete_parts_dict,
                complete_numbers=complete_part_numbers
            )
        except exceptions.S3Error as e:
            # 如果组合失败 返回原状态
            upload.status = MultipartUpload.UploadStatus.UPLOADING.value
            upload.save(update_fields=['status'])
            return view.exception_response(request, exc=e)

        location = request.build_absolute_uri()
        data = {'Location': location, 'Bucket': bucket.name, 'Key': obj.na, 'ETag': obj_etag}
        view.set_renderer(request, renders.CommonXMLRenderer(root_tag_name='ListMultipartUploadsResult'))
        return Response(data=data, status=status.HTTP_200_OK)

    def abort_multipart_upload(self, request, view: S3CustomGenericViewSet, upload_id):
        bucket_name = view.get_bucket_name(request)
        try:
            upload, bucket = self.get_upload_and_bucket(request=request, upload_id=upload_id, bucket_name=bucket_name,
                                                        view=view)
        except exceptions.S3Error as e:
            return view.exception_response(request, exc=e)

        if upload.status != MultipartUpload.UploadStatus.UPLOADING.value:
            return view.exception_response(request, exc=exceptions.S3NoSuchUpload())

        upload_time = int(time.mktime(upload.last_modifiedtimetuple()))
        obj_time = int(time.mktime(upload.part_json['ObjectCreateTime'].timetuple()))

        # 终止上传 删除文件对象
        mmanger = MultipartUploadManager()

        try:
            # 删除多部分上传数据
            is_del_obj = mmanger.delete_multipart_upload(upload_id=upload_id)
        except exceptions.S3Error as e:
            return view.exception_response(request, exc=exceptions.S3Error)

        hm = HarborManager()

        if obj_time <= upload_time:
            try:
                # 删除元数据 （误删）obj 时间戳
                obj = hm.delete_object(bucket_name=bucket_name, obj_path=upload.obj_key, user=request.user)
            except exceptions.S3Error as e:
                return view.exception_response(request, e)

        return Response(status=status.HTTP_204_NO_CONTENT)

    def list_multipart_upload(self, request, view: S3CustomGenericViewSet):
        """
        列出正在进行的分段上传
        """

        delimiter = request.query_params.get('delimiter', None)
        prefix = request.query_params.get('prefix', None)
        encoding_type = request.query_params.get('encoding-type', None)
        x_amz_expected_bucket_owner = request.headers.get('x-amz-expected-bucket-owner', None)
        bucket_name = view.get_bucket_name(request)

        if delimiter is not None:
            return view.exception_response(request, exc=exceptions.S3InvalidArgument(
                                               message=gettext('参数“delimiter”暂时不支持')))

        try:
            bucket = HarborManager().get_public_or_user_bucket(name=bucket_name, user=request.user)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        self.handler_expected_bucket_owner(x_amz_expected_bucket_owner=x_amz_expected_bucket_owner,
                                           bucket_id=bucket.id, request=request, view=view)
        mmanger = MultipartUploadManager()
        queryset = mmanger.list_multipart_uploads_queryset(bucket_name=bucket_name, prefix=prefix)
        paginator = paginations.ListUploadsKeyPagination(context={'bucket': bucket})
        ret_data = {
            'Bucket': bucket_name,
            'Prefix': prefix
        }

        if encoding_type:
            ret_data['EncodingType'] = encoding_type

        ups = paginator.paginate_queryset(queryset, request=request)
        serializer = serializers.ListMultipartUploadsSerializer(ups, many=True, context={'user': request.user})
        data = paginator.get_paginated_data()
        ret_data.update(data)
        ret_data['Upload'] = serializer.data
        view.set_renderer(request, renders.CommonXMLRenderer(root_tag_name='ListMultipartUploadsResult'))
        return Response(data=ret_data, status=status.HTTP_200_OK)

    def list_parts(self, request, view: S3CustomGenericViewSet):
        bucket_name = view.get_bucket_name(request)
        obj_key = view.get_s3_obj_key(request)
        upload_id = request.query_params.get('uploadId', None)
        max_parts = request.query_params.get('max-parts', None)
        part_number_marker = request.query_params.get('part-number-marker', None)
        x_amz_expected_bucket_owner = request.headers.get('x-amz-expected-bucket-owner', None)
        hm = HarborManager()

        try:
            bucket, obj = hm.get_bucket_and_obj_or_dir(bucket_name=bucket_name, path=obj_key)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        self.handler_expected_bucket_owner(x_amz_expected_bucket_owner=x_amz_expected_bucket_owner, bucket_id=bucket.id,
                                           request=request, view=view)

        try:
            upload_data = MultipartUploadManager().get_multipart_upload_by_id(upload_id=upload_id)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        try:
            upload_part_json = json.loads(upload_data.part_json)['Parts']
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        data = {'Bucket': bucket.name, 'Key': obj.na, 'UploadId': upload_id}

        if max_parts and part_number_marker:
            # 处理不为空的参数 PartNumberMarker 上一部分最后 NextPartNumberMarker 这一部分最后
            try:
                max_parts = int(max_parts)
            except ValueError:
                return view.exception_response(request, exc=exceptions.S3InvalidArgument('Invalid param MaxParts'))

            try:
                part_number_marker = int(part_number_marker)
            except ValueError:
                return view.exception_response(request, exc=exceptions.S3InvalidArgument(
                                                                'Invalid param PartNumberMarker'))

            upload_part_json = upload_part_json[part_number_marker:max_parts + 1]
            data['PartNumberMarker'] = part_number_marker
            data['NextPartNumberMarker'] = max_parts + 1
            data['MaxParts'] = max_parts

        data['Part'] = upload_part_json
        data['StorageClass'] = 'STANDARD'
        data['Owner'] = {'ID': request.user.id, "DisplayName": request.user.username}
        view.set_renderer(request, renders.CommonXMLRenderer(root_tag_name='ListPartsResult'))
        return Response(data=data, status=status.HTTP_200_OK)

    # ---------------------------------------------------------------------

    def handler_expected_bucket_owner(self, x_amz_expected_bucket_owner, bucket_id, request, view):

        if x_amz_expected_bucket_owner:
            try:
                if bucket_id != int(x_amz_expected_bucket_owner):
                    raise ValueError
            except ValueError:
                return view.exception_response(request, exc=exceptions.S3AccessDenied())

    def handler_complete_multipart_handers(self, request, view):

        try:
            data = request.data
        except Exception as e:
            return view.exception_response(request, exc=exceptions.S3MalformedXML())

        root = data.get('CompleteMultipartUpload')

        if not root:
            return view.exception_response(request, exc=exceptions.S3MalformedXML())

        complete_parts_list = root.get('Part')

        if not complete_parts_list:
            return view.exception_response(request, exc=exceptions.S3MalformedXML())

        # XML解析器行为有关，只有一个part时不是list
        if not isinstance(complete_parts_list, list):
            complete_parts_list = [complete_parts_list]

        return complete_parts_list

    def get_upload_and_bucket(self, request, upload_id: str, bucket_name: str, view: S3CustomGenericViewSet):
        """
        :return:
            upload, bucket
        :raises: S3Error
        """
        obj_path_name = view.get_s3_obj_key(request)
        mu_mgr = MultipartUploadManager()
        upload = mu_mgr.get_multipart_upload_by_id(upload_id=upload_id)

        if not upload:
            raise exceptions.S3NoSuchUpload()

        if upload.obj_key != obj_path_name:
            raise exceptions.S3NoSuchUpload(f'UploadId conflicts with this object key.Please Key "{upload.obj_key}"')

        hm = HarborManager()
        bucket = hm.get_user_own_bucket(name=bucket_name, user=request.user)

        if not bucket:
            raise exceptions.S3NoSuchBucket()

        if not upload.belong_to_bucket(bucket):
            raise exceptions.S3NoSuchUpload(f'UploadId conflicts with this bucket.'
                                            f'Please bucket "{bucket.name}".Maybe the UploadId is created for deleted bucket.')

        return upload, bucket

    def upload_part_handler_header(self, content_length, part_num, view, request):

        try:
            content_length = int(content_length)
        except exceptions.S3Error as e:
            return view.exception_response(request, exc=exceptions.S3InvalidContentLength())

        if content_length == 0:
            return view.exception_response(request, exc=exceptions.S3EntityTooSmall())

        if content_length > settings.S3_MULTIPART_UPLOAD_MAX_SIZE:
            return view.exception_response(request, exc=exceptions.S3EntityTooLarge())

        try:
            part_num = int(part_num)
        except ValueError:
            return view.exception_response(request, exc=exceptions.S3InvalidArgument('Invalid param PartNumber'))

        if not (0 < part_num <= 10000):
            return view.exception_response(request, exc=exceptions.S3InvalidArgument(
                                    'Invalid param PartNumber,must be a positive integer between 1 and 10,000.'))

        return content_length, part_num

    def rest_or_create_object(self, bucket, obj, hm, key, bucket_table):

        if obj:
            # 重置对象
            try:
                hm.s3_reset_upload(bucket=bucket, obj=obj)
            except exceptions.S3Error as e:
                raise exceptions.S3InternalError()
        else:
            obj, _ = hm.get_or_create_obj(table_name=bucket_table, obj_path_name=key)

        return obj
