import json
from datetime import datetime
from pytz import utc

from django.utils import timezone
from django.conf import settings
from django.utils.translation import gettext
from django.db import DatabaseError, transaction
from django.db.models import Case, Value, When, F, BigIntegerField

from buckets.models import BucketFileBase
from s3.harbor import HarborManager, MultipartUploadManager
from s3.viewsets import S3CustomGenericViewSet
from s3 import exceptions, renders, paginations, serializers
from s3.models import MultipartUpload
from utils import storagers
from utils.oss import build_harbor_object

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
                       'public-read-write': BucketFileBase.SHARE_ACCESS_READWRITE}
        x_amz_acl = request.headers.get('X-Amz-Acl', 'private').lower()
        if x_amz_acl not in acl_choices:
            return view.exception_response(
                request, exc=exceptions.S3InvalidRequest(message=gettext(f'The value {x_amz_acl} '
                                                                         f'of header "x-amz-acl" is not supported.')))
        obj_perms_code = acl_choices[x_amz_acl]
        hm = HarborManager()
        # 查看桶和对象是否存在
        try:
            bucket, obj = hm.get_bucket_and_obj_or_dir(bucket_name=bucket_name, path=key)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)
        bucket_table = bucket.get_bucket_table_name()
        if obj is None:
            # # 对象不存在 权限只能是 private创建对象 public-read-write没有创建权限
            # if obj_perms_code != 0:
            #     return view.exception_response(request,
            #                                    exc=exceptions.S3AccessDenied(extend_msg='No create permission'))
            # 对象不存在 - 创建 - MultipartUpload 创建 - 返回
            obj_create, is_create = hm.get_or_create_obj(table_name=bucket_table, obj_path_name=key)

            upload = MultipartUpload(bucket_id=bucket.id, bucket_name=bucket.name, obj_id=obj_create.id, obj_key=key,
                                     key_md5=obj_create.na_md5, obj_perms_code=obj_perms_code)
            upload.save()
            return self.create_multipart_upload_response_handler(request=request, view=view, bucket_name=bucket.name,
                                                                 key=key, upload_id=upload.id)

        # iharbor原来的数据对象存在(查看是否有s3多部分上传记录)
        upload_data = MultipartUpload.objects.filter(bucket_id=bucket.id, obj_id=obj.id, bucket_name=bucket.name,
                                                     obj_key=key, key_md5=obj.na_md5, obj_perms_code=obj_perms_code)
        rados = build_harbor_object(using=bucket.ceph_using, pool_name=bucket.pool_name, obj_id=str(obj.id),
                                    obj_size=obj.si)
        if not upload_data:
            # iharbor 元数据存在 - 重置原数据 - 更新 MultipartUpload 表 - 返回
            try:
                hm._pre_reset_upload(bucket=bucket, obj=obj, rados=rados)  # 重置元对象大小
            except exceptions.S3Error as e:
                # 无法重置，则删除对象重新创建
                # hm.delete_object(bucket_name=bucket_name, obj_path=key, user=request.user)
                return view.exception_response(request, e)
            upload = MultipartUpload(bucket_id=bucket.id, bucket_name=bucket.name,
                                     obj_id=obj.id, obj_key=key, key_md5=obj.na_md5, obj_perms_code=obj_perms_code)
            upload.save()
            return self.create_multipart_upload_response_handler(request=request, view=view, bucket_name=bucket.name,
                                                                 key=key, upload_id=upload.id)

        # 有S3有上传的历史记录 - 上传完成 - 重置对象 - 更新 MultipartUpload 表 - 返回
        #                  - 上传中（组合中） - 返回
        if upload_data.get().status != MultipartUpload.UploadStatus.COMPLETED.value:
            return view.exception_response(
                request, exc=exceptions.S3InvalidRequest(message=gettext(f'The operation on the object is in progress. '
                                                                         f'Do not perform any additional operations.')))
        # 重置对象
        try:
            hm._pre_reset_upload(bucket=bucket, obj=obj, rados=rados)  # 重置对象大小
        except exceptions.S3Error as e:
            # 无法重置，则删除对象重新创建
            # hm.delete_object(bucket_name=bucket_name, obj_path=key, user=request.user)
            return view.exception_response(request, e)

        upload_data.update(key_md5=obj.na_md5, status=MultipartUpload.UploadStatus.UPLOADING.value,
                           obj_perms_code=obj_perms_code,
                           part_num=0, part_json={}, last_modified=timezone.now())

        return self.create_multipart_upload_response_handler(request=request, view=view, bucket_name=bucket.name,
                                                             key=key, upload_id=upload_data.get().id)

    def create_multipart_upload_response_handler(self, request, view, bucket_name, key, upload_id):
        view.set_renderer(request, renders.CusXMLRenderer(root_tag_name='InitiateMultipartUploadResult'))
        data = {
            'Bucket': bucket_name,
            'Key': key,
            'UploadId': upload_id
        }
        return Response(data=data, status=status.HTTP_200_OK)

    def upload_part(self, request, view: S3CustomGenericViewSet):

        bucket_name = view.get_bucket_name(request)
        content_length = request.headers.get('Content-Length', 0)
        part_num = request.query_params.get('partNumber', None)
        upload_id = request.query_params.get('uploadId', None)
        obj_key = view.get_s3_obj_key(request)

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
            return view.exception_response(request, exc=exceptions.S3InvalidArgument('Invalid param PartNumber, '
                                                                                     'must be a positive integer between 1 and 10,000.'))
        try:
            # 如果终止上传 报错
            upload, bucket = self.get_upload_and_bucket(request=request, upload_id=upload_id, bucket_name=bucket_name,
                                                        view=view)
        except exceptions.S3Error as e:
            return view.exception_response(request, exc=e)

        # 检查上传状态
        if upload.status != MultipartUpload.UploadStatus.UPLOADING.value:
            return view.exception_response(request, exc=exceptions.S3CompleteMultipartAlreadyInProgress())

        hm = HarborManager()
        obj = hm.get_object(bucket_name=bucket_name, path_name=obj_key, user=request.user)
        ceph_obj_key = obj.get_obj_key(bucket.id)
        offset = 0
        # part_key = {'Parts': []}
        # 
        # # # 计算块的偏移量 （顺序传块）
        if part_num != 1:
            # 默认最后一块
            # if upload.chunk_size != content_length:
            #     # 默认为最后一块
            #     offset = (part_num - 1) * upload.chunk_size
            offset = (part_num - 1) * upload.chunk_size

        uploader = storagers.PartUploadToCephHandler(request=request, using=bucket.ceph_using,
                                                     pool_name=bucket.pool_name,
                                                     obj_key=ceph_obj_key, offset=offset)
        request.upload_handlers = [uploader]
        view.kwargs['filename'] = 'filename'
        put_data = request.data
        file = put_data.get('file')
        part_md5 = self.upload_part_handler(request=request, view=view, upload=upload, part_num=part_num, bucket=bucket,
                                            obj=obj, uploader=uploader, hm=hm, file=file)
        data = {'ETag': part_md5}
        return Response(headers=data, status=status.HTTP_200_OK)

    def upload_part_handler(self, request, view, upload, part_num, bucket, obj, uploader, hm, file):

        part_key = {'Parts': []}

        def clean_put(_uploader):
            # 删除数据
            f = getattr(_uploader, 'file', None)
            if f is not None:
                try_close_file(f)
                try:
                    f.delete()
                except Exception:
                    pass

        part_md5 = file.file_md5
        part_size = file.size
        part = {'PartNumber': part_num, 'lastModified': timezone.now(), 'ETag': part_md5, 'Size': part_size}
        if upload.part_json:
            # 非空
            # 如果part_number存在 需要替换
            flag = False
            part_key['Parts'] = json.loads(upload.part_json)['Parts']
            for part_info in part_key['Parts']:
                if part_info['PartNumber'] == part_num:
                    # 考虑不均等分块上传返回错误 i['Size']
                    if part_info['Size'] != part_size:
                        return view.exception_response(request, exc=exceptions.S3Error(
                            message='The upload blocks are inconsistent.'))
                    part_info['ETag'] = part_md5
                    part_info['lastModified'] = timezone.now()
                    flag = True
            if not flag:
                part_key['Parts'].append(part)
                obj.si += part_size

            upload.part_num = len(part_key['Parts'])
            upload.part_json = json.dumps(part_key, cls=DateEncoder)
            new_size = obj.si

                # 最后完成修改时间
            with transaction.atomic():
                try:
                    upload.save(update_fields=['part_json', 'part_num'])
                    hm._update_obj_metadata(obj=obj, size=new_size)
                except DatabaseError as e:
                    clean_put(uploader)
                    return view.exception_response(request, e)

        if not upload.part_json:
            # 防止 创建上传时失败
            if obj.si != 0:
                # 重置大小
                rados = build_harbor_object(using=bucket.ceph_using, pool_name=bucket.pool_name, obj_id=str(obj.id),
                                            obj_size=obj.si)
                try:
                    ok = hm._pre_reset_upload(bucket=bucket, obj=obj, rados=rados)
                except exceptions.S3Error as e:
                    return view.exception_response(request, e)
            part_key['Parts'].append(part)
            upload.part_num = len(part_key['Parts'])
            upload.part_json = json.dumps(part_key, cls=DateEncoder)
            upload.key_md5 = obj.na_md5
            upload.chunk_size = part_size
            obj.si += part_size
            new_size = obj.si
            with transaction.atomic():
                try:
                    upload.save(update_fields=['chunk_size', 'key_md5', 'part_json', 'part_num'])
                    hm._update_obj_metadata(obj=obj, size=new_size)
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
        # 查看文件是否存在
        hm = HarborManager()
        try:
            obj = hm.get_object(bucket_name=bucket_name, path_name=obj_key, user=request.user)
        except exceptions.S3Error as e:
            # 文件不存在
            return view.exception_response(request, exc=exceptions.S3NotFound())

        try:
            upload, bucket = self.get_upload_and_bucket(request=request, upload_id=upload_id, bucket_name=bucket_name,
                                                        view=view)
        except exceptions.S3Error as e:
            return view.exception_response(request, exc=e)

        #
        if upload.status == MultipartUpload.UploadStatus.COMPLETED.value:
            return view.exception_response(request, exc=exceptions.S3NoSuchUpload())

        # 组合状态
        if upload:
            upload.status = MultipartUpload.UploadStatus.COMPOSING.value
            upload.save(update_fields=['status'])

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

        mmanger = MultipartUploadManager()

        complete_parts_dict, complete_part_numbers = mmanger.handle_validate_complete_parts(complete_parts_list)

        # 获取文件对象
        # obj, created = hm.get_or_create_obj(table_name=bucket.get_bucket_table_name(), obj_path_name=obj_key)

        # obj_raods_key = obj.get_obj_key(bucket.id)
        # obj_rados = build_harbor_object(using=bucket.ceph_using, pool_name=bucket.pool_name, obj_id=obj_raods_key,
        #                                 obj_size=obj.si)

        # 获取需要组合的所有part元数据和对象ETag，和没有用到的part元数据列表
        obj_etag = mmanger.get_upload_parts_and_validate(
            bucket=bucket, upload=upload, complete_parts=complete_parts_dict, complete_numbers=complete_part_numbers)

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

        # 终止上传 删除文件对象
        mmanger = MultipartUploadManager()
        try:
            # 删除多部分上传数据
            del_obj = mmanger.delete_multipart_upload(upload_id=upload_id)
        except exceptions.S3Error as e:
            return view.exception_response(request, exc=exceptions.S3Error)
        hm = HarborManager()
        try:
            # 删除元数据
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
            return view.exception_response(request,
                                           exc=exceptions.S3InvalidArgument(message=gettext('参数“delimiter”暂时不支持')))
        try:
            bucket = HarborManager().get_public_or_user_bucket(name=bucket_name, user=request.user)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        if x_amz_expected_bucket_owner:
            try:
                if bucket.id != int(x_amz_expected_bucket_owner):
                    raise ValueError
            except ValueError:
                return view.exception_response(request, exc=exceptions.S3AccessDenied())
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

        if x_amz_expected_bucket_owner:
            try:
                if bucket.id != int(x_amz_expected_bucket_owner):
                    raise ValueError
            except ValueError:
                return view.exception_response(request, exc=exceptions.S3AccessDenied())
        try:
            upload_data = MultipartUploadManager().get_multipart_upload_by_id(upload_id=upload_id)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        upload_part_json = json.loads(upload_data.part_json)['Parts']
        data = {'Bucket': bucket.name, 'Key': obj.na, 'UploadId': upload_id}
        if max_parts and part_number_marker:

            # 处理不为空的参数
            # PartNumberMarker 上一部分最后
            # NextPartNumberMarker 这一部分最后
            try:
                max_parts = int(max_parts)
            except ValueError:
                return view.exception_response(request, exc=exceptions.S3InvalidArgument('Invalid param MaxParts'))
            try:
                part_number_marker = int(part_number_marker)
            except ValueError:
                return view.exception_response(request,
                                               exc=exceptions.S3InvalidArgument('Invalid param PartNumberMarker'))
            upload_part_json = upload_part_json[part_number_marker:max_parts + 1]
            data['PartNumberMarker'] = part_number_marker
            data['NextPartNumberMarker'] = max_parts + 1
            data['MaxParts'] = max_parts

        # owner = serializers.ListMultipartUploadsSerializer(upload_data, context={'user': request.user})
        # data['Owner'] = owner.data
        view.set_renderer(request, renders.CommonXMLRenderer(root_tag_name='ListPartsResult'))
        data['Part'] = upload_part_json

        return Response(data=data, status=status.HTTP_200_OK)

    # ---------------------------------------------------------------------

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


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return json.JSONEncoder.default(self, obj)
