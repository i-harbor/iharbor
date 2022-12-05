from datetime import datetime
from pytz import utc

from django.utils import timezone
from django.conf import settings
from django.utils.translation import gettext
from django.db import transaction
from rest_framework.exceptions import UnsupportedMediaType

from s3.harbor import HarborManager, MultipartUploadManager
from s3.responses import IterResponse
from s3.viewsets import S3CustomGenericViewSet
from s3 import exceptions, renders, paginations, serializers
from s3.models import MultipartUpload
from s3.handlers.s3object import create_object_metadata
from utils import storagers
from utils.oss.pyrados import RadosError
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
        x_amz_acl = request.headers.get('X-Amz-Acl', 'private').lower()
        # expires = request.headers.get('Expires', None)

        hm = HarborManager()
        # 查看桶和对象是否存在
        try:
            key = view.get_obj_path_name(request)
            bucket, obj, rados, created = create_object_metadata(
                user=request.user, bucket_or_name=bucket_name, obj_key=key, x_amz_acl=x_amz_acl
            )
            # 多部分上传表的查询：
            upload = MultipartUploadManager.get_multipart_upload(bucket=bucket, obj_key=key)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        if upload:
            upload.delete()

        upload_data = hm.create_multipart_data(
            bucket_id=bucket.id, bucket_name=bucket.name, obj=obj, obj_perms_code=obj.share)

        return self.create_multipart_upload_response(
            request=request, view=view, bucket_name=bucket.name, key=key, upload_id=upload_data.id)

    @staticmethod
    def create_multipart_upload_response(request, view, bucket_name, key, upload_id):
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

        try:
            content_length, part_num = self.upload_part_validate_header(
                content_length=content_length, part_num=part_num
            )
        except exceptions.S3Error as exc:
            return view.exception_response(request, exc=exc)

        try:
            upload, bucket = self.get_upload_and_bucket(request=request, upload_id=upload_id,
                                                        bucket_name=bucket_name, view=view)
        except exceptions.S3Error as e:
            return view.exception_response(request, exc=e)

        # 检查上传状态
        if upload.status != MultipartUpload.UploadStatus.UPLOADING.value:
            return view.exception_response(request, exc=exceptions.S3CompleteMultipartAlreadyInProgress())

        # 确保编号为1的块先上传
        # if part_num != 1:
        #     if not upload.is_part1_uploaded:
        #         return view.exception_response(request, exc=exceptions.S3InvalidRequest(
        #             message=gettext('The part numbered 1 must be uploaded first.')))

        # 检查是否设置 chunk_size 如果 多个同时获取的uplod实例，chunk_size 为None
        if not upload.chunk_size:
            try:
                with transaction.atomic(using='metadata'):
                    # 加锁
                    upload = MultipartUpload.objects.select_for_update().get(id=upload.id)
                    upload.chunk_size = content_length
                    upload.save(update_fields=['chunk_size'])
            except Exception as e:
                raise exceptions.S3Error(f'更新多部分上传元数据块大小错误，{str(e)}')

        try:
            return self.upload_part_handle(
                request=request, view=view, upload=upload, part_number=part_num,
                bucket=bucket, obj_key=obj_key
            )
        except Exception as exc:
            return view.exception_response(request=request, exc=exc)

    def upload_part_handle(self, request, view, upload, part_number, bucket, obj_key: str):

        def clean_put(_uploader):
            # 删除数据
            f = getattr(_uploader, 'file', None)
            if f is not None:
                try_close_file(f)
                try:
                    f.delete()
                except Exception:
                    pass

        offset = upload.get_part_offset(part_number=part_number)
        obj = HarborManager().get_object(bucket_name=bucket.name, path_name=obj_key, user=request.user)
        ceph_obj_key = obj.get_obj_key(bucket.id)
        uploader = storagers.PartUploadToCephHandler(request=request, using=bucket.ceph_using,
                                                     pool_name=bucket.pool_name, obj_key=ceph_obj_key, offset=offset)
        request.upload_handlers = [uploader]

        try:
            upload, part = self.upload_part_handle_save(
                request=request, view=view, upload=upload, part_number=part_number, obj=obj, offset=offset)
        except exceptions.S3Error as e:
            clean_put(uploader)
            return view.exception_response(request, e)
        except Exception as exc:
            clean_put(uploader)
            return view.exception_response(request, exceptions.S3InvalidRequest(extend_msg=str(exc)))

        return Response(status=status.HTTP_200_OK, headers={'ETag': f'"{part["ETag"]}"'})

    @staticmethod
    def upload_part_handle_save(request, view, upload: MultipartUpload, part_number: int, obj, offset: int):
        """
        :return:

        :raises: S3Error
        """
        try:
            view.kwargs['filename'] = 'filename'
            put_data = request.data
        except UnsupportedMediaType:
            raise exceptions.S3UnsupportedMediaType()
        except RadosError as e:
            raise exceptions.S3InternalError(extend_msg=str(e))
        except Exception as exc:
            raise exceptions.S3InvalidRequest(extend_msg=str(exc))

        file = put_data.get('file')
        part_md5 = file.file_md5
        part_size = file.size

        if not file:
            raise exceptions.S3InvalidRequest('Request body is empty.')

        amz_content_sha256 = request.headers.get('X-Amz-Content-SHA256', None)
        if amz_content_sha256 is None:
            raise exceptions.S3InvalidContentSha256Digest()

        if amz_content_sha256 != 'UNSIGNED-PAYLOAD':
            part_sha256 = file.sha256_handler.hexdigest()
            if amz_content_sha256 != part_sha256:
                raise exceptions.S3BadContentSha256Digest()

        part = MultipartUpload.build_part_item(
            part_number=part_number, last_modified=timezone.now(), etag=part_md5, size=part_size
        )
        try:
            with transaction.atomic(using='metadata'):
                upload = MultipartUpload.objects.select_for_update().get(id=upload.id)
                # 不更新 chunk_size
                upload.insert_part(part)
                # 写入part后，对象大小增大了才需更新
                new_obj_size = offset + part_size
                if new_obj_size > obj.si:
                    obj.si = new_obj_size
                    ok = HarborManager._update_obj_metadata(obj=obj, size=obj.si)
                    if not ok:
                        raise exceptions.S3Error('更新对象元数据大小错误。')
        except Exception as e:
            raise exceptions.S3Error(f'更新对象和多部分上传元数据错误，{str(e)}')

        return upload, part

    def complete_multipart_upload(self, request, view: S3CustomGenericViewSet, upload_id):
        """
        完成多部分上传处理
        """
        bucket_name = view.get_bucket_name(request)
        obj_key = view.get_s3_obj_key(request)

        try:
            complete_parts_list = self._get_request_complete_parts(request=request, view=view)
        except exceptions.S3Error as exc:
            return view.exception_response(request, exc=exc)

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
                upload=upload, complete_parts=complete_parts_dict,
                complete_numbers=complete_part_numbers
            )
        except exceptions.S3Error as e:
            # 如果组合失败 返回原状态
            upload.set_uploading()
            return view.exception_response(request, exc=e)

        # 检查最后一块是否先上传
        get_upload_first_part = upload.get_upload_first_part_info()

        if not get_upload_first_part:
            try:
                upload.set_completed(obj_etag=obj_etag)
            except exceptions.S3Error as e:
                upload.set_uploading()
                return view.exception_response(request, exc=e)

            location = request.build_absolute_uri()
            data = {'Location': location, 'Bucket': bucket.name, 'Key': obj.na, 'ETag': obj_etag}
            view.set_renderer(request, renders.CommonXMLRenderer(root_tag_name='ListMultipartUploadsResult'))
            return Response(data=data, status=status.HTTP_200_OK)

        return self.complete_multipart_upload_handle(get_upload_first_part=get_upload_first_part, view=view,
                                                     upload=upload, hm=hm, bucket=bucket, obj_key=obj_key,
                                                     old_obj=obj, complete_numbers=complete_part_numbers,
                                                     obj_etag=obj_etag, request=request)

    def complete_multipart_upload_handle(self, get_upload_first_part, view, upload, hm, bucket, obj_key, old_obj,
                                         complete_numbers, obj_etag, request):
        """
        处理 s3fs 最后一块先上传
        :param view:
        :param get_upload_first_part: 第一个上传的块信息
        :param upload: 上传实例
        :param hm: HarborManager()
        :param bucket: 桶实例
        :param obj_key: 当前上传的 key
        :param old_obj: 当前上传的对象
        :param complete_numbers: 当前对象的块编号列表
        :param obj_etag: 当前对象的etag
        :param request:
        :return:
        """

        get_list_first_part = upload.get_part_by_index(0)  # 通过索引查询到块编号 1 的块信息
        get_upload_first_part_size = get_upload_first_part['Size']
        get_list_first_part_size = get_list_first_part['Size']

        if get_upload_first_part_size > get_list_first_part_size:
            # 文件有 空隙
            # 文件实际大小
            new_obj_size = get_list_first_part_size * (get_upload_first_part['PartNumber'] - 1) + \
                           get_upload_first_part_size
            hmanage = MultipartUploadManager()
            # 创建新的文件对象
            bucket, obj, created, obj_rados = hmanage.handle_new_create_obj(hm=hm,
                                                                            bucket=bucket, old_obj_key=obj_key,
                                                                            upload=upload, new_obj_size=new_obj_size,
                                                                            user=request.user)

            return IterResponse(iter_content=hmanage.handler_complete_iter(bucket=bucket, obj=obj, old_obj=old_obj,
                                                                           complete_numbers=complete_numbers,
                                                                           new_chunk_size=get_list_first_part_size,
                                                                           new_obj_size=new_obj_size,
                                                                           old_chunk_size=get_upload_first_part_size,
                                                                           upload=upload, hm=hm, new_obj_rados=obj_rados,
                                                                           obj_etag=obj_etag, request=request))

        elif get_upload_first_part_size < get_list_first_part_size:
            # 文件块出现覆盖
            return view.exception_response(request, exc=exceptions.S3InvalidPart(
                message=gettext('The last block is uploaded first, and the merge cannot '
                                'be completed. Please upload again.')))

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
        try:
            upload.delete()
        except Exception as e:
            return view.exception_response(
                request, exc=exceptions.S3Error(message=f'删除多部分上传元数据失败，{str(e)}'))

        try:
            table_name = bucket.get_bucket_table_name()
            obj = HarborManager.get_metadata_obj(table_name=table_name, path=upload.obj_key)
            if obj is not None and obj.is_file():
                multipart_obj_ts = upload.get_obj_upload_timestamp()  # 时间戳
                obj_ts = int(obj.ult.timestamp())
                if multipart_obj_ts == obj_ts:
                    HarborManager().do_delete_obj_or_dir(bucket=bucket, obj=obj)
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
            self.check_header_x_amz_expected_bucket_owner(
                x_amz_expected_bucket_owner=x_amz_expected_bucket_owner, bucket_user_id=bucket.user_id)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        mmanger = MultipartUploadManager()
        queryset = mmanger.list_multipart_uploads_queryset(bucket_id=bucket.id, bucket_name=bucket_name, prefix=prefix)
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

        try:
            max_parts, part_number_marker = self._list_parts_validate_params(
                max_parts=max_parts, part_number_marker=part_number_marker
            )
        except exceptions.S3Error as exc:
            return view.exception_response(request, exc=exc)

        try:
            upload = MultipartUploadManager().get_multipart_upload_by_id(upload_id=upload_id)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        if upload is None:
            return view.exception_response(request, exc=exceptions.S3NoSuchUpload())

        if upload.status == upload.UploadStatus.COMPLETED.value:
            return view.exception_response(request, exc=exceptions.S3NoSuchUpload())

        if x_amz_expected_bucket_owner:
            try:
                bucket = HarborManager().get_public_or_user_bucket(name=bucket_name, user=request.user)
                self.check_header_x_amz_expected_bucket_owner(
                    x_amz_expected_bucket_owner=x_amz_expected_bucket_owner, bucket_user_id=bucket.user_id)
            except exceptions.S3Error as e:
                return view.exception_response(request, e)

        parts, is_truncated = upload.get_range_parts(part_number_marker=part_number_marker, max_parts=max_parts)
        if parts:
            next_part_number_marker = parts[-1]['PartNumber']
        else:
            next_part_number_marker = 0

        owner = {'ID': request.user.id, "DisplayName": request.user.username}
        data = {
            'Bucket': bucket_name,
            'Key': obj_key,
            'UploadId': upload_id,
            'PartNumberMarker': part_number_marker,
            'NextPartNumberMarker': next_part_number_marker,
            'MaxParts': max_parts,
            'IsTruncated': is_truncated,
            'Part': parts,
            'StorageClass': 'STANDARD',
            'Initiator': owner,
            'Owner': owner
        }

        view.set_renderer(request, renders.CommonXMLRenderer(root_tag_name='ListPartsResult'))
        return Response(data=data, status=status.HTTP_200_OK)

    # ---------------------------------------------------------------------
    @staticmethod
    def _list_parts_validate_params(max_parts: str, part_number_marker: str):
        if max_parts:
            try:
                max_parts = int(max_parts)
            except ValueError:
                raise exceptions.S3InvalidArgument('Invalid param "max-parts"')

            if max_parts > 1000 or max_parts < 1:
                raise exceptions.S3InvalidArgument('参数"max-parts"的值必须在1至1000之内。')
        else:
            max_parts = 1000

        if part_number_marker:
            try:
                part_number_marker = int(part_number_marker)
            except ValueError:
                raise exceptions.S3InvalidArgument('Invalid param "part-number-marker"')

            if part_number_marker < 0 or part_number_marker > 10000:
                raise exceptions.S3InvalidArgument('参数"part-number-marker"的值必须在0至10000之内。')
        else:
            part_number_marker = 0

        return max_parts, part_number_marker

    @staticmethod
    def check_header_x_amz_expected_bucket_owner(x_amz_expected_bucket_owner: str, bucket_user_id: int):
        if x_amz_expected_bucket_owner:
            try:
                if bucket_user_id != int(x_amz_expected_bucket_owner):
                    raise ValueError
            except ValueError:
                raise exceptions.S3AccessDenied()

    @staticmethod
    def _get_request_complete_parts(request, view) -> list:
        """
        请求要合并的对象part列表

        :return: parts: list
        :raises: S3Error
        """
        try:
            data = request.data
        except Exception as e:
            raise exceptions.S3MalformedXML()

        root = data.get('CompleteMultipartUpload')
        if not root:
            raise exceptions.S3MalformedXML()

        complete_parts_list = root.get('Part')
        if not complete_parts_list:
            raise exceptions.S3MalformedXML()

        # XML解析器行为有关，只有一个part时不是list
        if not isinstance(complete_parts_list, list):
            complete_parts_list = [complete_parts_list]

        return complete_parts_list

    @staticmethod
    def get_upload_and_bucket(request, upload_id: str, bucket_name: str, view: S3CustomGenericViewSet):
        """
        :return:
            upload, bucket
        :raises: S3Error
        """
        obj_path_name = view.get_s3_obj_key(request)

        upload = MultipartUploadManager.get_multipart_upload_by_id(upload_id=upload_id)
        if not upload:
            raise exceptions.S3NoSuchUpload()

        if upload.obj_key != obj_path_name:
            raise exceptions.S3NoSuchUpload(f'UploadId conflicts with this object key.Please Key "{upload.obj_key}"')

        hm = HarborManager()
        bucket = hm.get_user_own_bucket(name=bucket_name, user=request.user)

        if not bucket:
            raise exceptions.S3NoSuchBucket()

        if not upload.belong_to_bucket(bucket):
            raise exceptions.S3NoSuchUpload(
                f'UploadId conflicts with this bucket.'
                f'Please bucket "{bucket.name}".Maybe the UploadId is created for deleted bucket.'
            )

        return upload, bucket

    @staticmethod
    def upload_part_validate_header(content_length, part_num):
        """
        :raies: S3Error
        """
        try:
            content_length = int(content_length)
        except exceptions.S3Error as e:
            raise exceptions.S3InvalidContentLength()

        if content_length == 0:
            raise exceptions.S3EntityTooSmall()

        if content_length > settings.S3_MULTIPART_UPLOAD_MAX_SIZE:
            raise exceptions.S3EntityTooLarge()

        try:
            part_num = int(part_num)
        except ValueError:
            raise exceptions.S3InvalidArgument('Invalid param PartNumber')

        if not (0 < part_num <= 10000):
            raise exceptions.S3InvalidArgument(
                'Invalid param PartNumber,must be a positive integer between 1 and 10,000.')

        return content_length, part_num

    @staticmethod
    def reset_or_create_object(bucket, obj, hm, key, bucket_table):

        if obj:
            # 重置对象
            try:
                hm.s3_reset_upload(bucket=bucket, obj=obj)
            except exceptions.S3Error as e:
                raise exceptions.S3InternalError()
        else:
            obj, _ = hm.get_or_create_obj(table_name=bucket_table, obj_path_name=key)

        return obj
