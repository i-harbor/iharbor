import time
from datetime import datetime
from pytz import utc

from django.utils import timezone
from django.utils.translation import gettext
from django.db import transaction
from rest_framework.exceptions import UnsupportedMediaType
from rest_framework import status
from rest_framework.response import Response

from s3.harbor import HarborManager, MultipartUploadManager, S3_MULTIPART_UPLOAD_MAX_SIZE
from s3.responses import IterResponse
from s3.viewsets import S3CustomGenericViewSet
from s3 import exceptions, renders, paginations, serializers
from s3.models import MultipartUpload
from s3.handlers.s3object import create_object_metadata
from utils import storagers
from utils.oss.pyrados import RadosError, HarborObject
from utils.storagers import try_close_file
from utils.md5 import FileMD5Handler


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

        # 并发上传，确保已经设置了分块大小
        try:
            now_upload = self.ensure_upload_chunk_size_set(
                upload=upload, part_number=part_num, part_size=content_length)
        except exceptions.S3Error as exc:
            return view.exception_response(request=request, exc=exc)

        try:
            return self.upload_part_handle(
                request=request, view=view, upload=now_upload, part_number=part_num,
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
        pool_id = obj.get_pool_id()
        pool_name = obj.get_pool_name()
        uploader = storagers.PartUploadToCephHandler(request=request, using=str(pool_id),
                                                     pool_name=pool_name, obj_key=ceph_obj_key, offset=offset)
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

        # 分块大小正确，块的存储偏移量是正确的
        if upload.is_chunk_size_equal_part1_size():
            try:
                upload.set_completed(obj_etag=obj_etag)
            except exceptions.S3Error as e:
                upload.set_uploading()
                return view.exception_response(request, exc=e)

            location = request.build_absolute_uri()
            data = {'Location': location, 'Bucket': bucket.name, 'Key': obj.na, 'ETag': obj_etag}
            view.set_renderer(request, renders.CommonXMLRenderer(root_tag_name='ListMultipartUploadsResult'))
            return Response(data=data, status=status.HTTP_200_OK)
        # 块离散存储（先上传的最后一个块，且大于分块大小），需要移动块
        elif upload.is_chunk_size_gt_part1_size():
            return self.complete_multipart_upload_handle(
                view=view, upload=upload, hm=hm, bucket=bucket,
                old_obj=obj, obj_etag=obj_etag, request=request
            )

        return view.exception_response(request, exc=exceptions.S3InvalidPart(
            message=gettext('The last block is uploaded first, and the merge cannot '
                            'be completed. Please upload again.')))

    def complete_multipart_upload_handle(
            self, view: S3CustomGenericViewSet, request, upload, hm, bucket, old_obj, obj_etag):
        """
        处理 s3fs 最后一块先上传

        :param view:
        :param request:
        :param upload: 上传实例
        :param hm: HarborManager()
        :param bucket: 桶实例
        :param old_obj: 当前上传的对象
        :param obj_etag: 当前对象的etag
        :return: Response
        """

        part1 = upload.get_part_by_index(0)  # 通过索引查询到块编号 1 的块信息
        new_chunk_size = part1['Size']

        # 创建新的文件对象
        try:
            bucket, obj, created, obj_rados = self._create_temp_obj(
                hm=hm, bucket=bucket, old_obj_key=old_obj.na)
        except exceptions.S3Error as exc:
            return view.exception_response(request=request, exc=exc)

        return IterResponse(iter_content=self.handler_complete_iter(
            bucket=bucket, obj=obj, old_obj=old_obj,
            new_chunk_size=new_chunk_size,
            old_chunk_size=upload.chunk_size,
            upload=upload, hm=hm, new_obj_rados=obj_rados,
            obj_etag=obj_etag, request=request))

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

        if content_length > S3_MULTIPART_UPLOAD_MAX_SIZE:
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
    def ensure_upload_chunk_size_set(upload: MultipartUpload, part_number: int, part_size: int) -> MultipartUpload:
        """
        确保多部分上传分块大小设置，要加锁，防止并发问题
        只能设置一次，合并时要检查，以防止先上传了最后一个块，导致计算块偏移量不对，上传对象不对的问题。

        :raises: S3Error
        """
        if upload.chunk_size > 0:
            return upload

        try:
            with transaction.atomic('metadata'):
                now_upload = MultipartUpload.objects.select_for_update().filter(id=upload.id).first()
                # 加锁后检查是否已设置了分块大小，已设置直接返回，不能再修改。因为其他块可能已经按此分块大小上传了
                if now_upload.chunk_size > 0:
                    return now_upload

                if part_size < 5 * 1024 ** 2:  # 多部分除了最后一块不得小于5MiB；当前是最后一个块先上传的
                    if part_number != 1:    # 块编号为1时允许，多部分上传只有一个块的情况
                        raise exceptions.S3EntityTooSmall(message=gettext('最后一个part不得先上传。'))

                now_upload.chunk_size = part_size
                now_upload.save(update_fields=['chunk_size'])
                return now_upload
        except Exception as e:
            raise exceptions.S3Error(message=str(e))

    @staticmethod
    def _create_temp_obj(hm: HarborManager, bucket, old_obj_key: str):
        """
        合并有空洞的文件时，创建新的临时文件

        :param hm: HarborManager()
        :param bucket: 桶实例
        :param old_obj_key: 上的对象视为旧的
        :return:
            bucket, obj, created, obj_rados

        :raises: S3Error
        """
        ts = int(time.time() * 1000)
        new_key = old_obj_key + '_' + str(ts) + '_temp'
        obj, created = hm.get_or_create_obj(table_name=bucket.get_bucket_table_name(), obj_path_name=new_key)
        obj_rados = hm.get_obj_rados(bucket=bucket, obj=obj)
        return bucket, obj, created, obj_rados

    def handler_complete_iter(
            self, bucket, obj, old_obj, new_chunk_size,
            upload: MultipartUpload, old_chunk_size, hm: HarborManager,
            new_obj_rados: HarborObject, obj_etag, request
    ):
        """
        合并过程大文件耗时很长，合并过程中，先发送xml文件声明内容，
        之后再不断向客户端发送“空格” 防止与客户端连接超时中断，
        最后根据合并结果发送"成功"或者“错误”的xml内容

        :param new_obj_rados:
        :param bucket: 桶
        :param obj: 新对象
        :param old_obj: 旧对象
        :param new_chunk_size: 新的分块大小
        :param upload:
        :param old_chunk_size: 旧的分块大小
        :param hm:
        :param obj_etag:
        :param request:
        :return:
        """
        white_space_bytes = b' '
        xml_declaration_bytes = b'<?xml version="1.0" encoding="UTF-8"?>\n'
        start_time = time.time()
        yielded_doctype = False

        md5_handler = FileMD5Handler()
        try:
            old_obj_rados = hm.get_obj_rados(bucket=bucket, obj=old_obj)
            parts = upload.get_parts()
            for part in parts:
                for yield_bool in self.save_part_to_object_iter(
                        part=part, chunk_size=new_chunk_size, old_chunk_size=old_chunk_size,
                        old_obj_rados=old_obj_rados, new_obj_rados=new_obj_rados, md5_handler=md5_handler
                ):
                    if yield_bool is None:
                        if not yielded_doctype:
                            yielded_doctype = True
                            yield xml_declaration_bytes
                        else:
                            yield white_space_bytes
                    elif yield_bool is True:
                        break
                    elif isinstance(yield_bool, exceptions.S3Error):
                        raise yield_bool

                    # 间隔不断发送空字符防止客户端连接超时
                    now_time = time.time()
                    if now_time - start_time < 10:
                        start_time = now_time
                        continue
                    if not yielded_doctype:
                        yielded_doctype = True
                        yield xml_declaration_bytes
                    else:
                        yield white_space_bytes

            # ----块数据写入新的对象完成，删除旧的缓存的对象元数据和ceph rados数据，重命名新的对象 ----
            if not yielded_doctype:
                yielded_doctype = True
                yield xml_declaration_bytes
            else:
                yield white_space_bytes

            # 删除旧的元数据
            old_obj_id = old_obj.id
            try:
                old_obj.delete()
            except Exception as e:
                # 清除新的临时对象
                self._try_clear_temp_obj_and_rados(obj=obj, rados=new_obj_rados)
                raise exceptions.S3Error(message=f'删除旧对象和多部分上传元数据失败，{str(e)}')

            # 对象重命名
            try:
                new_obj_size = upload.calculate_obj_size_by_chunk_size(chunk_size=new_chunk_size)
                self._update_obj_key_size(
                    obj=obj, new_key=old_obj.na, name=old_obj.name,
                    new_obj_size=new_obj_size, obj_md5=md5_handler.hex_md5
                )
            except Exception as e:
                old_obj.id = old_obj_id
                old_obj.do_save()  # 恢复旧对象元数据
                # 清除新的临时对象
                self._try_clear_temp_obj_and_rados(obj=obj, rados=new_obj_rados)
                raise exceptions.S3Error(message=f'重命名对象失败，{str(e)}')

            # s3多部数据修改
            try:
                upload.set_completed(obj_etag=obj_etag, obj=obj, chunk_size=new_chunk_size)
            except exceptions.S3Error as e:
                raise exceptions.S3Error(message=str(e))

            if not yielded_doctype:
                yielded_doctype = True
                yield xml_declaration_bytes
            else:
                yield white_space_bytes

            # 删除旧对象的ceph数据
            old_obj_rados.delete()

            location = request.build_absolute_uri()
            data = {'Location': location, 'Bucket': bucket.name, 'Key': obj.na, 'ETag': obj_etag}
            content = renders.CommonXMLRenderer(root_tag_name='CompleteMultipartUploadResult',
                                                with_xml_declaration=not yielded_doctype).render(data)
            yield content.encode(encoding='utf-8')
        except exceptions.S3Error as e:
            upload.set_uploading()  # 上传状态改变
            content = renders.CommonXMLRenderer(root_tag_name='Error',
                                                with_xml_declaration=not yielded_doctype).render(e)
            yield content.encode(encoding='utf-8')

        except Exception as e:
            upload.set_uploading()
            content = renders.CommonXMLRenderer(root_tag_name='Error', with_xml_declaration=not yielded_doctype
                                                ).render(exceptions.S3InternalError(message=str(e)).err_data())
            yield content.encode(encoding='utf-8')

    @staticmethod
    def save_part_to_object_iter(
            part, chunk_size, old_chunk_size,
            new_obj_rados: HarborObject, old_obj_rados: HarborObject, md5_handler):
        """
        把一个part数据写入新对象

        :param old_chunk_size: 旧对象设置的块大小
        :param md5_handler: 计算md5
        :param new_obj_rados: 新对象 rados
        :param old_obj_rados: 旧对象 rados
        :param part: 旧数据块信息
        :param chunk_size: 要写入块数据的大小
        :return:
            yield True          # success
            yield None          # continue
            yield S3Error       # error
        """
        # 旧块数据偏移量
        old_offset = (part['PartNumber'] - 1) * old_chunk_size
        old_end = old_offset + part['Size'] - 1
        # 新块的偏移量
        offset = (part['PartNumber'] - 1) * chunk_size

        start_time = time.time()
        generator = old_obj_rados.read_obj_generator(offset=old_offset, end=old_end)

        for data in generator:
            if not data:
                break

            ok, msg = new_obj_rados.write(offset=offset, data_block=data)
            if not ok:
                ok, msg = new_obj_rados.write(offset=offset, data_block=data)

            if not ok:
                yield exceptions.S3InternalError(extend_msg=msg)

            md5_handler.update(offset=offset, data=data)
            offset = offset + len(data)

            now_time = time.time()
            if now_time - start_time < 10:
                start_time = now_time
                continue

            yield None

        yield True

    @staticmethod
    def _try_clear_temp_obj_and_rados(obj, rados: HarborObject):
        """
        尽可能清除临时创建的对象和ceph rados数据

        :return:
            True    # success
            False   # failed
        """
        ret = True
        # 删除元数据
        try:
            obj.do_delete()
        except Exception:
            ret = False

        try:
            rados.delete()
        except Exception:
            ret = False

        return ret

    @staticmethod
    def _update_obj_key_size(obj, new_obj_size: int, new_key: str, name: str, obj_md5: str):
        """
        更新对象元数据，key，size，md5
        """
        obj.na = new_key
        obj.na_md5 = ''
        obj.name = name
        obj.si = new_obj_size
        obj.md5 = obj_md5
        obj.upt = timezone.now()
        obj.save(update_fields=['na', 'name', 'na_md5', 'si', 'md5', 'upt'])
        return obj
