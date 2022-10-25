from django.utils import timezone
from django.conf import settings
from django.utils.translation import gettext

from buckets.models import BucketFileBase
from s3.harbor import HarborManager
from s3.viewsets import S3CustomGenericViewSet
from s3 import exceptions, renders
from s3.models import MultipartUpload
from utils.oss import build_harbor_object

from rest_framework.response import Response
from rest_framework import status


class MultipartUploadHandler:

    def create_multipart_upload(self, request, view: S3CustomGenericViewSet):
        # print(f'request = {request}')
        bucket_name = view.get_bucket_name(request)
        key = view.get_s3_obj_key(request)
        # print(f'name = {bucket_name}, key = {key}')
        # expires = request.headers.get('Expires', None) # 暂放

        # 访问权限
        acl_choices = {'private': BucketFileBase.SHARE_ACCESS_NO, 'public-read': BucketFileBase.SHARE_ACCESS_READONLY,
                       'public-read-write': BucketFileBase.SHARE_ACCESS_READWRITE}
        x_amz_acl = request.headers.get('X-Amz-Acl', 'private').lower()
        if x_amz_acl not in acl_choices:
            return view.exception_response(
                request, exc=exceptions.S3InvalidRequest(message=gettext(f'The value {x_amz_acl} '
                                                                         f'of header "x-amz-acl" is not supported.')))
        hm = HarborManager()
        try:
            bucket = hm.get_public_or_user_bucket(name=bucket_name, user=request.user)
        except exceptions.S3Error as e:
            return view.exception_response(request, exc=exceptions.S3NotFound)

        # 查看该对象是否存在
        bucket_table = bucket.get_bucket_table_name()
        try:
            bucket, obj = hm.get_bucket_and_obj_or_dir(bucket_name=bucket_name, path=key)
        except exceptions.S3Error as e:
            # 目录路径不存在

            # 对象不存在 - 创建 - MultipartUpload 创建 - 返回
            obj, is_create = hm.get_or_create_obj(table_name=bucket_table, obj_path_name=key)
            upload = MultipartUpload(bucket_id=bucket.id, bucket_name=bucket.name,
                                     obj_id=obj.id, obj_key=key)
            upload.save()
            view.set_renderer(request, renders.CusXMLRenderer(root_tag_name='InitiateMultipartUploadResult'))
            data = {
                'Bucket': bucket.name,
                'Key': key,
                'UploadId': upload.id
            }
            return Response(data=data, status=status.HTTP_200_OK)
        # iharbor原来的数据对象存在(是否是s3对象)
        upload_data = MultipartUpload.objects.filter(bucket_id=bucket.id, obj_id=obj.id, bucket_name=bucket.name,
                                                     obj_key=key)
        # print(f'upload_data = {upload_data}')
        rados = build_harbor_object(using=bucket.ceph_using, pool_name=bucket.pool_name, obj_id=str(obj.id), obj_size=obj.si)
        if not upload_data:
            # iharbor 原数据存在 - 重置原数据 - 更新 MultipartUpload 表 - 返回
            try:
                hm._pre_reset_upload(bucket=bucket, obj=obj, rados=rados)  # 重置对象大小
            except exceptions.S3Error as e:
                # 无法重置，则删除对象重新创建
                # hm.delete_object(bucket_name=bucket_name, obj_path=key, user=request.user)
                return view.exception_response(
                    request, exc=exceptions.S3ServerError(message=gettext(f'Please try this operation again.')))
            upload = MultipartUpload(bucket_id=bucket.id, bucket_name=bucket.name,
                                     obj_id=obj.id, obj_key=key)
            upload.save()
            view.set_renderer(request, renders.CusXMLRenderer(root_tag_name='InitiateMultipartUploadResult'))
            data = {
                'Bucket': bucket.name,
                'Key': key,
                'UploadId': upload.id
            }
            return Response(data=data, status=status.HTTP_200_OK)

        # 是S3对象，且有上传的历史记录 - 上传完成 - 重置对象 - 更新 MultipartUpload 表 - 返回
        #                           - 上传中（组合中） - 返回
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
            return view.exception_response(
                request, exc=exceptions.S3ServerError(message=gettext(f'Please try this operation again.')))

        upload_data.update(key_md5='', status=MultipartUpload.UploadStatus.UPLOADING.value, obj_perms_code=0, part_num=0,
                           part_json={}, create_time=timezone.now(), last_modified=timezone.now())

        view.set_renderer(request, renders.CusXMLRenderer(root_tag_name='InitiateMultipartUploadResult'))
        data = {
            'Bucket': bucket.name,
            'Key': key,
            'UploadId': upload_data.get().id
        }
        return Response(data=data, status=status.HTTP_200_OK)

    def upload_part(self, request, view: S3CustomGenericViewSet):
        # /a-%20.~b/django-3.2.14.zip?partNumber=1&uploadId=1d4bfa90535b11eda025000c29621471_MTY2NjU4ODYzMS42OTk3MjQw

        #  {'Content-Length': '0', 'Content-Type': 'text/plain', 'Host': 'test-bucket-s3.s3.obs.cstcloud.cn',
        #  'Accept-Encoding': 'identity', 'User-Agent': 'Boto3/1.24.89 Python/3.9.11 Windows/10 Botocore/1.27.89',
        #  'Content-Md5': '1B2M2Y8AsgTpgAmY7PhCfg==', 'Expect': '100-continue', 'X-Amz-Date': '20221024T061610Z',
        #  'X-Amz-Content-Sha256': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
        #  'Authorization': 'AWS4-HMAC-SHA256 Credential=139e295a511c11ed9952000c29621471/20221024/us-east-1/s3/aws4_request,
        #  SignedHeaders=content-length;content-md5;
        #  host;x-amz-content-sha256;x-amz-date,
        #  Signature=f3b1beabefc790c2997874c34c227817451bedfa68f4b825e4d5d753a8379e1e',
        #  'Amz-Sdk-Invocation-Id': '4739c120-95ca-44e3-acb3-5d2e29404dd4', 'Amz-Sdk-Request': 'attempt=1'}


        bucket_name = view.get_bucket_name(request)
        content_length = request.headers.get('Content-Length', 0)
        part_num = request.query_params.get('partNumber', None)
        upload_id = request.query_params.get('uploadId', None)

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
            upload, bucket = self.get_upload_and_bucket(request=request, upload_id=upload_id, bucket_name=bucket_name, view=view)
        except exceptions.S3Error as e:
            return view.exception_response(request, exc=e)

        upload_date = MultipartUpload.objects.get(id=upload_id)
        # 检查上传状态
        if upload_date.status != MultipartUpload.UploadStatus.UPLOADING.value:
            return view.exception_response(request, exc=exceptions.S3CompleteMultipartAlreadyInProgress())


        # from utils.storagers import PartUploadToCephHandler
        # obj_key = view.get_s3_obj_key(request)
        # uploader = PartUploadToCephHandler(request, using=bucket.ceph_using, part_key=obj_key)
        # request.upload_handlers = [uploader]
        #
        # put_data = request.data
        # file = put_data.get('file')
        # part_md5 = file.file_md5
        # part_size = file.size
        #
        # offset = 0
        #
        # # 计算块的偏移量 （顺序传块）
        # if part_num != 1:
        #     offset = part_num * part_size
        # obj_size = offset + part_size
        # # 文件存储
        # rados = build_harbor_object(using=bucket.ceph_using, pool_name=bucket.pool_name, obj_id=obj_key, obj_size=obj_size)
        #
        # hmanager = HarborManager()
        #
        # obj = hmanager.get_object(bucket_name='', path_name='', user='')
        #
        # hmanager._save_one_chunk(obj=obj, rados=rados, offset=offset, chunk=file)
        #
        # part_json= {}
        # part_json['Parts'] = []

        # amz_content_sha256 = request.headers.get('X-Amz-Content-SHA256', None)
        # if amz_content_sha256 is None:
        #     raise exceptions.S3InvalidContentSha256Digest()
        #
        # if amz_content_sha256 != 'UNSIGNED-PAYLOAD':
        #     part_sha256 = file.sha256_handler.hexdigest()
        #     if amz_content_sha256 != part_sha256:
        #         raise exceptions.S3BadContentSha256Digest()




        return Response(data='', status=status.HTTP_200_OK)

    def complete_multipart_upload(self, request, view: S3CustomGenericViewSet):
        pass

    def abort_multipart_upload(self, request, view: S3CustomGenericViewSet):
        pass

    def list_multipart_upload(self, request, view: S3CustomGenericViewSet):
        pass

    def list_part(self, request, view: S3CustomGenericViewSet):
        pass

    # ---------------------------------------------------------------------

    def get_upload_and_bucket(self, request, upload_id: str, bucket_name: str,  view: S3CustomGenericViewSet):
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



class MultipartUploadManager:

    def get_multipart_upload_by_id(self, upload_id: str):
        """
        查询多部分上传记录

        :param upload_id: uuid
        :return:
            MultipartUpload() or None

        :raises: S3Error
        """
        try:
            obj = MultipartUpload.objects.filter(id=upload_id).first()
        except Exception as e:
            raise exceptions.S3InternalError()

        return obj

    def part_is_completed(self, upload_id: str):
        try:
            obj = MultipartUpload.objects.filter(id=upload_id).first()
        except Exception as e:
            raise exceptions.S3InternalError()

        return obj.get().status == MultipartUpload.UploadStatus.COMPLETED.value

