from django.utils.translation import gettext as _
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError

from buckets.models import Bucket, get_next_bucket_max_id
from api.validators import DNSStringValidator, bucket_limit_validator
from s3 import exceptions
from s3 import renders
from s3 import serializers
from s3.viewsets import S3CustomGenericViewSet
from s3.harbor import HarborManager
from buckets.utils import create_bucket


class BucketHandler:
    @staticmethod
    def list_buckets(request, view: S3CustomGenericViewSet):
        user = request.user
        if not user.id:
            return view.exception_response(request, exceptions.S3AccessDenied(message=_('身份未认证')))

        buckets_qs = Bucket.objects.filter(user=user).all()  # user's own
        serializer = serializers.BucketListSerializer(buckets_qs, many=True)

        # xml渲染器
        view.set_renderer(request,
                          renders.CusXMLRenderer(root_tag_name='ListAllMyBucketsResult', item_tag_name='Bucket'))
        return Response(data={
            'Buckets': serializer.data,
            'Owner': {'DisplayName': user.username, 'ID': user.id}
        }, status=200)

    @staticmethod
    def head_bucket(request, view: S3CustomGenericViewSet, **kwargs):
        bucket_name = view.get_bucket_name(request)
        if not bucket_name:
            return view.exception_response(request, exceptions.S3InvalidRequest('Invalid request domain name'))

        bucket = Bucket.get_bucket_by_name(bucket_name)
        if not bucket:
            return view.exception_response(request, exceptions.S3NoSuchBucket())

        if bucket.is_public_permission():
            return Response(status=200)

        if not bucket.check_user_own_bucket(user=request.user):
            return view.exception_response(request, exceptions.S3AccessDenied())

        return Response(status=200)

    @staticmethod
    def delete_bucket(request, view: S3CustomGenericViewSet):
        bucket_name = view.get_bucket_name(request)
        if not bucket_name:
            return view.exception_response(request, exceptions.S3InvalidRequest('Invalid request domain name'))

        hm = HarborManager()
        try:
            bucket, qs = hm.get_bucket_objects_dirs_queryset(bucket_name=bucket_name, user=request.user)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        try:
            not_empty = qs.filter(fod=True).exists()        # 有无对象，忽略目录
        except Exception as e:
            return view.exception_response(request, e)

        if not_empty:
            return view.exception_response(request, exceptions.S3BucketNotEmpty())

        if not bucket.delete_and_archive():  # 删除归档
            return view.exception_response(request, exceptions.S3InternalError(_('删除存储桶失败')))

        return Response(status=204)

    @staticmethod
    def validate_create_bucket(request, bucket_name: str):
        """
        创建桶验证

        :return: bucket_name: str
        :raises: ValidationError
        """
        user = request.user

        if not bucket_name:
            raise exceptions.S3BucketNotEmpty()

        if bucket_name.startswith('-') or bucket_name.endswith('-'):
            raise exceptions.S3InvalidBucketName()  # 存储桶bucket名称不能以“-”开头或结尾

        try:
            DNSStringValidator(bucket_name)
        except ValidationError:
            raise exceptions.S3InvalidBucketName()

        bucket_name = bucket_name.lower()

        # 用户存储桶限制数量检测
        try:
            bucket_limit_validator(user=user)
        except ValidationError:
            raise exceptions.S3TooManyBuckets()

        b = Bucket.get_bucket_by_name(bucket_name)
        if b:
            if b.check_user_own_bucket(user):
                raise exceptions.S3BucketAlreadyOwnedByYou()
            raise exceptions.S3BucketAlreadyExists()
        return bucket_name

    @staticmethod
    def create_bucket(request, view: S3CustomGenericViewSet, bucket_name: str):
        """
        创建桶

        :return: Response()
        """
        acl_choices = {
            'private': Bucket.PRIVATE,
            'public-read': Bucket.PUBLIC,
            'public-read-write': Bucket.PUBLIC_READWRITE
        }
        acl = request.headers.get('x-amz-acl', 'private').lower()
        if acl not in acl_choices:
            e = exceptions.S3InvalidRequest('The value of header "x-amz-acl" is invalid and unsupported.')
            return view.exception_response(request, e)

        try:
            bucket_name = BucketHandler.validate_create_bucket(request, bucket_name)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        user = request.user
        perms = acl_choices[acl]
        try:
            bucket_id = get_next_bucket_max_id()
            bucket = create_bucket(name=bucket_name, user=user, _id=bucket_id, access_permission=perms)
        except Exception as exc:
            return view.exception_response(request, exceptions.S3InternalError(message=str(exc)))

        return Response(status=200, headers={'Location': '/' + bucket.name})
