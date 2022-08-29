from django.utils.translation import gettext_lazy, gettext as _
from drf_yasg.utils import swagger_auto_schema

from rest_framework.response import Response
from rest_framework.serializers import Serializer, ValidationError

from utils.view import CustomGenericViewSet
from api import permissions
from api import serializers
from api import exceptions
from api.validators import DNSStringValidator
from api.views import serializer_error_text
from buckets.utils import create_bucket
from buckets.models import get_next_bucket_max_id, Bucket
from users.models import UserProfile


class AdminBucketViewSet(CustomGenericViewSet):
    """
    管理员
    """
    queryset = {}
    permission_classes = [permissions.IsSuperOrAppSuperUser]
    lookup_field = 'id'

    @swagger_auto_schema(
        operation_summary=gettext_lazy('管理员为指定用户创建存储桶'),
        responses={
            200: ""
        }
    )
    def create(self, request, *args, **kwargs):
        """
        管理员为指定用户创建存储桶； 需要超级用户权限和APP超级用户权限

            http code 200 ok:
            {
              "id": 2,
              "name": "test2",
              "user": {
                "id": 2,
                "username": "sd"
              },
              "created_time": "2022-08-26T16:45:42.469555+08:00",
              "access_permission": "私有",
              "ftp_enable": false,
              "ftp_password": "28403e038c",
              "ftp_ro_password": "d14e2442ef",
              "remarks": ""
            }

            http code 400, 403, 409, 500 error:
            {
              "code": "BucketAlreadyExists",
              "message": "存储桶已存在，请更换另一个存储桶名程后再重试。"
            }

            * code:
            400：
                BadRequest: 请求格式错误
                InvalidBucketName: 无效的存储桶名
                InvalidUsername: 无效的用户名
            403：
                AccessDenied： 您没有执行该操作的权限。
            409：
                BucketAlreadyExists：存储桶已存在，请更换另一个存储桶名程后再重试。
            500：
                InternalError：创建存储桶时错误 / 创建用户错误 / 内部错误
        """
        serializer = self.get_serializer(data=request.data)
        if not serializer.is_valid(raise_exception=False):
            s_errors = serializer.errors
            if 'name' in s_errors:
                exc = exceptions.BadRequest(message=_('无效的存储桶名。') + s_errors['name'][0], code='InvalidBucketName')
            elif 'username' in s_errors:
                exc = exceptions.BadRequest(message=_('无效的用户名。') + s_errors['username'][0], code='InvalidUsername')
            else:
                code_text = serializer_error_text(serializer.errors, default='参数验证有误')
                exc = exceptions.BadRequest(message=_(code_text))

            return Response(data=exc.err_data(), status=exc.status_code)

        data = serializer.validated_data
        bucket_name = data['name']
        username = data['username']
        if not username:
            exc = exceptions.BadRequest(message=_('用户无效。'), code='InvalidUsername')
            return Response(data=exc.err_data(), status=exc.status_code)

        try:
            bucket_name = self.validate_bucket_name(bucket_name=bucket_name)
        except exceptions.Error as exc:
            return Response(data=exc.err_data(), status=exc.status_code)

        # 检测桶是否存在
        bucket = Bucket.get_bucket_by_name(bucket_name)
        if bucket:
            exc = exceptions.BucketAlreadyExists(message=_('存储桶已存在，请更换另一个存储桶名程后再重试。'))
            return Response(data=exc.err_data(), status=exc.status_code)

        user = UserProfile.objects.filter(username=username).first()
        if user is None:
            try:
                user = UserProfile(username=username)
                user.save(force_insert=True)
            except Exception as e:
                exc = exceptions.Error(message=_('创建用户错误。') + str(e))
                return Response(data=exc.err_data(), status=exc.status_code)

        bucket_id = get_next_bucket_max_id()
        try:
            bucket = create_bucket(_id=bucket_id, name=bucket_name, user=user)
        except Exception as e:
            exc = exceptions.Error(message=_('创建存储桶时错误。') + str(e))
            return Response(data=exc.err_data(), status=exc.status_code)

        return Response(data=serializers.BucketSerializer(instance=bucket).data, status=200)

    @staticmethod
    def validate_bucket_name(bucket_name: str):
        """
        :return:
            bucket_name: str

        :raises: ValidationError
        """
        if not bucket_name:
            raise exceptions.BadRequest(message=_('存储桶名称不能为空'), code='InvalidBucketName')

        if bucket_name.startswith('-') or bucket_name.endswith('-'):
            raise exceptions.BadRequest(message=_('存储桶名称不能以“-”开头或结尾'), code='InvalidBucketName')

        try:
            DNSStringValidator(bucket_name)
        except ValidationError:
            raise exceptions.BadRequest(message=_('存储桶名称只能包含小写英文字母、数字和连接线“-”'), code='InvalidBucketName')

        return bucket_name.lower()

    def get_serializer_class(self):
        if self.action == 'create':
            return serializers.AdminBucketCreateSerializer

        return Serializer

    def get_permissions(self):
        if self.action == 'create':
            return [permissions.IsSuperAndAppSuperUser()]

        return super().get_permissions()
