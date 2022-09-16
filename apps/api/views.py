from collections import OrderedDict
import logging
import os
import binascii
from io import BytesIO

from django.http import StreamingHttpResponse, FileResponse, QueryDict
from django.utils.http import urlquote
from django.utils.translation import gettext_lazy, gettext as _
from django.core.validators import validate_email
from django.core import exceptions as dj_exceptions
from django.urls import reverse as django_reverse
from rest_framework import status
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.serializers import Serializer, ValidationError
from rest_framework.authtoken.models import Token
from rest_framework import parsers
from rest_framework.decorators import action
from drf_yasg.utils import swagger_auto_schema, no_body
from drf_yasg import openapi

from buckets.utils import (BucketFileManagement, create_table_for_model_class, delete_table_for_model_class)
from users.views import send_active_url_email
from users.models import AuthKey
from users.auth.serializers import AuthKeyDumpSerializer
from utils import storagers
from utils.storagers import PathParser, FileUploadToCephHandler, try_close_file
from utils.oss import build_harbor_object, RadosError
from utils.log.decorators import log_used_time
from utils.jwt_token import JWTokenTool2
from utils.view import CustomGenericViewSet
from utils.time import to_django_timezone
from .models import User, Bucket, BucketToken
from . import serializers
from . import paginations
from . import permissions
from . import throttles
from .harbor import HarborManager
from . import exceptions


logger = logging.getLogger('django.request')    # 这里的日志记录器要和setting中的loggers选项对应，不能随意给参
debug_logger = logging.getLogger('debug')       # 这里的日志记录器要和setting中的loggers选项对应，不能随意给参


def rand_hex_string(length=10):
    return binascii.hexlify(os.urandom(length//2)).decode()


def rand_share_code():
    return rand_hex_string(4)


def serializer_error_text(errors, default: str = ''):
    """
    序列化器验证错误信息

    :param errors: serializer.errors, type: ReturnDict()
    :param default: 获取信息失败时默认返回信息
    """
    msg = default if default else '参数有误，验证未通过'
    try:
        for key in errors:
            val = errors[key]
            msg = f'{key}, {val[0]}'
            break
    except Exception as e:
        pass

    return msg


def get_user_own_bucket(bucket_name, request):
    """
    获取当前用户的存储桶

    :param bucket_name: 存储通名称
    :param request: 请求对象
    :return:
        success: bucket
        failure: None
    """
    bucket = Bucket.get_bucket_by_name(bucket_name)
    if not bucket:
        return None
    if not bucket.check_user_own_bucket(request.user):
        return None
    return bucket


def str_to_int_or_default(val, default):
    """
    字符串转int，转换失败返回设置的默认值

    :param val: 待转化的字符串
    :param default: 转换失败返回的值
    :return:
        int     # success
        default # failed
    """
    try:
        return int(val)
    except Exception:
        return default


def check_authenticated_or_bucket_token(request, bucket_name: str = None, bucket_id: int = None, act='read', view=None):
    """
    检查是否认证，或者bucket token认证

    :param act: 请求类型，读或写，用于桶token权限匹配; ['read', 'write']
    :param bucket_name: 用于匹配和token所属的桶是否一致, 默认忽略
    :param bucket_id: 用于匹配和token所属的桶是否一致, 默认忽略
    :return:
        None

    :raises: Error
    """
    if IsAuthenticated().has_permission(request, view=view):
        return

    if isinstance(request.auth, BucketToken):
        token = request.auth
        if bucket_name:
            if token.bucket.name != bucket_name:  # 桶名是否一致
                raise exceptions.AccessDenied(message=_('token和存储桶不匹配'))

        if isinstance(bucket_id, int):
            if token.bucket.id != bucket_id:  # 桶id是否一致
                raise exceptions.AccessDenied(message=_('token和存储桶不匹配'))

        if act == 'write' and token.permission != token.PERMISSION_READWRITE:
            raise exceptions.AccessDenied(message=_('token没有写权限'))

        user = token.bucket.user
        if user.is_authenticated:
            request.user = user
            return

    raise exceptions.NotAuthenticated()


class UserViewSet(CustomGenericViewSet):
    """
    用户类视图
    list:
        获取用户列表,需要超级用户权限

        >> http code 200 返回内容:
            {
              "count": 2,  # 总数
              "next": null, # 下一页url
              "previous": null, # 上一页url
              "results": [
                {
                  "id": 3,
                  "username": "xx@xx.com",
                  "email": "xx@xx.com",
                  "date_joined": "2018-12-03T17:03:00+08:00",
                  "last_login": "2019-03-15T09:36:49+08:00",
                  "first_name": "",
                  "last_name": "",
                  "is_active": true,
                  "telephone": "",
                  "company": ""
                },
                {
                  ...
                }
              ]
            }

    retrieve:
    获取一个用户详细信息

        需要超级用户权限，或当前用户信息

        http code 200 返回内容:
            {
              "id": 3,
              "username": "xx@xx.com",
              "email": "xx@xx.com",
              "date_joined": "2018-12-03T17:03:00+08:00",
              "last_login": "2019-03-15T09:36:49+08:00",
              "first_name": "",
              "last_name": "",
              "is_active": true,
              "telephone": "",
              "company": ""
            }
        http code 403 返回内容:
            {
                "detail": "您没有执行该操作的权限。"
            }

    create:
    注册一个用户

        http code 201 返回内容:
            {
                'code': 201,
                'code_text': '用户注册成功，请登录邮箱访问收到的连接以激活用户',
                'data': { }  # 请求提交的数据
            }
        http code 500:
            {
                'detail': '激活链接邮件发送失败'
            }

    destroy:
    删除一个用户，需要超级管理员权限

        http code 204 无返回内容

    partial_update:
    修改用户信息

    1、超级职员用户拥有所有权限；
    2、用户拥有修改自己信息的权限；
    3、超级用户只有修改普通用户信息的权限

        http code 200 返回内容:
            {
                'code': 200,
                'code_text': '修改成功',
                'data':{ }   # 请求时提交的数据
            }
        http code 403:
            {
                'detail': 'xxx'
            }
    """
    queryset = User.objects.all()
    lookup_field = 'username'
    lookup_value_regex = '.+'

    @swagger_auto_schema(
        operation_summary=gettext_lazy('获取用户列表'),
        responses={
            status.HTTP_200_OK: ''
        }
    )
    def list(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())

        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('注册一个用户'),
        responses={
            status.HTTP_201_CREATED: ''
        }
    )
    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.save()
        if not send_active_url_email(request._request, user.email, user):
            return Response({'detail': _('激活链接邮件发送失败')}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        data = {
            'code': 201,
            'code_text': _('用户注册成功，请登录邮箱访问收到的连接以激活用户'),
            'data': serializer.validated_data,
        }
        return Response(data, status=status.HTTP_201_CREATED)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('获取一个用户详细信息'),
    )
    def retrieve(self, request, *args, **kwargs):
        instance = self.get_object()
        if not self.has_update_user_permission(request, instance=instance):
            return Response(data={"detail": _("您没有执行该操作的权限")}, status=status.HTTP_403_FORBIDDEN)

        serializer = self.get_serializer(instance)
        return Response(serializer.data)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('修改用户信息'),
    )
    def partial_update(self, request, *args, **kwargs):
        instance = self.get_object()
        if not self.has_update_user_permission(request, instance=instance):
            return Response(data={'detail': 'You do not have permission to change this user information'},
                            status=status.HTTP_403_FORBIDDEN)

        serializer = self.get_serializer(instance, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()

        if getattr(instance, '_prefetched_objects_cache', None):
            # If 'prefetch_related' has been applied to a queryset, we need to
            # forcibly invalidate the prefetch cache on the instance.
            instance._prefetched_objects_cache = {}

        return Response({'code': 200, 'code_text': '修改成功', 'data': serializer.validated_data})

    @swagger_auto_schema(
        operation_summary=gettext_lazy('删除一个用户'),
    )
    def destroy(self, request, *args, **kwargs):
        user = self.get_object()
        if user.is_active is not False:
            user.is_active = False
            user.save()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @staticmethod
    def has_update_user_permission(request, instance):
        """
        当前用户是否有修改给定用户信息的权限
        1、超级职员用户拥有所有权限；
        2、用户拥有修改自己信息的权限；
        3、超级用户只有修改普通用户信息的权限；

        :param request:
        :param instance: 用户实例
        :return:
            True: has permission
            False: has not permission
        """
        user = request.user
        if not user.id:     # 未认证用户
            return False

        # 当前用户是超级职员用户，有超级权限
        if user.is_superuser and user.is_staff:
            return True

        # 当前用户不是APP超级用户，只有修改自己信息的权限
        if not user.is_app_superuser():
            # 当前用户修改自己的信息
            if user.id == instance.id:
                return True

            return False

        # 当前APP超级用户，只有修改普通用户的权限
        elif not instance.is_superuser:
            return True

        return False

    def get_serializer_class(self):
        """
        动态加载序列化器
        """
        if self.action == 'create':
            return serializers.UserCreateSerializer
        elif self.action == 'partial_update':
            return serializers.UserUpdateSerializer

        return serializers.UserDeitalSerializer

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action == 'list':
            return [permissions.IsSuperUser()]
        elif self.action == 'create':
            return []
        elif self.action in ['retrieve', 'update', 'partial_update']:
            return [IsAuthenticated()]
        elif self.action == 'delete':
            return [permissions.IsSuperAndStaffUser()]
        return [permissions.IsSuperUser()]


class BucketViewSet(CustomGenericViewSet):
    """
    存储桶视图

    create:
    创建一个新的存储桶
    存储桶名称，名称唯一，不可使用已存在的名称，符合DNS标准的存储桶名称，英文字母、数字和-组成，3-63个字符

        >>Http Code: 状态码201；创建成功时：
            {
              "code": 201,
              "code_text": "创建成功",
              "data": {                 //请求时提交数据
                "name": "333"
              },
              "bucket": {               //bucket对象信息
                "id": 225,
                "name": "333",
                "user": {
                  "id": 3,
                  "username": "869588058@qq.com"
                },
                "created_time": "2019-02-20T13:56:25+08:00",
                "access_permission": "私有",
                "ftp_enable": false,
                "ftp_password": "696674124f",
                "ftp_ro_password": "9563d3cc29"
              }
            }
        >>Http Code: 状态码400,参数有误：
            {
                'code': 400,
                'code_text': 'xxx',      //错误码表述信息
                'data': {}, //请求时提交数据
                'existing': true or  false  // true表示资源已存在
            }
        >>Http Code: 状态码409, 存储桶已存在：
            {
                'code': 'BucketAlreadyExists',    # or BucketAlreadyOwnedByYou
                'code_text': 'xxx',      //错误码表述信息
            }
    """
    queryset = Bucket.objects.select_related('user').all()
    permission_classes = [IsAuthenticated]
    pagination_class = paginations.BucketsLimitOffsetPagination
    lookup_field = 'id_or_name'
    lookup_value_regex = '[a-z0-9-]+'

    DETAIL_BASE_PARAMS = [
        openapi.Parameter(
            name='id_or_name',
            in_=openapi.IN_PATH,
            type=openapi.TYPE_STRING,
            required=True,
            description=gettext_lazy('默认为bucket ID，使用bucket name需要通过参数by-name指示')
        ),
        openapi.Parameter(
            name='by-name',
            in_=openapi.IN_QUERY,
            type=openapi.TYPE_BOOLEAN,
            required=False,
            description=gettext_lazy('true,表示使用bucket name指定bucket；其他值忽略')
        )
    ]

    @swagger_auto_schema(
        operation_summary=gettext_lazy('获取存储桶列表'),
        responses={
            status.HTTP_200_OK: """
                {
                  "count": 18,
                  "next": null,
                  "page": {
                    "current": 1,
                    "final": 1
                  },
                  "previous": null,
                  "buckets": [
                    {
                      "id": 222,
                      "name": "hhf",
                      "user": {
                        "id": 3,
                        "username": "869588058@qq.com"
                      },
                      "created_time": "2019-02-20T13:56:25+08:00",
                      "access_permission": "公有",
                      "ftp_enable": false,
                      "ftp_password": "1a0cdf3283",
                      "ftp_ro_password": "666666666"
                    },
                  ]
                }
            """
        }
    )
    def list(self, request, *args, **kwargs):
        """
        获取存储桶列表
        """
        self.queryset = Bucket.objects.select_related('user').filter(user=request.user).all()   # user's own

        queryset = self.filter_queryset(self.get_queryset())
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        else:
            serializer = self.get_serializer(queryset, many=True)
            data = {'code': 200, 'buckets': serializer.data}
        return Response(data)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('创建一个存储桶'),
        responses={
            status.HTTP_201_CREATED: 'OK'
        }
    )
    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        if not serializer.is_valid(raise_exception=False):
            code_text = serializer_error_text(serializer.errors, default='参数验证有误')
            data = {
                'code': 400,
                'code_text': code_text,
                'existing': False,
                'data': serializer.data,
            }

            return Response(data, status=status.HTTP_400_BAD_REQUEST)

        # 检测桶是否存在
        validated_data = serializer.validated_data
        bucket_name = validated_data.get('name')
        user = request.user
        bucket = Bucket.get_bucket_by_name(bucket_name)
        if bucket:
            if bucket.user_id == user.id:
                exc = exceptions.BucketAlreadyOwnedByYou()
            else:
                exc = exceptions.BucketAlreadyExists()

            data = exc.err_data_old()
            return Response(data=data, status=409)

        # 创建bucket,创建bucket的对象元数据表
        try:
            bucket = serializer.save()
        except Exception as e:
            logger.error(f'创建桶“{bucket_name}”失败, {str(e)}')
            exc = exceptions.Error(message=_('创建桶失败') + str(e))
            return Response(data=exc.err_data_old(), status=exc.status_code)

        data = {
            'code': 201,
            'code_text': '创建成功',
            'data': serializer.data,
            'bucket': serializers.BucketSerializer(serializer.instance).data
        }
        return Response(data, status=status.HTTP_201_CREATED)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('通过桶ID或name获取一个存储桶详细信息'),
        manual_parameters=DETAIL_BASE_PARAMS,
        responses={
            status.HTTP_200_OK: """
                {
                  "code": 200,
                  "bucket": {
                    "id": 222,
                    "name": "hhf",
                    "user": {
                      "id": 3,
                      "username": "869588058@qq.com"
                    },
                    "created_time": "2019-02-20T13:56:25+08:00",
                    "access_permission": "公有",
                    "ftp_enable": false,
                    "ftp_password": "1a0cdf3283",
                    "ftp_ro_password": "666666666"
                  }
                }
            """,
            "400, 403, 404": """
                {
                    'code': 'NoSuchBucket',    # or AccessDenied、BadRequest
                    'code_text': 'xxx',      //错误码表述信息
                }
            """
        }
    )
    def retrieve(self, request, *args, **kwargs):
        """
        获取一个存储桶详细信息
        """
        id_or_name, by_name = self.get_id_or_name_params(request, kwargs)
        if by_name:
            params = {'bucket_name': id_or_name}
        else:
            params = {'bucket_id': id_or_name}

        try:
            check_authenticated_or_bucket_token(request, **params, act='read', view=self)
        except exceptions.Error as exc:
            return Response(data=exc.err_data_old(), status=exc.status_code)

        try:
            bucket = self.get_user_bucket(id_or_name=id_or_name, by_name=by_name, user=request.user)
        except exceptions.Error as exc:
            return Response(data=exc.err_data_old(), status=exc.status_code)

        serializer = self.get_serializer(bucket)
        return Response({'code': 200, 'bucket': serializer.data})

    @swagger_auto_schema(
        operation_summary=gettext_lazy('删除一个存储桶'),
        manual_parameters=DETAIL_BASE_PARAMS + [
            openapi.Parameter(
                name='ids', in_=openapi.IN_QUERY,
                type=openapi.TYPE_ARRAY,
                items=openapi.Items(type=openapi.TYPE_INTEGER),
                description=gettext_lazy("存储桶id列表或数组，删除多个存储桶时，通过此参数传递其他存储桶id"),
                required=False
            ),
        ],
        responses={
            status.HTTP_204_NO_CONTENT: 'NO_CONTENT'
        }
    )
    def destroy(self, request, *args, **kwargs):
        """
        删除一个存储桶

            >>Http Code: 状态码204,存储桶删除成功
            >>Http Code: 400, 403, 404:
                {
                    'code': 'NoSuchBucket',    # or AccessDenied、BadRequest
                    'code_text': 'xxx',      //错误码表述信息
                }
        """
        try:
            ids = self.get_buckets_ids(request)
        except ValueError as e:
            return Response(data={'code': 400, 'code_text': str(e)}, status=status.HTTP_400_BAD_REQUEST)

        id_or_name, by_name = self.get_id_or_name_params(request, kwargs)
        try:
            bucket = self.get_user_bucket(id_or_name=id_or_name, by_name=by_name, user=request.user)
        except exceptions.Error as exc:
            return Response(data=exc.err_data_old(), status=exc.status_code)

        if not bucket.delete_and_archive():  # 删除归档
            return Response(data={'code': 500, 'code_text': _('删除存储桶失败')}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        if ids:
            buckets = Bucket.objects.select_related('user').filter(id__in=ids).filter(user=request.user).all()
            if not buckets.exists():
                return Response(data={'code': 404, 'code_text': _('未找到要删除的存储桶')}, status=status.HTTP_404_NOT_FOUND)
            for bucket in buckets:
                if not bucket.delete_and_archive():  # 删除归档
                    return Response(data={'code': 500, 'code_text': _('删除存储桶失败')},
                                    status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(status=status.HTTP_204_NO_CONTENT)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('存储桶访问权限设置'),
        request_body=no_body,
        manual_parameters=DETAIL_BASE_PARAMS + [
            openapi.Parameter(
                name='public', in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                description=gettext_lazy("设置访问权限, 1(公有)，2(私有)，3（公有可读可写）"),
                required=True
            ),
        ],
        responses={
            status.HTTP_200_OK: """
                {
                  "code": 200,
                  "code_text": "存储桶权限设置成功",
                  "public": 1,
                  "share": [
                    "http://159.226.91.140:8000/share/s/hhf"
                  ]
                }
            """
        }
    )
    def partial_update(self, request, *args, **kwargs):
        """
        存储桶访问权限设置

            Http Code: 状态码200：上传成功无异常时，返回数据：
            {
                'code': 200,
                'code_text': '对象共享设置成功'，
                'public': xxx,
            }
            >>Http Code: 400, 403, 404:
            {
                'code': 'NoSuchBucket',    # or AccessDenied、BadRequest
                'code_text': 'xxx',      //错误码表述信息
            }
            Http code: 状态码500：
            {
                "code": 500,
                "code_text": "保存到数据库时错误"
            }
        """
        public = str_to_int_or_default(request.query_params.get('public', ''), 0)
        if public not in [1, 2, 3]:
            return Response(data={'code': 400, 'code_text': _('public参数有误')}, status=status.HTTP_400_BAD_REQUEST)

        id_or_name, by_name = self.get_id_or_name_params(request, kwargs)
        if by_name:
            params = {'bucket_name': id_or_name}
        else:
            params = {'bucket_id': id_or_name}

        try:
            check_authenticated_or_bucket_token(request, **params, act='write', view=self)
        except exceptions.Error as exc:
            return Response(data=exc.err_data_old(), status=exc.status_code)

        try:
            bucket = self.get_user_bucket(id_or_name=id_or_name, by_name=by_name, user=request.user)
        except exceptions.Error as exc:
            return Response(data=exc.err_data_old(), status=exc.status_code)

        share_urls = []
        url = django_reverse('share:share-view', kwargs={'share_base': bucket.name})
        url = request.build_absolute_uri(url)
        share_urls.append(url)
        if not bucket.set_permission(public=public):
            return Response(data={'code': 500, 'code_text': _('更新数据库数据时错误')},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        data = {
            'code': 200,
            'code_text': _('存储桶权限设置成功'),
            'public': public,
            'share': share_urls
        }
        return Response(data=data, status=status.HTTP_200_OK)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('存储桶备注信息设置'),
        request_body=no_body,
        manual_parameters=DETAIL_BASE_PARAMS + [
            openapi.Parameter(
                name='remarks', in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("备注信息"),
                required=True
            ),
        ],
        responses={
            status.HTTP_200_OK: """
                    {
                      "code": 200,
                      "code_text": "存储桶备注信息设置成功",
                    }
                """
        }
    )
    @action(methods=['patch'], detail=True, url_path='remark', url_name='remark')
    def remarks(self, request, *args, **kwargs):
        """
        存储桶备注信息设置

            >>Http Code 200 OK;
            >>Http Code: 400, 403, 404:
                {
                    'code': 'NoSuchBucket',    # or AccessDenied、BadRequest
                    'code_text': 'xxx',      //错误码表述信息
                }
        """
        id_or_name, by_name = self.get_id_or_name_params(request, kwargs)
        if by_name:
            params = {'bucket_name': id_or_name}
        else:
            params = {'bucket_id': id_or_name}

        try:
            check_authenticated_or_bucket_token(request, **params, act='write', view=self)
        except exceptions.Error as exc:
            return Response(data=exc.err_data_old(), status=exc.status_code)

        remarks = request.query_params.get('remarks', '')
        if not remarks:
            return Response(data={'code': 400, 'code_text': _('备注信息不能为空')}, status=status.HTTP_400_BAD_REQUEST)

        try:
            bucket = self.get_user_bucket(id_or_name=id_or_name, by_name=by_name, user=request.user)
        except exceptions.Error as exc:
            return Response(data=exc.err_data_old(), status=exc.status_code)

        if not bucket.set_remarks(remarks=remarks):
            return Response(data={'code': 500, 'code_text': _('设置备注信息失败，更新数据库数据时错误')},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        data = {
            'code': 200,
            'code_text': _('存储桶备注信息设置成功'),
        }
        return Response(data=data, status=status.HTTP_200_OK)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('创建一个存储桶token'),
        request_body=no_body,
        manual_parameters=DETAIL_BASE_PARAMS + [
            openapi.Parameter(
                name='permission', in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("访问权限，[readwrite, readonly]"),
                required=True
            ),
        ],
        responses={
            status.HTTP_200_OK: """
                        
                    """
        }
    )
    @action(methods=['post'], detail=True, url_path='token/create', url_name='token-create')
    def token_create(self, request, *args, **kwargs):
        """
        创建存储桶token

            http 200:
            {
              "key": "365ad32dcfd3a6d2aa9f94673977d57b634a8339",
              "bucket": {
                "id": 3,
                "name": "ddd"
              },
              "permission": "readwrite",
              "created": "2020-12-21T11:13:07.022989+08:00"
            }
            >>Http Code: 400, 403, 404:
                {
                    'code': 'NoSuchBucket',    # or AccessDenied、BadRequest、TooManyBucketTokens
                    'code_text': 'xxx',      //错误码表述信息
                }
        """
        perm = request.query_params.get('permission', '').lower()
        if perm not in [BucketToken.PERMISSION_READWRITE, BucketToken.PERMISSION_READONLY]:
            exc = exceptions.BadRequest(message=_('参数permission的值无效。'))
            return Response(data=exc.err_data(), status=exc.status_code)

        id_or_name, by_name = self.get_id_or_name_params(request, kwargs)
        try:
            bucket = self.get_user_bucket(id_or_name=id_or_name, by_name=by_name, user=request.user)
        except exceptions.Error as exc:
            return Response(data=exc.err_data(), status=exc.status_code)

        c = BucketToken.objects.filter(bucket=bucket).count()
        if c >= 2:
            exc = exceptions.TooManyBucketTokens()
            return Response(data=exc.err_data(), status=exc.status_code)

        try:
            token = BucketToken(bucket=bucket, permission=perm)
            token.save()
        except Exception as e:
            exc = exceptions.Error(message=_('数据库错误，插入token数据失败。'), extend_msg=str(e))
            return Response(data=exc.err_data(), status=exc.status_code)

        serializer = serializers.BucketTokenSerializer(instance=token)
        return Response(data=serializer.data)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('列举存储桶token'),
        request_body=no_body,
        manual_parameters=DETAIL_BASE_PARAMS,
        responses={
            status.HTTP_200_OK: """"""
        }
    )
    @action(methods=['get'], detail=True, url_path='token/list', url_name='token-list')
    def token_list(self, request, *args, **kwargs):
        """
        列举存储桶token

            http 200:
                {
                  "count": 2,
                  "tokens": [
                    {
                      "key": "4e7be5d14dc868b6dbf843fb1d11b45ab28f6326",
                      "bucket": {
                        "id": 3,
                        "name": "ddd"
                      },
                      "permission": "readwrite",
                      "created": "2020-12-16T15:49:03.180761+08:00"
                    },
                    ...
                  ]
                }
            >>Http Code: 400, 403, 404:
                {
                    'code': 'NoSuchBucket',    # or AccessDenied、BadRequest
                    'code_text': 'xxx',      //错误码表述信息
                }
        """
        id_or_name, by_name = self.get_id_or_name_params(request, kwargs)
        try:
            bucket = self.get_user_bucket(id_or_name=id_or_name, by_name=by_name, user=request.user)
        except exceptions.Error as exc:
            return Response(data=exc.err_data(), status=exc.status_code)

        tokens = list(bucket.token_set.all())
        serializer = serializers.BucketTokenSerializer(instance=tokens, many=True)
        data = {
            'count': len(tokens),
            'tokens': serializer.data
        }
        return Response(data=data)

    def get_id_or_name_params(self, request, kwargs):
        """
        :return: str, bool
            name, True
            id, False
        """
        id_or_name = kwargs.get(self.lookup_field, '')
        by_name = request.query_params.get('by-name', '').lower()
        if by_name == 'true':
            return id_or_name, True

        return id_or_name, False

    @staticmethod
    def get_user_bucket(id_or_name: str, by_name: bool = False, user=None):
        """
        获取存储桶对象，并检测用户访问权限

        :return:
            Bucket()

        :raises: Error
        """
        if by_name:
            bucket = Bucket.objects.select_related('user').filter(name=id_or_name).first()
        else:
            try:
                bid = int(id_or_name)
            except Exception as e:
                raise exceptions.BadRequest(message=_('无效的存储桶ID'))

            bucket = Bucket.objects.filter(id=bid).first()

        if not bucket:
            raise exceptions.NoSuchBucket(message=_('存储桶不存在'))

        if not bucket.check_user_own_bucket(user):
            raise exceptions.AccessDenied(message=_('您没有操作此存储桶的权限'))

        return bucket

    @staticmethod
    def get_buckets_ids(request, **kwargs):
        """
        获取存储桶id列表
        :param request:
        :return:
            ids: list
        :raises: ValueError
        """
        if isinstance(request.query_params, QueryDict):
            ids = request.query_params.getlist('ids')
        else:
            ids = request.query_params.get('ids')

        if not isinstance(ids, list):
            return []

        try:
            ids = [int(i) for i in ids]
        except ValueError:
            return ValueError(_('无效的存储桶ID'))

        return ids

    def get_serializer_class(self):
        """
        Return the class to use for the serializer.
        Defaults to using `self.serializer_class`.
        Custom serializer_class
        """
        if self.action in ['list', 'retrieve']:
            return serializers.BucketSerializer
        elif self.action == 'create':
            return serializers.BucketCreateSerializer
        return Serializer

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action in ['list', 'create', 'delete', 'token_list', 'token_create']:
            return [IsAuthenticated()]

        return [permissions.IsAuthenticatedOrBucketToken()]


class ObjViewSet(CustomGenericViewSet):
    """
    文件对象视图集

    create_detail:
        通过文件对象绝对路径分片上传文件对象

        说明：
        * 请求类型ContentType = multipart/form-data；不是json，请求体中分片chunk当成是一个文件或类文件处理；
        * 小文件可以作为一个分片上传，大文件请自行分片上传，分片过大可能上传失败，建议分片大小5-10MB；对象上传支持部分上传，
          分片上传数据直接写入对象，已成功上传的分片数据永久有效且不可撤销，请自行记录上传过程以实现断点续传；
        * 文件对象已存在时，数据上传会覆盖原数据，文件对象不存在，会自动创建文件对象，并且文件对象的大小只增不减；
          如果覆盖（已存在同名的对象）上传了一个新文件，新文件的大小小于原同名对象，上传完成后的对象大小仍然保持
          原对象大小（即对象大小只增不减），如果这不符合你的需求，参考以下2种方法：
          (1)先尝试删除对象（对象不存在返回404，成功删除返回204），再上传；
          (2)访问API时，提交reset参数，reset=true时，再保存分片数据前会先调整对象大小（如果对象已存在），未提供reset参
            数或参数为其他值，忽略之。
          ## 特别提醒：切记在需要时只在上传第一个分片时提交reset参数，否者在上传其他分片提交此参数会调整对象大小，
          已上传的分片数据会丢失。

        注意：
        分片上传现不支持并发上传，并发上传可能造成脏数据，上传分片顺序没有要求，请一个分片上传成功后再上传另一个分片

        Http Code: 状态码200：上传成功无异常时，返回数据：
        {
          "chunk_offset": 0,    # 请求参数
          "chunk": null,
          "chunk_size": 34,     # 请求参数
          "created": true       # 上传第一个分片时，可用于判断对象是否是新建的，True(新建的)
        }
        Http Code: 状态码400：参数有误时，返回数据：
            {
                'code': 400,
                'code_text': '对应参数错误信息'
            }
        Http Code: 状态码500
            {
                'code': 500,
                'code_text': '文件块rados写入失败'
            }

    partial_update:
    对象共享或私有权限设置

        Http Code: 状态码200：上传成功无异常时，返回数据：
        {
            'code': 200,
            'code_text': '对象共享设置成功'，
            "share_uri": "xxx"    # 分享下载uri
            'share': xxx,
            'days': xxx
        }
        >>Http Code: 400 401 403 404 500
        {
            'code': "NoSuchKey",   // AccessDenied、BadRequest、BucketLockWrite
            'code_text': '参数有误'
        }

    """
    queryset = {}
    permission_classes = [permissions.IsAuthenticatedOrBucketToken]
    lookup_field = 'objpath'
    lookup_value_regex = '.+'
    parser_classes = (parsers.MultiPartParser, parsers.FormParser)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('分片上传文件对象'),
        manual_parameters=[
            openapi.Parameter(
                name='objpath', in_=openapi.IN_PATH,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("文件对象绝对路径"),
                required=True
            ),
            openapi.Parameter(
                name='reset', in_=openapi.IN_QUERY,
                type=openapi.TYPE_BOOLEAN,
                description=gettext_lazy("reset=true时，如果对象已存在，重置对象大小为0"),
                required=False
            ),
        ],
        responses={
            status.HTTP_200_OK: """
                {
                  "chunk_offset": 0,
                  "chunk": null,
                  "chunk_size": 34,
                  "created": true
                }
            """
        }
    )
    def create_detail(self, request, *args, **kwargs):
        objpath = kwargs.get(self.lookup_field, '')
        bucket_name = kwargs.get('bucket_name', '')

        try:
            check_authenticated_or_bucket_token(request, bucket_name=bucket_name, act='write', view=self)
        except exceptions.Error as exc:
            return Response(data=exc.err_data(), status=exc.status_code)

        reset = request.query_params.get('reset', '').lower()
        if reset == 'true':
            reset = True
        else:
            reset = False

        # 数据验证
        try:
            put_data = self.get_data(request)
        except Exception as e:
            logger.error(f'in request.data during upload file: {e}')
            return Response({
                'code': 500, 'code_text': 'SERVER ERROR',
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        serializer = self.get_serializer(data=put_data)
        if not serializer.is_valid(raise_exception=False):
            msg = serializer_error_text(serializer.errors)
            return Response({'code': 400, 'code_text': msg}, status=status.HTTP_400_BAD_REQUEST)

        data = serializer.data
        offset = data.get('chunk_offset')
        file = request.data.get('chunk')

        hmanager = HarborManager()
        try:
            created = hmanager.write_file(bucket_name=bucket_name, obj_path=objpath, offset=offset, file=file,
                                          reset=reset, user=request.user)
        except exceptions.HarborError as e:
            return Response(data=e.err_data_old(), status=e.status_code)

        data['created'] = created
        return Response(data, status=status.HTTP_200_OK)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('上传一个完整对象'),
        manual_parameters=[
            openapi.Parameter(
                name='objpath', in_=openapi.IN_PATH,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("文件对象绝对路径"),
                required=True
            ),
        ],
        responses={
            status.HTTP_200_OK: """
                {
                    "code": 200,
                    "created": true   # true: 上传创建一个新对象; false: 上传覆盖一个已存在的旧对象 
                }
                """,
            status.HTTP_400_BAD_REQUEST: """
                {
                    "code": 400,
                    "code_text": "xxxx"
                }
                """
        }
    )
    def update(self, request, *args, **kwargs):
        """
        上传一个完整对象, 如果同名对象已存在，会覆盖旧对象；
        上传对象大小限制10GB，超过限制的对象请使用分片上传方式；
        如果担心上传过程中数据损坏不一致，可以使用标头Content-MD5，当您使用此标头时，将根据提供的MD5值检查对象，如果不匹配，则返回错误。
        不提供对象锁定，如果同时对同一对象发起多个写请求，会造成数据混乱，损坏数据一致性；
        """
        objpath = kwargs.get(self.lookup_field, '')
        bucket_name = kwargs.get('bucket_name', '')

        try:
            check_authenticated_or_bucket_token(request, bucket_name=bucket_name, act='write', view=self)
        except exceptions.Error as exc:
            return Response(data=exc.err_data(), status=exc.status_code)

        hmanager = HarborManager()
        try:
            bucket, obj, created = hmanager.create_empty_obj(
                bucket_name=bucket_name, obj_path=objpath, user=request.user)
        except exceptions.HarborError as e:
            return Response(data=e.err_data_old(), status=e.status_code)

        pool_name = bucket.get_pool_name()
        obj_key = obj.get_obj_key(bucket.id)

        rados = build_harbor_object(using=bucket.ceph_using, pool_name=pool_name, obj_id=obj_key, obj_size=obj.si)
        if created is False:  # 对象已存在，不是新建的
            try:
                hmanager._pre_reset_upload(obj=obj, rados=rados)    # 重置对象大小
            except Exception as e:
                return Response({'code': 400, 'code_text': f'reset object error, {str(e)}'},
                                status=status.HTTP_400_BAD_REQUEST)

        return self.update_handle(request=request, bucket=bucket, obj=obj, rados=rados, created=created)

    def update_handle(self, request, bucket, obj, rados, created):
        pool_name = bucket.get_pool_name()
        obj_key = obj.get_obj_key(bucket.id)
        uploader = FileUploadToCephHandler(request, using=bucket.ceph_using, pool_name=pool_name, obj_key=obj_key)
        request.upload_handlers = [uploader]

        def clean_put(_uploader, _obj, _created, _rados):
            # 删除数据和元数据
            f = getattr(_uploader, 'file', None)
            if f is not None:
                try_close_file(f)

            s = f.size if f else 0
            _rados.delete(obj_size=s)
            if _created:
                _obj.delete()

        # 数据验证
        try:
            put_data = self.get_data(request)
        except Exception as e:
            clean_put(uploader, obj, created, rados)
            return Response({
                'code': 400, 'code_text': str(e),
            }, status=status.HTTP_400_BAD_REQUEST)

        serializer = self.get_serializer(data=put_data)
        if not serializer.is_valid(raise_exception=False):
            # 删除数据和元数据
            clean_put(uploader, obj, created, rados)
            msg = serializer_error_text(serializer.errors)
            return Response({'code': 400, 'code_text': msg}, status=status.HTTP_400_BAD_REQUEST)

        file = serializer.validated_data.get('file')
        content_md5 = self.request.headers.get('Content-MD5', '').lower()
        if content_md5:
            if content_md5 != file.file_md5.lower():
                # 删除数据和元数据
                clean_put(uploader, obj, created, rados)
                return Response({'code': 400,
                                 'code_text': _('标头Content-MD5和上传数据的MD5值不一致，数据在上传过程中可能损坏')},
                                status=status.HTTP_400_BAD_REQUEST)
        else:
            content_md5 = file.file_md5.lower()

        try:
            obj.si = file.size
            obj.md5 = content_md5
            obj.save(update_fields=['si', 'md5', 'upt'])
        except Exception as e:
            # 删除数据和元数据
            clean_put(uploader, obj, created, rados)
            return Response({'code': 400, 'code_text': str(e)}, status=status.HTTP_400_BAD_REQUEST)

        data = {'code': 200, 'created': created}
        return Response(data, status=status.HTTP_200_OK)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('下载文件对象，自定义读取对象数据块'),
        manual_parameters=[
            openapi.Parameter(
                name='offset', in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                description=gettext_lazy("要读取的文件块在整个文件中的起始位置(bytes偏移量)"),
                required=False
            ),
            openapi.Parameter(
                name='size', in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                description=gettext_lazy("要读取的文件块的字节大小"),
                required=False
            ),
        ],
        responses={
            status.HTTP_200_OK: """
                Content-Type: application/octet-stream
            """
        }
    )
    def retrieve(self, request, *args, **kwargs):
        """
        通过文件对象绝对路径,下载文件对象，或者自定义读取对象数据块

            *注：
            1. offset && size参数校验失败时返回状态码400和对应参数错误信息，无误时，返回bytes数据流
            2. 不带参数时，返回整个文件对象；

            >>Http Code: 状态码200：
                 evhb_obj_size,文件对象总大小信息,通过标头headers传递：自定义读取时：返回指定大小的bytes数据流；
                其他,返回整个文件对象bytes数据流

            >>Http Code: 400 401 403 404 500
            {
                'code': "NoSuchKey",   // AccessDenied、BadRequest、BucketLockWrite
                'code_text': '参数有误'
            }
        """
        objpath = kwargs.get(self.lookup_field, '')
        bucket_name = kwargs.get('bucket_name', '')

        try:
            check_authenticated_or_bucket_token(request, bucket_name=bucket_name, act='read', view=self)
        except exceptions.Error as exc:
            pass

        validated_param, valid_response = self.custom_read_param_validate_or_response(request)
        if not validated_param and valid_response:
            return valid_response

        # 自定义读取文件对象
        if validated_param:
            offset = validated_param.get('offset')
            size = validated_param.get('size')
            return self.range_response(user=request.user, bucket_name=bucket_name,
                                       obj_path=objpath, offset=offset, size=size, status_code=200)
        # 下载整个文件对象
        h_manager = HarborManager()
        try:
            file_generator, obj = h_manager.get_obj_generator(bucket_name=bucket_name, obj_path=objpath,
                                                              user=request.user, all_public=True)
        except exceptions.HarborError as e:
            return Response(data=e.err_data_old(), status=e.status_code)

        filename = obj.name
        filename = urlquote(filename)  # 中文文件名需要
        response = FileResponse(file_generator)
        response['Content-Type'] = 'application/octet-stream'  # 注意格式
        response['Content-Length'] = obj.si
        response['Content-Disposition'] = f"attachment;filename*=utf-8''{filename}"  # 注意filename 这个是下载后的名字
        response['evob_obj_size'] = obj.si
        return response

    @swagger_auto_schema(
        operation_summary=gettext_lazy('删除一个对象')
    )
    def destroy(self, request, *args, **kwargs):
        """
        通过文件对象绝对路径,删除文件对象；

            >>Http Code: 状态码204：删除成功，NO_CONTENT；
            >>Http Code: 400 401 403 404 500
                {
                    'code': "NoSuchKey",   // AccessDenied、BadRequest、BucketLockWrite
                    'code_text': '参数有误'
                }
        """
        objpath = kwargs.get(self.lookup_field, '')
        bucket_name = kwargs.get('bucket_name', '')

        try:
            check_authenticated_or_bucket_token(request, bucket_name=bucket_name, act='write', view=self)
        except exceptions.Error as exc:
            return Response(data=exc.err_data(), status=exc.status_code)

        hmanager = HarborManager()
        try:
            ok = hmanager.delete_object(bucket_name=bucket_name, obj_path=objpath, user=request.user)
        except exceptions.HarborError as e:
            return Response(data=e.err_data_old(), status=e.status_code)

        if not ok:
            return Response(data={'code': 500, 'code_text': _('删除失败')}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(status=status.HTTP_204_NO_CONTENT)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('对象共享或私有权限设置'),
        manual_parameters=[
            openapi.Parameter(
                name='share', in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                description=gettext_lazy("分享访问权限，0（不分享禁止访问），1（分享只读），2（分享可读可写）"),
                required=True
            ),
            openapi.Parameter(
                name='days', in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                description=gettext_lazy("对象公开分享天数(share!=0时有效)，0表示永久公开，负数表示不公开，默认为0"),
                required=False
            ),
            openapi.Parameter(
                name='password', in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("分享密码，此参数不存在，不设密码；可指定4-8字符；若为空，随机分配密码"),
                required=False
            ),
        ],
        responses={
            status.HTTP_200_OK: """
                {
                  "code": 200,
                  "code_text": "对象共享权限设置成功",
                  "share": 1,
                  "days": 2,
                  "share_uri": "xxx"    # 分享下载uri,
                  "access_code": 1
                }        
            """
        }
    )
    def partial_update(self, request, *args, **kwargs):
        bucket_name = kwargs.get('bucket_name', '')
        objpath = kwargs.get(self.lookup_field, '')
        pw = request.query_params.get('password', None)

        try:
            check_authenticated_or_bucket_token(request, bucket_name=bucket_name, act='write', view=self)
        except exceptions.Error as exc:
            return Response(data=exc.err_data(), status=exc.status_code)

        if pw:  # 指定密码
            if not (4 <= len(pw) <= 8):
                return Response(data={'code': 400, 'code_text': _('password参数长度为4-8个字符')},
                                status=status.HTTP_400_BAD_REQUEST)
            password = pw
        elif pw is None:  # 不设密码
            password = ''
        else:  # 随机分配密码
            password = rand_share_code()

        days = str_to_int_or_default(request.query_params.get('days', 0), None)
        if days is None:
            return Response(data={'code': 400, 'code_text': _('days参数有误')}, status=status.HTTP_400_BAD_REQUEST)

        share = request.query_params.get('share', None)
        if share is None:
            return Response(data={'code': 400, 'code_text': _('缺少share参数')}, status=status.HTTP_400_BAD_REQUEST)

        share = str_to_int_or_default(share, -1)
        if share not in [0, 1, 2]:
            return Response(data={'code': 400, 'code_text': _('share参数有误')}, status=status.HTTP_400_BAD_REQUEST)

        hmanager = HarborManager()
        try:
            ok, access_code = hmanager.share_object(
                bucket_name=bucket_name, obj_path=objpath, share=share,
                days=days, password=password, user=request.user)
        except exceptions.HarborError as e:
            return Response(data=e.err_data_old(), status=e.status_code)

        if not ok:
            return Response(data={'code': 500, 'code_text': _('对象共享权限设置失败')},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        share_uri = django_reverse('share:obs-detail', kwargs={'objpath': f'{bucket_name}/{objpath}'})
        if password:
            share_uri = f'{share_uri}?p={password}'
        share_uri = request.build_absolute_uri(share_uri)
        data = {
            'code': 200,
            'code_text': _('对象共享权限设置成功'),
            'share': share,
            'days': days,
            'share_uri': share_uri,
            'access_code': access_code
        }
        return Response(data=data, status=status.HTTP_200_OK)

    def get_serializer_class(self):
        """
        Return the class to use for the serializer.
        Defaults to using `self.serializer_class`.
        Custom serializer_class
        """
        if self.action == 'create_detail':
            return serializers.ObjPutSerializer
        elif self.action == 'update':
            return serializers.ObjPutFileSerializer
        return Serializer

    @staticmethod
    def do_bucket_limit_validate(bfm: BucketFileManagement):
        """
        存储桶的限制验证
        :return: True(验证通过); False(未通过)
        """
        # 存储桶对象和文件夹数量上限验证
        if bfm.get_count() >= 10**7:
            return False

        return True

    @staticmethod
    def custom_read_param_validate_or_response(request):
        """
        自定义读取文件对象参数验证
        :param request:
        :return:
                (None, None) -> 未携带参数
                (None, response) -> 参数有误
                ({data}, None) -> 参数验证通过

        """
        chunk_offset = request.query_params.get('offset', None)
        chunk_size = request.query_params.get('size', None)

        validated_data = {}
        if chunk_offset is not None and chunk_size is not None:
            try:
                offset = int(chunk_offset)
                size = int(chunk_size)
                # if offset < 0 or size < 0 or size > 20*1024**2: #20Mb
                #     raise Exception()
                validated_data['offset'] = offset
                validated_data['size'] = size
            except Exception:
                response = Response(data={'code': 400, 'code_text': _('offset或size参数有误')},
                                    status=status.HTTP_400_BAD_REQUEST)
                return None, response
        # 未提交参数
        elif chunk_offset is None and chunk_size is None:
            return None, None
        # 参数提交不全
        else:
            response = Response(data={'code': 400, 'code_text': _('offset和size参数必须同时提交')},
                                status=status.HTTP_400_BAD_REQUEST)
            return None, response
        return validated_data, None

    @staticmethod
    def wrap_chunk_response(chunk: bytes, obj_size: int):
        """
        文件对象自定义读取response

        :param chunk: 数据块
        :param obj_size: 文件对象总大小
        :return: HttpResponse
        """
        c_len = len(chunk)
        response = StreamingHttpResponse(BytesIO(chunk), status=status.HTTP_200_OK)
        response['Content-Type'] = 'application/octet-stream'  # 注意格式
        response['evob_chunk_size'] = c_len
        response['Content-Length'] = c_len
        response['evob_obj_size'] = obj_size
        return response

    @staticmethod
    def offset_size_to_start_end(offset, size, obj_size):
        filesize = obj_size
        if offset >= filesize:
            offset = filesize
        else:
            offset = max(0, offset)

        end = min(offset + size - 1, filesize - 1)
        return offset, end

    def range_response(self, user, bucket_name: str, obj_path: str, offset: int, size: int,
                       status_code: int = status.HTTP_206_PARTIAL_CONTENT):
        try:
            file_generator, obj = HarborManager().get_obj_generator(
                bucket_name=bucket_name, obj_path=obj_path, offset=offset,
                end=(offset + size - 1), user=user, all_public=True)
        except exceptions.HarborError as e:
            return Response(data=e.err_data_old(), status=e.status_code)

        filesize = obj.si
        offset, end = self.offset_size_to_start_end(offset=offset, size=size, obj_size=filesize)
        conten_len = end - offset + 1
        response = StreamingHttpResponse(file_generator, status=status_code)
        response['Content-Type'] = 'application/octet-stream'  # 注意格式
        response['Content-Ranges'] = f'bytes {offset}-{end}/{filesize}'
        response['Content-Length'] = conten_len
        response['evob_chunk_size'] = conten_len
        response['evob_obj_size'] = filesize
        return response

    @staticmethod
    def get_data(request):
        return request.data

    def get_permissions(self):
        if self.action == 'retrieve':
            return []

        return super().get_permissions()

    def get_parsers(self):
        """
        动态分配请求体解析器
        """
        method = self.request.method.lower()
        action = self.action_map.get(method)
        if action == 'create_detail':
            self.request.upload_handlers = [
                storagers.AllFileUploadInMemoryHandler(request=self.request)
            ]
            return super().get_parsers()
        return super().get_parsers()


class DirectoryViewSet(CustomGenericViewSet):
    """
    目录视图集

    list:
    获取存储桶根目录下的文件和文件夹信息

        >>Http Code: 状态码200:
            {
                'code': 200,
                'files': [fileobj, fileobj, ...],//文件信息对象列表
                'bucket_name': xxx,             //存储桶名称
                'dir_path': xxx,                //当前目录路径
            }
        >>Http Code: 400 401 403 404 500
        {
            "code": "xxx",   // NoSuchBucket、AccessDenied、BadRequest
            "code_text": ""
        }
    """
    queryset = []
    permission_classes = [permissions.IsAuthenticatedOrBucketToken]
    lookup_field = 'dirpath'
    lookup_value_regex = '.+'
    pagination_class = paginations.BucketFileLimitOffsetPagination

    @swagger_auto_schema(
        operation_summary=gettext_lazy('获取存储桶根目录下的文件和文件夹信息'),
        manual_parameters=[
            openapi.Parameter(
                name='only-obj', in_=openapi.IN_QUERY,
                type=openapi.TYPE_BOOLEAN,
                description=gettext_lazy("true(只列举对象，不含目录); 其他值忽略"),
                required=False
            ),
        ],
        responses={
            status.HTTP_200_OK: """
                {
                  "code": 200,
                  "bucket_name": "666",
                  "dir_path": "",
                  "files": [
                    {
                      "na": "sacva",                    # 全路径文件或目录名称
                      "name": "sacva",                  # 文件或目录名称
                      "fod": false,                     # true: 文件；false: 目录
                      "did": 0,
                      "si": 0,                          # size byte，目录为0
                      "ult": "2019-02-20T13:56:25+08:00",     # 上传创建时间
                      "upt": null,                      # 修改时间，目录为null
                      "dlc": 0,                         # 下载次数
                      "download_url": "",               # 下载url
                      "access_permission": "公有"
                      "async1": null,                     # 备份点同步时间
                      "async2": null
                    }
                  ],
                  "count": 5,
                  "next": null,
                  "page": {
                    "current": 1,
                    "final": 1
                  },
                  "previous": null
                }
            """
        }
    )
    def list(self, request, *args, **kwargs):
        return self.list_v1(request, *args, **kwargs)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('获取一个目录下的文件和文件夹信息'),
        manual_parameters=[
            openapi.Parameter(
                name='only-obj', in_=openapi.IN_QUERY,
                type=openapi.TYPE_BOOLEAN,
                description=gettext_lazy("true(只列举对象，不含目录); 其他值忽略"),
                required=False
            ),
            openapi.Parameter(
                name='dirpath', in_=openapi.IN_PATH,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("目录绝对路径"),
                required=True
            ),
            openapi.Parameter(
                name='offset', in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                description="The initial index from which to return the results",
                required=False,
            ),
            openapi.Parameter(
                name='limit', in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                description="Number of results to return per page",
                required=False,
            )
        ],
        responses={
            status.HTTP_200_OK: """
                {
                  "code": 200,
                  "bucket_name": "666",
                  "dir_path": "sacva",
                  "files": [
                    {
                      "na": "sacva/client.ovpn",        # 全路径文件或目录名称
                      "name": "client.ovpn",            # 文件或目录名称
                      "fod": true,                      # true: 文件；false: 目录
                      "did": 11,
                      "si": 1185,                       # size byte，目录为0
                      "ult": "2019-02-20T13:56:25+08:00",     # 上传创建时间
                      "upt": "2019-02-20T13:56:25+08:00",     # 修改时间
                      "dlc": 1,
                      "download_url": "http://159.226.91.140:8000/share/obs/666/sacva/client.ovpn",
                      "access_permission": "公有"
                      "async1": null,                     # 备份点同步时间
                      "async2": null
                    }
                  ],
                  "count": 1,
                  "next": null,
                  "page": {
                    "current": 1,
                    "final": 1
                  },
                  "previous": null
                }
            """
        }
    )
    def list_detail(self, request, *args, **kwargs):
        """
         获取一个目录下的文件和文件夹信息
        """
        return self.list_v1(request, *args, **kwargs)

    def list_v1(self, request, *args, **kwargs):
        bucket_name = kwargs.get('bucket_name', '')
        dir_path = kwargs.get(self.lookup_field, '')
        only_obj = request.query_params.get('only-obj', None)
        if only_obj and only_obj.lower() == 'true':
            only_obj = True
        else:
            only_obj = None

        try:
            check_authenticated_or_bucket_token(request, bucket_name=bucket_name, act='read', view=self)
        except exceptions.Error as exc:
            return Response(data=exc.err_data(), status=exc.status_code)

        paginator = self.paginator
        paginator.request = request
        try:
            offset = paginator.get_offset(request)
            limit = paginator.get_limit(request)
        except Exception as e:
            return Response(data={'code': 400, 'code_text': _('offset或limit参数无效')}, status=status.HTTP_400_BAD_REQUEST)

        h_manager = HarborManager()
        try:
            files, bucket = h_manager.list_dir(bucket_name=bucket_name, path=dir_path, offset=offset, limit=limit,
                                               user=request.user, paginator=paginator, only_obj=only_obj)
        except exceptions.HarborError as e:
            return Response(data=e.err_data_old(), status=e.status_code)

        data_dict = OrderedDict([
            ('code', 200),
            ('bucket_name', bucket_name),
            ('dir_path', dir_path),
        ])

        serializer = self.get_serializer(files, many=True, context={
            'bucket_name': bucket_name, 'dir_path': dir_path, 'bucket': bucket})
        data_dict['files'] = serializer.data
        return paginator.get_paginated_response(data_dict)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('创建一个目录'),
        request_body=no_body,
        manual_parameters=[
            openapi.Parameter(
                name='dirpath', in_=openapi.IN_PATH,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("目录绝对路径"),
                required=True
            ),
        ],
        responses={
            status.HTTP_201_CREATED: """
                {
                  "code": 201,
                  "code_text": "创建文件夹成功",
                  "data": {
                    "dir_name": "aaa",
                    "bucket_name": "666",
                    "dir_path": ""
                  },
                  "dir": {
                    "na": "aaa",
                    "name": "aaa",
                    "fod": false,
                    "did": 0,
                    "si": 0,
                    "ult": "2019-02-20T13:56:25+08:00",
                    "upt": null,
                    "dlc": 0,
                    "download_url": "",
                    "access_permission": "私有"
                    "async1": null, 
                    "async2": null
                  }
                }
            """
        }
    )
    def create_detail(self, request, *args, **kwargs):
        """
        创建一个目录

            >>Http Code: 状态码201,创建文件夹成功：
                {
                    'code': 201,
                    'code_text': '创建文件夹成功',
                    'data': {},      //请求时提交的数据
                    'dir': {}，      //新目录对象信息
                }
            >>Http Code: 400 401 403 404 500
                {
                    "code": "xxx",   // NoSuchBucket、AccessDenied、BadRequest、BucketLockWrite
                    "code_text": ""
                }
        """
        bucket_name = kwargs.get('bucket_name', '')
        path = kwargs.get(self.lookup_field, '')

        try:
            check_authenticated_or_bucket_token(request, bucket_name=bucket_name, act='write', view=self)
        except exceptions.Error as exc:
            return Response(data={'code': exc.code, 'code_text': exc.message}, status=exc.status_code)

        h_manager = HarborManager()
        try:
            ok, dir_obj = h_manager.mkdir(bucket_name=bucket_name, path=path, user=request.user)
        except exceptions.HarborError as e:
            return Response(data=e.err_data_old(), status=e.status_code)

        data = {
            'code': 201,
            'code_text': _('创建文件夹成功'),
            'data': {'dir_name': dir_obj.name, 'bucket_name': bucket_name, 'dir_path': dir_obj.get_parent_path()},
            'dir': serializers.ObjInfoSerializer(dir_obj).data
        }
        return Response(data, status=status.HTTP_201_CREATED)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('删除一个目录'),
        request_body=no_body,
        manual_parameters=[
            openapi.Parameter(
                name='dirpath', in_=openapi.IN_PATH,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("目录绝对路径"),
                required=True
            ),
        ],
        responses={
            status.HTTP_204_NO_CONTENT: 'NO CONTENT'
        }
    )
    def destroy(self, request, *args, **kwargs):
        """
        删除一个目录, 目录必须为空，否则400错误

            >>Http Code: 状态码204,成功删除;
            >>Http Code: 400 401 403 404 500
                {
                    "code": "NoSuchKey",   // NoSuchBucket、AccessDenied、BadRequest、BucketLockWrite、NoEmptyDir
                    "code_text": ""
                }
        """
        bucket_name = kwargs.get('bucket_name', '')
        dirpath = kwargs.get(self.lookup_field, '')

        try:
            check_authenticated_or_bucket_token(request, bucket_name=bucket_name, act='write', view=self)
        except exceptions.Error as exc:
            return Response(data={'code': exc.code, 'code_text': exc.message}, status=exc.status_code)

        h_manager = HarborManager()
        try:
            h_manager.rmdir(bucket_name=bucket_name, dirpath=dirpath, user=request.user)
        except exceptions.HarborError as e:
            return Response(data=e.err_data_old(), status=e.status_code)

        return Response(status=status.HTTP_204_NO_CONTENT)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('设置目录访问权限'),
        request_body=no_body,
        manual_parameters=[
            openapi.Parameter(
                name='dirpath', in_=openapi.IN_PATH,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("目录绝对路径"),
                required=True
            ),
            openapi.Parameter(
                name='share', in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                description=gettext_lazy("用于设置目录访问权限, 0（私有），1(公有只读)，2(公有可读可写)"),
                required=True
            ),
            openapi.Parameter(
                name='days', in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                description=gettext_lazy("公开分享天数(share=1或2时有效)，0表示永久公开，负数表示不公开，默认为0"),
                required=False
            ),
            openapi.Parameter(
                name='password', in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("分享密码，此参数不存在，不设密码；可指定4-8字符；若为空，随机分配密码"),
                required=False
            ),
        ],
        responses={
            status.HTTP_200_OK: """
                {
                  "code": 200,
                  "code_text": "设置目录权限成功",
                  "share": "http://159.226.91.140:8000/share/s/666/aaa",
                  "share_code": "ad46"          # 未设置共享密码时为空
                }
            """
        }
    )
    def partial_update(self, request, *args, **kwargs):
        """
        设置目录访问权限

            >>Http Code: 400 401 403 404 500
                {
                    "code": "NoSuchKey",   // NoSuchBucket、AccessDenied、BadRequest、BucketLockWrite
                    "code_text": ""
                }
        """
        bucket_name = kwargs.get('bucket_name', '')

        try:
            check_authenticated_or_bucket_token(request, bucket_name=bucket_name, act='write', view=self)
        except exceptions.Error as exc:
            return Response(data={'code': exc.code, 'code_text': exc.message}, status=exc.status_code)

        dirpath = kwargs.get(self.lookup_field, '')
        days = str_to_int_or_default(request.query_params.get('days', 0), 0)
        share = str_to_int_or_default(request.query_params.get('share', ''), -1)
        pw = request.query_params.get('password', None)

        if pw:  # 指定密码
            if not (4 <= len(pw) <= 8):
                return Response(data={'code': 400, 'code_text': _('password参数长度为4-8个字符')},
                                status=status.HTTP_400_BAD_REQUEST)
            password = pw
        elif pw is None:  # 不设密码
            password = ''
        else:  # 随机分配密码
            password = rand_share_code()

        if share not in [0, 1, 2]:
            return Response(data={'code': 400, 'code_text': _('share参数有误')}, status=status.HTTP_400_BAD_REQUEST)

        h_manager = HarborManager()
        try:
            ok, access_code = h_manager.share_dir(bucket_name=bucket_name, path=dirpath,
                                                  share=share, days=days, password=password, user=request.user)
        except exceptions.HarborError as e:
            return Response(data=e.err_data_old(), status=e.status_code)

        if not ok:
            return Response(data={'code': 400, 'code_text': _('设置目录权限失败')}, status=status.HTTP_400_BAD_REQUEST)

        share_base = f'{bucket_name}/{dirpath}'
        share_url = django_reverse('share:share-view', kwargs={'share_base': share_base})
        share_url = request.build_absolute_uri(share_url)
        return Response(data={'code': 200, 'code_text': _('设置目录权限成功'),
                              'share': share_url, 'share_code': password,
                              'access_code': access_code
                              }, status=status.HTTP_200_OK)

    def get_serializer_class(self):
        """
        Return the class to use for the serializer.
        Defaults to using `self.serializer_class`.
        Custom serializer_class
        """
        if self.action in ['create_detail', 'partial_update']:
            return Serializer
        return serializers.ObjInfoSerializer

    @log_used_time(debug_logger, 'paginate in dir')
    def paginate_queryset(self, queryset):
        return super(DirectoryViewSet, self).paginate_queryset(queryset)


class BucketStatsViewSet(CustomGenericViewSet):
    """
        retrieve:
            存储桶资源统计，普通用户只能查询自己的桶，超级用户和第三方APP超级用户查询所有的桶

            统计存储桶对象数量和所占容量，字节

            >>Http Code: 状态码200:
                {
                    "stats": {
                      "space": 12500047770969,             # 桶内对象总大小，单位字节
                      "count": 5000004,                    # 桶内对象总数量
                    },
                    "stats_time": "2020-03-04T06:01:50+00:00", # 统计时间
                    "code": 200,
                    "bucket_name": "xxx",    # 存储桶名称
                    "user_id": 1,
                    "username": "shun"
                }

            >>Http Code: 状态码404:
                {
                    'code': 404,
                    'code_text': xxx  //错误码描述
                }
        """
    queryset = []
    permission_classes = [permissions.IsAuthenticatedOrBucketToken]
    lookup_field = 'bucket_name'
    lookup_value_regex = '[a-z0-9-_]{3,64}'

    @swagger_auto_schema(
        operation_summary=gettext_lazy('获取一个存储桶资源统计信息'),
        responses={}
    )
    def retrieve(self, request, *args, **kwargs):
        bucket_name = kwargs.get(self.lookup_field)
        try:
            check_authenticated_or_bucket_token(request, bucket_name=bucket_name, act='read', view=self)
        except exceptions.Error as exc:
            return Response(data=exc.err_data_old(), status=exc.status_code)

        if permissions.IsSuperOrAppSuperUser().has_permission(request=request, view=self):
            bucket = Bucket.get_bucket_by_name(bucket_name)
        else:
            bucket = get_user_own_bucket(bucket_name, request)

        if not bucket:
            return Response(data={'code': 404, 'code_text': _('bucket_name参数有误，存储桶不存在')},
                            status=status.HTTP_404_NOT_FOUND)

        data = bucket.get_stats()
        data.update({
            'code': 200,
            'bucket_name': bucket_name,
            'user_id': bucket.user.id,
            'username': bucket.user.username
        })

        return Response(data)


class SecurityViewSet(CustomGenericViewSet):
    """
    安全凭证视图集

    retrieve:
        获取指定用户的安全凭证, 需要超级用户权限

            *注：默认只返回用户Auth Token和JWT(json web token)，如果希望返回内容包含访问密钥对，请显示携带query参数key,服务器不要求key有值

            >>Http Code: 状态码200:
                {
                  "user": {
                    "id": 3,
                    "username": "xxx"
                  },
                  "token": "xxx",
                  "jwt": "xxx",
                  "keys": [                                 # 此内容只在携带query参数key时存在
                    {
                      "access_key": "xxx",
                      "secret_key": "xxxx",
                      "user": "xxx",
                      "create_time": "2020-03-03T20:52:04.187179+08:00",
                      "state": true,                        # true(使用中) false(停用)
                      "permission": "可读可写"
                    },
                  ]
                }

            >>Http Code: 状态码400:
                {
                    'username': 'Must be a valid email.'
                }

            >>Http Code: 状态码403:
                {
                    "detail":"您没有执行该操作的权限。"
                }
        """
    queryset = []
    permission_classes = [permissions.IsSuperOrAppSuperUser]
    lookup_field = 'username'
    lookup_value_regex = '.+'

    @swagger_auto_schema(
        operation_summary=gettext_lazy('获取指定用户的安全凭证'),
        manual_parameters=[
            openapi.Parameter(
                name='key', in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("访问密钥对"),
                required=False
            ),
        ]
    )
    def retrieve(self, request, *args, **kwargs):
        username = kwargs.get(self.lookup_field)
        key = request.query_params.get('key', None)

        try:
            self.validate_username(username)
        except dj_exceptions.ValidationError as e:
            msg = e.message or 'Must be a valid email.'
            return Response({'username': msg}, status=status.HTTP_400_BAD_REQUEST)

        user = self.get_user_or_create(username)
        token, created = Token.objects.get_or_create(user=user)

        # jwt token
        jwtoken = JWTokenTool2().obtain_one_jwt(user=user)

        data = {
            'user': {
                'id': user.id,
                'username': user.username
            },
            'token': token.key,
            'jwt': jwtoken
        }

        # param key exists
        if key is not None:
            authkeys = AuthKey.objects.filter(user=user).all()
            serializer = AuthKeyDumpSerializer(authkeys, many=True)
            data['keys'] = serializer.data

        return Response(data)

    @staticmethod
    def get_user_or_create(username):
        """
        通过用户名获取用户，或创建用户
        :param username:  用户名
        :return:
        """
        try:
            user = User.objects.get(username=username)
        except dj_exceptions.ObjectDoesNotExist:
            user = None

        if user:
            return user

        user = User(username=username, email=username)
        user.save()

        return user

    @staticmethod
    def validate_username(username):
        """
        验证用户名是否是邮箱

        failed: raise ValidationError
        """
        validate_email(username)


class MoveViewSet(CustomGenericViewSet):
    """
    对象移动或重命名

    create_detail:
        移动或重命名一个对象

        参数move_to指定对象移动的目标路径（bucket桶下的目录路径），/或空字符串表示桶下根目录；参数rename指定重命名对象的新名称；
        请求时至少提交其中一个参数，亦可同时提交两个参数；只提交参数move_to只移动对象，只提交参数rename只重命名对象；

        >>Http Code: 状态码201,成功：
        >>Http Code: 状态码400, 请求参数有误，已存在同名的对象或目录:
            {
                "code": 400,
                "code_text": 'xxxxx'        //错误信息
            }
        >>Http Code: 状态码404, bucket桶、对象或移动目标路径不存在:
            {
                "code": 404,
                "code_text": 'xxxxx'        //错误信息
            }
        >>Http Code: 状态码500, 服务器错误，无法完成操作:
            {
                "code": 500,
                "code_text": 'xxxxx'        //错误信息
            }
    """
    queryset = []
    permission_classes = [permissions.IsAuthenticatedOrBucketToken]
    lookup_field = 'objpath'
    lookup_value_regex = '.+'

    @swagger_auto_schema(
        operation_summary=gettext_lazy('移动或重命名一个对象'),
        operation_id='v1_move_create_detail',
        request_body=no_body,
        manual_parameters=[
            openapi.Parameter(
                name='objpath', in_=openapi.IN_PATH,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("文件对象绝对路径"),
                required=True
            ),
            openapi.Parameter(
                name='move_to', in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("移动对象到此目录路径下，/或空字符串表示桶下根目录"),
                required=False
            ),
            openapi.Parameter(
                name='rename', in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("重命名对象的新名称"),
                required=False
            )
        ],
        responses={
            status.HTTP_201_CREATED: """
                {
                  "code": 201,
                  "code_text": "移动对象操作成功",
                  "bucket_name": "666",
                  "dir_path": "d d",
                  "obj": {                      # 移动操作成功后文件对象详细信息
                    "na": "d d/data.json2",
                    "name": "data.json2",
                    "fod": true,
                    "did": 6,
                    "si": 149888,
                    "ult": "2020-03-03T20:52:04.187179+08:00",
                    "upt": "2020-03-03T20:52:04.187179+08:00",
                    "dlc": 1,
                    "download_url": "http://159.226.91.140:8000/share/obs/666/d%20d/data.json2",
                    "access_permission": "公有",
                    "async1": null,
                    "async2": null
                  }
                }
            """
        }
    )
    def create_detail(self, request, *args, **kwargs):
        bucket_name = kwargs.get('bucket_name', '')

        try:
            check_authenticated_or_bucket_token(request, bucket_name=bucket_name, act='write', view=self)
        except exceptions.Error as exc:
            return Response(data=exc.err_data_old(), status=exc.status_code)

        objpath = kwargs.get(self.lookup_field, '')
        move_to = request.query_params.get('move_to', None)
        rename = request.query_params.get('rename', None)

        h_manager = HarborManager()
        try:
            obj, bucket = h_manager.move_rename(bucket_name=bucket_name, obj_path=objpath,
                                                rename=rename, move=move_to, user=request.user)
        except exceptions.HarborError as e:
            return Response(data=e.err_data_old(), status=e.status_code)

        context = self.get_serializer_context()
        context.update({'bucket_name': bucket.name, 'bucket': bucket})
        return Response(data={'code': 201, 'code_text': _('移动对象操作成功'),
                              'bucket_name': bucket.name,
                              'dir_path': obj.get_parent_path(),
                              'obj': serializers.ObjInfoSerializer(obj, context=context).data},
                        status=status.HTTP_201_CREATED)

    def get_serializer_class(self):
        """
        Return the class to use for the serializer.
        Defaults to using `self.serializer_class`.
        Custom serializer_class
        """
        if self.action in ['create_detail']:
            return Serializer
        return Serializer


class MetadataViewSet(CustomGenericViewSet):
    """
    对象或目录元数据视图集

    retrieve:
        获取对象或目录元数据

        >>Http Code: 状态码200,成功：
            {
                "code": 200,
                "bucket_name": "xxx",
                "dir_path": "xxx",
                "code_text": "获取元数据成功",
                "obj": {
                    "na": "upload/Firefox-latest.exe",  # 对象或目录全路径名称
                    "name": "Firefox-latest.exe",       # 对象或目录名称
                    "fod": true,                        # true(文件对象)；false(目录)
                    "did": 42,                          # 父目录节点id
                    "si": 399336,                       # 对象大小，单位字节； 目录时此字段为0
                    "ult": "2019-02-20T13:56:25+08:00",       # 创建时间
                    "upt": "2019-02-20T13:56:25+08:00",       # 最后修改时间； 目录时此字段为空
                    "dlc": 2,                           # 下载次数； 目录时此字段为0
                    "download_url": "http://10.0.86.213/obs/gggg/upload/Firefox-latest.exe", # 对象下载url; 目录此字段为空
                    "access_permission": "私有"，          # 访问权限，‘私有’或‘公有’； 目录此字段为空
                    "async1": null,                     # 备份点同步时间
                    "async2": null
                },
                "info": {                               # 目录时为null
                    "rados": [                          # 对象对应rados信息，格式：iharbor:{cluster_name}/{pool_name}/{rados-key}
                        "iharbor:ceph/obs/217_12",      # 大小为 chunk_size
                        "iharbor:ceph/obs/217_12_1",     # 大小为 chunk_size
                        ...
                        "iharbor:ceph/obs/217_12_N",     # 最后一个数据块大小=(size - chunk_size * N)；N = len(rados数组) - 1
                     ],
                    "chunk_size": 2147483648,                  # 对象分片（rados）的大小
                    "size": 399336,                       # 对象大小Byte
                    "filename": "Firefox-latest.exe"           # 对象名称
                }
            }
        >>Http Code: 400 401 403 404 500
            {
                'code': "NoSuchKey",   // NoSuchBucket、AccessDenied、BadRequest
                'code_text': '参数有误'
            }
    """
    queryset = []
    permission_classes = [permissions.IsAuthenticatedOrBucketToken]
    lookup_field = 'path'
    lookup_value_regex = '.+'

    @swagger_auto_schema(
        operation_summary=gettext_lazy('获取对象或目录元数据'),
        request_body=no_body,
        manual_parameters=[
            openapi.Parameter(
                name='path', in_=openapi.IN_PATH,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("对象或目录绝对路径"),
                required=True
            )
        ],
        responses={
            status.HTTP_200_OK: ''
        }
    )
    def retrieve(self, request, *args, **kwargs):
        path_name = kwargs.get(self.lookup_field, '')
        bucket_name = kwargs.get('bucket_name', '')

        try:
            check_authenticated_or_bucket_token(request, bucket_name=bucket_name, act='read', view=self)
        except exceptions.Error as exc:
            return Response(data={'code': exc.code, 'code_text': exc.message}, status=exc.status_code)

        path, name = PathParser(filepath=path_name).get_path_and_filename()
        if not bucket_name or not name:
            return Response(data={'code': 400, 'code_text': _('path参数有误')}, status=status.HTTP_400_BAD_REQUEST)

        h_manager = HarborManager()
        try:
            bucket, obj = h_manager.get_bucket_and_obj_or_dir(
                bucket_name=bucket_name, path=path_name, user=request.user)
        except exceptions.HarborError as e:
            if e.code == exceptions.NoParentPath.default_code:
                e = exceptions.NoSuchKey.from_error(e)
            return Response(data=e.err_data_old(), status=e.status_code)
        except Exception as e:
            return Response(data={'code': 500, 'code_text': f'error，{str(e)}'},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        if not obj:
            exc = exceptions.NoSuchKey(message=_('对象或目录不存在'))
            return Response(data=exc.err_data_old(), status=404)

        serializer = self.get_serializer(obj, context={'bucket': bucket, 'bucket_name': bucket_name, 'dir_path': path})

        if obj.is_file():
            obj_key = obj.get_obj_key(bucket.id)
            pool_name = bucket.get_pool_name()
            chunk_size, keys = build_harbor_object(
                using=bucket.ceph_using, pool_name=pool_name, obj_id=obj_key, obj_size=obj.obj_size
            ).get_rados_key_info()

            info = {
                'rados': keys,
                'chunk_size': chunk_size,
                'size': obj.obj_size,
                'filename': obj.name
            }
        else:
            info = None

        return Response(data={'code': 200, 'code_text': _('获取元数据成功'), 'bucket_name': bucket_name,
                              'dir_path': path, 'obj': serializer.data, 'info': info})

    @swagger_auto_schema(
        operation_summary=gettext_lazy('创建一个空对象元数据'),
        request_body=no_body,
        manual_parameters=[
            openapi.Parameter(
                name='path', in_=openapi.IN_PATH,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("对象绝对路径"),
                required=True
            )
        ],
        responses={
            status.HTTP_200_OK: """
            {
              "code": 200,
              "code_text": "创建空对象元数据成功",
              "info": {
                "rados": "iharbor:ceph/obs_test/471_5",
                "size": 0,
                "filename": "test2.txt"
              },
              "obj": {
                "na": "test5",
                "name": "test5",
                "fod": true,
                "did": 0,
                "si": 0,
                "ult": "2020-03-04T14:21:01.422096+08:00",
                "upt": null,
                "dlc": 0,
                "download_url": "http://xxx/share/obs/6666/test5",
                "access_permission": "私有",
                "async1": null,  
                "async2": null
              }
            }
            """
        }
    )
    def create_detail(self, request, *args, **kwargs):
        path_name = kwargs.get(self.lookup_field, '')
        bucket_name = kwargs.get('bucket_name', '')
        path, name = PathParser(filepath=path_name).get_path_and_filename()
        if not bucket_name or not name:
            return Response(data={'code': 400, 'code_text': _('path参数有误')}, status=status.HTTP_400_BAD_REQUEST)

        try:
            check_authenticated_or_bucket_token(request, bucket_name=bucket_name, act='write', view=self)
        except exceptions.Error as exc:
            return Response(data={'code': exc.code, 'code_text': exc.message}, status=exc.status_code)

        h_manager = HarborManager()
        try:
            bucket, obj, created = h_manager.create_empty_obj(
                bucket_name=bucket_name, obj_path=path_name, user=request.user)
        except exceptions.HarborError as e:
            return Response(data=e.err_data_old(), status=e.status_code)
        except Exception as e:
            return Response(data={'code': 500, 'code_text': f'error，{str(e)}'},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        if not obj or not created:
            return Response(data={'code': 404, 'code_text': _('创建失败，对象已存在')}, status=status.HTTP_404_NOT_FOUND)

        obj_key = obj.get_obj_key(bucket.id)
        pool_name = bucket.get_pool_name()
        ho = build_harbor_object(using=bucket.ceph_using, pool_name=pool_name, obj_id=obj_key, obj_size=obj.obj_size)
        rados_key = ho.get_rados_key_info()
        info = {
            'rados': rados_key,
            'size': obj.obj_size,
            'filename': obj.name
        }
        serializer = self.get_serializer(obj, context={'bucket': bucket, 'bucket_name': bucket_name, 'dir_path': path})
        return Response(data={'code': 200, 'code_text': _('创建空对象元数据成功'), 'info': info, 'obj': serializer.data})

    def get_serializer_class(self):
        """
        Return the class to use for the serializer.
        Defaults to using `self.serializer_class`.
        Custom serializer_class
        """
        if self.action in ['retrieve', 'create_detail']:
            return serializers.ObjInfoSerializer
        return Serializer


class RefreshMetadataViewSet(CustomGenericViewSet):
    queryset = []
    permission_classes = [IsAuthenticated]
    lookup_field = 'path'
    lookup_value_regex = '.+'

    @swagger_auto_schema(
        operation_summary=gettext_lazy('自动同步对象大小元数据'),
        operation_id='v1_refresh-meta_create_detail',
        request_body=no_body,
        manual_parameters=[
            openapi.Parameter(
                name='path', in_=openapi.IN_PATH,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("对象绝对路径"),
                required=True
            )
        ],
        responses={
            status.HTTP_200_OK: ''
        }
    )
    def create_detail(self, request, *args, **kwargs):
        """
        自动更新对象大小元数据

            警告：特殊API，未与技术人员沟通不可使用；对象大小2GB内适用

            >>Http Code: 状态码200,成功：
                {
                  "code": 200,
                  "code_text": "更新对象大小元数据成功",
                  "info": {
                    "size": 867840,
                    "filename": "7zFM.exe",
                    "mtime": "2020-03-04T08:05:28.210658+00:00"     # 修改时间
                  }
                }
            >>Http Code: 状态码400, 请求参数有误，已存在同名的对象或目录:
                {
                    "code": 400,
                    "code_text": 'xxxxx'        //错误信息
                }
            >>Http Code: 状态码404, bucket桶、对象或目录不存在:
                {
                    "code": 404,
                    "code_text": 'xxxxx'        //错误信息，
        """
        path_name = kwargs.get(self.lookup_field, '')
        bucket_name = kwargs.get('bucket_name', '')
        path, name = PathParser(filepath=path_name).get_path_and_filename()
        if not bucket_name or not name:
            return Response(data={'code': 400, 'code_text': _('path参数有误')}, status=status.HTTP_400_BAD_REQUEST)

        h_manager = HarborManager()
        try:
            bucket, obj = h_manager.get_bucket_and_obj(bucket_name=bucket_name, obj_path=path_name, user=request.user)
        except exceptions.HarborError as e:
            return Response(data=e.err_data_old(), status=e.status_code)
        except Exception as e:
            return Response(data={'code': 500, 'code_text': f'error，{str(e)}'},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        if not obj:
            return Response(data={'code': 404, 'code_text': _('对象不存在')}, status=status.HTTP_404_NOT_FOUND)

        obj_key = obj.get_obj_key(bucket.id)
        pool_name = bucket.get_pool_name()
        ho = build_harbor_object(using=bucket.ceph_using, pool_name=pool_name, obj_id=obj_key, obj_size=obj.obj_size)
        ok, ret = ho.get_rados_stat(obj_id=obj_key)
        if not ok:
            return Response(data={'code': 400, 'code_text': f'failed to get size of rados object，{ret}'},
                            status=status.HTTP_400_BAD_REQUEST)

        size, mtime = ret
        if size == 0 and mtime is None:  # rados对象不存在
            mtime = obj.upt if obj.upt else obj.ult
            mtime = to_django_timezone(mtime)

        if obj.upt != mtime:
            pass
        if obj.si != size or obj.upt != mtime:
            obj.si = size
            obj.upt = mtime
            try:
                obj.save(update_fields=['si', 'upt'])
            except Exception as e:
                return Response(data={'code': 400, 'code_text': _('更新对象大小元数据失败') + str(e)},
                                status=status.HTTP_400_BAD_REQUEST)

        info = {
            'size': size,
            'filename': obj.name,
            'mtime': mtime.isoformat()
        }
        return Response(data={'code': 200, 'code_text': _('更新对象大小元数据成功'), 'info': info})


class CephStatsViewSet(CustomGenericViewSet):
    """
        ceph集群视图集

        list:
            CEPH集群资源统计

            统计ceph集群总容量、已用容量，可用容量、对象数量

            >>Http Code: 状态码200:
                {
                  "code": 200,
                  "code_text": "successful",
                  "stats": {
                    "kb": 762765762560,     # 总容量，单位kb
                    "kb_used": 369591170304,# 已用容量
                    "kb_avail": 393174592256,# 可用容量
                    "num_objects": 40750684  # rados对象数量
                  }
                }

            >>Http Code: 状态码404:
                {
                    'code': 404,
                    'code_text': URL中包含无效的版本  //错误码描述
                }

            >>Http Code: 状态码500:
                {
                    'code': 500,
                    'code_text': xxx  //错误码描述
                }
        """
    queryset = []
    permission_classes = [permissions.IsSuperUser]
    pagination_class = None

    @swagger_auto_schema(
        operation_summary=gettext_lazy('CEPH集群资源统计'),
    )
    def list(self, request, *args, **kwargs):
        try:
            stats = build_harbor_object(using='default', pool_name='', obj_id='').get_cluster_stats()
        except RadosError as e:
            return Response(data={'code': 500, 'code_text': _('获取ceph集群信息错误：') + str(e)},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({
            'code': 200,
            'code_text': 'successful',
            'stats': stats
        })


class UserStatsViewSet(CustomGenericViewSet):
    """
        用户资源统计视图集

        retrieve:

            获取指定用户的资源统计信息，需要超级用户权限

             >>Http Code: 状态码200:
            {
                "code": 200,
                "space": 12991806596545,  # 已用总容量，byte
                "count": 5864125,         # 总对象数量
                "buckets": [              # 每个桶的统计信息
                    {
                        "stats": {
                            "space": 16843103, # 桶内对象总大小，单位字节
                            "count": 4          # 桶内对象总数量
                        },
                        "stats_time": "2020-03-04T14:21:01.422096+08:00", # 统计时间
                        "bucket_name": "wwww"       # 存储桶名称
                    },
                    {
                        "stats": {
                            "space": 959820827,
                            "count": 17
                        },
                        "stats_time": "2020-03-04T06:01:50+00:00",
                        "bucket_name": "gggg"
                    },
                ]
            }

        list:
            获取当前用户的资源统计信息

            >>Http Code: 状态码200:
            {
                "code": 200,
                "space": 12991806596545,  # 已用总容量，byte
                "count": 5864125,         # 总对象数量
                "buckets": [              # 每个桶的统计信息
                    {
                        "stats": {
                            "space": 16843103, # 桶内对象总大小，单位字节
                            "count": 4          # 桶内对象总数量
                        },
                        "stats_time": "2020-03-04T06:01:50+00:00", # 统计时间
                        "bucket_name": "wwww"       # 存储桶名称
                    },
                    {
                        "stats": {
                            "space": 959820827,
                            "count": 17
                        },
                        "stats_time": "2020-03-04T06:01:50+00:00",
                        "bucket_name": "gggg"
                    },
                ]
            }

            >>Http Code: 状态码404:
                {
                    'code': 404,
                    'code_text': xxx  //错误码描述
                }
        """
    queryset = []
    permission_classes = [IsAuthenticated]
    lookup_field = 'username'
    lookup_value_regex = '.+'
    pagination_class = None

    @swagger_auto_schema(
        operation_summary=gettext_lazy('获取当前用户的资源统计信息'),
    )
    def list(self, request, *args, **kwargs):
        user = request.user
        data = self.get_user_stats(user)
        data['code'] = 200
        data['username'] = user.username
        return Response(data)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('获取指定用户的资源统计信息'),
    )
    def retrieve(self, request, *args, **kwargs):
        username = kwargs.get(self.lookup_field)
        try:
            user = User.objects.get(username=username)
        except dj_exceptions.ObjectDoesNotExist:
            return Response(data={'code': 404, 'code_text': _('username参数有误，用户不存在')},
                            status=status.HTTP_404_NOT_FOUND)

        data = self.get_user_stats(user)
        data['code'] = 200
        data['username'] = user.username
        return Response(data)

    @staticmethod
    def get_user_stats(user):
        """获取用户的资源统计信息"""
        all_count = 0
        all_space = 0
        li = []
        buckets = Bucket.objects.filter(user=user)
        for b in buckets:
            s = b.get_stats()
            s['bucket_name'] = b.name
            li.append(s)

            stats = s.get('stats', {})
            all_space += stats.get('space', 0)
            all_count += stats.get('count', 0)

        return {
            'space': all_space,
            'count': all_count,
            'buckets': li
        }

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action == 'retrieve':
            return [permissions.IsSuperUser()]

        return super(UserStatsViewSet, self).get_permissions()


class CephComponentsViewSet(CustomGenericViewSet):
    """
        ceph集群组件信息视图集

        list:
            ceph的mon，osd，mgr，mds组件信息

            需要超级用户权限

            >>Http Code: 状态码200:
                {
                    "code": 200,
                    "mon": {},
                    "osd": {},
                    "mgr": {},
                    "mds": {}
                }

            >>Http Code: 状态码404:
                {
                    'code': 404,
                    'code_text': URL中包含无效的版本  //错误码描述
                }
        """
    queryset = []
    permission_classes = [permissions.IsSuperUser]
    pagination_class = None

    @swagger_auto_schema(
        operation_summary=gettext_lazy('ceph的mon，osd，mgr，mds组件信息'),
    )
    def list(self, request, *args, **kwargs):
        return Response({
            'code': 200,
            'mon': {},
            'osd': {},
            'mgr': {},
            'mds': {}
        })


class CephErrorViewSet(CustomGenericViewSet):
    """
        ceph集群当前故障信息查询

        list:
            ceph集群当前故障信息查询

            需要超级用户权限

            >>Http Code: 状态码200:
                {
                    "code": 200,
                    'errors': {
                    }
                }

            >>Http Code: 状态码404:
                {
                    'code': 404,
                    'code_text': URL中包含无效的版本  //错误码描述
                }
        """
    queryset = []
    permission_classes = [permissions.IsSuperUser]
    pagination_class = None

    @swagger_auto_schema(
        operation_summary=gettext_lazy('ceph集群当前故障信息查询'),
    )
    def list(self, request, *args, **kwargs):
        return Response({
            'code': 200,
            'errors': {

            }
        })


class CephPerformanceViewSet(CustomGenericViewSet):
    """
        ceph集群性能，需要超级用户权限

        list:
            ceph集群的IOPS，I/O带宽

            需要超级用户权限

            >>Http Code: 状态码200:
                {
                    "bw_rd": 0,     # Kb/s, io读带宽
                    "bw_wr": 4552,  # Kb/s, io写带宽
                    "bw": 4552,     # Kb/s, io读写总带宽
                    "op_rd": 220,   # op/s, io读操作数
                    "op_wr": 220,   # op/s, io写操作数
                    "op": 441       # op/s, io读写操作数
                }

            >>Http Code: 状态码404:
                {
                    'code': 404,
                    'code_text': URL中包含无效的版本  //错误码描述
                }
        """
    queryset = []
    permission_classes = [permissions.IsSuperUser]
    pagination_class = None

    @swagger_auto_schema(
        operation_summary=gettext_lazy('ceph集群的IOPS，I/O带宽'),
    )
    def list(self, request, *args, **kwargs):
        ok, data = build_harbor_object(using='default', pool_name='', obj_id='').get_ceph_io_status()
        if not ok:
            return Response(data={'code': 500, 'code_text': 'Get io status error:' + data},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        return Response(data=data)


class UserCountViewSet(CustomGenericViewSet):
    """
        系统用户总数查询

        list:
            系统用户总数查询，需要超级用户权限

            >>Http Code: 状态码200:
                {
                    "code": 200,
                    'count': xxx
                }

            >>Http Code: 状态码404:
                {
                    'code': 404,
                    'code_text': URL中包含无效的版本  //错误码描述
                }
        """
    queryset = []
    permission_classes = [permissions.IsSuperUser]
    pagination_class = None

    @swagger_auto_schema(
        operation_summary=gettext_lazy('系统用户总数查询'),
    )
    def list(self, request, *args, **kwargs):
        count = User.objects.filter(is_active=True).count()
        return Response({
            'code': 200,
            'count': count
        })


class AvailabilityViewSet(CustomGenericViewSet):
    """
        系统可用性

        list:
            系统可用性查询，需要超级用户权限

            >>Http Code: 状态码200:
                {
                    "code": 200,
                    'availability': '100%'
                }
        """
    queryset = None
    permission_classes = [permissions.IsSuperUser]
    pagination_class = None

    @swagger_auto_schema(
        operation_summary=gettext_lazy('系统可用性查询'),
    )
    def list(self, request, *args, **kwargs):
        return Response({
            'code': 200,
            'availability': '100%'
        })


class VisitStatsViewSet(CustomGenericViewSet):
    """
        访问统计

        list:
            系统访问统计查询，需要超级用户权限

            >>Http Code: 状态码200:
                {
                    "code": 200,
                    "stats": {
                        "active_users": 100,  # 日活跃用户数
                        "register_users": 10,# 日注册用户数
                        "visitors": 100,    # 访客数
                        "page_views": 1000,  # 访问量
                        "ips": 50           # IP数
                    }
                }

            >>Http Code: 状态码404:
                {
                    'code': 404,
                    'code_text': URL中包含无效的版本  //错误码描述
                }
        """
    queryset = []
    permission_classes = [permissions.IsSuperUser]
    pagination_class = None

    @swagger_auto_schema(
        operation_summary=gettext_lazy('系统访问统计查询'),
    )
    def list(self, request, *args, **kwargs):
        stats = User.active_user_stats()
        stats.update({
            'visitors': 100,
            'page_views': 1000,
            'ips': 50
        })
        return Response({
            'code': 200,
            'stats': stats
        })


class TestViewSet(CustomGenericViewSet):
    """
        系统是否可用查询

        list:
            系统是否可用查询

            >>Http Code: 状态码200:
                {
                    "code": 200,
                    "code_text": "系统可用",
                    "status": true     # true: 可用；false: 不可用
                }

            >>Http Code: 状态码404:
                {
                    'code': 404,
                    'code_text': URL中包含无效的版本  //错误码描述
                }
        """
    queryset = []
    permission_classes = []
    throttle_classes = (throttles.TestRateThrottle,)
    pagination_class = None

    @swagger_auto_schema(
        operation_summary=gettext_lazy('系统是否可用查询'),
    )
    def list(self, request, *args, **kwargs):
        return Response({
            'code': 200,
            'code_text': '系统可用',
            'status': True     # True: 可用；False: 不可用
        })


class FtpViewSet(CustomGenericViewSet):
    """
    存储桶FTP服务配置相关API

    partial_update:
    开启或关闭存储桶ftp访问限制，开启存储桶的ftp访问权限后，可以通过ftp客户端访问存储桶

        Http Code: 状态码200，返回数据：
        {
            "code": 200,
            "code_text": "ftp配置成功"，
            "data": {               # 请求时提交的数据
                "enable": xxx,      # 此项提交时才存在
                "password": xxx     # 此项提交时才存在
                "ro_password": xxx     # 此项提交时才存在
            }
        }
        Http Code: 状态码400：参数有误时，返回数据：
            对应参数错误信息;
        Http Code: 状态码404;
        Http Code: 500
    """
    queryset = []
    permission_classes = [permissions.IsAuthenticatedOrBucketToken]
    lookup_field = 'bucket_name'
    lookup_value_regex = '[a-z0-9-_]{3,64}'
    pagination_class = None

    @swagger_auto_schema(
        operation_summary=gettext_lazy('开启或关闭存储桶ftp访问限制'),
        request_body=no_body,
        manual_parameters=[
            openapi.Parameter(
                name='bucket_name', in_=openapi.IN_PATH,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("存储桶名称"),
                required=True
            ),
            openapi.Parameter(
                name='enable', in_=openapi.IN_QUERY,
                type=openapi.TYPE_BOOLEAN,
                description=gettext_lazy("存储桶ftp访问,true(开启)；false(关闭)"),
                required=False
            ),
            openapi.Parameter(
                name='password', in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("存储桶ftp新的读写访问密码"),
                required=False
            ),
            openapi.Parameter(
                name='ro_password', in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("存储桶ftp新的只读访问密码"),
                required=False
            ),
        ],
        responses={
            status.HTTP_200_OK: ''
        }
    )
    def partial_update(self, request, *args, **kwargs):
        bucket_name = kwargs.get(self.lookup_field, '')
        if not bucket_name:
            return Response(data={'code': 400, 'code_text': _('存储桶名称有误')}, status=status.HTTP_400_BAD_REQUEST)

        try:
            check_authenticated_or_bucket_token(request, bucket_name=bucket_name, act='write', view=self)
        except exceptions.Error as exc:
            return Response(data=exc.err_data_old(), status=exc.status_code)

        try:
            params = self.validate_patch_params(request)
        except ValidationError as e:
            return Response(data={'code': 400, 'code_text': e.detail}, status=status.HTTP_400_BAD_REQUEST)

        enable = params.get('enable')
        password = params.get('password')
        ro_password = params.get('ro_password')

        # 存储桶验证和获取桶对象
        bucket = get_user_own_bucket(bucket_name=bucket_name, request=request)
        if not bucket:
            return Response(data={'code': 404, 'code_text': _('存储桶不存在')},
                            status=status.HTTP_404_NOT_FOUND)

        data = {}
        if enable is not None:
            bucket.ftp_enable = enable
            data['enable'] = enable

        if password is not None:
            ok, msg = bucket.set_ftp_password(password)
            if not ok:
                return Response(data={'code': 400, 'code_text': msg}, status=status.HTTP_400_BAD_REQUEST)
            data['password'] = password

        if ro_password is not None:
            ok, msg = bucket.set_ftp_ro_password(ro_password)
            if not ok:
                return Response(data={'code': 400, 'code_text': msg}, status=status.HTTP_400_BAD_REQUEST)
            data['ro_password'] = ro_password

        try:
            bucket.save()
        except Exception as e:
            return Response(data={'code': 500, 'code_text': 'ftp配置失败'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({
            'code': 200,
            'code_text': 'ftp配置成功',
            'data': data     # 请求提交的参数
        })

    @staticmethod
    def validate_patch_params(request):
        """
        patch请求方法参数验证
        :return:
            {
                'enable': xxx, # None(未提交此参数) 或 bool
                'password': xxx   # None(未提交此参数) 或 string
            }
        """
        validated_data = {'enable': None, 'password': None, 'ro_password': None}
        enable = request.query_params.get('enable', None)
        password = request.query_params.get('password', None)
        ro_password = request.query_params.get('ro_password', None)

        if not enable and not password and not ro_password:
            raise ValidationError(_('参数enable,password或ro_password必须提交一个'))

        if enable is not None:
            if isinstance(enable, str):
                enable = enable.lower()
                if enable == 'true':
                    enable = True
                elif enable == 'false':
                    enable = False
                else:
                    raise ValidationError(_('无效的enable参数'))

            validated_data['enable'] = enable

        if password is not None:
            password = password.strip()
            if not (6 <= len(password) <= 20):
                raise ValidationError(_('密码长度必须为6-20个字符'))

            validated_data['password'] = password

        if ro_password is not None:
            ro_password = ro_password.strip()
            if not (6 <= len(ro_password) <= 20):
                raise ValidationError(_('密码长度必须为6-20个字符'))

            validated_data['ro_password'] = ro_password

        return validated_data

    def get_serializer_class(self):
        """
        Return the class to use for the serializer.
        Defaults to using `self.serializer_class`.
        Custom serializer_class
        """
        return Serializer


class ObjKeyViewSet(CustomGenericViewSet):
    """
    对象CEPH RADOS KEY视图集

    retrieve:
        获取对象对应的ceph rados key信息

        >>Http Code: 状态码200：
            {
                "code": 200,
                "code_text": "请求成功",
                "info": {
                    "ceph_using": "default",            # 多ceph集群时，区分那个ceph集群
                    "rados": [                          # 对象对应rados信息，格式：iharbor:{cluster_name}/{pool_name}/{rados-key}
                        "iharbor:ceph/obs/217_12",      # 大小为 chunk_size
                        "iharbor:ceph/obs/217_12_1",     # 大小为 chunk_size
                        ...
                        "iharbor:ceph/obs/217_12_N",     # 最后一个数据块大小=(size - chunk_size * N)；N = len(rados数组) - 1
                     ],
                    "chunk_size": 2147483648,                  # 对象分片（rados）的大小
                    "size": xxxx,                       # 对象大小Byte
                    "filename": "client.ovpn"           # 对象名称
                }
            }

        >>Http Code: 状态码400：文件路径参数有误：对应参数错误信息;
            {
                'code': 400,
                'code_text': 'xxxx参数有误'
            }
        >>Http Code: 状态码404：找不到资源;
        >>Http Code: 状态码500：服务器内部错误;

    """
    queryset = {}
    permission_classes = [permissions.IsAuthenticatedOrBucketToken]
    lookup_field = 'objpath'
    lookup_value_regex = '.+'

    @swagger_auto_schema(
        operation_summary=gettext_lazy('获取对象对应的ceph rados key信息'),
        request_body=no_body,
        manual_parameters=[
            openapi.Parameter(
                name='objpath', in_=openapi.IN_PATH,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("文件对象绝对路径"),
                required=True
            )
        ]
    )
    def retrieve(self, request, *args, **kwargs):
        objpath = kwargs.get(self.lookup_field, '')
        bucket_name = kwargs.get('bucket_name', '')

        try:
            check_authenticated_or_bucket_token(request, bucket_name=bucket_name, act='read', view=self)
        except exceptions.Error as exc:
            return Response(data={'code': exc.code, 'code_text': exc.message}, status=exc.status_code)

        h_manager = HarborManager()
        try:
            bucket, obj = h_manager.get_bucket_and_obj(bucket_name=bucket_name, obj_path=objpath, user=request.user)
        except exceptions.HarborError as e:
            return Response(data=e.err_data_old(), status=e.status_code)

        if not obj:
            return Response(data={'code': 404, 'code_text': _('对象不存在')}, status=status.HTTP_404_NOT_FOUND)

        obj_key = obj.get_obj_key(bucket.id)
        pool_name = bucket.get_pool_name()
        chunk_size, keys = build_harbor_object(using=bucket.ceph_using, pool_name=pool_name, obj_id=obj_key, obj_size=obj.obj_size).get_rados_key_info()
        info = {
            'ceph_using': bucket.ceph_using,
            'rados': keys,
            'chunk_size': chunk_size,
            'size': obj.obj_size,
            'filename': obj.name
        }
        return Response(data={'code': 200, 'code_text': 'ok', 'info': info}, status=status.HTTP_200_OK)


class ShareViewSet(CustomGenericViewSet):
    """
    对象或目录分享视图

    retrieve:
        获取对象对应的ceph rados key信息

        >>Http Code: 状态码200：
            {
              "share_uri": "http://159.226.91.140:8000/share/s/ddd/ggg",
              "is_obj": false,          # 对象或目录
              "share_code": "c32b"      # 分享密码；is_obj为true或者未设置分享密码时此内容不存在
            }

        >>Http Code: 状态码400：文件路径参数有误：对应参数错误信息;
            {
              "code": "BadRequest",
              "message": "参数有误"
            }
        >>Http Code: 403:
            {
              "code": "NotShared",      # 未设置分享或分享已过期
              "message": "This resource has not been publicly shared."
            }
            or
            {
              "code": "AccessDenied",   # 没有访问权限
              "message": "Access Denied."
            }
        >>Http Code: 状态码404
            {
              "code": "NoSuchBucket",
              "message": "The specified bucket does not exist."
            }
            or
            {
              "code": "NoSuchKey",
              "message": "对象或目录不存在"
            }
        >>Http Code: 状态码500：服务器内部错误;

    """
    queryset = {}
    permission_classes = [permissions.IsAuthenticatedOrBucketToken]
    lookup_field = 'path'
    lookup_value_regex = '.+'

    @swagger_auto_schema(
        operation_summary=gettext_lazy('获取对象或目录的分享连接'),
        request_body=no_body,
        manual_parameters=[
            openapi.Parameter(
                name='path', in_=openapi.IN_PATH,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("对象或目录绝对路径"),
                required=True
            )
        ]
    )
    def retrieve(self, request, *args, **kwargs):
        path = kwargs.get(self.lookup_field, '')
        bucket_name = kwargs.get('bucket_name', '')

        try:
            check_authenticated_or_bucket_token(request, bucket_name=bucket_name, act='read', view=self)
        except exceptions.Error as exc:
            return Response(data=exc.err_data(), status=exc.status_code)

        h_manager = HarborManager()
        try:
            bucket, obj = h_manager.get_bucket_and_obj_or_dir(bucket_name=bucket_name, path=path, user=request.user)
        except exceptions.HarborError as exc:
            return Response(data=exc.err_data(), status=exc.status_code)

        if not obj:
            exc = exceptions.NoSuchKey(message=_('对象或目录不存在'))
            return Response(data=exc.err_data(), status=exc.status_code)

        if not obj.get_access_permission_code(bucket):
            exc = exceptions.NotShared()
            return Response(data=exc.err_data(), status=exc.status_code)

        if obj.is_file():
            share_uri = django_reverse('share:obs-detail', kwargs={'objpath': f'{bucket_name}/{path}'})
            # 是否设置了分享密码
            if obj.has_share_password():
                password = obj.get_share_password()
                share_uri = f'{share_uri}?p={password}'

            share_uri = request.build_absolute_uri(share_uri)
            return Response(data={'share_uri': share_uri, 'is_obj': True}, status=status.HTTP_200_OK)
        else:
            share_base = f'{bucket_name}/{path}'
            share_url = django_reverse('share:share-view', kwargs={'share_base': share_base})
            share_uri = request.build_absolute_uri(share_url)
            data = {'share_uri': share_uri, 'is_obj': False}
            if obj.has_share_password():
                password = obj.get_share_password()
                data['share_code'] = password

        return Response(data=data, status=status.HTTP_200_OK)


class SearchObjectViewSet(CustomGenericViewSet):
    """
    检索对象视图
    """
    queryset = {}
    permission_classes = [permissions.IsAuthenticatedOrBucketToken]
    # lookup_field = 'path'
    # lookup_value_regex = '.+'
    pagination_class = paginations.SearchBucketFileLimitOffsetPagination

    @swagger_auto_schema(
        operation_summary=gettext_lazy('检索存储桶内对象'),
        request_body=no_body,
        manual_parameters=[
            openapi.Parameter(
                name='bucket', in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("检索的存储桶"),
                required=True
            ),
            openapi.Parameter(
                name='search', in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("检索的对象关键字"),
                required=True
            )
        ]
    )
    def list(self, request, *args, **kwargs):
        """
        检索存储桶内对象

            >>Http Code: 状态码200：
            {
              "bucket": "testcopy",
              "files": [
                {
                  "na": "abc.py",
                  "name": "abc.py",
                  "fod": true,
                  "did": 0,
                  "si": 35,
                  "ult": "2020-12-18T18:02:45.715877+08:00",
                  "upt": "2021-03-01T16:56:10.524571+08:00",
                  "dlc": 0,
                  "download_url": "http://xxx/share/obs/testcopy/abc.py",
                  "access_permission": "私有",
                  "access_code": 0,
                  "md5": "585e699b26602e7dd6054798d8c22d9b"
                  "async1": null,
                  "async2": null
                }
              ],
              "count": 2,
              "next": null,
              "page": {
                "current": 1,
                "final": 1
              },
              "previous": null
            }

            >>Http Code 400, 401, 403, 404, 500:
            {
              "code": "xxx",    # 错误码， BadRequest、AccessDenied等
              "message": "xxx"  # 错误描述
            }
        """
        search = request.query_params.get('search', '')
        bucket_name = request.query_params.get('bucket', '')
        if not search:
            exc = exceptions.BadRequest('invalid param "search"')
            return Response(data=exc.err_data(), status=exc.status_code)

        if not bucket_name:
            exc = exceptions.BadRequest('invalid param "bucket"')
            return Response(data=exc.err_data(), status=exc.status_code)

        try:
            check_authenticated_or_bucket_token(request, bucket_name=bucket_name, act='read', view=self)
        except exceptions.Error as exc:
            return Response(data=exc.err_data(), status=exc.status_code)

        h_manager = HarborManager()
        try:
            bucket, queryset = h_manager.search_object_queryset(bucket=bucket_name, search=search, user=request.user)
        except exceptions.HarborError as exc:
            return Response(data=exc.err_data(), status=exc.status_code)

        try:
            paginator = paginations.BucketFileLimitOffsetPagination()
            files = paginator.paginate_queryset(queryset, request=request)
            serializer = serializers.ObjInfoSerializer(files, many=True, context={
                'bucket_name': bucket_name, 'bucket': bucket, 'request': request})
            files = serializer.data
        except Exception as exc:
            exc = exceptions.Error(message=str(exc))
            return Response(data=exc.err_data(), status=exc.status_code)

        data_dict = OrderedDict([
            ('bucket', bucket_name),
            ('files', files)
        ])
        return paginator.get_paginated_response(data_dict)


class ListBucketObjectViewSet(CustomGenericViewSet):
    """
    列举存储桶内对象和目录
    """
    queryset = {}
    permission_classes = [permissions.IsAuthenticatedOrBucketToken]
    lookup_field = 'bucket_name'

    @swagger_auto_schema(
        operation_summary=gettext_lazy('列举存储桶内对象和目录'),
        manual_parameters=[
            openapi.Parameter(
                name='prefix', in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("列举指定前缀开头的对象"),
                required=False
            ),
            openapi.Parameter(
                name='delimiter', in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("分隔符是用于对键进行分组的字符"),
                required=False
            ),
            openapi.Parameter(
                name='continuation-token', in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("指示使用令牌在该存储桶上继续该列表"),
                required=False
            ),
            openapi.Parameter(
                name='max-keys', in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("设置响应中返回的最大对象数"),
                required=False
            ),
            openapi.Parameter(
                name='exclude-dir', in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("排除目录，只列举对象；此参数不需要值（忽略），存在即有效"),
                required=False
            ),
        ]
    )
    def retrieve(self, request, *args, **kwargs):
        """
        列举存储桶内对象和目录

            * prefix与delimiter配合可以列举一个目录;
             当使用delimiter参数时，prefix必须存在（对象或目录）；
             prefix=""或"/" 并且 delimiter="/"时列举桶根目录

            * 由于path唯一，即不允许同名的对象和目录存在，prefix="test"和prefix="test/"结果相同

            http code 200:
            {
              "Name": "ddd",            # bucket name
              "Prefix": "test",
              "Delimiter": "/",         # 只有提交参数delimiter时，此内容才存在
              "IsTruncated": "false",
              "MaxKeys": 1000,
              "KeyCount": 2,
              "Next": "http://159.226.91.140:8000/api/v1/list/bucket/ddd/?continuation-token=cD01NQ%3D%3D&delimiter=%2F&max-keys=1",
              "Previous": "http://159.226.91.140:8000/api/v1/list/bucket/ddd/?continuation-token=cj0xJnA9NTU%3D&delimiter=%2F&max-keys=1",
              "ContinuationToken": "cD03Ng==",      # 只有提交参数continuation-token时，此内容才存在
              "NextContinuationToken": "cD01Ng==",  # 此内容在"IsTruncated" == "true"时存在
              "Contents": [
                {
                  "Key": "test/月报",     # 对象完整路径
                  "LastModified": "2021-03-31T01:40:05.190793Z",
                  "ETag": "d41d8cd98f00b204e9800998ecf8427e",
                  "Size": 0,
                  "IsObject": false
                }
              ]
            }

            http code 404: 当使用delimiter参数时，prefix不存在（对象或目录）
            {
              "code": "NoSuchKey",
              "message": "无效的参数prefix，对象或目录不存在"
            }
        """
        return self.list_objects(request=request, **kwargs)

    def list_objects(self, request, **kwargs):
        delimiter = request.query_params.get('delimiter', None)
        prefix = request.query_params.get('prefix', '')
        bucket_name = kwargs.get('bucket_name', '')
        exclude_dir = request.query_params.get('exclude-dir', None)

        only_obj = False
        if exclude_dir is not None:
            only_obj = True

        if not delimiter:    # list所有对象和目录
            return self.list_objects_list_prefix(request=request, bucket_name=bucket_name,
                                                 prefix=prefix, only_obj=only_obj)

        if delimiter != '/':
            exc = exceptions.BadRequest(message='参数“delimiter”必须是“/”')
            return Response(data=exc.err_data(), status=exc.status_code)

        try:
            check_authenticated_or_bucket_token(request, bucket_name=bucket_name, act='read', view=self)
        except exceptions.Error as exc:
            return Response(data=exc.err_data(), status=exc.status_code)

        hm = HarborManager()
        path = prefix.strip('/')
        if not path and delimiter:     # list root dir
            try:
                bucket = hm.get_user_own_bucket(bucket_name, request.user)
            except exceptions.Error as exc:
                return Response(data=exc.err_data(), status=exc.status_code)

            root_dir = hm.get_root_dir()
            return self.list_objects_list_dir(request=request, bucket=bucket,
                                              dir_obj=root_dir, only_obj=only_obj)

        try:
            bucket, obj = hm.get_bucket_and_obj_or_dir(bucket_name=bucket_name, path=path, user=request.user)
        except exceptions.Error as exc:
            return Response(data=exc.err_data(), status=exc.status_code)

        if obj is None:
            exc = exceptions.NoSuchKey('无效的参数prefix，对象或目录不存在')
            return Response(data=exc.err_data(), status=exc.status_code)

        paginator = paginations.ListObjectsCursorPagination()
        max_keys = paginator.get_page_size(request=request)

        # list dir
        if obj.is_dir():
            return self.list_objects_list_dir(request=request, bucket=bucket,
                                              dir_obj=obj, only_obj=only_obj)

        # list object metadata
        ret_data = {
            'IsTruncated': False,
            'Name': bucket_name,
            'Prefix': prefix,
            'MaxKeys': max_keys,
            'Delimiter': delimiter
        }
        serializer = serializers.ListBucketObjectsSerializer(obj)
        ret_data['Contents'] = [serializer.data]
        ret_data['KeyCount'] = 1
        return Response(data=ret_data, status=status.HTTP_200_OK)

    @staticmethod
    def list_objects_list_dir(request, bucket, dir_obj, only_obj: bool = False):
        delimiter = request.query_params.get('delimiter', None)
        prefix = request.query_params.get('prefix', '')

        paginator = paginations.ListObjectsCursorPagination()
        ret_data = {
            'Name': bucket.name,
            'Prefix': prefix,
            'Delimiter': delimiter
        }
        objs_qs = HarborManager().get_queryset_list_dir(bucket=bucket, dir_id=dir_obj.id,
                                                        only_obj=only_obj)
        objs = paginator.paginate_queryset(objs_qs, request=request)
        serializer = serializers.ListBucketObjectsSerializer(objs, many=True)

        data = paginator.get_paginated_data()
        ret_data.update(data)
        ret_data['Contents'] = serializer.data
        return Response(data=ret_data, status=status.HTTP_200_OK)

    @staticmethod
    def list_objects_list_prefix(request, bucket_name, prefix, only_obj: bool = False):
        """
        列举所有对象和目录
        """
        hm = HarborManager()
        try:
            bucket, objs_qs = hm.get_bucket_objects_dirs_queryset(
                bucket_name=bucket_name, user=request.user, prefix=prefix, only_obj=only_obj)
        except exceptions.Error as exc:
            return Response(data=exc.err_data(), status=exc.status_code)

        paginator = paginations.ListObjectsCursorPagination()
        objs_dirs = paginator.paginate_queryset(objs_qs, request=request)
        serializer = serializers.ListBucketObjectsSerializer(objs_dirs, many=True)

        data = paginator.get_paginated_data()
        data['Contents'] = serializer.data
        data['Name'] = bucket_name
        data['Prefix'] = prefix
        return Response(data=data, status=status.HTTP_200_OK)
