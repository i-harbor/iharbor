import random
from collections import OrderedDict
import logging
from io import BytesIO

from django.http import StreamingHttpResponse, FileResponse, Http404, QueryDict
from django.utils.http import urlquote
from django.utils import timezone
from django.db.models import Q as dQ
from django.db.models import Case, Value, When, F
from django.core.validators import validate_email
from django.core import exceptions
from rest_framework import viewsets, status, mixins
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.schemas import AutoSchema
from rest_framework.compat import coreapi, coreschema
from rest_framework.serializers import Serializer, ValidationError
from rest_framework.authtoken.models import Token

from buckets.utils import (BucketFileManagement, create_table_for_model_class, delete_table_for_model_class)
from users.views import send_active_url_email
from users.models import AuthKey
from users.auth.serializers import AuthKeyDumpSerializer
from utils.storagers import PathParser
from utils.oss import HarborObject, RadosWriteError, RadosError
from utils.log.decorators import log_used_time
from utils.jwt_token import JWTokenTool
from .models import User, Bucket
from buckets.models import ModelSaveError
from . import serializers
from . import paginations
from . import permissions
from . import throttles
from .harbor import HarborError, HarborManager

# Create your views here.
logger = logging.getLogger('django.request')#这里的日志记录器要和setting中的loggers选项对应，不能随意给参
debug_logger = logging.getLogger('debug')#这里的日志记录器要和setting中的loggers选项对应，不能随意给参


class CustomAutoSchema(AutoSchema):
    '''
    自定义Schema
    '''
    def get_manual_fields(self, path, method):
        '''
        重写方法，为每个方法自定义参数字段, action或method做key
        '''
        extra_fields = []
        action = None
        try:
            action = self.view.action
        except AttributeError:
            pass

        if action and type(self._manual_fields) is dict and action in self._manual_fields:
            extra_fields = self._manual_fields[action]
            return extra_fields

        if type(self._manual_fields) is dict and method in self._manual_fields:
            extra_fields = self._manual_fields[method]

        return extra_fields


class CustomGenericViewSet(viewsets.GenericViewSet):
    '''
    自定义GenericViewSet类，重写get_serializer方法，以通过context参数传递自定义参数
    '''
    def get_serializer(self, *args, **kwargs):
        """
        Return the serializer instance that should be used for validating and
        deserializing input, and for serializing output.
        """
        serializer_class = self.get_serializer_class()
        context = self.get_serializer_context()
        context.update(kwargs.get('context', {}))
        kwargs['context'] = context
        return serializer_class(*args, **kwargs)

    def perform_authentication(self, request):
        super(CustomGenericViewSet, self).perform_authentication(request)

        # 用户最后活跃日期
        user = request.user
        if user.id and user.id > 0:
            try:
                date = timezone.now().date()
                if user.last_active < date:
                    user.last_active = date
                    user.save(update_fields=['last_active'])
            except:
                pass


def get_user_own_bucket(bucket_name, request):
    '''
    获取当前用户的存储桶

    :param bucket_name: 存储通名称
    :param request: 请求对象
    :return:
        success: bucket
        failure: None
    '''
    bucket = Bucket.get_bucket_by_name(bucket_name)
    if not bucket:
        return None
    if not bucket.check_user_own_bucket(request.user):
        return None
    return bucket

def get_bucket_collection_name_or_response(bucket_name, request):
    '''
    获取存储通对应集合名称，或者Response对象
    :param bucket_name: 存储通名称
    :return: (collection_name, response)
            collection_name=None时，存储通不存在，response有效；
            collection_name!=''时，存储通存在，response=None；
    '''
    bucket = get_user_own_bucket(bucket_name, request)
    if not isinstance(bucket, Bucket):
        return None, Response(data={'code': 404, 'code_text': 'bucket_name参数有误，存储桶不存在'}, status=status.HTTP_404_NOT_FOUND)

    collection_name = bucket.get_bucket_table_name()
    return (collection_name, None)


class UserViewSet(mixins.ListModelMixin,
                  CustomGenericViewSet):
    '''
    用户类视图
    list:
    获取用户列表,需要超级用户权限

        http code 200 返回内容:
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
    获取一个用户详细信息，需要超级用户权限，或当前用户信息

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
    '''
    queryset = User.objects.all()
    lookup_field = 'username'
    lookup_value_regex = '.+'

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.save()
        if not send_active_url_email(request._request, user.email, user):
            return Response({'detail': '激活链接邮件发送失败'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        data = {
            'code': 201,
            'code_text': '用户注册成功，请登录邮箱访问收到的连接以激活用户',
            'data': serializer.validated_data,
        }
        return Response(data, status=status.HTTP_201_CREATED)

    def retrieve(self, request, *args, **kwargs):
        instance = self.get_object()
        if not (request.user.id == instance.id):
            return Response(data={"detail": "您没有执行该操作的权限。"}, status=status.HTTP_403_FORBIDDEN)
        serializer = self.get_serializer(instance)
        return Response(serializer.data)

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

        return Response({'code': 200, 'code_text': '修改成功', 'data':serializer.validated_data})

    def destroy(self, request, *args, **kwargs):
        user = self.get_object()
        if user.is_active != False:
            user.is_active = False
            user.save()
        return Response(status=status.HTTP_204_NO_CONTENT)

    def has_update_user_permission(self, request, instance):
        '''
        当前用户是否有修改给定用户信息的权限
        1、超级职员用户拥有所有权限；
        2、用户拥有修改自己信息的权限；
        3、超级用户只有修改普通用户信息的权限；

        :param request:
        :param instance: 用户实例
        :return:
            True: has permission
            False: has not permission
        '''
        user = request.user
        if not user.id: # 未认证用户
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
        '''
        动态加载序列化器
        '''
        if self.action == 'create':
            return serializers.UserCreateSerializer
        elif self.action == 'partial_update':
            return serializers.UserUpdateSerializer

        return serializers.UserDeitalSerializer

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action =='list':
            return [permissions.IsSuperUser()]
        elif self.action == 'create':
            return []
        elif self.action in ['retrieve', 'update', 'partial_update']:
            return [IsAuthenticated()]
        elif self.action == 'delete':
            return [permissions.IsSuperAndStaffUser()]
        return [permissions.IsSuperUser()]


class BucketViewSet(CustomGenericViewSet):
    '''
    存储桶视图

    list:
    获取存储桶列表

        >>Http Code: 状态码200：无异常时，返回所有的存储桶信息；
            {
                'code': 200,
                'buckets': [], // bucket对象列表
            }

    retrieve:
    获取一个存储桶详细信息

        >>Http Code: 状态码200：无异常时，返回存储桶的详细信息；
            {
                'code': 200,
                'bucket': {}, // bucket对象
            }

    create:
    创建一个新的存储桶

        >>Http Code: 状态码201；
            创建成功时：
            {
                'code': 201,
                'code_text': '创建成功',
                'data': serializer.data, //请求时提交数据
                'bucket': {}             //bucket对象信息
            }
        >>Http Code: 状态码400,参数有误：
            {
                'code': 400,
                'code_text': 'xxx',      //错误码表述信息
                'data': serializer.data, //请求时提交数据
                'existing': true or  false  // true表示资源已存在
            }

    delete:
    删除一个存储桶

        >>Http Code: 状态码204,存储桶删除成功
        >>Http Code: 状态码400
            {
                'code': 400,
                'code_text': '存储桶id有误'
            }
        >>Http Code: 状态码404：
            {
                'code': 404,
                'code_text': 'xxxxx'
            }

    partial_update:
    存储桶公有或私有权限设置

        Http Code: 状态码200：上传成功无异常时，返回数据：
        {
            'code': 200,
            'code_text': '对象共享设置成功'，
            'public': xxx,
        }
        Http Code: 状态码400：参数有误时，返回数据：
            对应参数错误信息;
        Http Code: 状态码404;

        Http code: 状态码500：
        {
            "code": 500,
            "code_text": "保存到数据库时错误"
        }

    '''
    queryset = Bucket.objects.filter(soft_delete=False).all()
    permission_classes = [IsAuthenticated, permissions.IsOwnBucket]
    pagination_class = paginations.BucketsLimitOffsetPagination

    # api docs
    schema = CustomAutoSchema(
        manual_fields={
            'destroy': [
                coreapi.Field(
                    name='ids',
                    required=False,
                    location='query',
                    schema=coreschema.Array(description='存储桶id列表或数组，删除多个存储桶时，通过此参数传递其他存储桶id'),
                ),
            ],
            'partial_update': [
                coreapi.Field(
                    name='public',
                    required=True,
                    location='query',
                    schema=coreschema.Boolean(description='是否分享，用于设置对象公有或私有, true(公开)，false(私有)'),
                ),
                coreapi.Field(
                    name='ids',
                    required=False,
                    location='query',
                    schema=coreschema.Array(description='存储桶id列表或数组，设置多个存储桶时，通过此参数传递其他存储桶id'),
                ),
            ]
        }
    )

    def list(self, request, *args, **kwargs):
        self.queryset = Bucket.objects.filter(dQ(user=request.user) & dQ(soft_delete=False)).all() # user's own

        queryset = self.filter_queryset(self.get_queryset())
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        else:
            serializer = self.get_serializer(queryset, many=True)
            data = {'code': 200, 'buckets': serializer.data,}
        return Response(data)

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        if not serializer.is_valid(raise_exception=False):
            code_text = '参数验证有误'
            existing = False
            try:
                for key, err_list in serializer.errors.items():
                    for err in err_list:
                        code_text = err
                        if err.code == 'existing':
                            existing = True
            except:
                pass

            data = {
                'code': 400,
                'code_text': code_text,
                'existing': existing,
                'data': serializer.data,
            }

            return Response(data, status=status.HTTP_400_BAD_REQUEST)

        # 创建bucket,创建bucket的shard集合
        bucket = serializer.save()
        col_name = bucket.get_bucket_table_name()
        bfm = BucketFileManagement(collection_name=col_name)
        model_class = bfm.get_obj_model_class()
        if not create_table_for_model_class(model=model_class):
            if not create_table_for_model_class(model=model_class):
                bucket.delete()
                delete_table_for_model_class(model=model_class)
                logger.error(f'创建桶“{bucket.name}”的数据库表失败')
                return Response(data={'code': 500, 'code_text': '创建桶失败，数据库错误'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        data = {
            'code': 201,
            'code_text': '创建成功',
            'data': serializer.data,
            'bucket': serializers.BucketSerializer(serializer.instance).data
        }
        return Response(data, status=status.HTTP_201_CREATED)

    def retrieve(self, request, *args, **kwargs):
        instance = self.get_object()
        serializer = self.get_serializer(instance)
        return Response({'code': 200, 'bucket': serializer.data})

    def destroy(self, request, *args, **kwargs):
        ids, response = self.get_buckets_ids_or_error_response(request, **kwargs)
        if not ids and response:
            return response

        buckets = Bucket.objects.filter(id__in=ids)
        if not buckets.exists():
            return Response(data={'code': 404, 'code_text': '未找到要删除的存储桶'}, status=status.HTTP_404_NOT_FOUND)
        for bucket in buckets:
            # 只删除用户自己的buckets
            if bucket.user.id == request.user.id:
                if not bucket.do_soft_delete():  # 软删除
                    if not bucket.do_soft_delete():
                        return Response(data={'code': 500, 'code_text': '删除存储桶失败'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(status=status.HTTP_204_NO_CONTENT)

    def partial_update(self, request, *args, **kwargs):
        public = request.query_params.get('public', '').lower()
        if public == 'true':
            public = True
        elif public == 'false':
            public = False
        else:
            return Response(data={'code': 400, 'code_text': 'public参数有误'}, status=status.HTTP_400_BAD_REQUEST)

        ids, response = self.get_buckets_ids_or_error_response(request, **kwargs)
        if not ids and response:
            return response

        buckets = Bucket.objects.filter(id__in=ids)
        if not buckets.exists():
            return Response(data={'code': 404, 'code_text': '未找到存储桶'}, status=status.HTTP_404_NOT_FOUND)
        for bucket in buckets:
            # 只设置用户自己的buckets
            if bucket.user.id == request.user.id:
                if not bucket.set_permission(public=public):
                    return Response(data={'code': 500, 'code_text': '更新数据库数据时错误'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        data = {
            'code': 200,
            'code_text': '存储桶权限设置成功',
            'public': public,
        }
        return Response(data=data, status=status.HTTP_200_OK)

    def get_buckets_ids_or_error_response(self, request, **kwargs):
        '''
        获取存储桶id列表
        :param request:
        :return:
            error: None, Response
            success:[ids], None
        '''
        id = kwargs.get(self.lookup_field, None)

        if isinstance(request.query_params, QueryDict):
            ids = request.query_params.getlist('ids')
        else:
            ids = request.query_params.get('ids')

        if not isinstance(ids, list):
            ids = []

        if id and id not in ids:
            ids.append(id)
        try:
            ids = [int(i) for i in ids]
        except ValueError:
            return None, Response(data={'code': 400, 'code_text': '存储桶id有误'}, status=status.HTTP_400_BAD_REQUEST)

        return ids, None

    def get_serializer_class(self):
        """
        Return the class to use for the serializer.
        Defaults to using `self.serializer_class`.
        Custom serializer_class
        """
        if self.action in ['list', 'retrieve']:
            return serializers.BucketSerializer
        elif self.action =='create':
            return serializers.BucketCreateSerializer
        return Serializer

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action in ['list', 'create', 'delete']:
            return [IsAuthenticated()]
        return [permission() for permission in self.permission_classes]


class ObjViewSet(CustomGenericViewSet):
    '''
    文件对象视图集

    update:
    通过文件对象绝对路径分片上传文件对象

        同POST方法，请使用POST，此方法后续废除

    create_detail:
    通过文件对象绝对路径分片上传文件对象

        说明：
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
            'created': True, # 上传第一个分片时，可用于判断对象是否是新建的，True(新建的)
            'data': 客户端请求时，携带的参数,不包含数据块；
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

    retrieve:
        通过文件对象绝对路径,下载文件对象，或者自定义读取对象数据块

        *注：
        1. offset && size(最大20MB，否则400错误) 参数校验失败时返回状态码400和对应参数错误信息，无误时，返回bytes数据流
        2. 不带参数时，返回整个文件对象；

    	>>Http Code: 状态码200：
             evhb_obj_size,文件对象总大小信息,通过标头headers传递：自定义读取时：返回指定大小的bytes数据流；
            其他,返回整个文件对象bytes数据流

        >>Http Code: 状态码400：文件路径参数有误：对应参数错误信息;
            {
                'code': 400,
                'code_text': 'xxxx参数有误'
            }
        >>Http Code: 状态码404：找不到资源;
        >>Http Code: 状态码500：服务器内部错误;

    destroy:
        通过文件对象绝对路径,删除文件对象；

        >>Http Code: 状态码204：删除成功，NO_CONTENT；
        >>Http Code: 状态码400：文件路径参数有误：对应参数错误信息;
            {
                'code': 400,
                'code_text': '参数有误'
            }
        >>Http Code: 状态码404：找不到资源;
        >>Http Code: 状态码500：服务器内部错误;

    partial_update:
    对象共享或私有权限设置

        Http Code: 状态码200：上传成功无异常时，返回数据：
        {
            'code': 200,
            'code_text': '对象共享设置成功'，
            'share': xxx,
            'days': xxx
        }
        Http Code: 状态码400：参数有误时，返回数据：
            对应参数错误信息;
        Http Code: 状态码404;

    '''
    queryset = {}
    # permission_classes = [IsAuthenticated]
    lookup_field = 'objpath'
    lookup_value_regex = '.+'

    # api docs
    VERSION_METHOD_FEILD = [
        coreapi.Field(
            name='version',
            required=True,
            location='path',
            schema=coreschema.String(description='API版本（v1, v2）')
        ),
    ]

    OBJ_PATH_METHOD_FEILD = [
        coreapi.Field(
            name='objpath',
            required=True,
            location='path',
            schema=coreschema.String(description='文件对象绝对路径，类型String'),
        ),
    ]

    schema = CustomAutoSchema(
        manual_fields={
            'retrieve': VERSION_METHOD_FEILD + OBJ_PATH_METHOD_FEILD + [
                coreapi.Field(
                    name='offset',
                    required=False,
                    location='query',
                    schema=coreschema.String(description='要读取的文件块在整个文件中的起始位置（bytes偏移量), 类型int'),
                ),
                coreapi.Field(
                    name='size',
                    required=False,
                    location='query',
                    schema=coreschema.String(description='要读取的文件块的字节大小, 类型int'),
                ),
            ],
            'destroy': VERSION_METHOD_FEILD + OBJ_PATH_METHOD_FEILD,
            'update': VERSION_METHOD_FEILD + OBJ_PATH_METHOD_FEILD + [
                coreapi.Field(
                    name='reset',
                    required=False,
                    location='query',
                    schema=coreschema.Boolean(description='reset=true时，如果对象已存在，重置对象大小为0')
                ),
            ],
            'create_detail': VERSION_METHOD_FEILD + OBJ_PATH_METHOD_FEILD + [
                coreapi.Field(
                    name='reset',
                    required=False,
                    location='query',
                    schema=coreschema.Boolean(description='reset=true时，如果对象已存在，重置对象大小为0')
                ),
            ],
            'partial_update': VERSION_METHOD_FEILD + [
                coreapi.Field(
                    name='share',
                    required=False,
                    location='query',
                    schema=coreschema.Boolean(description='是否分享，用于设置对象公有或私有, true(公开)，false(私有)'),
                ),
                coreapi.Field(
                    name='days',
                    required=False,
                    location='query',
                    schema=coreschema.String(description='对象公开分享天数(share=true时有效)，0表示永久公开，负数表示不公开，默认为0'),
                ),
            ],
        }
    )

    def create_detail(self, request, *args, **kwargs):
        if request.version == 'v1':
            return self.upload_chunk_v1(request, *args, **kwargs)

        return Response(data={'code': 404, 'code_text': 'URL中包含无效的版本'}, status=status.HTTP_404_NOT_FOUND)

    def update(self, request, *args, **kwargs):
        if request.version == 'v1':
            return self.upload_chunk_v1(request, *args, **kwargs)

        return Response(data={'code': 404, 'code_text': 'URL中包含无效的版本'}, status=status.HTTP_404_NOT_FOUND)

    @log_used_time(debug_logger, mark_text='upload chunks')
    def upload_chunk_v1(self, request, *args, **kwargs):

        objpath = kwargs.get(self.lookup_field, '')
        bucket_name = kwargs.get('bucket_name', '')
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
            return Response({
                'code': 400,
                'code_text': serializer.errors.get('non_field_errors', '参数有误，验证未通过'),
            }, status=status.HTTP_400_BAD_REQUEST)

        data = serializer.data
        offset = data.get('chunk_offset')
        file = request.data.get('chunk')

        hManager = HarborManager()
        try:
            created = hManager.write_file(bucket_name=bucket_name, obj_path=objpath, offset=offset, file=file,
                                           reset=reset, user=request.user)
        except HarborError as e:
            return Response(data={'code':e.code, 'code_text': e.msg}, status=e.code)

        data['created'] = created
        return Response(data, status=status.HTTP_200_OK)

    def retrieve(self, request, *args, **kwargs):

        objpath = kwargs.get(self.lookup_field, '')
        bucket_name = kwargs.get('bucket_name','')

        validated_param, valid_response = self.custom_read_param_validate_or_response(request)
        if not validated_param and valid_response:
            return valid_response

        # 自定义读取文件对象
        if validated_param:
            offset = validated_param.get('offset')
            size = validated_param.get('size')
            hManager = HarborManager()
            try:
                chunk, obj = hManager.read_chunk(bucket_name=bucket_name, obj_path=objpath,
                                                      offset=offset, size=size, user = request.user)
            except HarborError as e:
                return Response(data={'code': e.code, 'code_text': e.msg}, status=e.code)

            return self.wrap_chunk_response(chunk=chunk, obj_size=obj.si)

        # 下载整个文件对象
        hManager = HarborManager()
        try:
            file_generator, obj = hManager.get_obj_generator(bucket_name=bucket_name, obj_path=objpath,
                                                                  user=request.user)
        except HarborError as e:
            return Response(data={'code': e.code, 'code_text': e.msg}, status=e.code)

        filename = obj.name
        filename = urlquote(filename)  # 中文文件名需要
        response = FileResponse(file_generator)
        response['Content-Type'] = 'application/octet-stream'  # 注意格式
        response['Content-Length'] = obj.si
        response['Content-Disposition'] = f"attachment;filename*=utf-8''{filename}"  # 注意filename 这个是下载后的名字
        response['evob_obj_size'] = obj.si
        return response

    def destroy(self, request, *args, **kwargs):
        objpath = kwargs.get(self.lookup_field, '')
        bucket_name = kwargs.get('bucket_name','')
        hManager = HarborManager()
        try:
            ok = hManager.delete_object(bucket_name=bucket_name, obj_path=objpath, user=request.user)
        except HarborError as e:
            return Response(data={'code': e.code, 'code_text': e.msg}, status=e.code)

        if not ok:
            return Response(data={'code': 500, 'code_text': '删除失败'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(status=status.HTTP_204_NO_CONTENT)

    def partial_update(self, request, *args, **kwargs):
        bucket_name = kwargs.get('bucket_name', '')
        objpath = kwargs.get(self.lookup_field, '')

        validated_param, valid_response = self.shared_param_validate_or_response(request)
        if not validated_param and valid_response:
            return valid_response
        share = validated_param.get('share')
        days = validated_param.get('days')

        hManager = HarborManager()
        try:
            ok = hManager.share_object(bucket_name=bucket_name, obj_path=objpath, share=share, days=days, user=request.user)
        except HarborError as e:
            return Response(data={'code': e.code, 'code_text': e.msg}, status=e.code)

        if not ok:
            return Response(data={'code': 500, 'code_text': '对象共享权限设置失败'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        data = {
            'code': 200,
            'code_text': '对象共享权限设置成功',
            'share': share,
            'days': days
        }
        return Response(data=data, status=status.HTTP_200_OK)


    def get_serializer_class(self):
        """
        Return the class to use for the serializer.
        Defaults to using `self.serializer_class`.
        Custom serializer_class
        """
        if self.action in  ['update', 'create_detail']:
            return serializers.ObjPutSerializer
        return Serializer

    def get_file_obj_or_404(self, collection_name, path, filename):
        """
        获取文件对象
        """
        bfm = BucketFileManagement(path=path, collection_name=collection_name)
        ok, obj = bfm.get_dir_or_obj_exists(name=filename)
        if ok and obj and obj.is_file():
            return obj

        raise Http404

    def do_bucket_limit_validate(self, bfm:BucketFileManagement):
        '''
        存储桶的限制验证
        :return: True(验证通过); False(未通过)
        '''
        # 存储桶对象和文件夹数量上限验证
        if bfm.get_count() >= 10**7:
            return False

        return True

    @log_used_time(debug_logger, mark_text='select obj info')
    def get_obj_and_check_limit_or_create_or_404(self, collection_name, path, filename):
        '''
        获取文件对象, 验证集合文档数量上限，不存在并且验证通过则创建，其他错误(如对象父路径不存在)会抛404错误

        :param collection_name: 桶对应的数据库表名
        :param path: 文件对象所在的父路径
        :param filename: 文件对象名称
        :return: (obj, created); obj: 对象; created: 指示对象是否是新创建的，True(是)
                (obj, False) # 对象存在
                (obj, True)  # 对象不存在，创建一个新对象
                (None, None) # 集合文档数量已达上限，不允许再创建新的对象
                (dir, None)  # 已存在同名的目录
        '''
        bfm = BucketFileManagement(path=path, collection_name=collection_name)

        ok, obj = bfm.get_dir_or_obj_exists(name=filename)
        # 父路经不存在或有错误
        if not ok:
            raise Http404

        # 文件对象已存在
        if obj and obj.is_file():
            return obj, False

        # 已存在同名的目录
        if obj and obj.is_dir():
            return obj, None

        ok, did = bfm.get_cur_dir_id()
        if not ok:
            raise Http404 # 目录路径不存在

        # 验证集合文档上限
        # if not self.do_bucket_limit_validate(bfm):
        #     return None, None

        # 创建文件对象
        BucketFileClass = bfm.get_obj_model_class()
        full_filename = bfm.build_dir_full_name(filename)
        bfinfo = BucketFileClass(na=full_filename,  # 全路径文件名
                                 name=filename, #  文件名
                                fod=True,  # 文件
                                si=0)  # 文件大小
        # 有父节点
        if did:
            bfinfo.did = did

        try:
            bfinfo.save()
            obj = bfinfo
        except:
            logger.error(f'新建对象元数据保存数据库错误：{bfinfo.na}')
            raise Http404
        return obj, True

    def get_obj_info_response(self, request, fileobj, bucket_name, path):
        '''
        文件对象信息Response
        :param request:
        :param fileobj: 文件对象
        :param bucket_name: 存储桶
        :param path: 文件对象所在目录路径
        :return: Response
        '''
        serializer = serializers.ObjInfoSerializer(fileobj, context={'request': request,
                                                                           'bucket_name': bucket_name,
                                                                           'dir_path': path})
        return Response(data={
            'code': 200,
            'bucket_name': bucket_name,
            'dir_path': path,
            'obj': serializer.data,
            # 'breadcrumb': PathParser(path).get_path_breadcrumb()
        })

    def custom_read_param_validate_or_response(self, request):
        '''
        自定义读取文件对象参数验证
        :param request:
        :return:
                (None, None) -> 未携带参数
                (None, response) -> 参数有误
                ({data}, None) -> 参数验证通过

        '''
        chunk_offset = request.query_params.get('offset', None)
        chunk_size = request.query_params.get('size', None)

        validated_data = {}
        if chunk_offset is not None and chunk_size is not None:
            try:
                offset = int(chunk_offset)
                size = int(chunk_size)
                if offset < 0 or size < 0 or size > 20*1024**2: #20Mb
                    raise Exception()
                validated_data['offset'] = offset
                validated_data['size'] = size
            except:
                response = Response(data={'code': 400, 'code_text': 'offset或size参数有误'},
                                status=status.HTTP_400_BAD_REQUEST)
                return None, response
        # 未提交参数
        elif chunk_offset is None and chunk_size is None:
            return None, None
        # 参数提交不全
        else:
            response = Response(data={'code': 400, 'code_text': 'offset和size参数必须同时提交'},
                                status=status.HTTP_400_BAD_REQUEST)
            return None, response
        return validated_data, None

    def shared_param_validate_or_response(self, request):
        '''
        文件对象共享或私有权限参数验证
        :param request:
        :return:
            (None, response) -> 参数有误
            ({data}, None) -> 参数验证通过

        '''
        days = request.query_params.get('days', 0)
        share = request.query_params.get('share', '').lower()

        validated_data = {}
        if share == 'true':
            share = True
        elif share == 'false':
            share = False
        else:
            response = Response(data={'code': 400, 'code_text': 'share参数有误'}, status=status.HTTP_400_BAD_REQUEST)
            return (None, response)

        try:
            days = int(days)
            # if days < 0:
            #     raise Exception()
        except:
            response = Response(data={'code': 400, 'code_text': 'days参数有误'}, status=status.HTTP_400_BAD_REQUEST)
            return (None, response)

        validated_data['share'] = share
        validated_data['days'] = days
        return (validated_data, None)

    def wrap_chunk_response(self, chunk:bytes, obj_size:int):
        '''
        文件对象自定义读取response

        :param chunk: 数据块
        :param size: 文件对象总大小
        :return: HttpResponse
        '''
        c_len = len(chunk)
        response = StreamingHttpResponse(BytesIO(chunk), status=status.HTTP_200_OK)
        response['Content-Type'] = 'application/octet-stream'  # 注意格式
        response['evob_chunk_size'] = c_len
        response['Content-Length'] = c_len
        response['evob_obj_size'] = obj_size
        return response

    def pre_reset_upload(self, obj, rados):
        '''
        覆盖上传前的一些操作

        :param obj: 文件对象元数据
        :param rados: rados接口类对象
        :return:
                正常：True
                错误：Response
        '''
        # 先更新元数据，后删除rados数据（如果删除失败，恢复元数据）
        # 更新文件上传时间
        old_ult = obj.ult
        old_size = obj.si

        obj.ult = timezone.now()
        obj.si = 0
        if not obj.do_save(update_fields=['ult', 'si']):
            logger.error('修改对象元数据失败')
            return Response({'code': 500, 'code_text': '修改对象元数据失败'},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        ok, _ = rados.delete()
        if not ok:
            # 恢复元数据
            obj.ult = old_ult
            obj.si = old_size
            obj.do_save(update_fields=['ult', 'si'])
            logger.error('rados文件对象删除失败')
            return Response({'code': 500, 'code_text': 'rados文件对象删除失败'},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return True

    @log_used_time(debug_logger, mark_text='save_one_chunk')
    def save_one_chunk(self, obj, rados, chunk_offset, chunk):
        '''
        保存一个上传的分片

        :param obj: 对象元数据
        :param rados: rados接口
        :param chunk_offset: 分片偏移量
        :param chunk: 分片数据
        :return:
            成功：True
            失败：Response
        '''
        # 先更新元数据，后写rados数据
        try:
            # 更新文件修改时间和对象大小
            new_size = chunk_offset + chunk.size # 分片数据写入后 对象偏移量大小
            if not self.update_obj_metadata(obj, size=new_size):
                raise ModelSaveError()

            # 存储文件块
            try:
                ok, msg = rados.write_file(offset=chunk_offset, file=chunk)
            except Exception as e:
                raise RadosWriteError(str(e))
            if not ok:
                raise RadosWriteError(msg)
        except RadosWriteError as e:
            # 手动回滚对象元数据
            model = obj._meta.model
            try:
                model.objects.filter(id=obj.id).update(si=obj.si, upt=obj.upt)
            except:
                pass
            error = '文件块rados写入失败:' + str(e)
            logger.error(error)
            return Response({'code': 500, 'code_text': error}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        except ModelSaveError:
            logger.error('修改对象元数据失败')
            return Response({'code': 500, 'code_text': '修改对象元数据失败'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return True

    def update_obj_metadata(self, obj, size):
        '''
        更新对象元数据
        :param obj: 对象
        :param size: 对象大小
        :return:
            success: True
            failed: False
        '''
        model = obj._meta.model

        # 更新文件修改时间和对象大小
        old_size = obj.si if obj.si else 0
        new_size = max(size, old_size)  # 更新文件大小（只增不减）
        try:
            # r = model.objects.filter(id=obj.id, si=obj.si).update(si=new_size, upt=timezone.now())  # 乐观锁方式
            r = model.objects.filter(id=obj.id).update(si=Case(When(si__lt=new_size, then=Value(new_size)),
                                                               default=F('si')), upt=timezone.now())
        except Exception as e:
            return False
        if r > 0:  # 更新行数
            return True

        return False

    @log_used_time(debug_logger, mark_text='get request.data during upload file')
    def get_data(self, request):
        return request.data


class DirectoryViewSet(CustomGenericViewSet):
    '''
    目录视图集

    list:
    获取一个目录下的文件和文件夹信息

        >>Http Code: 状态码200:
            {
                'code': 200,
                'files': [fileobj, fileobj, ...],//文件信息对象列表
                'bucket_name': xxx,             //存储桶名称
                'dir_path': xxx,                //当前目录路径
            }
        >>Http Code: 状态码400:
            {
                'code': 400,
                'code_text': '参数有误'
            }
        >>Http Code: 状态码404:
            {
                'code': xxx,      //404
                'code_text': xxx  //错误码描述
            }

    create_detail:
        创建一个目录

        >>Http Code: 状态码400, 请求参数有误:
            {
                "code": 400,
                "code_text": 'xxxxx'        //错误信息
                "existing": true or  false  // true表示资源已存在
            }
        >>Http Code: 状态码201,创建文件夹成功：
            {
                'code': 201,
                'code_text': '创建文件夹成功',
                'data': {},      //请求时提交的数据
                'dir': {}，      //新目录对象信息
            }

    destroy:
        删除一个目录, 目录必须为空，否则400错误

        >>Http Code: 状态码204,成功删除;
        >>Http Code: 状态码400,参数无效或目录不为空;
            {
                'code': 400,
                'code_text': 'xxx'
            }
        >>Http Code: 状态码404;
            {
                'code': 404,
                'code_text': '文件不存在
            }
    '''
    queryset = []
    permission_classes = [IsAuthenticated]
    lookup_field = 'dirpath'
    lookup_value_regex = '.+'
    pagination_class = paginations.BucketFileLimitOffsetPagination

    # api docs
    VERSION_FIELDS = [
        coreapi.Field(
            name='version',
            required=True,
            location='path',
            schema=coreschema.String(description='API版本（v1, v2）')
        ),
    ]

    BASE_METHOD_FIELDS = VERSION_FIELDS + [
        coreapi.Field(
            name='dirpath',
            required=False,
            location='path',
            schema=coreschema.String(description='目录绝对路径')
        ),
    ]
    schema = CustomAutoSchema(
        manual_fields={
            'list': BASE_METHOD_FIELDS,
            'create_detail': BASE_METHOD_FIELDS,
            'destroy': BASE_METHOD_FIELDS,
        }
    )

    def list(self, request, *args, **kwargs):
        if request.version == 'v1':
            return self.list_v1(request, *args, **kwargs)

        return Response(data={'code': 404, 'code_text': 'URL中包含无效的版本'}, status=status.HTTP_404_NOT_FOUND)

    @log_used_time(debug_logger, mark_text='get dir files list')
    def list_v1(self, request, *args, **kwargs):
        bucket_name = kwargs.get('bucket_name', '')
        dir_path = kwargs.get(self.lookup_field, '')

        paginator = self.paginator
        try:
            offset = paginator.get_offset(request)
            limit = paginator.get_limit(request)
        except Exception as e:
            return Response(data={'code': 400, 'code_text': 'offset或limit参数无效'}, status=status.HTTP_400_BAD_REQUEST)

        hManager = HarborManager()
        try:
            files, bucket = hManager.list_dir(bucket_name=bucket_name, path=dir_path, offset=offset, limit=limit,
                                              user=request.user, paginator=paginator)
        except HarborError as e:
            return Response(data={'code': e.code, 'code_text': e.msg}, status=e.code)

        data_dict = OrderedDict([
            ('code', 200),
            ('bucket_name', bucket_name),
            ('dir_path', dir_path),
        ])

        serializer = self.get_serializer(files, many=True, context={'bucket_name': bucket_name, 'dir_path': dir_path, 'bucket': bucket})
        data_dict['files'] = serializer.data
        return paginator.get_paginated_response(data_dict)

    def create_detail(self, request, *args, **kwargs):
        bucket_name = kwargs.get('bucket_name', '')
        path = kwargs.get(self.lookup_field, '')
        hManager = HarborManager()
        try:
            ok, dir = hManager.mkdir(bucket_name=bucket_name, path=path, user=request.user)
        except HarborError as e:
            return Response(data={'code': e.code, 'code_text': e.msg}, status=e.code)

        data = {
            'code': 201,
            'code_text': '创建文件夹成功',
            'data': {'dir_name': dir.name, 'bucket_name': bucket_name, 'dir_path': dir.get_parent_path()},
            'dir': serializers.ObjInfoSerializer(dir).data
        }
        return Response(data, status=status.HTTP_201_CREATED)

    def destroy(self, request, *args, **kwargs):
        bucket_name = kwargs.get('bucket_name', '')
        dirpath = kwargs.get(self.lookup_field, '')

        hManager = HarborManager()
        try:
            ok = hManager.rmdir(bucket_name=bucket_name, dirpath=dirpath, user=request.user)
        except HarborError as e:
            return Response(data={'code': e.code, 'code_text': e.msg}, status=e.code)

        return Response(status=status.HTTP_204_NO_CONTENT)

    def get_serializer_class(self):
        """
        Return the class to use for the serializer.
        Defaults to using `self.serializer_class`.
        Custom serializer_class
        """
        if self.action in ['create_detail']:
            return Serializer
        return serializers.ObjInfoSerializer

    def get_dir_object(self, path, dir_name, collection_name):
        """
        Returns the object the view is displaying.
        """
        bfm = BucketFileManagement(path=path, collection_name=collection_name)
        ok, obj = bfm.get_dir_or_obj_exists(name=dir_name)
        if ok and obj:
            return obj
        return None

    def post_detail_params_validate_or_response(self, request, kwargs):
        '''
        post_detail参数验证

        :param request:
        :param kwargs:
        :return:
                success: ({data}, None)
                failure: (None, Response())
        '''
        data = {}

        bucket_name = kwargs.get('bucket_name', '')
        dirpath = kwargs.get(self.lookup_field, '')
        dir_path, dir_name = PathParser(filepath=dirpath).get_path_and_filename()

        if not bucket_name or not dir_name:
            return None, Response({'code': 400, 'code_text': '目录路径参数无效，要同时包含有效的存储桶和目录名称'},
                                  status=status.HTTP_400_BAD_REQUEST)

        if '/' in dir_name:
            return None, Response({'code': 400, 'code_text': 'dir_name不能包含‘/’'}, status=status.HTTP_400_BAD_REQUEST)

        if len(dir_name) > 255:
            return None, Response({'code': 400, 'code_text': 'dir_name长度最大为255字符'}, status=status.HTTP_400_BAD_REQUEST)

        # bucket是否属于当前用户,检测存储桶名称是否存在
        _collection_name, response = get_bucket_collection_name_or_response(bucket_name, request)
        if not _collection_name and response:
            return None, response

        data['collection_name'] = _collection_name

        bfm = BucketFileManagement(path=dir_path, collection_name=_collection_name)
        ok, dir = bfm.get_dir_or_obj_exists(name=dir_name)
        if not ok:
            return None, Response({'code': 400, 'code_text': '目录路径参数无效，父节点目录不存在'},
                                  status=status.HTTP_400_BAD_REQUEST)
        # 目录已存在
        if dir and dir.is_dir():
            return None, Response({'code': 400, 'code_text': f'"{dir_name}"目录已存在', 'existing': True},
                                  status=status.HTTP_400_BAD_REQUEST)

        # 同名对象已存在
        if dir and dir.is_file():
            return None, Response({'code': 400, 'code_text': f'"指定目录名称{dir_name}"已存在重名对象，请重新指定一个目录名称'},
                                  status=status.HTTP_400_BAD_REQUEST)

        data['did'] = bfm.cur_dir_id if bfm.cur_dir_id else bfm.get_cur_dir_id()[-1]
        data['bucket_name'] = bucket_name
        data['dir_path'] = dir_path
        data['dir_name'] = dir_name
        return data, None

    @log_used_time(debug_logger, 'paginate in dir')
    def paginate_queryset(self, queryset):
        return super(DirectoryViewSet, self).paginate_queryset(queryset)


class BucketStatsViewSet(CustomGenericViewSet):
    '''
        视图集

        retrieve:
            统计存储桶对象数量和所占容量，字节

            >>Http Code: 状态码200:
                {
                    "stats": {
                      "space": 12500047770969,             # 桶内对象总大小，单位字节
                      "count": 5000004,                    # 桶内对象总数量
                    },
                    "stats_time": "2019-03-06 08:19:43", # 统计时间
                    "code": 200,
                    "bucket_name": "xxx"    # 存储桶名称
                }

            >>Http Code: 状态码404:
                {
                    'code': 404,
                    'code_text': xxx  //错误码描述
                }
        '''
    queryset = []
    permission_classes = [IsAuthenticated]
    lookup_field = 'bucket_name'
    lookup_value_regex = '[a-z0-9-]{3,64}'

    # api docs
    BASE_METHOD_FIELDS = [
        coreapi.Field(
            name='version',
            required=True,
            location='path',
            schema=coreschema.String(description='API版本（v1, v2）')
        ),
        coreapi.Field(
            name='bucket_name',
            required=True,
            location='path',
            schema=coreschema.String(description='存储桶名称')
        ),
    ]
    schema = CustomAutoSchema(
        manual_fields={
            'retrieve': BASE_METHOD_FIELDS,
        }
    )

    def retrieve(self, request, *args, **kwargs):
        bucket_name = kwargs.get(self.lookup_field)

        bucket = get_user_own_bucket(bucket_name, request)
        if not isinstance(bucket, Bucket):
            return Response(data={'code': 404, 'code_text': 'bucket_name参数有误，存储桶不存在'},
                                  status=status.HTTP_404_NOT_FOUND)

        data = bucket.get_stats()
        data.update({
            'code': 200,
            'bucket_name': bucket_name,
        })

        return Response(data)


class SecurityViewSet(CustomGenericViewSet):
    '''
    安全凭证视图集

    retrieve:
        获取指定用户的安全凭证，需要超级用户权限
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
                      "create_time": "2019-02-20 13:56:25",
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
        '''
    queryset = []
    permission_classes = [ permissions.IsSuperOrAppSuperUser]
    lookup_field = 'username'
    lookup_value_regex = '.+'

    # api docs
    BASE_METHOD_FIELDS = [
        coreapi.Field(
            name='version',
            required=True,
            location='path',
            schema=coreschema.String(description='API版本（v1, v2）')
        ),
        coreapi.Field(
            name='username',
            required=True,
            location='path',
            schema=coreschema.String(description='用户名')
        ),
        coreapi.Field(
            name='key',
            required=False,
            location='query',
            schema=coreschema.String(description='访问密钥对')
        ),
    ]
    schema = CustomAutoSchema(
        manual_fields={
            'retrieve': BASE_METHOD_FIELDS,
        }
    )

    def retrieve(self, request, *args, **kwargs):
        username = kwargs.get(self.lookup_field)
        key = request.query_params.get('key', None)

        try:
            self.validate_username(username)
        except exceptions.ValidationError as e:
            msg = e.message or 'Must be a valid email.'
            return Response({'username': msg}, status=status.HTTP_400_BAD_REQUEST)

        user = self.get_user_or_create(username)
        token, created = Token.objects.get_or_create(user=user)

        # jwt token
        jwtoken = JWTokenTool().obtain_one_jwt_token(user=user)

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

    def get_user_or_create(self, username):
        '''
        通过用户名获取用户，或创建用户
        :param username:  用户名
        :return:
        '''
        try:
            user = User.objects.get(username=username)
        except exceptions.ObjectDoesNotExist:
            user = None

        if user:
            return user

        user = User(username=username, email=username)
        user.save()

        return user

    def validate_username(self, username):
        '''
        验证用户名是否是邮箱

        failed: raise ValidationError
        '''
        validate_email(username)


class MoveViewSet(CustomGenericViewSet):
    '''
    对象移动或重命名

    create_detail:
        移动或重命名一个对象

        参数move_to指定对象移动的目标路径（bucket桶下的目录路径），/或空字符串表示桶下根目录；参数rename指定重命名对象的新名称；
        请求时至少提交其中一个参数，亦可同时提交两个参数；只提交参数move_to只移动对象，只提交参数rename只重命名对象；

        >>Http Code: 状态码201,成功：
            {
                "code": 201,
                "code_text": "移动对象操作成功",
                "bucket_name": "6666",
                "dir_path": "ddd/动次打次",
                "obj": {},       //移动操作成功后文件对象详细信息
            }
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
    '''
    queryset = []
    permission_classes = [IsAuthenticated]
    lookup_field = 'objpath'
    lookup_value_regex = '.+'

    # api docs
    VERSION_METHOD_FEILD = [
        coreapi.Field(
            name='version',
            required=True,
            location='path',
            schema=coreschema.String(description='API版本（v1, v2）')
        ),
    ]

    OBJ_PATH_METHOD_FEILD = [
        coreapi.Field(
            name='objpath',
            required=True,
            location='path',
            schema=coreschema.String(description='文件对象绝对路径，类型String'),
        ),
    ]
    schema = CustomAutoSchema(
        manual_fields={
            'create_detail': VERSION_METHOD_FEILD + OBJ_PATH_METHOD_FEILD + [
                coreapi.Field(
                    name='move_to',
                    required=False,
                    location='query',
                    schema=coreschema.String(description='移动对象到此目录路径下，/或空字符串表示桶下根目录，类型String'),
                ),
                coreapi.Field(
                    name='rename',
                    required=False,
                    location='query',
                    schema=coreschema.String(description='重命名对象的新名称，类型String'),
                ),
            ],
        }
    )

    def create_detail(self, request, *args, **kwargs):
        if request.version == 'v1':
            return self.create_detail_v1(request, *args, **kwargs)

        return Response(data={'code': 404, 'code_text': 'URL中包含无效的版本'}, status=status.HTTP_404_NOT_FOUND)

    def create_detail_v1(self, request, *args, **kwargs):
        bucket_name = kwargs.get('bucket_name', '')
        objpath = kwargs.get(self.lookup_field, '')
        move_to = request.query_params.get('move_to', None)
        rename = request.query_params.get('rename', None)

        hManager = HarborManager()
        try:
            obj, bucket = hManager.move_rename(bucket_name=bucket_name, obj_path=objpath, rename=rename, move=move_to, user=request.user)
        except HarborError as e:
            return Response(data={'code':e.code, 'code_text': e.msg}, status=e.code)

        context = self.get_serializer_context()
        context.update({'bucket_name': bucket.name, 'bucket': bucket})
        return Response(data={'code': 201, 'code_text': '移动对象操作成功',
                              'bucket_name': bucket.name,
                              'dir_path': obj.get_parent_path(),
                              'obj': serializers.ObjInfoSerializer(obj, context=context).data},
                        status=status.HTTP_201_CREATED)

    def move_rename_obj(self, bucket, obj, move_to, rename):
        '''
        移动重命名对象

        :param bucket: 对象所在桶
        :param obj: 文件对象
        :param move_to: 移动目标路径
        :param rename: 重命名的新名称
        :return:
            Response()
        '''
        table_name = bucket.get_bucket_table_name()
        new_obj_name = rename if rename else obj.name # 移动后对象的名称，对象名称不变或重命名

        # 检查是否符合移动或重命名条件，目标路径下是否已存在同名对象或子目录
        if move_to is None: # 仅仅重命名对象，不移动
            bfm = BucketFileManagement( collection_name=table_name)
            ok, target_obj = bfm.get_dir_or_obj_exists(name=new_obj_name, cur_dir_id=obj.did)
        else: # 需要移动对象
            bfm = BucketFileManagement(path=move_to, collection_name=table_name)
            ok, target_obj = bfm.get_dir_or_obj_exists(name=new_obj_name)

        if not ok:
            return Response(data={'code': 404, 'code_text': '无法完成对象的移动操作，指定的目标路径未找到'},
                            status=status.HTTP_404_NOT_FOUND)

        if target_obj:
            return Response(data={'code': 400, 'code_text': '无法完成对象的移动操作，指定的目标路径下已存在同名的对象或目录'},
                            status=status.HTTP_400_BAD_REQUEST)

        # 仅仅重命名对象，不移动
        if move_to is None:
            path, _ = PathParser(filepath=obj.na).get_path_and_filename()
            obj.na = path + '/' + new_obj_name if path else  new_obj_name
            obj.name = new_obj_name
        else: # 移动对象或重命名
            _, did = bfm.get_cur_dir_id()
            obj.did = did
            obj.na = bfm.build_dir_full_name(new_obj_name)
            obj.name = new_obj_name
            path = move_to

        if not obj.do_save():
            return Response(data={'code': 500, 'code_text': '移动对象操作失败'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        context = self.get_serializer_context()
        context.update({'bucket_name': bucket.name, 'bucket': bucket})
        return Response(data={'code': 201, 'code_text': '移动对象操作成功',
                              'bucket_name': bucket.name,
                              'dir_path': path,
                              'obj': serializers.ObjInfoSerializer(obj, context=context).data}, status=status.HTTP_201_CREATED)

    def validate_params(self, request):
        '''
        校验请求参数
        :param request:
        :return:
            {
                'move_to': xxx, # None(未提交此参数) 或 string
                'rename': xxx   # None(未提交此参数) 或 string
            }
        '''
        validated_data = {'move_to': None, 'rename': None}
        move_to = request.query_params.get('move_to')
        rename = request.query_params.get('rename')

        # 移动对象参数
        if move_to is not None:
            validated_data['move_to'] = move_to.strip('/')

        # 重命名对象参数
        if rename is not None:
            if '/' in rename:
                raise ValidationError('对象名称不能含“/”')

            if len(rename) > 255:
                raise ValidationError('对象名称不能大于255个字符长度')

            validated_data['rename'] = rename

        return validated_data

    def get_obj_or_dir_404(self, table_name, path, name):
        '''
        获取文件对象或目录

        :param table_name: 数据库表名
        :param path: 父目录路经
        :param name: 对象名称或目录名称
        :return:
            None: 父目录路径错误，不存在
            obj: 对象或目录
            raise Http404: 目录或对象不存在
        '''
        bfm = BucketFileManagement(path=path, collection_name=table_name)
        ok, obj = bfm.get_dir_or_obj_exists(name=name)
        if not ok:
            return None

        if obj:
            return obj

        raise Http404

    def get_obj_or_404(self, table_name, path, name):
        '''
        获取文件对象

        :param table_name: 数据库表名
        :param path: 父目录路经
        :param name: 对象名称
        :return:
            None: 父目录路径错误，不存在
            obj: 对象
            raise Http404: 对象不存在
        '''
        obj = self.get_obj_or_dir_404(table_name=table_name, path=path, name=name)
        if not obj:
            return None

        if obj.is_file():
            return obj
        else:
            raise Http404

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
    '''
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
                    "ult": "2019-01-31 10:55:51",       # 创建时间
                    "upt": "2019-01-31 10:55:51",       # 最后修改时间； 目录时此字段为空
                    "dlc": 2,                           # 下载次数； 目录时此字段为0
                    "download_url": "http://10.0.86.213/obs/gggg/upload/Firefox-latest.exe", # 对象下载url; 目录此字段为空
                    "access_permission": "私有"          # 访问权限，‘私有’或‘公有’； 目录此字段为空
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
    '''
    queryset = []
    permission_classes = [IsAuthenticated]
    lookup_field = 'path'
    lookup_value_regex = '.+'

    # api docs
    VERSION_METHOD_FEILD = [
        coreapi.Field(
            name='version',
            required=True,
            location='path',
            schema=coreschema.String(description='API版本（v1, v2）')
        ),
    ]

    OBJ_PATH_METHOD_FEILD = [
        coreapi.Field(
            name='path',
            required=True,
            location='path',
            schema=coreschema.String(description='对象或目录绝对路径，类型String'),
        ),
    ]
    schema = CustomAutoSchema(
        manual_fields={
            'retrieve': VERSION_METHOD_FEILD + OBJ_PATH_METHOD_FEILD,
        }
    )

    def retrieve(self, request, *args, **kwargs):
        if request.version == 'v1':
            return self.retrieve_v1(request, *args, **kwargs)

        return Response(data={'code': 404, 'code_text': 'URL中包含无效的版本'}, status=status.HTTP_404_NOT_FOUND)

    def retrieve_v1(self, request, *args, **kwargs):
        path_name = kwargs.get(self.lookup_field, '')
        bucket_name = kwargs.get('bucket_name', '')
        path, name = PathParser(filepath=path_name).get_path_and_filename()
        if not bucket_name or not name:
            return Response(data={'code': 400, 'code_text': 'path参数有误'}, status=status.HTTP_400_BAD_REQUEST)

        # 存储桶验证和获取桶对象
        bucket = get_user_own_bucket(bucket_name=bucket_name, request=request)
        if not bucket:
            return Response(data={'code': 404, 'code_text': '存储桶不存在'},
                            status=status.HTTP_404_NOT_FOUND)

        table_name = bucket.get_bucket_table_name()
        try:
            obj = self.get_obj_or_dir_404(table_name, path, name)
        except Http404:
            return Response(data={'code': 404, 'code_text': '指定对象或目录不存在'},
                            status=status.HTTP_404_NOT_FOUND)

        serializer = self.get_serializer(obj, context={'bucket': bucket, 'bucket_name': bucket_name, 'dir_path': path})
        return Response(data={'code': 200, 'code_text': '获取元数据成功', 'bucket_name': bucket_name,
                              'dir_path': path, 'obj': serializer.data})

    def get_obj_or_dir_404(self, table_name, path, name):
        '''
        获取文件对象或目录

        :param table_name: 数据库表名
        :param path: 父目录路经
        :param name: 对象名称或目录名称
        :return:
            obj: 对象或目录
            raise Http404: 目录或对象不存在，父目录路径错误，不存在
        '''
        bfm = BucketFileManagement(path=path, collection_name=table_name)
        ok, obj = bfm.get_dir_or_obj_exists(name=name)
        if not ok:
            raise Http404

        if obj:
            return obj

        raise Http404

    def get_serializer_class(self):
        """
        Return the class to use for the serializer.
        Defaults to using `self.serializer_class`.
        Custom serializer_class
        """
        if self.action in ['retrieve']:
            return serializers.ObjInfoSerializer
        return Serializer


class CephStatsViewSet(CustomGenericViewSet):
    '''
        ceph集群视图集

        list:
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
        '''
    queryset = []
    permission_classes = [permissions.IsSuperUser]
    # lookup_field = 'bucket_name'
    # lookup_value_regex = '[a-z0-9-]{3,64}'
    pagination_class = None

    # api docs
    BASE_METHOD_FIELDS = [
        coreapi.Field(
            name='version',
            required=True,
            location='path',
            schema=coreschema.String(description='API版本（v1, v2）')
        ),
    ]
    schema = CustomAutoSchema(
        manual_fields={
            'list': BASE_METHOD_FIELDS,
        }
    )

    def list(self, request, *args, **kwargs):
        if request.version == 'v1':
            return self.list_v1(request, *args, **kwargs)

        return Response(data={'code': 404, 'code_text': 'URL中包含无效的版本'}, status=status.HTTP_404_NOT_FOUND)

    def list_v1(self, request, *args, **kwargs):
        try:
            stats = HarborObject(obj_id='').get_cluster_stats()
        except RadosError as e:
            return Response(data={'code': 500, 'code_text': '获取ceph集群信息错误：' + str(e)},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({
            'code': 200,
            'code_text': 'successful',
            'stats': stats
        })

class UserStatsViewSet(CustomGenericViewSet):
    '''
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
                        "stats_time": "2019-05-14 10:49:39", # 统计时间
                        "bucket_name": "wwww"       # 存储桶名称
                    },
                    {
                        "stats": {
                            "space": 959820827,
                            "count": 17
                        },
                        "stats_time": "2019-05-14 10:50:02",
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
                        "stats_time": "2019-05-14 10:49:39", # 统计时间
                        "bucket_name": "wwww"       # 存储桶名称
                    },
                    {
                        "stats": {
                            "space": 959820827,
                            "count": 17
                        },
                        "stats_time": "2019-05-14 10:50:02",
                        "bucket_name": "gggg"
                    },
                ]
            }

            >>Http Code: 状态码404:
                {
                    'code': 404,
                    'code_text': xxx  //错误码描述
                }
        '''
    queryset = []
    permission_classes = [IsAuthenticated]
    lookup_field = 'username'
    lookup_value_regex = '.+'
    pagination_class = None

    # api docs
    BASE_METHOD_FIELDS = [
        coreapi.Field(
            name='version',
            required=True,
            location='path',
            schema=coreschema.String(description='API版本（v1, v2）')
        ),
    ]
    schema = CustomAutoSchema(
        manual_fields={
            'list': BASE_METHOD_FIELDS,
        }
    )

    def list(self, request, *args, **kwargs):
        if request.version == 'v1':
            return self.list_v1(request, *args, **kwargs)

        return Response(data={'code': 404, 'code_text': 'URL中包含无效的版本'}, status=status.HTTP_404_NOT_FOUND)

    def list_v1(self, request, *args, **kwargs):
        user = request.user
        data = self.get_user_stats(user)
        data['code'] = 200
        data['username'] = user.username
        return Response(data)

    def retrieve(self, request, *args, **kwargs):
        if request.version == 'v1':
            return self.retrieve_v1(request, *args, **kwargs)

        return Response(data={'code': 404, 'code_text': 'URL中包含无效的版本'}, status=status.HTTP_404_NOT_FOUND)

    def retrieve_v1(self, request, *args, **kwargs):
        username = kwargs.get(self.lookup_field)
        try:
            user = User.objects.get(username=username)
        except exceptions.ObjectDoesNotExist:
            return Response(data={'code': 404, 'code_text': 'username参数有误，用户不存在'},
                            status=status.HTTP_404_NOT_FOUND)

        data = self.get_user_stats(user)
        data['code'] = 200
        data['username'] = user.username
        return Response(data)

    def get_user_stats(self, user):
        '''获取用户的资源统计信息'''
        all_count = 0
        all_space = 0
        li = []
        buckets = Bucket.objects.filter(dQ(user=user) & dQ(soft_delete=False))
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
        if self.action =='retrieve':
            return [permissions.IsSuperUser()]

        return super(UserStatsViewSet, self).get_permissions()


class CephComponentsViewSet(CustomGenericViewSet):
    '''
        ceph集群组件信息视图集

        list:
            ceph的mon，osd，mgr，mds组件信息， 需要超级用户权限

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
        '''
    queryset = []
    permission_classes = [permissions.IsSuperUser]
    # lookup_field = 'bucket_name'
    # lookup_value_regex = '[a-z0-9-]{3,64}'
    pagination_class = None

    # api docs
    BASE_METHOD_FIELDS = [
        coreapi.Field(
            name='version',
            required=True,
            location='path',
            schema=coreschema.String(description='API版本（v1, v2）')
        ),
    ]
    schema = CustomAutoSchema(
        manual_fields={
            'list': BASE_METHOD_FIELDS,
        }
    )

    def list(self, request, *args, **kwargs):
        if request.version == 'v1':
            return self.list_v1(request, *args, **kwargs)

        return Response(data={'code': 404, 'code_text': 'URL中包含无效的版本'}, status=status.HTTP_404_NOT_FOUND)

    def list_v1(self, request, *args, **kwargs):
        return Response({
            'code': 200,
            'mon': {},
            'osd': {},
            'mgr': {},
            'mds': {}
        })


class CephErrorViewSet(CustomGenericViewSet):
    '''
        ceph集群当前故障信息查询

        list:
            ceph集群当前故障信息查询，需要超级用户权限

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
        '''
    queryset = []
    permission_classes = [permissions.IsSuperUser]
    # lookup_field = 'bucket_name'
    # lookup_value_regex = '[a-z0-9-]{3,64}'
    pagination_class = None

    # api docs
    BASE_METHOD_FIELDS = [
        coreapi.Field(
            name='version',
            required=True,
            location='path',
            schema=coreschema.String(description='API版本（v1, v2）')
        ),
    ]
    schema = CustomAutoSchema(
        manual_fields={
            'list': BASE_METHOD_FIELDS,
        }
    )

    def list(self, request, *args, **kwargs):
        if request.version == 'v1':
            return self.list_v1(request, *args, **kwargs)

        return Response(data={'code': 404, 'code_text': 'URL中包含无效的版本'}, status=status.HTTP_404_NOT_FOUND)

    def list_v1(self, request, *args, **kwargs):
        return Response({
            'code': 200,
            'errors': {

            }
        })


class CephPerformanceViewSet(CustomGenericViewSet):
    '''
        ceph集群性能，需要超级用户权限

        list:
            ceph集群的IOPS，I/O带宽，需要超级用户权限

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
        '''
    queryset = []
    permission_classes = [permissions.IsSuperUser]
    # lookup_field = 'bucket_name'
    # lookup_value_regex = '[a-z0-9-]{3,64}'
    pagination_class = None

    # api docs
    BASE_METHOD_FIELDS = [
        coreapi.Field(
            name='version',
            required=True,
            location='path',
            schema=coreschema.String(description='API版本（v1, v2）')
        ),
    ]
    schema = CustomAutoSchema(
        manual_fields={
            'list': BASE_METHOD_FIELDS,
        }
    )

    def list(self, request, *args, **kwargs):
        if request.version == 'v1':
            return self.list_v1(request, *args, **kwargs)

        return Response(data={'code': 404, 'code_text': 'URL中包含无效的版本'}, status=status.HTTP_404_NOT_FOUND)

    def list_v1(self, request, *args, **kwargs):
        ok, data = HarborObject(obj_id='').get_ceph_io_status()
        if not ok:
            return Response(data={'code': 500, 'code_text': 'Get io status error:' + data}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        return Response(data=data)


class UserCountViewSet(CustomGenericViewSet):
    '''
        对象云存储系统用户总数查询

        list:
            对象云存储系统用户总数查询，需要超级用户权限

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
        '''
    queryset = []
    permission_classes = [permissions.IsSuperUser]
    # lookup_field = 'bucket_name'
    # lookup_value_regex = '[a-z0-9-]{3,64}'
    pagination_class = None

    # api docs
    BASE_METHOD_FIELDS = [
        coreapi.Field(
            name='version',
            required=True,
            location='path',
            schema=coreschema.String(description='API版本（v1, v2）')
        ),
    ]
    schema = CustomAutoSchema(
        manual_fields={
            'list': BASE_METHOD_FIELDS,
        }
    )

    def list(self, request, *args, **kwargs):
        if request.version == 'v1':
            return self.list_v1(request, *args, **kwargs)

        return Response(data={'code': 404, 'code_text': 'URL中包含无效的版本'}, status=status.HTTP_404_NOT_FOUND)

    def list_v1(self, request, *args, **kwargs):
        count = User.objects.filter(is_active=True).count()
        return Response({
            'code': 200,
            'count': count
        })


class AvailabilityViewSet(CustomGenericViewSet):
    '''
        系统可用性

        list:
            系统可用性查询，需要超级用户权限

            >>Http Code: 状态码200:
                {
                    "code": 200,
                    'availability': '100%'
                }

            >>Http Code: 状态码404:
                {
                    'code': 404,
                    'code_text': URL中包含无效的版本  //错误码描述
                }
        '''
    queryset = []
    permission_classes = [permissions.IsSuperUser]
    # lookup_field = 'bucket_name'
    # lookup_value_regex = '[a-z0-9-]{3,64}'
    pagination_class = None

    # api docs
    BASE_METHOD_FIELDS = [
        coreapi.Field(
            name='version',
            required=True,
            location='path',
            schema=coreschema.String(description='API版本（v1, v2）')
        ),
    ]
    schema = CustomAutoSchema(
        manual_fields={
            'list': BASE_METHOD_FIELDS,
        }
    )

    def list(self, request, *args, **kwargs):
        if request.version == 'v1':
            return self.list_v1(request, *args, **kwargs)

        return Response(data={'code': 404, 'code_text': 'URL中包含无效的版本'}, status=status.HTTP_404_NOT_FOUND)

    def list_v1(self, request, *args, **kwargs):
        return Response({
            'code': 200,
            'availability': '100%'
        })


class VisitStatsViewSet(CustomGenericViewSet):
    '''
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
        '''
    queryset = []
    permission_classes = [permissions.IsSuperUser]
    # lookup_field = 'bucket_name'
    # lookup_value_regex = '[a-z0-9-]{3,64}'
    pagination_class = None

    # api docs
    BASE_METHOD_FIELDS = [
        coreapi.Field(
            name='version',
            required=True,
            location='path',
            schema=coreschema.String(description='API版本（v1, v2）')
        ),
    ]
    schema = CustomAutoSchema(
        manual_fields={
            'list': BASE_METHOD_FIELDS,
        }
    )

    def list(self, request, *args, **kwargs):
        if request.version == 'v1':
            return self.list_v1(request, *args, **kwargs)

        return Response(data={'code': 404, 'code_text': 'URL中包含无效的版本'}, status=status.HTTP_404_NOT_FOUND)

    def list_v1(self, request, *args, **kwargs):
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
    '''
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
        '''
    queryset = []
    permission_classes = []
    throttle_classes = (throttles.TestRateThrottle,)
    # lookup_field = 'bucket_name'
    # lookup_value_regex = '[a-z0-9-]{3,64}'
    pagination_class = None

    # api docs
    BASE_METHOD_FIELDS = [
        coreapi.Field(
            name='version',
            required=True,
            location='path',
            schema=coreschema.String(description='API版本（v1, v2）')
        ),
    ]
    schema = CustomAutoSchema(
        manual_fields={
            'list': BASE_METHOD_FIELDS,
        }
    )

    def list(self, request, *args, **kwargs):
        if request.version == 'v1':
            return self.list_v1(request, *args, **kwargs)

        return Response(data={'code': 404, 'code_text': 'URL中包含无效的版本'}, status=status.HTTP_404_NOT_FOUND)

    def list_v1(self, request, *args, **kwargs):
        return Response({
            'code': 200,
            'code_text': '系统可用',
            'status': True     # True: 可用；False: 不可用
        })


class FtpViewSet(CustomGenericViewSet):
    '''
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
            }
        }
        Http Code: 状态码400：参数有误时，返回数据：
            对应参数错误信息;
        Http Code: 状态码404;
        Http Code: 500
    '''
    queryset = []
    permission_classes = [IsAuthenticated]
    lookup_field = 'bucket_name'
    lookup_value_regex = '[a-z0-9-]{3,64}'
    pagination_class = None

    # api docs
    BASE_METHOD_FIELDS = [
        coreapi.Field(
            name='version',
            required=True,
            location='path',
            schema=coreschema.String(description='API版本（v1, v2）')
        ),
    ]
    schema = CustomAutoSchema(
        manual_fields={
            'partial_update': BASE_METHOD_FIELDS + [
                coreapi.Field(
                    name='bucket_name',
                    required=True,
                    location='path',
                    schema=coreschema.String(description='存储桶名称')
                ),
                coreapi.Field(
                    name='enable',
                    required=False,
                    location='query',
                    schema=coreschema.Boolean(description='是否使能存储桶ftp访问')
                ),
                coreapi.Field(
                    name='password',
                    required=False,
                    location='query',
                    schema=coreschema.String(description='存储桶ftp新的访问密码')
                ),
            ],
        }
    )

    def partial_update(self, request, *args, **kwargs):
        if request.version == 'v1':
            return self.patch_v1(request, *args, **kwargs)

        return Response(data={'code': 404, 'code_text': 'URL中包含无效的版本'}, status=status.HTTP_404_NOT_FOUND)

    def patch_v1(self, request, *args, **kwargs):
        bucket_name = kwargs.get(self.lookup_field, '')
        if not bucket_name:
            return Response(data={'code': 400, 'code_text': '桶名称有误'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            params = self.validate_patch_params(request)
        except ValidationError as e:
            return Response(data={'code': 400, 'code_text': e.detail}, status=status.HTTP_400_BAD_REQUEST)

        enable = params.get('enable')
        password = params.get('password')

        # 存储桶验证和获取桶对象
        bucket = get_user_own_bucket(bucket_name=bucket_name, request=request)
        if not bucket:
            return Response(data={'code': 404, 'code_text': 'bucket_name参数有误，存储桶不存在'},
                            status=status.HTTP_404_NOT_FOUND)

        data = {}
        if enable is not None:
            bucket.ftp_enable = enable
            data['enable'] = enable

        if password is not None:
            bucket.ftp_password = password
            data['password'] = password

        try:
            bucket.save()
        except Exception as e:
            return Response(data={'code': 500, 'code_text': 'ftp配置失败'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({
            'code': 200,
            'code_text': '系统可用',
            'data': data     # 请求提交的参数
        })

    def validate_patch_params(self, request):
        '''
        patch请求方法参数验证
        :return:
            {
                'enable': xxx, # None(未提交此参数) 或 bool
                'password': xxx   # None(未提交此参数) 或 string
            }
        '''
        validated_data = {'enable': None, 'password': None}
        enable = request.query_params.get('enable', None)
        password = request.query_params.get('password', None)

        if not enable and not password:
            raise ValidationError('参数enable或password必须提交一个')

        if enable is not None:
            if isinstance(enable, str):
                enable = enable.lower()
                if enable == 'true':
                    enable = True
                elif enable == 'false':
                    enable = False
                else:
                    raise ValidationError('无效的enable参数')

            validated_data['enable'] = enable

        if password is not None:
            password = password.strip()
            if not (6 <= len(password) <= 20):
                raise ValidationError('密码长度必须为6-20个字符')

            validated_data['password'] = password

        return validated_data

    def get_serializer_class(self):
        """
        Return the class to use for the serializer.
        Defaults to using `self.serializer_class`.
        Custom serializer_class
        """
        return Serializer
