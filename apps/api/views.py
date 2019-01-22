from collections import OrderedDict
from datetime import datetime
import logging

from django.http import StreamingHttpResponse, FileResponse, Http404, QueryDict
from django.utils.http import urlquote
from django.utils import timezone
from django.db.models import Q as dQ
from rest_framework import viewsets, status, generics, mixins
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated, IsAdminUser, BasePermission
from rest_framework.schemas import AutoSchema
from rest_framework.compat import coreapi, coreschema
from rest_framework.serializers import Serializer
from rest_framework.exceptions import ErrorDetail

from buckets.utils import (BucketFileManagement, create_table_for_model_class, delete_table_for_model_class)
from users.views import send_active_url_email
from utils.storagers import FileStorage, PathParser
from utils.oss.rados_interfaces import CephRadosObject
from utils.log.decorators import log_used_time
from .models import User, Bucket
from . import serializers
from . import paginations

# Create your views here.
logger = logging.getLogger('django.request')#这里的日志记录器要和setting中的loggers选项对应，不能随意给参
debug_logger = logging.getLogger('debug')#这里的日志记录器要和setting中的loggers选项对应，不能随意给参

class IsSuperUser(BasePermission):
    '''
    Allows access only to super users.
    '''
    def has_permission(self, request, view):
        return request.user and request.user.is_superuser


class CustomAutoSchema(AutoSchema):
    '''
    自定义Schema
    '''
    def get_manual_fields(self, path, method):
        '''
        重写方法，为每个方法自定义参数字段
        '''
        extra_fields = []
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

@log_used_time(debug_logger, mark_text='select bucket')
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
    if not bucket.check_user_own_bucket(request):
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

    collection_name = bucket.get_bucket_mongo_collection_name()
    return (collection_name, None)


class UserViewSet(mixins.DestroyModelMixin,
                   mixins.ListModelMixin,
                   viewsets.GenericViewSet):
    '''
    用户类视图
    list:
    获取用户列表,需要管理员权限

    retrieve:
    获取一个用户详细信息，需要管理员权限，或当前用户信息

    create:
    注册一个用户

    destroy:
    删除一个用户，需要管理员权限
    '''
    queryset = User.objects.all()

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.save()
        if not send_active_url_email(request._request, user.email, user):
            return Response('激活链接邮件发送失败', status=status.HTTP_500_INTERNAL_SERVER_ERROR)
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

    def get_serializer_class(self):
        '''
        动态加载序列化器
        '''
        if self.action == 'create':
            return serializers.UserCreateSerializer

        return serializers.UserDeitalSerializer

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action in ['list', 'delete']:
            return [IsAuthenticated(), IsSuperUser()]
        elif self.action == 'create':
            return []
        elif self.action == 'retrieve':
            return [IsAuthenticated()]
        return [IsSuperUser()]


class BucketViewSet(viewsets.GenericViewSet):
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
    permission_classes = [IsAuthenticated]
    pagination_class = paginations.BucketsLimitOffsetPagination

    # api docs
    schema = CustomAutoSchema(
        manual_fields={
            'DELETE': [
                coreapi.Field(
                    name='ids',
                    required=False,
                    location='body',
                    schema=coreschema.String(description='存储桶id列表或数组，删除多个存储桶时，通过此参数传递其他存储桶id'),
                ),
            ],
            'PATCH': [
                coreapi.Field(
                    name='public',
                    required=True,
                    location='query',
                    schema=coreschema.Boolean(description='是否分享，用于设置对象公有或私有, true(公开)，false(私有)'),
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
        col_name = bucket.get_bucket_mongo_collection_name()
        bfm = BucketFileManagement(collection_name=col_name)
        model_class = bfm.get_bucket_file_class()
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
                bucket.do_soft_delete()  # 软删除

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
            return Response(data={'code': 404, 'code_text': '未找到要删除的存储桶'}, status=status.HTTP_404_NOT_FOUND)
        for bucket in buckets:
            # 只删除用户自己的buckets
            if bucket.user.id == request.user.id:
                if not bucket.set_permission(public=public):
                    return Response(data={'code': 500, 'code_text': '保存到数据库时错误'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

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
        ids = request.POST.getlist('ids')

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
        if self.action == 'list':
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


class ObjViewSet(viewsets.GenericViewSet):
    '''
    文件对象视图集

    update:
    通过文件对象绝对路径（以存储桶名开始）分片上传文件对象

        说明：
        * 小文件可以作为一个分片上传，大文件请自行分片上传，分片过大可能上传失败，建议分片大小5-10MB；对象上传支持部分上传，
          分片上传数据直接写入对象，已成功上传的分片数据永久有效且不可撤销，请自行记录上传过程以实现断点续传；
        * 文件对象已存在时，数据上传会覆盖原数据，文件对象不存在，会自动创建文件对象，并且文件对象的大小只增不减；
          如果覆盖（已存在同名的对象）上传了一个新文件，新文件的大小小于原同名对象，上传完成后的对象大小仍然保持
          原对象大小（即对象大小只增不减），如果这不符合你的需求，参考以下2种方法：
          (1)先尝试删除对象（对象不存在返回404，成功删除返回204），再上传；
          (2)访问API时，提交reset参数，reset=true时，再保存分片数据前会先调整对象大小（如果对象已存在），为提供reset参
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
        通过文件对象绝对路径（以存储桶名开始）,下载文件对象,可通过query参数获取文件对象详细信息，或者自定义读取对象数据块

        *注：可选参数优先级判定顺序：info > offset && size
        1. 如果携带了info参数，info=true时,返回文件对象详细信息，其他返回400错误；
        2. offset && size(最大20MB，否则400错误) 参数校验失败时返回状态码400和对应参数错误信息，无误时，返回bytes数据流
        3. 不带参数时，返回整个文件对象；

    	>>Http Code: 状态码200：
            * info=true,返回文件对象详细信息：
            {
                'code': 200,
                'bucket_name': 'xxx',   //所在存储桶名称
                'dir_path': 'xxxx',      //所在目录
                'obj': {},              //文件对象详细信息
                'breadcrumb': [[xxx, xxx],]    //路径面包屑
            }
            * 自定义读取时：返回bytes数据流，其他信息通过标头headers传递：
            {
                evhb_chunk_size: 返回文件块大小
                evhb_obj_size: 文件对象总大小
            }
            * 其他,返回FileResponse对象,bytes数据流；

        >>Http Code: 状态码400：文件路径参数有误：对应参数错误信息;
            {
                'code': 400,
                'code_text': 'xxxx参数有误'
            }
        >>Http Code: 状态码404：找不到资源;
        >>Http Code: 状态码500：服务器内部错误;

    destroy:
        通过文件对象绝对路径（以存储桶名开始）,删除文件对象；

        >>Http Code: 状态码204：删除成功，NO_CONTENT；
        >>Http Code: 状态码400：文件路径参数有误：对应参数错误信息;
            {
                'code': 400,
                'code_text': 'objpath参数有误'
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
            schema=coreschema.String(description='以存储桶名称开头的文件对象绝对路径，类型String'),
        ),
    ]

    schema = CustomAutoSchema(
        manual_fields={
            'GET': VERSION_METHOD_FEILD + OBJ_PATH_METHOD_FEILD + [
                coreapi.Field(
                    name='info',
                    required=False,
                    location='query',
                    schema=coreschema.String(description='可选参数，info=true时返回文件对象详细信息，不返回文件对象数据，其他值忽略，类型boolean'),
                ),
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
            'DELETE': VERSION_METHOD_FEILD + OBJ_PATH_METHOD_FEILD,
            'PUT': VERSION_METHOD_FEILD + OBJ_PATH_METHOD_FEILD + [
                coreapi.Field(
                    name='reset',
                    required=False,
                    location='query',
                    schema=coreschema.Boolean(description='reset=true时，如果对象已存在，重置对象大小为0')
                ),
            ],
            'POST': VERSION_METHOD_FEILD,
            'PATCH': VERSION_METHOD_FEILD + [
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

    # def create(self, request, *args, **kwargs):
    #     serializer = self.get_serializer(data=request.data)
    #     serializer.is_valid(raise_exception=True)
    #     serializer.save()
    #     return Response(serializer.response_data, status=status.HTTP_201_CREATED)

    @log_used_time(debug_logger, mark_text='upload chunks')
    def update(self, request, *args, **kwargs):
        objpath = kwargs.get(self.lookup_field, '')

        # 对象路径分析
        pp = PathParser(filepath=objpath)
        bucket_name, path, filename = pp.get_bucket_path_and_filename()
        if not bucket_name or not filename:
            return Response(data={'code': 400, 'code_text': 'objpath参数有误'}, status=status.HTTP_400_BAD_REQUEST)

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

        # 存储桶验证和获取桶对象
        bucket = get_user_own_bucket(bucket_name, request)
        if not bucket:
            return Response(data={'code': 404, 'code_text': 'bucket_name参数有误，存储桶不存在'}, status=status.HTTP_404_NOT_FOUND)

        data = serializer.data
        chunk_offset = data.get('chunk_offset')
        chunk = request.data.get('chunk')
        reset = request.query_params.get('reset', '').lower()

        collection_name = bucket.get_bucket_mongo_collection_name()
        obj, created = self.get_obj_and_check_limit_or_create_or_404(collection_name, path, filename)
        if obj is None:
            return Response({'code': 400, 'code_text': '存储桶内对象数量已达容量上限'}, status=status.HTTP_400_BAD_REQUEST)

        obj_key = obj.get_obj_key(bucket.id)
        rados = CephRadosObject(obj_key, obj_size=obj.si)
        if created is False: # 对象已存在，不是新建的
            if reset == 'true': # 重置对象大小
                response = self.pre_reset_upload(obj=obj, rados=rados)
                if response is not True:
                    return response

        response = self.save_one_chunk(obj=obj, rados=rados, chunk_offset=chunk_offset, chunk=chunk)
        if response is not True:
            # 如果对象是新创建的，上传失败删除对象元数据
            if created is True:
                obj.do_delete()
            return response
        data['created'] = created
        return Response(data, status=status.HTTP_200_OK)

    def retrieve(self, request, *args, **kwargs):
        info = request.query_params.get('info', None)
        objpath = kwargs.get(self.lookup_field, '')

        if (info is not None) and (info.lower() != 'true'):
            return Response(data={'code': 400, 'code_text': 'info参数有误'}, status=status.HTTP_400_BAD_REQUEST)

        pp = PathParser(filepath=objpath)
        bucket_name, path, filename = pp.get_bucket_path_and_filename()
        if not bucket_name or not filename:
            return Response(data={'code': 400, 'code_text': 'objpath参数有误'}, status=status.HTTP_400_BAD_REQUEST)

        validated_param, valid_response = self.custom_read_param_validate_or_response(request)
        if not validated_param and valid_response:
            return valid_response

        # 存储桶验证和获取桶对象
        bucket = get_user_own_bucket(bucket_name=bucket_name, request=request)
        if not bucket:
            return Response(data={'code': 404, 'code_text': 'bucket_name参数有误，存储桶不存在'},
                            status=status.HTTP_404_NOT_FOUND)

        collection_name = bucket.get_bucket_mongo_collection_name()
        fileobj = self.get_file_obj_or_404(collection_name, path, filename)

        # 返回文件对象详细信息
        if info:
            return self.get_obj_info_response(request=request, fileobj=fileobj, bucket_name=bucket_name, path=path)

        # 自定义读取文件对象
        if validated_param:
            offset = validated_param.get('offset')
            size = validated_param.get('size')
            return self.get_custom_read_obj_response(obj=fileobj, offset=offset, size=size, bucket_id=bucket.id)

        # 下载整个文件对象
        obj_key = fileobj.get_obj_key(bucket.id)
        response = self.get_file_download_response(obj_key, filename, filesize=fileobj.si)
        if not response:
            return Response(data={'code': 500, 'code_text': '服务器发生错误，获取文件返回对象错误'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # 增加一次下载次数
        fileobj.download_cound_increase()
        return response

    def destroy(self, request, *args, **kwargs):
        objpath = kwargs.get(self.lookup_field, '')
        bucket_name, path, filename = PathParser(filepath=objpath).get_bucket_path_and_filename()
        if not bucket_name or not filename:
            return Response(data={'code': 400, 'code_text': 'objpath参数有误'}, status=status.HTTP_400_BAD_REQUEST)

        # 存储桶验证和获取桶对象
        bucket = get_user_own_bucket(bucket_name=bucket_name, request=request)
        if not bucket:
            return Response(data={'code': 404, 'code_text': 'bucket_name参数有误，存储桶不存在'},
                            status=status.HTTP_404_NOT_FOUND)

        collection_name = bucket.get_bucket_mongo_collection_name()
        fileobj = self.get_file_obj_or_404(collection_name, path, filename)
        # 先删除元数据，后删除rados对象（删除失败恢复元数据）
        if not fileobj.do_delete():
            logger.error('删除对象数据库原数据时错误')
            return Response(data={'code': 500, 'code_text': '对象数据已删除，删除对象数据库原数据时错误'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        obj_key = fileobj.get_obj_key(bucket.id)
        cro = CephRadosObject(obj_key, obj_size=fileobj.si)
        if not cro.delete():
            # 恢复元数据
            fileobj.do_save(force_insert=True) # 仅尝试创建文档，不修改已存在文档
            logger.error('删除rados对象数据时错误')
            return Response(data={'code': 500, 'code_text': '删除对象数据时错误'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(status=status.HTTP_204_NO_CONTENT)

    def partial_update(self, request, *args, **kwargs):
        objpath = kwargs.get(self.lookup_field, '')

        validated_param, valid_response = self.shared_param_validate_or_response(request)
        if not validated_param and valid_response:
            return valid_response
        share = validated_param.get('share')
        days = validated_param.get('days')

        pp = PathParser(filepath=objpath)
        bucket_name, path, filename = pp.get_bucket_path_and_filename()
        if not bucket_name or not filename:
            return Response(data={'code': 400, 'code_text': 'objpath参数有误'}, status=status.HTTP_400_BAD_REQUEST)

        collection_name, response = get_bucket_collection_name_or_response(bucket_name, request)
        if not collection_name and response:
            return response

        fileobj = self.get_file_obj_or_404(collection_name, path, filename)
        if not fileobj.set_shared(sh=share, days=days):
            return Response(data={'code': 500, 'code_text': '更新数据库数据失败'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

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
        if self.action == 'create':
            return serializers.ObjPostSerializer
        elif self.action == 'update':
            return serializers.ObjPutSerializer
        return Serializer

    def get_serializer_context(self):
        """
        Extra context provided to the serializer class.
        """
        context = super(ObjViewSet, self).get_serializer_context()
        context['kwargs'] = self.kwargs
        return context

    def get_file_obj_or_404(self, collection_name, path, filename):
        """
        获取文件对象
        """
        bfm = BucketFileManagement(path=path, collection_name=collection_name)
        obj = bfm.get_file_exists(file_name=filename)
        if not obj:
            raise Http404
        return obj

    def do_bucket_limit_validate(self, bfm:BucketFileManagement):
        '''
        存储桶的限制验证
        :return: True(验证通过); False(未通过)
        '''
        # 存储桶对象和文件夹数量上限验证
        if bfm.get_document_count() >= 10**7:
            return False

        return True

    @log_used_time(debug_logger, mark_text='select obj info')
    def get_obj_and_check_limit_or_create_or_404(self, collection_name, path, filename):
        '''
        获取文件对象, 验证集合文档数量上限，不存在并且验证通过则创建，其他错误(如对象父路径不存在)会抛404错误

        :param collection_name:
        :param path:
        :param filename:
        :return: (obj, created); obj: 对象; created: 指示对象是否是新创建的，True(是)
                (obj, False) # 对象存在
                (obj, True)  # 对象不存在，创建一个新对象
                (None, None) # 集合文档数量已达上限，不允许再创建新的对象
        '''
        bfm = BucketFileManagement(path=path, collection_name=collection_name)

        obj = bfm.get_file_exists(file_name=filename)
        if obj:
            return obj, False

        ok, did = bfm.get_cur_dir_id()
        if not ok:
            raise Http404 # 目录路径不存在

        # 验证集合文档上限
        # if not self.do_bucket_limit_validate(bfm):
        #     return None, None

        # 创建文件对象
        BucketFileClass = bfm.get_bucket_file_class()
        full_filename = bfm.build_dir_full_name(filename)
        bfinfo = BucketFileClass(na=full_filename,  # 文件名
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


    def get_file_download_response(self, file_id, filename, filesize):
        '''
        获取文件下载返回对象
        :param file_id: 文件Id, type: str
        :filename: 文件名， type: str
        :return:
            success：http返回对象，type: dict；
            error: None
        '''
        cro = CephRadosObject(file_id, obj_size=filesize)
        file_generator = cro.read_obj_generator
        if not file_generator:
            return None

        filename = urlquote(filename)# 中文文件名需要
        response = FileResponse(file_generator())
        response['Content-Type'] = 'application/octet-stream'  # 注意格式
        response['Content-Length'] = filesize
        response['Content-Disposition'] = f"attachment;filename*=utf-8''{filename}"  # 注意filename 这个是下载后的名字
        return response

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
            'breadcrumb': PathParser(path).get_path_breadcrumb()
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
        else:
            return None, None
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

    def get_custom_read_obj_response(self, obj, offset, size, bucket_id):
        '''
        文件对象自定义读取response
        :param obj: 文件对象
        :param offset: 读起始偏移量
        :param size: 读取大小
        :param bucket_id: 桶id
        :return: HttpResponse
        '''
        if size == 0:
            chunk = bytes()
        else:
            obj_key = obj.get_obj_key(bucket_id)
            rados = CephRadosObject(obj_key, obj_size=obj.si)
            ok, chunk = rados.read(offset=offset, size=size)
            if not ok:
                data = {'code':500, 'code_text': 'server error,文件块读取失败'}
                return Response(data, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # 如果从0读文件就增加一次下载次数
        if offset == 0:
            obj.download_cound_increase()

        response = StreamingHttpResponse(chunk, status=status.HTTP_200_OK)
        response['Content-Type'] = 'application/octet-stream'  # 注意格式
        response['evob_chunk_size'] = len(chunk)
        response['Content-Length'] = len(chunk)
        response['evob_obj_size'] = obj.si
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
        if not obj.do_save():
            logger.error('修改对象元数据失败')
            return Response({'code': 500, 'code_text': '修改对象元数据失败'},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        if not rados.delete():
            # 恢复元数据
            obj.ult = old_ult
            obj.si = old_size
            obj.do_save()
            logger.error('rados文件对象删除失败')
            return Response({'code': 500, 'code_text': 'rados文件对象删除失败'},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return True

    @log_used_time(debug_logger, 'save_one_chunk')
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
        # 先更新元数据，后写rados数据（如果写入失败，恢复元数据）
        # 更新文件修改时间和对象大小
        old_size = obj.si if obj.si else 0
        old_upt = obj.upt
        obj.upt = timezone.now()
        obj.si = max(chunk_offset + chunk.size, old_size)  # 更新文件大小（只增不减）

        if not obj.do_save():
            logger.error('修改对象元数据失败')
            return Response({'code': 500, 'code_text': '修改对象元数据失败'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # 存储文件块
        # ok, msg = rados.write(offset=chunk_offset, data_block=chunk.read())
        ok, msg = rados.write_file(offset=chunk_offset, file=chunk)
        if not ok:
            obj.si = old_size
            obj.upt = old_upt
            obj.do_save()

            error = '文件块rados写入失败:' + msg
            logger.error(error)
            return Response({'code': 500, 'code_text': error}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return True

    @log_used_time(debug_logger, mark_text='get request.data during upload file')
    def get_data(self, request):
        return request.data


class DirectoryViewSet(viewsets.GenericViewSet):
    '''
    目录视图集

    retrieve:
    获取一个目录下的文件和文件夹信息

        >>Http Code: 状态码200:
            {
                'code': 200,
                'files': [fileobj, fileobj, ...],//文件信息对象列表
                'bucket_name': xxx,             //存储桶名称
                'dir_path': xxx,                //当前目录路径
                'breadcrumb': [[xxx, xxx],],    //路径面包屑
            }
        >>Http Code: 状态码400:
            {
                'code': 400,
                'code_text': 'ab_path参数有误'
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
    lookup_field = 'ab_path'
    lookup_value_regex = '.+'
    pagination_class = paginations.BucketFileLimitOffsetPagination

    # api docs
    BASE_METHOD_FIELDS = [
        coreapi.Field(
            name='version',
            required=True,
            location='path',
            schema=coreschema.String(description='API版本（v1, v2）')
        ),
        coreapi.Field(
            name='ab_path',
            required=True,
            location='path',
            schema=coreschema.String(description='以存储桶名称开头的目录绝对路径')
        ),
    ]
    schema = CustomAutoSchema(
        manual_fields={
            'GET': BASE_METHOD_FIELDS,
            'POST': BASE_METHOD_FIELDS,
            'DELETE': BASE_METHOD_FIELDS,
        }
    )
    @log_used_time(debug_logger, mark_text='get dir files list')
    def retrieve(self, request, *args, **kwargs):
        ab_path = kwargs.get(self.lookup_field, '')
        pp = PathParser(filepath=ab_path)
        bucket_name, dir_path = pp.get_bucket_and_dirpath()
        if not bucket_name:
            return Response(data={'code': 400, 'code_text': 'ab_path参数有误'}, status=status.HTTP_400_BAD_REQUEST)

        collection_name, response = get_bucket_collection_name_or_response(bucket_name, request)
        if not collection_name and response:
            return response

        bfm = BucketFileManagement(path=dir_path, collection_name=collection_name)
        ok, files = bfm.get_cur_dir_files()
        if not ok:
            return Response({'code': 404, 'code_text': '参数有误，未找到相关记录'}, status=status.HTTP_404_NOT_FOUND)

        data_dict = OrderedDict([
            ('code', 200),
            ('bucket_name', bucket_name),
            ('dir_path', dir_path),
            ('breadcrumb', pp.get_path_breadcrumb(dir_path))
        ])

        start_time = datetime.now()

        queryset = files
        page = self.paginate_queryset(queryset)

        debug_logger.debug(f'All used time: {datetime.now() - start_time} get files list page in dir.')

        if page is not None:
            serializer = self.get_serializer(page, many=True, context={'bucket_name': bucket_name, 'dir_path': dir_path})
            data_dict['files'] = serializer.data
            return self.get_paginated_response(data_dict)

        serializer = self.get_serializer(queryset, many=True, context={'bucket_name': bucket_name, 'dir_path': dir_path})

        data_dict['files'] = serializer.data
        return Response(data_dict)

    # def create(self, request, *args, **kwargs):
    #     pass

    def create_detail(self, request, *args, **kwargs):
        validated_data, response = self.post_detail_params_validate_or_response(request=request, kwargs=kwargs)
        if response:
            return response

        bucket_name = validated_data.get('bucket_name', '')
        dir_path = validated_data.get('dir_path', '')
        dir_name = validated_data.get('dir_name', '')
        did = validated_data.get('did', None)
        collection_name = validated_data.get('collection_name')

        bfm = BucketFileManagement(dir_path, collection_name=collection_name)
        dir_path_name = bfm.build_dir_full_name(dir_name)
        BucketFileClass = bfm.get_bucket_file_class()
        bfinfo = BucketFileClass(na=dir_path_name,  # 目录名
                                fod=False,  # 目录
                                )
        # 有父节点
        if did:
            bfinfo.did = did
        try:
            bfinfo.save(force_insert=True) # 仅尝试创建文档，不修改已存在文档
        except:
            return Response(data={'code': 500, 'code_text': '数据存入数据库错误'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        data = {
            'code': 201,
            'code_text': '创建文件夹成功',
            'data': {'dir_name': dir_name, 'bucket_name': bucket_name, 'dir_path': dir_path},
            'dir': serializers.ObjInfoSerializer(bfinfo).data
        }
        return Response(data, status=status.HTTP_201_CREATED)

    def destroy(self, request, *args, **kwargs):
        ab_path = kwargs.get(self.lookup_field, '')
        bucket_name, path, dir_name = PathParser(filepath=ab_path).get_bucket_path_and_dirname()

        if not bucket_name or not dir_name:
            return Response(data={'code': 400, 'code_text': 'ab_path参数无效'}, status=status.HTTP_400_BAD_REQUEST)

        collection_name, response = get_bucket_collection_name_or_response(bucket_name, request)
        if not collection_name and response:
            return response

        obj = self.get_dir_object(path, dir_name, collection_name)
        if not obj:
            return Response(data = {'code': 404, 'code_text': '文件不存在'}, status=status.HTTP_404_NOT_FOUND)

        bfm = BucketFileManagement(collection_name=collection_name)
        if not bfm.dir_is_empty(obj):
            return Response(data={'code': 400, 'code_text': '无法删除非空目录'}, status=status.HTTP_400_BAD_REQUEST)

        if not obj.do_delete():
            return Response(data={'code': 500, 'code_text': '删除数据库元数据错误'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(status=status.HTTP_204_NO_CONTENT)

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

    def get_serializer_class(self):
        """
        Return the class to use for the serializer.
        Defaults to using `self.serializer_class`.
        Custom serializer_class
        """
        if self.action in ['create', 'delete']:
            return serializers.DirectoryCreateSerializer
        elif self.action in ['create_detail']:
            return Serializer
        return serializers.ObjInfoSerializer

    def get_dir_object(self, path, dir_name, collection_name):
        """
        Returns the object the view is displaying.
        """
        bfm = BucketFileManagement(path=path, collection_name=collection_name)
        ok, obj = bfm.get_dir_exists(dir_name=dir_name)
        if not ok:
            return None
        return obj

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

        ab_path = kwargs.get(self.lookup_field, '')
        bucket_name, dir_path, dir_name = PathParser(filepath=ab_path).get_bucket_path_and_dirname()

        if not bucket_name or not dir_name:
            return None, Response({'code': 400, 'code_text': '目录路径参数无效，要同时包含有效的存储桶和目录名称'},
                                  status=status.HTTP_400_BAD_REQUEST)

        if '/' in dir_name:
            return None, Response({'code': 400, 'code_text': 'dir_name不能包含‘/’'}, status=status.HTTP_400_BAD_REQUEST)

        # bucket是否属于当前用户,检测存储桶名称是否存在
        _collection_name, response = get_bucket_collection_name_or_response(bucket_name, request)
        if not _collection_name and response:
            return None, response

        data['collection_name'] = _collection_name

        bfm = BucketFileManagement(path=dir_path, collection_name=_collection_name)
        ok, dir = bfm.get_dir_exists(dir_name=dir_name)
        if not ok:
            return None, Response({'code': 400, 'code_text': '目录路径参数无效，父节点目录不存在'})
        # 目录已存在
        if dir:
            return None, Response({'code': 400, 'code_text': f'"{dir_name}"目录已存在', 'existing': True},
                                  status=status.HTTP_400_BAD_REQUEST)

        data['did'] = bfm.cur_dir_id if bfm.cur_dir_id else bfm.get_cur_dir_id()[-1]
        data['bucket_name'] = bucket_name
        data['dir_path'] = dir_path
        data['dir_name'] = dir_name
        return data, None


