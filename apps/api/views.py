from bson import ObjectId
from collections import OrderedDict
from datetime import datetime

from django.http import StreamingHttpResponse, FileResponse, Http404, QueryDict
from django.utils.http import urlquote
from django.db.models import Q as dQ
from rest_framework import viewsets, status, generics, mixins
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated, IsAdminUser, BasePermission
from rest_framework.schemas import AutoSchema
from rest_framework.compat import coreapi, coreschema
from rest_framework.serializers import Serializer

from buckets.utils import BucketFileManagement
from users.views import send_active_url_email
from utils.storagers import FileStorage, PathParser
from utils.oss.rados_interfaces import CephRadosObject
from .models import User, Bucket
from . import serializers
from . import paginations

# Create your views here.

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

def get_bucket_collection_name_or_response(bucket_name, request):
    '''
    获取存储通对应集合名称，或者Response对象
    :param bucket_name: 存储通名称
    :return: (collection_name, response)
            collection_name=None时，存储通不存在，response有效；
            collection_name!=''时，存储通存在，response=None；
    '''
    bucket = Bucket.get_bucket_by_name(bucket_name)
    if not bucket:
        response = Response(data={'code': 404, 'code_text': 'bucket_name参数有误，存储通不存在'}, status=status.HTTP_404_NOT_FOUND)
        return (None, response)
    if not bucket.check_user_own_bucket(request):
        response = Response(data={'code': 404, 'code_text': 'bucket_name参数有误，存储通不存在'}, status=status.HTTP_404_NOT_FOUND)
        return (None, response)

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
        serializer = self.get_serializer(data=request.data, context={'request': request})
        if not serializer.is_valid(raise_exception=False):
            data = {
                'code': 400,
                'code_text': serializer.errors.get('non_field_errors', '参数验证未通过'),
                'data': serializer.data,
            }
            return Response(data, status=status.HTTP_400_BAD_REQUEST)
        serializer.save()
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

    create:
    创建一个文件对象，并返回文件对象的id：

    	Http Code: 状态码201：无异常时，返回数据：
    	{
            data: 客户端请求时，携带的数据,
            id: 文件id，上传文件块时url中需要,
        }
        Http Code: 状态码400：参数有误时，返回数据：
            对应参数错误信息;

    update:
    通过文件对象绝对路径（以存储桶名开始）分片上传文件对象

        注意：
        文件对象已存在，数据上传会覆盖原数据，文件对象不存在，会自动创建文件对象，并且文件对象的大小只增不减；
        当chunk_offset=0时会被认为一次新文件对象上传，如果文件对象已存在，此时overwrite参数有效，
            overwrite=False时为不覆盖上传，会返回400错误码和已存在同名文件的错误提示。
            overwrite=True时会重置原文件对象大小为0，相当于删除已存在的同名文件对象，创建一个新同名文件对象，


        Http Code: 状态码200：上传成功无异常时，返回数据：
        {
            data: 客户端请求时，携带的参数,不包含数据块；
        }
        Http Code: 状态码400：参数有误时，返回数据：
            对应参数错误信息;
        Http Code: 状态码500
            {
                'code': 500,
                'code_text': '文件块rados写入失败'
            }

    retrieve:
        通过文件对象绝对路径（以存储桶名开始）,下载文件对象,可通过query参数获取文件对象详细信息，或者自定义读取对象数据块

        *注：参数优先级判定顺序：info > chunk_offset && chunk_size
        1. info=true时,返回文件对象详细信息，其他忽略此参数；
        2. chunk_offset && chunk_size 参数校验失败时返回状态码400和对应参数错误信息，无误时，返回bytes数据流
        3. 不带参数或者info无效时，返回整个文件对象；

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
                    name='chunk_offset',
                    required=False,
                    location='query',
                    schema=coreschema.String(description='要读取的文件块在整个文件中的起始位置（bytes偏移量), 类型int'),
                ),
                coreapi.Field(
                    name='chunk_size',
                    required=False,
                    location='query',
                    schema=coreschema.String(description='要读取的文件块的字节大小, 类型int'),
                ),
            ],
            'DELETE': VERSION_METHOD_FEILD + OBJ_PATH_METHOD_FEILD,
            'PUT': VERSION_METHOD_FEILD + OBJ_PATH_METHOD_FEILD,
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

    def update(self, request, *args, **kwargs):
        objpath = kwargs.get(self.lookup_field, '')

        # 对象路径分析
        pp = PathParser(filepath=objpath)
        bucket_name, path, filename = pp.get_bucket_path_and_filename()
        if not bucket_name or not filename:
            return Response(data={'code': 400, 'code_text': 'objpath参数有误'}, status=status.HTTP_400_BAD_REQUEST)

        # 数据验证
        serializer = self.get_serializer(data=request.data)
        if not serializer.is_valid(raise_exception=False):
            return Response({
                'code': 400,
                'code_text': serializer.errors.get('non_field_errors', '参数有误，验证未通过'),
            }, status=status.HTTP_400_BAD_REQUEST)

        # 存储桶验证和获取桶对象mongodb集合名
        collection_name, response = get_bucket_collection_name_or_response(bucket_name, request)
        if not collection_name and response:
            return response

        data = serializer.data
        chunk_offset = data.get('chunk_offset')
        chunk = request.data.get('chunk')
        overwrite = data.get('overwrite')

        obj, created = self.get_file_obj_or_create_or_404(collection_name, path, filename)
        rados = CephRadosObject(str(obj.id))
        if not created: # 对象存在 ，
            if chunk_offset == 0:
                if not overwrite: # 不覆盖
                    return Response({'code': 400, 'code_text': 'objpath参数有误，已存在同名文件'}, status=status.HTTP_400_BAD_REQUEST)
                else:
                    rados.delete()
                    obj.si = 0

        # 存储文件块
        ok, bytes = rados.write(offset=chunk_offset, data_block=chunk.read())
        if ok:
            # 更新文件修改时间
            obj.upt = datetime.utcnow()
            obj.si = max(chunk_offset + chunk.size, obj.si if obj.si else 0)  # 更新文件大小（只增不减）
            try:
                obj.save()
            except:
                pass
        else:
            return Response({'code': 500, 'code_text': '文件块rados写入失败'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(data, status=status.HTTP_200_OK)

    def retrieve(self, request, *args, **kwargs):
        info = request.query_params.get('info', None)
        objpath = kwargs.get(self.lookup_field, '')

        pp = PathParser(filepath=objpath)
        bucket_name, path, filename = pp.get_bucket_path_and_filename()
        if not bucket_name or not filename:
            return Response(data={'code': 400, 'code_text': 'objpath参数有误'}, status=status.HTTP_400_BAD_REQUEST)

        validated_param, valid_response = self.custom_read_param_validate_or_response(request)
        if not validated_param and valid_response:
            return valid_response

        collection_name, response = get_bucket_collection_name_or_response(bucket_name, request)
        if not collection_name and response:
            return response

        fileobj = self.get_file_obj_or_404(collection_name, path, filename)

        # 返回文件对象详细信息
        if info == 'true':
            return self.get_obj_info_response(request=request, fileobj=fileobj, bucket_name=bucket_name, path=path)

        # 自定义读取文件对象
        if validated_param:
            offset = validated_param.get('offset')
            size = validated_param.get('size')
            return self.get_custom_read_obj_response(obj=fileobj, offset=offset, size=size, collection_name=collection_name)

        # 下载整个文件对象
        response = self.get_file_download_response(str(fileobj.id), filename)
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

        collection_name, response = get_bucket_collection_name_or_response(bucket_name, request)
        if not collection_name and response:
            return response

        fileobj = self.get_file_obj_or_404(collection_name, path, filename)
        fileobj.do_soft_delete()
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
        ok, obj = bfm.get_file_exists(file_name=filename)
        if not ok or not obj:
            raise Http404
        return obj

    def get_file_obj_or_create_or_404(self, collection_name, path, filename):
        '''
        获取文件对象, 不存在则创建，其他错误(如对象父路径不存在)会抛404错误

        :param collection_name:
        :param path:
        :param filename:
        :return: (obj, created)
                obj: 对象
                created: 指示对象是否是新创建的，True(是)
        '''
        bfm = BucketFileManagement(path=path, collection_name=collection_name)
        ok, did = bfm.get_cur_dir_id()
        if not ok:
            raise Http404 # 目录路径不存在

        ok, obj = bfm.get_file_exists(file_name=filename)
        if not ok:
            raise Http404

        if not obj:
            # 创建文件对象
            BucketFileClass = bfm.get_bucket_file_class()
            bfinfo = BucketFileClass(na=filename,  # 文件名
                                    fod=True,  # 文件
                                    si=0)  # 文件大小
            # 有父节点
            if did:
                bfinfo.did = ObjectId(did)

            try:
                obj = bfinfo.save()
            except:
                raise Http404
            return obj, True
        return obj, False

    def get_file_download_response(self, file_id, filename):
        '''
        获取文件下载返回对象
        :param file_id: 文件Id, type: str
        :filename: 文件名， type: str
        :return:
            success：http返回对象，type: dict；
            error: None
        '''
        cro = CephRadosObject(file_id)
        file_generator = cro.read_obj_generator
        if not file_generator:
            return None

        filename = urlquote(filename)# 中文文件名需要
        response = FileResponse(file_generator())
        response['Content-Type'] = 'application/octet-stream'  # 注意格式
        response['Content-Disposition'] = f'attachment; filename="{filename}";'  # 注意filename 这个是下载后的名字
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
        chunk_offset = request.query_params.get('chunk_offset', None)
        chunk_size = request.query_params.get('chunk_size', None)

        validated_data = {}
        if chunk_offset is not None and chunk_size is not None:
            try:
                offset = int(chunk_offset)
                size = int(chunk_size)
                if offset < 0 or size < 0:
                    raise Exception()
                validated_data['offset'] = offset
                validated_data['size'] = size
            except:
                response = Response(data={'code': 400, 'code_text': 'chunk_offset或chunk_size参数有误'},
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

    def get_custom_read_obj_response(self, obj, offset, size, collection_name):
        '''
        文件对象自定义读取response
        :param obj: 文件对象
        :param offset: 读起始偏移量
        :param size: 读取大小
        :param collection_name: 对象所在mongodb集合名
        :return: HttpResponse
        '''
        if size == 0:
            chunk = bytes()
        else:
            rados = CephRadosObject(str(obj.id))
            ok, chunk = rados.read(offset=offset, size=size)
            if not ok:
                data = {'code':500, 'code_text': 'server error,文件块读取失败'}
                return Response(data, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # 如果从0读文件就增加一次下载次数
        if offset == 0:
            obj.download_cound_increase()

        reponse = StreamingHttpResponse(chunk, content_type='application/octet-stream', status=status.HTTP_200_OK)
        reponse['evob_chunk_size'] = len(chunk)
        reponse['evob_obj_size'] = obj.si
        return reponse


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

    create:
        创建一个目录

        >>Http Code: 状态码400, 请求参数有误:
            {
                "code": 400,
                "code_text": {
                    "error_text": ["xxxx", ] // 错误列表
                }
            }
        >>Http Code: 状态码201,创建文件夹成功：
            {
                'code': 201,
                'code_text': '创建文件夹成功',
                'data': {},      //请求时提交的数据
                'dir': {}，      //新目录对象信息
            }

    destroy:
        删除一个目录

        >>Http Code: 状态码204,成功删除;
        >>Http Code: 状态码400,参数无效;
            {
                'code': 400,
                'code_text': 'ab_path参数无效'
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
    pagination_class = paginations.BucketFileCursorPagination

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
            'DELETE': BASE_METHOD_FIELDS,
        }
    )

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

        queryset = files
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True, context={'bucket_name': bucket_name, 'dir_path': dir_path})
            data_dict['files'] = serializer.data
            return self.get_paginated_response(data_dict)

        serializer = self.get_serializer(queryset, many=True, context={'bucket_name': bucket_name, 'dir_path': dir_path})

        data_dict['files'] = serializer.data
        return Response(data_dict)

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data, context={'request': request})
        if not serializer.is_valid(raise_exception=False):
            return Response({'code': 400, 'code_text': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)

        validated_data = serializer.validated_data
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
            bfinfo.save()
        except:
            return Response(data={'code': 500, 'code_text': '数据存入数据库错误'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        data = {
            'code': 201,
            'code_text': '创建文件夹成功',
            'data': serializer.data,
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
        else:
            obj.do_soft_delete()

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

