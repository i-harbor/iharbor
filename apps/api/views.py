from django.http import StreamingHttpResponse, FileResponse, Http404, QueryDict
from django.utils.http import urlquote
from django.db.models import Q as dQ
from mongoengine.context_managers import switch_collection
from rest_framework import viewsets, status, generics, mixins
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated, IsAdminUser, BasePermission
from rest_framework.schemas import AutoSchema
from rest_framework.compat import coreapi, coreschema
from rest_framework.serializers import Serializer

from buckets.utils import BucketFileManagement
from utils.storagers import FileStorage, PathParser
from .models import User, Bucket, BucketFileInfo
from . import serializers
from utils.oss.rados_interfaces import CephRadosObject

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
        serializer.save()
        return Response(serializer.validated_data, status=status.HTTP_201_CREATED, )

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


class BucketViewSet(mixins.RetrieveModelMixin,
                   viewsets.GenericViewSet):
    '''
    存储桶视图

    list:
    获取存储桶列表

    retrieve:
    获取一个存储桶详细信息

    create:
    创建一个新的存储桶

    delete:
    删除一个存储桶
    '''
    queryset = Bucket.objects.filter(soft_delete=False).all()
    permission_classes = [IsAuthenticated]
    # serializer_class = serializers.BucketCreateSerializer

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
            ]
        }
    )

    def list(self, request, *args, **kwargs):
        self.queryset = Bucket.objects.filter(dQ(user=request.user) & dQ(soft_delete=False)).all() # user's own

        queryset = self.filter_queryset(self.get_queryset())
        serializer = self.get_serializer(queryset, many=True)
        data = {
            'buckets': serializer.data,
        }
        return Response(data)

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data, context={'request': request})
        if not serializer.is_valid(raise_exception=False):
            data = {
                'code': 400,
                'code_text': serializer.errors['non_field_errors'],
                'data': serializer.data,
            }
            return Response(data, status=status.HTTP_201_CREATED)
        serializer.save()
        data = {
            'code': 200,
            'code_text': '创建成功',
            'data': serializer.data,
            'bucket': serializers.BucketSerializer(serializer.instance).data
        }
        return Response(data, status=status.HTTP_201_CREATED)

    def destroy(self, request, *args, **kwargs):
        id = kwargs.get(self.lookup_field, None)
        ids = request.POST.getlist('ids')
        if id and id not in ids:
            ids.append(id)
        if ids:
            buckets = Bucket.objects.filter(id__in=ids)
            if not buckets.exists():
                return Response(data={'code': 404, 'code_text': '未找到要删除的存储桶'}, status=status.HTTP_404_NOT_FOUND)
            for bucket in buckets:
                # 只删除用户自己的buckets
                if bucket.user.id == request.user.id:
                    bucket.do_soft_delete()  # 软删除

            return Response(data={'code': 200, 'code_text': '存储桶删除成功'}, status=status.HTTP_200_OK)

        return Response(data={'code': 404, 'code_text': '存储桶id为空'}, status=status.HTTP_404_NOT_FOUND)


    def get_serializer_class(self):
        """
        Return the class to use for the serializer.
        Defaults to using `self.serializer_class`.
        Custom serializer_class
        """
        if self.action in ['create', 'delete']:
            return serializers.BucketCreateSerializer

        return serializers.BucketSerializer

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
    通过对象ID分片上传文件对象
        Http Code: 状态码200：上传成功无异常时，返回数据：
        {
            data: 客户端请求时，携带的参数,不包含数据块；
        }
        Http Code: 状态码400：参数有误时，返回数据：
            对应参数错误信息;

    retrieve:
        通过文件绝对路径（以存储桶名开始）,下载文件对象或文件对象详细信息；
    	Http Code: 状态码200：无异常时,
    	    query参数info=true时返回文件对象详细信息，否则返回bytes数据流；
        Http Code: 状态码400：文件路径参数有误：对应参数错误信息;
        Http Code: 状态码404：找不到资源;
        Http Code: 状态码500：服务器内部错误;

    destroy:
        通过文件绝对路径（以存储桶名开始）,删除文件对象；
    	Http Code: 状态码204：删除成功；
        Http Code: 状态码400：文件路径参数有误：对应参数错误信息;
        Http Code: 状态码404：找不到资源;
        Http Code: 状态码500：服务器内部错误;

    '''
    queryset = {}
    permission_classes = [IsAuthenticated]
    lookup_field = 'objpath'
    lookup_value_regex = '.+'

    # api docs
    BASE_METHOD_FEILD = [
        coreapi.Field(
            name='version',
            required=True,
            location='path',
            schema=coreschema.String(description='API版本（v1, v2）')
        ),
    ]

    schema = CustomAutoSchema(
        manual_fields={
            'GET': BASE_METHOD_FEILD + [
                coreapi.Field(
                    name='objpath',
                    required=True,
                    location='path',
                    schema=coreschema.String(description='以存储桶名称开头的文件对象绝对路径，类型String'),
                ),
                coreapi.Field(
                    name='info',
                    required=False,
                    location='query',
                    schema=coreschema.String(description='可选参数，info=true时返回文件对象详细信息，不返回文件对象数据，其他值忽略，类型boolean'),
                )
            ],
            'DELETE': BASE_METHOD_FEILD + [
                coreapi.Field(
                    name='objpath',
                    required=True,
                    location='path',
                    schema=coreschema.String(description='以存储桶名称开头的文件对象绝对路径，类型String'),
                )
            ],
            'PUT': BASE_METHOD_FEILD + [
                coreapi.Field(
                    name='objpath',
                    required=True,
                    location='path',
                    schema=coreschema.String(description='ObjectID，post请求创建文件对象返回的对象ID,类型String')
                )
            ],
            'POST': BASE_METHOD_FEILD
        }
    )

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.response_data, status=status.HTTP_201_CREATED)

    def update(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def retrieve(self, request, *args, **kwargs):
        info = request.query_params.get('info', None)
        filepath = kwargs.get(self.lookup_field, '')
        bucket_name, path, filename = PathParser(filepath=filepath).get_bucket_path_and_filename()
        if not bucket_name or not filename:
            return Response(data={'code': 400, 'code_text': 'filepath参数有误'}, status=status.HTTP_400_BAD_REQUEST)

        collection_name, response = get_bucket_collection_name_or_response(bucket_name, request)
        if not collection_name and response:
            return response

        fileobj = self.get_file_obj_or_404(collection_name, path, filename)

        # 返回文件对象详细信息
        if info == 'true':
            bfm = BucketFileManagement(path=path)
            serializer = serializers.DirectoryListSerializer(fileobj, context={'request': request,
                                                                               'bucket_name': bucket_name,
                                                                               'dir_path': path})
            return Response(data={
                                'code': 200,
                                'bucket_name': bucket_name,
                                'dir_path': path,
                                'obj': serializer.data,
                                'breadcrumb': bfm.get_dir_link_paths()
                            })

        # 增加一次下载次数
        with switch_collection(BucketFileInfo, collection_name):
            fileobj.dlc = (fileobj.dlc or 0) + 1  # 下载次数+1
            fileobj.save()

        response = self.get_file_download_response(str(fileobj.id), filename)
        if not response:
            return Response(data={'code': 500, 'code_text': '服务器发生错误，获取文件返回对象错误'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        return response

    def destroy(self, request, *args, **kwargs):
        filepath = kwargs.get(self.lookup_field, '')
        bucket_name, path, filename = PathParser(filepath=filepath).get_bucket_path_and_filename()
        if not bucket_name or not filename:
            return Response(data={'code': 400, 'code_text': 'filepath参数有误'}, status=status.HTTP_400_BAD_REQUEST)

        collection_name, response = get_bucket_collection_name_or_response(bucket_name, request)
        if not collection_name and response:
            return response

        fileobj = self.get_file_obj_or_404(collection_name, path, filename)
        with switch_collection(BucketFileInfo, collection_name):
            fileobj.do_soft_delete()
        return Response(status=status.HTTP_204_NO_CONTENT)


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
        获取文件对象信息
        """
        bfm = BucketFileManagement(path=path)
        with switch_collection(BucketFileInfo, collection_name):
            ok, obj = bfm.get_file_exists(file_name=filename)
            if not ok:
                return None
            if not obj:
                raise Http404
            return obj

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
        response['Content-Disposition'] = f'attachment; filename="{filename}"; filename*=utf-8 ${filename}'  # 注意filename 这个是下载后的名字
        return response


class DownloadFileViewSet(viewsets.GenericViewSet):
    '''
    分片下载文件数据块视图集

    create:
    通过文件id,自定义读取文件对象数据块；
    	Http Code: 状态码200：无异常时，返回bytes数据流，其他信息通过标头headers传递：
    	{
            evob_request_data: 客户端请求时，携带的数据,
            evob_chunk_size: 返回文件块大小
            evob_obj_size: 文件对象总大小
        }
        Http Code: 状态码400：参数有误时，返回数据：
            对应参数错误信息;
        Http Code: 状态码404：找不到资源;
    '''
    queryset = []
    permission_classes = [IsAuthenticated]
    serializer_class = serializers.FileDownloadSerializer

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data, context={'request': request})
        serializer.is_valid(raise_exception=True)

        validated_data = serializer.validated_data
        id = validated_data.get('id')
        chunk_offset = validated_data.get('chunk_offset')
        chunk_size = validated_data.get('chunk_size')
        collection_name = validated_data.get('_collection_name')

        with switch_collection(BucketFileInfo, collection_name):
            bfi = BucketFileInfo.objects(id=id).first()
            if not bfi:
                return Response({'id': '未找到id对应文件'}, status=status.HTTP_404_NOT_FOUND)

            # 读文件块
            # fstorage = FileStorage(str(bfi.id))
            # chunk = fstorage.read(chunk_size, offset=chunk_offset)
            rados = CephRadosObject(str(bfi.id))
            ok, chunk = rados.read(offset=chunk_offset, size=chunk_size)
            if not ok:
                response_data = {'data': serializer.data}
                response_data['error_text'] = 'server error,文件块读取失败'
                return Response(response_data, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            # 如果从0读文件就增加一次下载次数
            if chunk_offset == 0:
                bfi.dlc = (bfi.dlc or 0) + 1# 下载次数+1
                bfi.save()

        reponse = StreamingHttpResponse(chunk, content_type='application/octet-stream', status=status.HTTP_200_OK)
        reponse['evob_request_data'] = serializer.data
        reponse['evob_chunk_size'] = len(chunk)
        reponse['evob_obj_size'] = bfi.si
        return reponse


class DirectoryViewSet(viewsets.GenericViewSet):
    '''
    目录视图集

    list:
    获取一个目录下的文件和文件夹信息

    create:
    创建一个目录
    	Http Code: 状态码200：无异常时，返回数据：
    	{
            data: 客户端请求时，携带的数据,
        }
        Http Code: 状态码400：参数有误时，返回数据：
        {
            error_text: 对应参数错误信息;
        }

    destroy:
    删除一个目录
    Http Code: 状态码200;
        无异常时，返回数据：{'code': 200, 'code_text': '已成功删除'};
        异常时，返回数据：{'code': 404, 'code_text': '文件不存在'};
    '''
    queryset = []
    permission_classes = [IsAuthenticated]
    lookup_field = 'dir_path'
    lookup_value_regex = '.+'

    # api docs
    schema = CustomAutoSchema(
        manual_fields={
            'GET':[
                coreapi.Field(
                    name='bucket_name',
                    required=True,
                    location='query',
                    schema = coreschema.String(description='存储桶名称'),
                    ),
                coreapi.Field(
                    name='dir_path',
                    required=False,
                    location='query',
                    schema=coreschema.String(description='存储桶下目录路径')
                ),
            ],
            'DELETE': [
                coreapi.Field(
                    name='bucket_name',
                    required=True,
                    location='query',
                    schema=coreschema.String(description='存储桶名称'),
                ),
            ]
        }
    )

    def list(self, request, *args, **kwargs):
        bucket_name = request.query_params.get('bucket_name')
        dir_path = request.query_params.get('dir_path', '')

        collection_name, response = get_bucket_collection_name_or_response(bucket_name, request)
        if not collection_name and response:
            return response

        bfm = BucketFileManagement(path=dir_path)
        with switch_collection(BucketFileInfo, collection_name):
            ok, files = bfm.get_cur_dir_files()
            if not ok:
                return Response({'code': 404, 'code_text': '参数有误，未找到相关记录'})

            queryset = files
            page = self.paginate_queryset(queryset)
            if page is not None:
                serializer = self.get_serializer(page, many=True)
                return self.get_paginated_response(serializer.data)

            serializer = self.get_serializer(queryset, many=True, context={'bucket_name': bucket_name, 'dir_path': dir_path})
            data = {
                'code': 200,
                'files': serializer.data,
                'bucket_name': bucket_name,
                'dir_path': dir_path,
                'breadcrumb': bfm.get_dir_link_paths()
            }
            return Response(data)

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data, context={'request': request})
        if not serializer.is_valid(raise_exception=False):
            return Response({'code': 400, 'code_text': serializer.errors}, status=status.HTTP_200_OK)

        validated_data = serializer.validated_data
        dir_path = validated_data.get('dir_path', '')
        dir_name = validated_data.get('dir_name', '')
        did = validated_data.get('did', None)
        collection_name = validated_data.get('collection_name')

        with switch_collection(BucketFileInfo, collection_name):
            bfm = BucketFileManagement(dir_path)
            dir_path_name = bfm.build_dir_full_name(dir_name)
            bfinfo = BucketFileInfo(na=dir_path_name,  # 目录名
                                    fod=False,  # 目录
                                    )
            # 有父节点
            if did:
                bfinfo.did = did
            bfinfo.save()

        data = {
            'code': 200,
            'code_text': '创建文件夹成功',
            'data': serializer.data,
            'dir': serializers.DirectoryListSerializer(bfinfo).data
        }
        return Response(data, status=status.HTTP_200_OK)

    def destroy(self, request, *args, **kwargs):
        dir_path = kwargs.get(self.lookup_field, '')
        bucket_name = request.query_params.get('bucket_name', '')

        pp = PathParser(filepath=dir_path)
        path, dir_name = pp.get_path_and_filename()
        if not bucket_name or not dir_name:
            return Response(data={'code': 400, 'code_text': 'bucket_name or dir_name不能为空'}, status=status.HTTP_400_BAD_REQUEST)

        collection_name, response = get_bucket_collection_name_or_response(bucket_name, request)
        if not collection_name and response:
            return response

        obj = self.get_dir_object(path, dir_name, collection_name)
        if not obj:
            return Response(data = {'code': 404, 'code_text': '文件不存在'}, status=status.HTTP_404_NOT_FOUND)
        else:
            with switch_collection(BucketFileInfo, collection_name):
                obj.do_soft_delete()
            data = {'code': 200, 'code_text': '已成功删除'}
        return Response(data=data, status=status.HTTP_200_OK)

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
        return serializers.DirectoryListSerializer

    def get_dir_object(self, path, dir_name, collection_name):
        """
        Returns the object the view is displaying.
        """
        bfm = BucketFileManagement(path=path)
        with switch_collection(BucketFileInfo, collection_name):
            ok, obj = bfm.get_dir_exists(dir_name=dir_name)
            if not ok:
                return None
            return obj

