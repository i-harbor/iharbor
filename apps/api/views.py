from django.shortcuts import render
from mongoengine.context_managers import switch_collection
from rest_framework import viewsets, status, generics, mixins
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated, IsAdminUser, BasePermission
from rest_framework.authentication import SessionAuthentication

from buckets.utils import get_collection_name, FileStorage
from .models import User, Bucket, BucketFileInfo
from . import serializers

# Create your views here.

class IsSuperUser(BasePermission):
    '''
    Allows access only to super users.
    '''
    def has_permission(self, request, view):
        return request.user and request.user.is_superuser



class UserViewSet( mixins.CreateModelMixin,
                   mixins.RetrieveModelMixin,
                   mixins.DestroyModelMixin,
                   mixins.ListModelMixin,
                   viewsets.GenericViewSet):
    '''
    用户类视图
    list:
    return user list.

    retrieve：
    return user infomation.

    create:
    create a user
    '''
    queryset = User.objects.all()
    # permission_classes = [IsAuthenticated]

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.data, status=status.HTTP_201_CREATED, )

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
        if self.action in ['list', 'create', 'delete']:
            return [IsSuperUser()]
        return [IsSuperUser()]


class BucketViewSet(mixins.CreateModelMixin,
                   mixins.RetrieveModelMixin,
                   mixins.DestroyModelMixin,
                   mixins.ListModelMixin,
                   viewsets.GenericViewSet):
    '''
    存储桶视图

    list:
    return bucket list.

    retrieve:
    return bucket infomation.

    create:
    create a bucket

    delete:
    delete a bucket
    '''
    queryset = Bucket.objects.all()
    permission_classes = [IsAuthenticated]
    # serializer_class = serializers.BucketCreateSerializer

    def list(self, request, *args, **kwargs):
        if IsSuperUser().has_permission(request, view=None):
            pass # superuser return all
        else:
            self.queryset = Bucket.objects.filter(user=request.user).all() # user's own

        return super(BucketViewSet, self).list(request, *args, **kwargs)

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data, context={'request': request})
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.data, status=status.HTTP_201_CREATED)


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


class UploadFileViewSet(viewsets.GenericViewSet):
    '''
    上传文件视图集

    create:
    文件上传请求，服务器会生成一条文件记录，并返回文件上传的方式：
    	Http Code: 状态码201：无异常时，返回数据：
    	{
            data: 客户端请求时，携带的数据,
            id: 文件id，上传文件块时url中需要,
            chunks:文件分片数量,
            chunk_size:每个分片的大小；如20*1024*1024(20Mb)；最后一个分片除外,
            chunk_start:上传分片起始编号，如上传一个新文件时，从0开始；断点续传时，指示续传的起始分片编号,
        }
        Http Code: 状态码400：参数有误时，返回数据：
            对应参数错误信息;

    update:
    文件块上传
        Http Code: 状态码201：上传成功无异常时，返回数据：
        {
            data: 客户端请求时，携带的数据,
            chunk_id: 上传成功的分片编号,
            upload_status: 上传文件的状态，上传中(False)，上传完成(True),
        }
        Http Code: 状态码400：参数有误时，返回数据：
            对应参数错误信息;

    destroy:
    通过文件id,删除一个文件，或者取消上传一个文件
    '''
    queryset = {}
    permission_classes = [IsAuthenticated]
    # serializer_class = serializers.BucketCreateSerializer

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.response_data, status=status.HTTP_201_CREATED)


    def update(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        return Response(serializer.data, status=status.HTTP_200_OK)


    def get_serializer_class(self):
        """
        Return the class to use for the serializer.
        Defaults to using `self.serializer_class`.
        Custom serializer_class
        """
        if self.action == 'update':
            return serializers.ChunkedUploadUpdateSerializer
        elif self.action == 'create':
            return serializers.ChunkedUploadCreateSerializer
        return serializers.ChunkedUploadUpdateSerializer

    def get_serializer_context(self):
        """
        Extra context provided to the serializer class.
        """
        context = super(UploadFileViewSet, self).get_serializer_context()
        context['kwargs'] = self.kwargs
        return context


class DeleteFileViewSet(viewsets.GenericViewSet):
    '''
    删除或者取消上传文件视图集

    create:
    通过文件id,删除一个文件，或者取消上传一个文件：
    	Http Code: 状态码201：无异常时，返回数据：
    	{
            data: 客户端请求时，携带的数据,
        }
        Http Code: 状态码400：参数有误时，返回数据：
            对应参数错误信息;
    '''
    queryset = []
    permission_classes = [IsAuthenticated]
    serializer_class = serializers.FileDeleteSerializer

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data, context={'request': request})
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.response_data, status=status.HTTP_201_CREATED)


class DownloadFileViewSet(viewsets.GenericViewSet):
    '''
    下载文件视图集

    create:
    通过文件id,删除一个文件，或者取消上传一个文件：
    	Http Code: 状态码200：无异常时，返回数据：
    	{
            data: 客户端请求时，携带的数据,
            chunk: 文件快数据
            chunk_size: 文件块大小
            file_size: 文件总大小
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

        # 存储文件块
        fstorage = FileStorage(str(bfi.id))
        chunk = fstorage.read(chunk_size, offset=chunk_offset)

        response_data = {'data': serializer.data}
        if isinstance(chunk, bytes):
            response_data['chunk'] = chunk # 文件快数据
            response_data['chunk_size'] = chunk.count() # 文件块大小
            response_data['file_size'] = bfi.si # 文件大小

            # 如果从0读文件就增加一次下载次数
            if chunk_offset == 0:
                bfi.dlc = 1 + bfi.dlc if bfi.dlc else 0 # 下载次数+1
            bfi.save()

        return Response(response_data, status=status.HTTP_200_OK)









