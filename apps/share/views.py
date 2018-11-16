from django.shortcuts import render
from rest_framework import viewsets, status, generics, mixins
from rest_framework.response import Response
from rest_framework.compat import coreapi, coreschema
from rest_framework.reverse import reverse
from mongoengine.context_managers import switch_collection

from buckets.utils import BucketFileManagement
from buckets.models import Bucket, BucketFileInfo
from api.views import CustomAutoSchema
from utils.storagers import PathParser
from . import serializers

# Create your views here.

class ShareViewSet(viewsets.GenericViewSet):
    '''
    分享视图集

    retrieve:
    获取分享文件信息或文件夹文件列表信息；

    create:
    创建一个分享连接

    destroy:
    删除分享连接；
    Http Code: 状态码200;
        无异常时，返回数据：{'code': 200, 'code_text': '已成功删除'};
        异常时，返回数据：{'code': 404, 'code_text': '文件不存在'};
    '''
    queryset = []
    permission_classes = []
    lookup_field = 'shared_code'
    lookup_value_regex = '.+'

    # api docs
    schema = CustomAutoSchema(
        manual_fields={
            'POST':[
                coreapi.Field(
                    name='path',
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

    def retrieve(self, request, *args, **kwargs):
        bucket_name = request.query_params.get('bucket_name')
        dir_path = request.query_params.get('dir_path', '')

        bucket = Bucket.get_bucket_by_name(bucket_name)
        if not bucket:
            return Response({'code': 404, 'code_text': f'不存在一个名称为“{bucket_name}”的存储桶'})
        collection_name = bucket.get_bucket_mongo_collection_name()

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
                'ajax_upload_url': reverse('api:upload-list', kwargs={'version': 'v1'}),
                'breadcrumb': bfm.get_dir_link_paths()
            }
            return Response(data)

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data, context={'request': request})
        if not serializer.is_valid(raise_exception=False):
            return Response({'code': 400, 'code_text': serializer.errors}, status=status.HTTP_200_OK)

        validated_data = serializer.validated_data
        bucket_name = validated_data.get('bucket_name', '')
        dir_path = validated_data.get('dir_path', '')
        dir_name = validated_data.get('dir_name', '')
        did = validated_data.get('did', None)

        bucket = Bucket.get_bucket_by_name(bucket_name)
        if not bucket:
            return Response({'code': 404, 'code_text': f'不存在一个名称为“{bucket_name}”的存储桶'})
        collection_name = bucket.get_bucket_mongo_collection_name()

        with switch_collection(BucketFileInfo, collection_name):
            bfinfo = BucketFileInfo(na=dir_path + '/' + dir_name if dir_path else dir_name,  # 目录名
                                    fod=False,  # 目录
                                    )
            # 有父节点
            if did:
                bfinfo.did = did
            bfinfo.save()

        data = {
            'code': 200,
            'code_text': '创建分享连接成功',
            'data': serializer.data,
        }
        return Response(data, status=status.HTTP_200_OK)

    def destroy(self, request, *args, **kwargs):
        dir_path = kwargs.get(self.lookup_field, '')
        bucket_name = request.query_params.get('bucket_name', '')

        pp = PathParser(path=dir_path)
        path, dir_name = pp.get_path_and_filename()
        if not bucket_name or not dir_name:
            return Response(data={'code': 400, 'code_text': 'bucket_name or dir_name不能为空'}, status=status.HTTP_400_BAD_REQUEST)

        bucket = Bucket.get_bucket_by_name(bucket_name)
        if not bucket:
            return Response({'code': 404, 'code_text': f'不存在一个名称为“{bucket_name}”的存储桶'})
        collection_name = bucket.get_bucket_mongo_collection_name()

        obj = self.get_dir_object(collection_name, path, dir_path)
        if not obj:
            data = {'code': 404, 'code_text': '文件不存在'}
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
        if self.action in ['create']:
            return serializers.SharedPostSerializer
        return serializers.SharedPostSerializer

    def get_dir_object(self, collection_name, path, dir_name):
        """
        Returns the object the view is displaying.
        """
        bfm = BucketFileManagement(path=path)
        with switch_collection(BucketFileInfo, collection_name):
            ok, obj = bfm.get_dir_exists(dir_name=dir_name)
            if not ok:
                return None
            return obj



