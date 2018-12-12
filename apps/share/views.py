from django.http import FileResponse, Http404
from django.utils.http import urlquote
from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.compat import coreapi, coreschema
from rest_framework.reverse import reverse
from mongoengine.context_managers import switch_collection

from buckets.utils import BucketFileManagement
from buckets.models import Bucket, BucketFileInfo
from api.views import CustomAutoSchema
from utils.storagers import PathParser
from utils.oss.rados_interfaces import CephRadosObject
from . import serializers

# Create your views here.

class ObsViewSet(viewsets.GenericViewSet):
    '''
    分享视图集

    retrieve:
    浏览器端下载文件对象，公共文件对象或当前用户(如果用户登录了)文件对象下载，没有权限下载非公共文件对象或不属于当前用户文件对象

    >>Http Code: 状态码200：
            返回FileResponse对象,bytes数据流；

    >>Http Code: 状态码400：文件路径参数有误：对应参数错误信息;
        {
            'code': 400,
            'code_text': 'xxxx参数有误'
        }
    >>Http Code: 状态码403
        {
            'code': 403,
            'code_text': '您没有访问权限'
        }
    >>Http Code: 状态码404：找不到资源;
    >>Http Code: 状态码500：服务器内部错误;

    '''
    queryset = []
    permission_classes = []
    lookup_field = 'objpath'
    lookup_value_regex = '.+'

    # api docs
    schema = CustomAutoSchema(
        manual_fields={
            'GET':[
                coreapi.Field(
                    name='objpath',
                    required=False,
                    location='query',
                    schema=coreschema.String(description='以存储桶名称开头文件对象绝对路径')
                ),
            ],
        }
    )

    def retrieve(self, request, *args, **kwargs):
        objpath = kwargs.get(self.lookup_field, '')

        pp = PathParser(filepath=objpath)
        bucket_name, path, filename = pp.get_bucket_path_and_filename()
        if not bucket_name or not filename:
            return Response(data={'code': 400, 'code_text': 'objpath参数有误'}, status=status.HTTP_400_BAD_REQUEST)

        bucket = Bucket.get_bucket_by_name(bucket_name)
        if not bucket:
            return Response(data={'code': 404, 'code_text': 'bucket_name参数有误，存储通不存在'},
                                status=status.HTTP_404_NOT_FOUND)

        collection_name = bucket.get_bucket_mongo_collection_name()
        fileobj = self.get_file_obj_or_404(collection_name, path, filename)

        # 文件对象是否属于当前用户 或 文件是否是共享的
        if not bucket.check_user_own_bucket(request) and not fileobj.is_shared_and_in_shared_time():
            return Response(data={'code': 403, 'code_text': '您没有访问权限'}, status=status.HTTP_403_FORBIDDEN)

        # 下载整个文件对象
        response = self.get_file_download_response(str(fileobj.id), filename)
        if not response:
            return Response(data={'code': 500, 'code_text': '服务器发生错误，获取文件返回对象错误'},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # 增加一次下载次数
        fileobj.download_cound_increase(collection_name)
        return response

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

    def get_file_obj_or_404(self, collection_name, path, filename):
        """
        获取文件对象
        """
        bfm = BucketFileManagement(path=path, collection_name=collection_name)
        ok, obj = bfm.get_file_exists(file_name=filename)
        if not ok or not obj:
            raise Http404
        return obj

