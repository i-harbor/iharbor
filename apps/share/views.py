import re

from django.http import FileResponse, Http404
from django.utils.http import urlquote
from django.contrib.auth.models import AnonymousUser
from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.compat import coreapi, coreschema
from rest_framework.authentication import TokenAuthentication
from rest_framework.exceptions import AuthenticationFailed

from buckets.utils import BucketFileManagement
from buckets.models import Bucket
from api.views import CustomAutoSchema
from utils.storagers import PathParser
from utils.oss import HarborObject
from utils.jwt_token import JWTokenTool
from . import serializers


# Create your views here.

class ObsViewSet(viewsets.GenericViewSet):
    '''
    分享视图集

    retrieve:
    浏览器端下载文件对象，公共文件对象或当前用户(如果用户登录了)文件对象下载，没有权限下载非公共文件对象或不属于当前用户文件对象

        * 支持断点续传，通过HTTP头 Range和Content-Range
        * 跨域访问和安全
            跨域又需要传递token进行权限认证，我们推荐token通过header传递，不推荐在url中传递token,处理不当会增加token泄露等安全问题的风险。
            我们支持token通过url参数传递，auth-token和jwt token两种token对应参数名称分别为token和jwt。出于安全考虑，请不要直接把token明文写到前端<a>标签href属性中，以防token泄密。请动态拼接token到url，比如如下方式：
            $("xxx").on('click', function(e){
                e.preventDefault();
                let token = 从SessionStorage、LocalStorage、内存等等存放token的安全地方获取
                let url = $(this).attr('href') + '?token=' + token; // auth-token
                let url = $(this).attr('href') + '?jwt=' + token;   // jwt token
                window.location.href = url;
            }

        >>Http Code: 状态码200：
                返回FileResponse对象,bytes数据流；

        >>Http Code: 状态码206 Partial Content：
                返回FileResponse对象,bytes数据流；

        >>Http Code: 状态码416 Requested Range Not Satisfiable:
            {
                'code': 416,
                'code_text': 'Header Ranges is invalid'
            }

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
            'retrieve': [
                coreapi.Field(
                    name='objpath',
                    required=False,
                    location='path',
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

        collection_name = bucket.get_bucket_table_name()
        fileobj = self.get_file_obj_or_404(collection_name, path, filename)

        # 是否有文件对象的访问权限
        if not self.has_access_permission(request=request, bucket=bucket, obj=fileobj):
            return Response(data={'code': 403, 'code_text': '您没有访问权限'}, status=status.HTTP_403_FORBIDDEN)

        # 下载整个文件对象
        obj_key = fileobj.get_obj_key(bucket.id)
        response, offset = self.get_file_download_response(request, obj_key, filename, filesize=fileobj.si)

        # 增加一次下载次数
        if offset == 0:
            fileobj.download_cound_increase()
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

    def get_file_download_response(self, request, file_id, filename, filesize):
        '''
        获取文件下载返回对象, 对象起始下载偏移量
        :param file_id: 文件Id, type: str
        :filename: 文件名， type: str
        :return:
            success：Response, offset
        '''
        offset = 0
        ho = HarborObject(file_id, obj_size=filesize)

        # 是否是断点续传部分读取
        ranges = request.headers.get('range')
        if ranges:
            start, end = self.parse_header_ranges(ranges)
            # 无法解析header ranges,返回整个对象
            if start is None and end is None:
                return Response({'code': 416, 'code_text': 'Header Ranges is invalid'},
                                status=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE), offset
            # 读最后end个字节
            if (start is None) and (end is not None):
                offset = max(filesize - end, 0)
                end = filesize - 1
            else:
                offset = start
                if end is None:
                    end = filesize - 1
                else:
                    end = min(end, filesize - 1)

            response = FileResponse(ho.read_obj_generator(offset=offset, end=end), status=status.HTTP_206_PARTIAL_CONTENT)
            response['Content-Ranges'] = f'bytes {offset}-{end}/{filesize}'
            response['Content-Length'] = end - offset + 1
        else:
            response = FileResponse(ho.read_obj_generator(offset=offset))
            response['Content-Length'] = filesize

        filename = urlquote(filename)  # 中文文件名需要
        response['Accept-Ranges'] = 'bytes'  # 接受类型，支持断点续传
        response['Content-Type'] = 'application/octet-stream'  # 注意格式
        response['Content-Disposition'] = f"attachment;filename*=utf-8''{filename}"  # 注意filename 这个是下载后的名字

        return response, offset

    def parse_header_ranges(self, ranges):
        '''
        parse Range header string

        :param ranges: 'bytes={start}-{end}'  下载第M－N字节范围的内容
        :return: (M, N)
            start: int or None
            end: int or None
        '''
        m = re.match(r'bytes=(\d*)-(\d*)', ranges)
        if not m:
            return None, None
        items = m.groups()

        start = int(items[0]) if items[0] else None
        end = int(items[1]) if items[1] else None
        if isinstance(start, int) and isinstance(end, int) and start > end:
            return None, None
        return start, end

    def get_file_obj_or_404(self, collection_name, path, filename):
        """
        获取文件对象
        """
        bfm = BucketFileManagement(path=path, collection_name=collection_name)
        ok, obj = bfm.get_dir_or_obj_exists(name=filename)
        if ok and obj and obj.is_file():
            return obj

        raise Http404

    def has_access_permission(self, request, bucket, obj):
        '''
        当前已认证用户或未认证用户是否有访问对象的权限

        :param request: 请求体对象
        :param bucket: 存储桶对象
        :param obj: 文件对象
        :return: True(可访问)；False（不可访问）
        '''
        # 存储桶是否是公有权限
        if bucket.is_public_permission():
            return True

        # 可能通过url传递token的身份权限认证
        self.authentication_url_token(request)

        # 存储桶是否属于当前用户
        if bucket.check_user_own_bucket(request):
            return True

        # 对象是否共享的，并且在有效共享事件内
        if obj.is_shared_and_in_shared_time():
            return True

        return False

    def authentication_url_token(self, request):
        '''
        通过url中可能存在的token进行身份验证
        :param request:
        :return:
        '''
        self.authenticate_auth_token(request)
        self.authenticate_jwt_token(request)

    def authenticate_auth_token(self, request):
        '''
        auth-token验证
        :param request:
        :return: None
        '''
        # 已身份认证
        if not isinstance(request.user, AnonymousUser):
            return

        key = request.query_params.get('token')
        if not key:
            return

        authenticator = TokenAuthentication()
        try:
            user, token = authenticator.authenticate_credentials(key=key)
            request._authenticator = authenticator
            request.user, request.auth = user, token
        except AuthenticationFailed:
            pass

    def authenticate_jwt_token(self, request):
        '''
        jwt-token验证
        :param request:
        :return: None
        '''
        # 已身份认证
        if not isinstance(request.user, AnonymousUser):
            return

        jwt = request.query_params.get('jwt')
        if not jwt:
            return

        authenticator = JWTokenTool()
        try:
            user, token = authenticator.authenticate_jwt(jwt_value=jwt)
            request._authenticator = authenticator
            request.user, request.auth = user, token
        except AuthenticationFailed:
            pass
