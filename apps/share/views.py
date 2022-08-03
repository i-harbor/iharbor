import re
from collections import OrderedDict

from django.shortcuts import render, redirect
from django.views import View
from django.http import FileResponse, QueryDict
from django.utils.http import urlquote
from django.contrib.auth.models import AnonymousUser
from django.utils import translation
from django.utils.translation import gettext as _
from django.utils.translation import gettext_lazy
from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.authentication import TokenAuthentication
from rest_framework.exceptions import AuthenticationFailed
from rest_framework.serializers import Serializer
from rest_framework.utils.urls import replace_query_param, remove_query_param
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi

from buckets.utils import BucketFileManagement
from api.paginations import BucketFileLimitOffsetPagination
from utils.storagers import PathParser
from utils.jwt_token import JWTokenTool2, InvalidToken
from utils.view import CustomGenericViewSet, set_language_redirect
from utils.time import time_to_gmt, datetime_from_gmt
from . import serializers
from api.harbor import HarborManager
from api.exceptions import HarborError
from api import exceptions
from .forms import SharePasswordForm


LANGUAGE_QUERY_PARAMETER = 'language'


class InvalidError(Exception):
    def __init__(self, code: int, msg: str = 'invalid'):
        self.code = code
        self.msg = msg


class ObsViewSet(viewsets.GenericViewSet):
    """
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

    """
    queryset = []
    permission_classes = []
    lookup_field = 'objpath'
    lookup_value_regex = '.+'

    @swagger_auto_schema(
        operation_summary=gettext_lazy('浏览器端下载文件对象'),
        manual_parameters=[
            openapi.Parameter(
                name='objpath', in_=openapi.IN_PATH,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("以存储桶名称开头文件对象绝对路径"),
                required=True
            ),
            openapi.Parameter(
                name='p', in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("分享密码"),
                required=False
            )
        ],
        responses={
            status.HTTP_200_OK: "Content-Type: application/octet-stream"
        }
    )
    def retrieve(self, request, *args, **kwargs):

        objpath = kwargs.get(self.lookup_field, '')
        pp = PathParser(filepath=objpath)
        bucket_name, obj_path = pp.get_bucket_and_dirpath()

        # 存储桶验证和获取桶对象
        h_manager = HarborManager()
        try:
            bucket, fileobj = h_manager.get_bucket_and_obj(bucket_name=bucket_name, obj_path=obj_path)
        except HarborError as e:
            return Response(data=e.err_data_old(), status=e.status_code)

        if fileobj is None:
            return Response(data={'code': 404, 'code_text': _('文件对象不存在')}, status=status.HTTP_404_NOT_FOUND)

        # 是否有文件对象的访问权限
        try:
            self.has_access_permission(request=request, bucket=bucket, obj=fileobj)
        except InvalidError as e:
            return Response(data={'code': e.code, 'code_text': e.msg}, status=e.code)

        try:
            r = self.if_modified_304(request, obj=fileobj)
            if r is not None:
                return r
        except Exception as e:
            pass

        filesize = fileobj.si
        filename = fileobj.name
        # 是否是断点续传部分读取
        ranges = request.headers.get('range')
        if ranges:
            try:
                offset, end = self.get_offset_and_end(ranges, filesize=filesize)
            except InvalidError as e:
                return Response(data={'code': e.code, 'code_text': e.msg}, status=e.code)

            generator = h_manager._get_obj_generator(bucket=bucket, obj=fileobj, offset=offset, end=end)
            response = FileResponse(generator, status=status.HTTP_206_PARTIAL_CONTENT)
            response['Content-Ranges'] = f'bytes {offset}-{end}/{filesize}'
            response['Content-Length'] = end - offset + 1
        else:
            generator = h_manager._get_obj_generator(bucket=bucket, obj=fileobj)
            response = FileResponse(generator)
            response['Content-Length'] = filesize

            # 增加一次下载次数
            fileobj.download_cound_increase()

        filename = urlquote(filename)  # 中文文件名需要
        response['Accept-Ranges'] = 'bytes'  # 接受类型，支持断点续传
        response['Content-Type'] = 'application/octet-stream'  # 注意格式
        response['Content-Disposition'] = f"attachment;filename*=utf-8''{filename}"  # 注意filename 这个是下载后的名字
        response['Cache-Control'] = 'max-age=20'
        last_modified = time_to_gmt(fileobj.upt)
        if last_modified:
            response['Last-Modified'] = last_modified

        if fileobj.md5:
            response['ETag'] = fileobj.md5

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
        return Serializer

    @staticmethod
    def if_modified_304(request, obj):
        """
        检测对象数据是否修改，if*标头条件是否满足

        :return:
            Response(304)   # 满足if*标头条件，304 not modified
            None            # 数据有修改或请求未携带if*标头
        """
        if_none_match = False
        if_not_modified_since = False

        etag = request.headers.get('If-None-Match', None)
        if etag is None:
            if_none_match = None
        elif etag == obj.md5:
            if_none_match = True

        last_modified = obj.upt
        modified_since = request.headers.get('If-Modified-Since')
        if modified_since:
            modified_since = datetime_from_gmt(modified_since)
            last_modified = last_modified.replace(microsecond=0)
            if last_modified <= modified_since:
                if_not_modified_since = True

        if if_none_match is not False and if_not_modified_since:
            r = Response(status=status.HTTP_304_NOT_MODIFIED)
            r['ETag'] = obj.md5
            last_modified = time_to_gmt(obj.upt)
            if last_modified:
                r['Last-Modified'] = last_modified

            return r

        return None

    def get_offset_and_end(self, h_range: str, filesize: int):
        """
        获取读取开始偏移量和结束偏移量

        :param h_range: range Header
        :param filesize: 对象大小
        :return:
            (offset:int, end:int)

        :raise InvalidError
        """
        start, end = self.parse_header_ranges(h_range)
        # 无法解析header ranges,返回整个对象
        if start is None and end is None:
            raise InvalidError(code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE, msg='Header Ranges is invalid')

        # 读最后end个字节
        if (start is None) and isinstance(end, int):
            offset = max(filesize - end, 0)
            end = filesize - 1
        else:
            offset = start
            if end is not None and isinstance(end, int):
                end = min(end, filesize - 1)
            else:
                end = filesize - 1

        return offset, end

    @staticmethod
    def parse_header_ranges(ranges):
        """
        parse Range header string

        :param ranges: 'bytes={start}-{end}'  下载第M－N字节范围的内容
        :return: (M, N)
            start: int or None
            end: int or None
        """
        m = re.match(r'bytes=(\d*)-(\d*)', ranges)
        if not m:
            return None, None
        items = m.groups()

        start = int(items[0]) if items[0] else None
        end = int(items[1]) if items[1] else None
        if isinstance(start, int) and isinstance(end, int) and start > end:
            return None, None
        return start, end

    def has_access_permission(self, request, bucket, obj):
        """
        当前已认证用户或未认证用户是否有访问对象的权限

        :param request: 请求体对象
        :param bucket: 存储桶对象
        :param obj: 文件对象
        :return:
            True(可访问)
            raise InvalidError  # 不可访问

        :raises: InvalidError
        """
        # 存储桶是否是公有权限
        if bucket.is_public_permission():
            return True

        # 可能通过url传递token的身份权限认证
        self.authentication_url_token(request)

        # 存储桶是否属于当前用户
        if bucket.check_user_own_bucket(request.user):
            return True

        # 对象是否共享的，并且在有效共享事件内
        if not obj.is_shared_and_in_shared_time():
            raise InvalidError(code=status.HTTP_403_FORBIDDEN, msg=_('您没有访问权限'))

        # 是否设置了分享密码
        if obj.has_share_password():
            p = request.query_params.get('p', None)
            if p is None:
                raise InvalidError(code=status.HTTP_403_FORBIDDEN, msg=_('资源设有共享密码访问权限'))
            if not obj.check_share_password(password=p):
                raise InvalidError(code=status.HTTP_401_UNAUTHORIZED, msg= _('共享密码无效'))

        return True

    def authentication_url_token(self, request):
        """
        通过url中可能存在的token进行身份验证
        :param request:
        :return:
        """
        self.authenticate_auth_token(request)
        self.authenticate_jwt_token(request)

    @staticmethod
    def authenticate_auth_token(request):
        """
        auth-token验证
        :param request:
        :return: None
        """
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

    @staticmethod
    def authenticate_jwt_token(request):
        """
        jwt-token验证
        :param request:
        :return: None
        """
        # 已身份认证
        if not isinstance(request.user, AnonymousUser):
            return

        jwt = request.query_params.get('jwt')
        if not jwt:
            return

        authenticator = JWTokenTool2()
        try:
            user = authenticator.verify_jwt_return_user(jwt=jwt)
            request._authenticator = authenticator
            request.user, request.auth = user, jwt
        except (AuthenticationFailed, InvalidToken):
            pass


class ShareDownloadViewSet(CustomGenericViewSet):
    """
    分享下载视图集

    retrieve:
    下载分享的目录下载的文件对象

        * 支持断点续传，通过HTTP头 Range和Content-Range

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

    """
    queryset = []
    permission_classes = []
    lookup_field = 'share_base'
    lookup_value_regex = '.+'

    @swagger_auto_schema(
        operation_summary=gettext_lazy('下载分享的目录下的文件对象'),
        manual_parameters=[
            openapi.Parameter(
                name='share_base', in_=openapi.IN_PATH,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("分享根目录,以存储桶名称开头的目录的绝对路径"),
                required=True
            ),
            openapi.Parameter(
                name='subpath', in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("分享根目录下的对象相对子路径"),
                required=True
            ),
            openapi.Parameter(
                name='p', in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("分享密码"),
                required=False
            )
        ],
        responses={
            status.HTTP_200_OK: "Content-Type: application/octet-stream"
        }
    )
    def retrieve(self, request, *args, **kwargs):
        share_base = kwargs.get(self.lookup_field, '')
        subpath = request.query_params.get('subpath', '')

        if not subpath:
            return Response(data={'code': 400, 'code_text': _('subpath参数无效')}, status=status.HTTP_400_BAD_REQUEST)

        pp = PathParser(filepath=share_base)
        bucket_name, dir_base = pp.get_bucket_and_dirpath()
        if not bucket_name:
            return Response(data={'code': 400, 'code_text': _('分享路径无效')}, status=status.HTTP_400_BAD_REQUEST)

        obj_path = f'{dir_base}/{subpath}' if dir_base else subpath
        # 存储桶验证和获取桶对象
        h_manager = HarborManager()
        try:
            bucket, fileobj = h_manager.get_bucket_and_obj(bucket_name=bucket_name, obj_path=obj_path)
        except HarborError as e:
            return Response(data=e.err_data_old(), status=e.status_code)

        if fileobj is None:
            return Response(data={'code': 404, 'code_text': _('文件对象不存在')}, status=status.HTTP_404_NOT_FOUND)

        # 是否有文件对象的访问权限
        try:
            if not self.has_access_permission(request=request, bucket=bucket, base_dir=dir_base):
                return Response(data={'code': 403, 'code_text': _('您没有访问权限')}, status=status.HTTP_403_FORBIDDEN)
        except InvalidError as e:
            return Response(data={'code': e.code, 'code_text': e.msg}, status=e.code)

        filesize = fileobj.si
        filename = fileobj.name
        # 是否是断点续传部分读取
        ranges = request.headers.get('range')
        if ranges:
            try:
                offset, end = self.get_offset_and_end(ranges, filesize=filesize)
            except InvalidError as e:
                return Response(data={'code': e.code, 'code_text': e.msg}, status=e.code)

            generator = h_manager._get_obj_generator(bucket=bucket, obj=fileobj, offset=offset, end=end)
            response = FileResponse(generator, status=status.HTTP_206_PARTIAL_CONTENT)
            response['Content-Ranges'] = f'bytes {offset}-{end}/{filesize}'
            response['Content-Length'] = end - offset + 1
        else:
            generator = h_manager._get_obj_generator(bucket=bucket, obj=fileobj)
            response = FileResponse(generator)
            response['Content-Length'] = filesize

            # 增加一次下载次数
            fileobj.download_cound_increase()

        filename = urlquote(filename)  # 中文文件名需要
        response['Accept-Ranges'] = 'bytes'  # 接受类型，支持断点续传
        response['Content-Type'] = 'application/octet-stream'  # 注意格式
        response['Content-Disposition'] = f"attachment;filename*=utf-8''{filename}"  # 注意filename 这个是下载后的名字
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
        return Serializer

    def get_offset_and_end(self, h_range: str, filesize: int):
        """
        获取读取开始偏移量和结束偏移量

        :param h_range: range Header
        :param filesize: 对象大小
        :return:
            (offset:int, end:int)

        :raise InvalidError
        """
        start, end = self.parse_header_ranges(h_range)
        # 无法解析header ranges,返回整个对象
        if start is None and end is None:
            InvalidError(code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE, msg='Header Ranges is invalid')

        # 读最后end个字节
        if (start is None) and isinstance(end, int):
            offset = max(filesize - end, 0)
            end = filesize - 1
        else:
            offset = start
            if end is not None and isinstance(end, int):
                end = min(end, filesize - 1)
            else:
                end = filesize - 1

        return offset, end

    @staticmethod
    def parse_header_ranges(ranges):
        """
        parse Range header string

        :param ranges: 'bytes={start}-{end}'  下载第M－N字节范围的内容
        :return: (M, N)
            start: int or None
            end: int or None
        """
        m = re.match(r'bytes=(\d*)-(\d*)', ranges)
        if not m:
            return None, None
        items = m.groups()

        start = int(items[0]) if items[0] else None
        end = int(items[1]) if items[1] else None
        if isinstance(start, int) and isinstance(end, int) and start > end:
            return None, None

        return start, end

    @staticmethod
    def has_access_permission(request, bucket, base_dir: str):
        """
        是否有访问对象的权限

        :param request:
        :param bucket: 存储桶对象
        :param base_dir: 分享根目录
        :return:
            True(可访问)；False（不可访问）

        :raises: InvalidError
        """
        # 存储桶是否是公有权限
        if bucket.is_public_permission():
            return True

        # 分享根目录是存储通
        if not base_dir:
            return False

        bf = BucketFileManagement(collection_name=bucket.get_bucket_table_name())
        try:
            obj = bf.get_obj(path=base_dir)
        except Exception as e:
            raise InvalidError(code=400, msg=str(e))

        if (not obj) or (obj and obj.is_file()):
            return False

        # 检查目录读写权限，并且在有效共享事件内
        if not obj.is_shared_and_in_shared_time():
            return False

        # 是否设置了分享密码
        if obj.has_share_password():
            p = request.query_params.get('p', None)
            if (p is None) or (not obj.check_share_password(password=p)):
                raise InvalidError(code=401, msg=_('共享密码无效'))

        return True


class ShareDirViewSet(CustomGenericViewSet):
    """
    list分享目录视图集

    retrieve:
    获取分享目录下的子目录和文件对象列表

        >>Http Code: 状态码200：
        {
          "code": 200,
          "bucket_name": "ddd",
          "subpath": "",
          "share_base": "ddd/occi规范",
          "files": [
            {
              "na": "occi规范/英文",
              "name": "英文",
              "fod": false,
              "did": 22,
              "si": 0,
              "ult": "2021-01-27T11:02:21.658748+08:00",
              "upt": "2021-01-27T11:02:21.658787+08:00",
              "dlc": 0,
              "download_url": ""
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

        >>Http Code: 状态码400：文件路径参数有误：对应参数错误信息;
            {
                'code': 400,
                'code_text': 'xxxx参数有误'
            }
        >>Http Code: 状态码403
            {
                'code': “SharedExpired”,      # NotShared:未分享或者分享已取消；InvalidShareCode：分享密码无效
                'code_text': '分享已过期'
            }
        >>Http Code: 状态码404：找不到资源;
        >>Http Code: 状态码500：服务器内部错误;

    """
    queryset = []
    permission_classes = []
    pagination_class = BucketFileLimitOffsetPagination
    lookup_field = 'share_base'
    lookup_value_regex = '.+'

    @swagger_auto_schema(
        operation_summary=gettext_lazy('获取分享目录下的子目录和文件对象列表'),
        manual_parameters=[
            openapi.Parameter(
                name='share_base', in_=openapi.IN_PATH,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("分享根目录,以存储桶名称开头的目录的绝对路径"),
                required=True
            ),
            openapi.Parameter(
                name='subpath', in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("子目录路径，list此子目录"),
                required=False
            ),
            openapi.Parameter(
                name='p', in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("分享密码"),
                required=False
            )
        ],
        responses={
            status.HTTP_200_OK: ""
        }
    )
    def retrieve(self, request, *args, **kwargs):
        share_base = kwargs.get(self.lookup_field, '')
        subpath = request.query_params.get('subpath', '')
        share_code = request.query_params.get('p', None)

        pp = PathParser(filepath=share_base)
        bucket_name, dir_base = pp.get_bucket_and_dirpath()
        if not bucket_name:
            return Response(data={'code': 'NotFound', 'code_text': _('分享路径无效')}, status=status.HTTP_400_BAD_REQUEST)

        # 存储桶验证和获取桶对象
        h_manager = HarborManager()
        try:
            bucket = h_manager.get_bucket(bucket_name=bucket_name)
            if not bucket:
                return Response(data={'code': 'NotFound', 'code_text': _('存储桶不存在')}, status=status.HTTP_404_NOT_FOUND)

            if dir_base:
                base_obj = h_manager.get_metadata_obj(table_name=bucket.get_bucket_table_name(), path=dir_base)
                if not base_obj:
                    return Response(data={'code': 'NotFound', 'code_text': _('分享根目录不存在')}, status=status.HTTP_404_NOT_FOUND)
            else:
                base_obj = None

            # 是否有文件对象的访问权限
            try:
                self.check_access_permission(bucket=bucket, base_dir_obj=base_obj)
            except exceptions.Error as exc:
                return Response(data=exc.err_data_old(), status=status.HTTP_403_FORBIDDEN)

            # 分享根路径存在，检查分享密码
            if base_obj and base_obj.has_share_password():
                if (share_code is None) or (not base_obj.check_share_password(password=share_code)):
                    return Response(data={'code': 'InvalidShareCode', 'code_text': _('共享密码无效')}, status=403)

            if subpath:     # 是否list子目录
                if dir_base:
                    sub_path = f'{dir_base}/{subpath}'
                else:
                    sub_path = subpath
                sub_obj = h_manager.get_metadata_obj(table_name=bucket.get_bucket_table_name(), path=sub_path)
                if not sub_obj or (sub_obj and sub_obj.is_file()):
                    return Response(data={'code': 'NotFound', 'code_text': _('子目录不存在')}, status=status.HTTP_404_NOT_FOUND)
            else:
                sub_obj = None
        except HarborError as e:
            return Response(data=e.err_data_old(), status=e.status_code)

        if sub_obj:
            list_dir_id = sub_obj.id
        elif base_obj:
            list_dir_id = base_obj.id
        else:
            list_dir_id = BucketFileManagement.ROOT_DIR_ID

        collection_name = bucket.get_bucket_table_name()
        bfm = BucketFileManagement(collection_name=collection_name)
        try:
            files = bfm.get_cur_dir_files(cur_dir_id=list_dir_id)
        except Exception as exc:
            return Response(data={'code': 'NotFound', 'code_text': str(exc)}, status=status.HTTP_404_NOT_FOUND)

        page = self.paginate_queryset(files)
        return self._wrap_list_response(
            bucket_name=bucket_name, share_base=share_base, subpath=subpath,
            share_code=share_code, files=page
        )

    def _wrap_list_response(self, bucket_name: str, share_base: str, subpath: str, share_code: str, files: list):
        data_dict = OrderedDict([
            ('code', 200),
            ('bucket_name', bucket_name),
            ('subpath', subpath),
            ('share_base', share_base),
        ])

        serializer = self.get_serializer(files, many=True, context={
            'share_base': share_base, 'subpath': subpath, 'share_code': share_code})
        data_dict['files'] = serializer.data
        return self.get_paginated_response(data_dict)

    def get_serializer_class(self):
        """
        Return the class to use for the serializer.
        Defaults to using `self.serializer_class`.
        Custom serializer_class
        """
        return serializers.ShareObjInfoSerializer

    @staticmethod
    def check_access_permission(bucket, base_dir_obj):
        """
        是否有访问对象的权限

        :param bucket: 存储桶对象
        :param base_dir_obj: 分享根目录对象, None为分享的存储桶
        :return: None
        :raises: Error
        """
        obj = base_dir_obj
        # 存储桶是否是公有权限
        if bucket.is_public_permission():
            return
        if not obj:
            raise exceptions.NotShared(message=_('分享已取消'))

        if obj.is_file():
            raise exceptions.NotShared(message=_('分享目录不存在'), status_code=404)

        # 检查目录读写权限，并且在有效共享事件内
        obj.check_shared_and_in_shared_time()
        return


class ShareView(View):
    """
    list分享目录视图
    """
    lookup_field = 'share_base'

    def get(self, request, *args, **kwargs):
        """获取分享目录网页"""
        lang_code = request.GET.get(LANGUAGE_QUERY_PARAMETER)
        if lang_code:
            try:
                lang_code = translation.get_supported_language_variant(lang_code)
                translation.activate(lang_code)
                next_url = self.request.build_absolute_uri()
                next_url = remove_query_param(next_url, LANGUAGE_QUERY_PARAMETER)
                return set_language_redirect(lang_code=lang_code, next_url=next_url)
            except LookupError:
                pass

        share_base = kwargs.get(self.lookup_field, '')

        pp = PathParser(filepath=share_base)
        bucket_name, dir_base = pp.get_bucket_and_dirpath()
        if not bucket_name:
            return render(request, 'info.html', context={'code': 400, 'code_text': _('分享路径无效')})

        # 存储桶验证和获取桶对象
        h_manager = HarborManager()
        try:
            bucket = h_manager.get_bucket(bucket_name=bucket_name)
            if not bucket:
                return render(request, 'info.html', context={'code': 404, 'code_text': _('存储桶不存在')})

            if dir_base:
                base_obj = h_manager.get_metadata_obj(table_name=bucket.get_bucket_table_name(), path=dir_base)
                if not base_obj:
                    return render(request, 'info.html', context={'code': 404, 'code_text': _('分享根目录不存在')})
            else:
                base_obj = None

            # 是否有文件对象的访问权限
            self.check_access_permission(bucket=bucket, base_dir_obj=base_obj)
        except exceptions.Error as e:
            return render(request, 'info.html', context=e.err_data_old())

        # 无分享密码
        if (not base_obj) or (not base_obj.has_share_password()):
            return render(request, 'share.html', context={
                'share_base': share_base, 'share_user': bucket.user.username, 'share_code': None})

        # 验证分享密码
        p = request.GET.get('p', None)
        if p:
            data = QueryDict(mutable=True)
            data.setlist('password', [p])
            form = SharePasswordForm(data)  # 模拟post请求数据
            if base_obj.check_share_password(p):
                return render(request, 'share.html', context={
                    'share_base': share_base, 'share_user': bucket.user.username, 'share_code': p})
            else:
                form.is_valid()
                form.add_error('password', error=_('分享密码有误'))
        else:
            form = SharePasswordForm()

        content = {
            'form_title': _('验证分享密码'),
            'submit_text': _('确定'),
            'form': form,
            'share_base': share_base,
            'share_user': bucket.user.username
        }
        return render(request, 'share_form.html', context=content)

    def post(self, request, *args, **kwargs):
        p = request.POST.get('password', '')
        url = self.request.build_absolute_uri()
        url = replace_query_param(url, 'p', p)
        return redirect(to=url)

    @staticmethod
    def check_access_permission(bucket, base_dir_obj):
        """
        是否有访问对象的权限

        :param bucket: 存储桶对象
        :param base_dir_obj: 分享根目录对象, None为分享的存储桶
        :return: None
        :raises: Error
        """
        obj = base_dir_obj
        # 存储桶是否是公有权限
        if bucket.is_public_permission():
            return True
        if not obj:
            raise exceptions.NotShared(message=_('分享已取消'))

        if obj.is_file():
            raise exceptions.NotShared(message=_('分享目录不存在'), status_code=404)

        # 检查目录读写权限，并且在有效共享时间内
        try:
            obj.check_shared_and_in_shared_time()
        except exceptions.NotShared as exc:
            exc.message += _('分享已取消')
            raise exc

        return True
