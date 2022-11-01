from django.conf import settings
from django.utils import timezone
from django.http import Http404
from django.core.exceptions import PermissionDenied
from rest_framework.viewsets import GenericViewSet
from rest_framework import status
from rest_framework.views import set_rollback
from rest_framework.response import Response
from rest_framework.exceptions import (APIException, NotAuthenticated, AuthenticationFailed)

from . import exceptions
from .renders import CommonXMLRenderer
from .auth import S3V4Authentication


def exception_handler(exc, context):
    """
    Returns the response that should be used for any given exception.

    By default we handle the REST framework `APIException`, and also
    Django's built-in `Http404` and `PermissionDenied` exceptions.

    Any unhandled exceptions may return `None`, which will cause a 500 error
    to be raised.
    """
    if isinstance(exc, exceptions.S3Error):
        set_rollback()
        return Response(exc.err_data(), status=exc.status_code)

    if isinstance(exc, Http404):
        exc = exceptions.S3NotFound()
    elif isinstance(exc, PermissionDenied):
        exc = exceptions.S3AccessDenied()
    elif isinstance(exc, (NotAuthenticated, AuthenticationFailed)):
        exc = exceptions.S3AccessDenied()
    elif isinstance(exc, APIException):
        if isinstance(exc.detail, (list, dict)):
            data = exc.detail
        else:
            data = {'detail': exc.detail}

        exc = exceptions.S3Error(message=str(data), status_code=exc.status_code, code=exc.default_code)
    else:
        return None

    set_rollback()
    return Response(exc.err_data(), status=exc.status_code)


class S3CustomGenericViewSet(GenericViewSet):
    """
    自定义GenericViewSet类，重写get_serializer方法，以通过context参数传递自定义参数
    """
    authentication_classes = [S3V4Authentication]

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
        super().perform_authentication(request)

        # 用户最后活跃日期
        user = request.user
        if user.id:
            try:
                date = timezone.now().date()
                if user.last_active < date:
                    user.last_active = date
                    user.save(update_fields=['last_active'])
            except:
                pass

    @staticmethod
    def get_bucket_name(request):
        """
        从域名host中取bucket name

        :return: str
            bucket name     # BucketName.SERVER_HTTP_HOST_NAME
            ''              # SERVER_HTTP_HOST_NAME or other
        """
        main_hosts = getattr(settings, 'SERVER_HTTP_HOST_NAME', ['s3.obs.cstcloud.cn'])
        host = request.get_host()
        for main_host in main_hosts:
            if host.endswith('.' + main_host):
                bucket_name, _ = host.split('.', maxsplit=1)
                return bucket_name

        return ''

    @staticmethod
    def get_s3_obj_key(request):
        """
        从url path中获取对象key

        :return: str
        :raises: S3KeyTooLongError
        """
        s3_key = request.path
        s3_key = s3_key.lstrip('/')
        if len(s3_key) > 1024:
            raise exceptions.S3KeyTooLongError()

        return s3_key

    def get_obj_path_name(self, request):
        """
        获取对象路径

        :return: str
        :raises: S3KeyTooLongError
        """
        key = self.get_s3_obj_key(request)
        return key.strip('/')

    @staticmethod
    def set_renderer(request, renderer):
        """
        设置渲染器

        :param request: 请求对象
        :param renderer: 渲染器对象
        :return:
        """
        request.accepted_renderer = renderer
        request.accepted_media_type = renderer.media_type

    def handle_exception(self, exc):
        """
        Handle any exception that occurs, by returning an appropriate response,
        or re-raising the error.
        """
        if isinstance(exc, (NotAuthenticated,
                            AuthenticationFailed)):
            # WWW-Authenticate header for 401 responses, else coerce to 403
            auth_header = self.get_authenticate_header(self.request)

            if auth_header:
                exc.auth_header = auth_header
            else:
                exc.status_code = status.HTTP_403_FORBIDDEN

        # exception_handler = self.get_exception_handler()

        context = self.get_exception_handler_context()
        response = exception_handler(exc, context)

        if response is None:
            self.raise_uncaught_exception(exc)

        response.exception = True
        self.set_renderer(self.request, CommonXMLRenderer(root_tag_name='Error'))
        return response

    def exception_response(self, request, exc):
        """
        异常回复

        :param request:
        :param exc: S3Error()
        :return: Response()
        """
        self.set_renderer(request, CommonXMLRenderer(root_tag_name='Error'))  # xml渲染器
        return Response(data=exc.err_data(), status=exc.status_code)

    def head(self, request, *args, **kwargs):
        """
        Default handler method for HTTP 'HEAD' request.
        """
        return self.exception_response(request, exceptions.S3MethodNotAllowed())

    def finalize_response(self, request, response, *args, **kwargs):
        response['Server'] = 'iHarborS3'
        return super().finalize_response(request=request, response=response, *args, **kwargs)
