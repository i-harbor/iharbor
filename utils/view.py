from django.utils import timezone
from django.core.exceptions import PermissionDenied
from django.http import Http404, HttpResponseRedirect
from django.conf import settings
from rest_framework.schemas import AutoSchema
from rest_framework import viewsets, exceptions as rf_exceptions
from rest_framework.views import set_rollback
from rest_framework.exceptions import APIException
from rest_framework.response import Response

from api import exceptions


def exception_handler(exc, context):
    """
    Returns the response that should be used for any given exception.
    """
    headers = {}

    if isinstance(exc, Http404):
        exc = exceptions.NotFound()
    elif isinstance(exc, PermissionDenied):
        exc = exceptions.AccessDenied()
    elif isinstance(exc, APIException):
        auth_header = getattr(exc, 'auth_header', None)
        if auth_header:
            headers['WWW-Authenticate'] = auth_header
        wait = getattr(exc, 'wait', None)
        if wait:
            headers['Retry-After'] = '%d' % wait

        if isinstance(exc, rf_exceptions.AuthenticationFailed):
            exc = exceptions.AuthenticationFailed(message=str(exc))
        elif isinstance(exc, rf_exceptions.NotAuthenticated):
            exc = exceptions.NotAuthenticated(message=str(exc))
        elif isinstance(exc, rf_exceptions.PermissionDenied):
            exc = exceptions.AccessDenied(message=str(exc))
        elif isinstance(exc, rf_exceptions.MethodNotAllowed):
            exc = exceptions.MethodNotAllowed()
        elif isinstance(exc, rf_exceptions.Throttled):
            exc = exceptions.Throttled(message=str(exc))
        else:
            exc = exceptions.Error(message=str(exc), status_code=exc.status_code)

    if isinstance(exc, exceptions.Error):
        set_rollback()
        return Response(data=exc.err_data(), status=exc.status_code, headers=headers)

    return None


class CustomAutoSchema(AutoSchema):
    """
    自定义Schema
    """
    def get_manual_fields(self, path, method):
        """
        重写方法，为每个方法自定义参数字段, action或method做key
        """
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
    """
    自定义GenericViewSet类，重写get_serializer方法，以通过context参数传递自定义参数
    """
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
            except Exception:
                pass


def set_language_redirect(lang_code: str, next_url: str):
    """
    Redirect to a given URL while setting the chosen language in a cookie.
    """
    response = HttpResponseRedirect(next_url)
    response.set_cookie(
        settings.LANGUAGE_COOKIE_NAME, lang_code,
        max_age=settings.LANGUAGE_COOKIE_AGE,
        path=settings.LANGUAGE_COOKIE_PATH,
        domain=settings.LANGUAGE_COOKIE_DOMAIN,
        secure=settings.LANGUAGE_COOKIE_SECURE,
        httponly=settings.LANGUAGE_COOKIE_HTTPONLY,
        samesite=settings.LANGUAGE_COOKIE_SAMESITE,
    )
    return response
