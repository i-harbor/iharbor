import time
import json
from urllib.parse import unquote

from django.utils.translation import ugettext_lazy as _
from rest_framework.authentication import BaseAuthentication, get_authorization_header
from rest_framework import exceptions

from .auth_key import urlsafe_base64_decode, generate_token


class AuthKeyAuthentication(BaseAuthentication):
    """
    AuthKey based authentication.

    Clients should authenticate by passing the token key in the "Authorization"
    HTTP header, prepended with the string "evhb-auth ".  For example:

        Authorization: evhb-auth xxx:xxx:xxx
    """

    keyword = 'evhb-auth'
    model = None

    def get_model(self):
        if self.model is not None:
            return self.model
        from users.models import AuthKey
        return AuthKey

    def authenticate(self, request):
        auth = get_authorization_header(request).split()

        if not auth or auth[0].lower() != self.keyword.lower().encode():
            return None

        if len(auth) == 1:
            msg = _('Invalid auth header. No credentials provided.')
            raise exceptions.AuthenticationFailed(msg)
        elif len(auth) > 2:
            msg = _('Invalid auth header. Auth string should not contain spaces.')
            raise exceptions.AuthenticationFailed(msg)

        try:
            auth_key_str = auth[1].decode()
        except UnicodeError:
            msg = _('Invalid auth header. Auth string should not contain invalid characters.')
            raise exceptions.AuthenticationFailed(msg)

        return self.authenticate_credentials(request, auth_key_str)

    def authenticate_credentials(self, request, auth_key_str):
        access_key, token, data_b64 = self.parse_auth_key_string(auth_key_str)

        data = self.parse_data(data_b64) # 解析data
        self.validate_url_path(request, data) # 验证path of url
        self.validate_deadline(data) # 验证有效期
        self.validate_method(request, data) # 验证请求方法

        model = self.get_model()
        try:
            auth_key = model.objects.select_related('user').get(id=access_key)
        except model.DoesNotExist:
            raise exceptions.AuthenticationFailed(_('Invalid access_key.'))

        if not auth_key.user.is_active:
            raise exceptions.AuthenticationFailed(_('User inactive or deleted.'))

        # 是否未激活暂停使用
        if not auth_key.is_key_active():
            raise exceptions.AuthenticationFailed(_('Invalid access_key. Key is inactive and unavailable'))

        # 验证加密token
        if generate_token(data_b64, auth_key.secret_key) != token:
            raise exceptions.AuthenticationFailed(_('Invalid auth header'))

        return (auth_key.user, auth_key) # request.user, request.auth

    def parse_auth_key_string(self, auth_key):
        auth = auth_key.split(':')
        if len(auth) != 3:
            msg = _('Invalid auth header. Auth string should contain 2 colons.')
            raise exceptions.AuthenticationFailed(msg)

        return auth

    def parse_data(self, data_b64):
        '''
        解析data字符串

        :param data_b64: data的base64编码字符串
        :return:
            success: dict
            failed: 抛出认证失败异常
        '''
        try:
            data_json = urlsafe_base64_decode(data_b64)
            data = json.loads(data_json)
        except:
            raise exceptions.AuthenticationFailed(_('Invalid auth header.'))

        if not isinstance(data, dict):
            raise exceptions.AuthenticationFailed(_('Invalid auth header.'))

        return data

    def validate_url_path(self, request, data):
        '''
        验证 访问的url的路径和当前路径是否一致
        '''
        full_path = data.get('path_of_url', None)
        local_url_path = request.get_full_path()
        local_url_path = unquote(local_url_path) # 解码url
        if local_url_path != full_path:
            raise exceptions.AuthenticationFailed(_('Invalid auth header. Invalid path of request url.'))

    def validate_deadline(self, data):
        '''
        验证 认证key的有效期
        '''
        deadline = data.get('deadline', 0)
        if int(time.time()) > deadline:
            raise exceptions.AuthenticationFailed(_('Invalid auth header. Now the deadline has passed.'))

    def validate_method(self, request, data):
        '''
        验证 认证key的有效期
        '''
        method = data.get('method', '')
        m = request.method
        if method.upper() != m:
            raise exceptions.AuthenticationFailed(_('Invalid auth header. Request method is different.'))

    def authenticate_header(self, request):
        return self.keyword

