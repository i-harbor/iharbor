import jwt
from django.contrib.auth import get_user_model
from django.utils.translation import ugettext as _
from rest_framework_jwt.settings import api_settings
from rest_framework import exceptions

jwt_payload_handler = api_settings.JWT_PAYLOAD_HANDLER
jwt_encode_handler = api_settings.JWT_ENCODE_HANDLER
jwt_decode_handler = api_settings.JWT_DECODE_HANDLER
jwt_get_username_from_payload = api_settings.JWT_PAYLOAD_GET_USERNAME_HANDLER


class JWTokenTool():
    '''
    JWT token
    '''
    def obtain_one_jwt_token(self, user):
        '''
        获取一个JWT token字符串
        :param user: 用户对象
        :return:
            正常：token，type:string
            错误：None
        '''
        User = get_user_model()
        if not isinstance(user, User):
            return None
        payload = jwt_payload_handler(user)
        token = jwt_encode_handler(payload)
        return token

    def authenticate(self, request):
        """
        Returns a two-tuple of `User` and token if a valid signature has been
        supplied using JWT-based authentication.  Otherwise returns `None`.
        """
        jwt_value = self.get_jwt_value(request)
        if jwt_value is None:
            return None

        return self.authenticate_jwt(jwt_value)

    def authenticate_jwt(self, jwt_value):
        '''
        jwt身份验证
        :param jwt_value:
        :return: (user, jwt_value)
        '''
        try:
            payload = jwt_decode_handler(jwt_value)
        except jwt.ExpiredSignature:
            msg = _('Signature has expired.')
            raise exceptions.AuthenticationFailed(msg)
        except jwt.DecodeError:
            msg = _('Error decoding signature.')
            raise exceptions.AuthenticationFailed(msg)
        except jwt.InvalidTokenError:
            raise exceptions.AuthenticationFailed()

        user = self.authenticate_credentials(payload)

        return (user, jwt_value)

    def authenticate_credentials(self, payload):
        """
        Returns an active user that matches the payload's user id and email.
        """
        User = get_user_model()
        username = jwt_get_username_from_payload(payload)

        if not username:
            msg = _('Invalid payload.')
            raise exceptions.AuthenticationFailed(msg)

        try:
            user = User.objects.get_by_natural_key(username)
        except User.DoesNotExist:
            msg = _('Invalid signature.')
            raise exceptions.AuthenticationFailed(msg)

        if not user.is_active:
            msg = _('User account is disabled.')
            raise exceptions.AuthenticationFailed(msg)

        return user

    def get_jwt_value(self, request):
        jwt = request.GET.get('jwt')
        return jwt if jwt else None
