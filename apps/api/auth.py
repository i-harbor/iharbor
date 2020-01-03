from rest_framework.authtoken.views import ObtainAuthToken
from rest_framework.authtoken.models import Token
from rest_framework.authtoken.serializers import AuthTokenSerializer
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework import status
from drf_yasg.utils import swagger_auto_schema, no_body
from drf_yasg import openapi
from rest_framework_jwt.views import ObtainJSONWebToken, RefreshJSONWebToken
from rest_framework_jwt.serializers import JSONWebTokenSerializer, RefreshJSONWebTokenSerializer

from .serializers import AuthTokenDumpSerializer

class CustomAuthToken(ObtainAuthToken):
    '''
    get:
    获取当前用户的token

    需要通过身份认证权限(如session认证)

        返回内容：
        {
            "token": {
                "key": "655e0bcc7216d0ccf7d2be7466f94fa241dc32cb",
                "user": "username",
                "created": "2018-12-10 14:04:01"
            }
        }

    post:
    身份验证并返回一个token，用于其他API验证身份

        令牌应包含在AuthorizationHTTP标头中。密钥应以字符串文字“Token”为前缀，空格分隔两个字符串。
        例如Authorization: Token 9944b09199c62bcf9418ad846dd0e4bbdfc6ee4b；
    '''
    def get(self, request, *args, **kwargs):
        user = request.user
        if user.is_authenticated:
            token, created = Token.objects.get_or_create(user=user)
            slr = AuthTokenDumpSerializer(token)
            return Response({'token': slr.data})
        return Response({'code': 403, 'code_text': '您没有访问权限'}, status=status.HTTP_403_FORBIDDEN)

    @swagger_auto_schema(
        responses={
            status.HTTP_200_OK: """
                        {
                          "token": {
                            "key": "a9da4ebad962036ca76ba748907ea71aa7cc502d",
                            "user": "869588058@qq.com",
                            "created": "2019-05-08 13:59:22"
                          }
                        }
                    """
        }
    )
    def put(self, request, *args, **kwargs):
        '''
        刷新当前用户的token

        刷新当前用户的token，旧token失效，需要通过身份认证权限
        '''
        user = request.user
        if user.is_authenticated:
            token, created = Token.objects.get_or_create(user=user)
            if not created:
                token.delete()
                token.key = token.generate_key()
                token.save()
            slr = AuthTokenDumpSerializer(token)
            return Response({'token': slr.data})
        return Response({'code': 403, 'code_text': '您没有访问权限'}, status=status.HTTP_403_FORBIDDEN)

    @swagger_auto_schema(
        request_body=AuthTokenSerializer,
        manual_parameters=[
            openapi.Parameter(
                name='new', in_=openapi.IN_QUERY,
                type=openapi.TYPE_BOOLEAN,
                description="为true时,生成一个新token",
                required=False
            ),
        ],
        responses={
            status.HTTP_200_OK: """
                    {
                      "token": {
                        "key": "a9da4ebad962036ca76ba748907ea71aa7cc502d",
                        "user": "869588058@qq.com",
                        "created": "2019-05-08 13:59:22"
                      }
                    }
                """
        }
    )
    def post(self, request, *args, **kwargs):
        new = request.query_params.get('new', None)
        serializer = self.serializer_class(data=request.data,
                                           context={'request': request})
        serializer.is_valid(raise_exception=True)
        user = serializer.validated_data['user']
        token, created = Token.objects.get_or_create(user=user)
        if new == 'true' and not created:
            token.delete()
            token.key = token.generate_key()
            token.save()

        slr = AuthTokenDumpSerializer(token)
        return Response({'token': slr.data})

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.request.method.upper() in ['POST']:
            return []
        return [IsAuthenticated()]


obtain_auth_token = CustomAuthToken.as_view()


class ObtainJSONWebTokenView(ObtainJSONWebToken):
    @swagger_auto_schema(
        operation_summary='身份认证获取json web token',
        request_body=JSONWebTokenSerializer,
        responses={
            status.HTTP_200_OK: """
                {
                  "token": "eyJ0eXAiOiJKV1QiLC.eyJzc3MDgs3OTUxMzA4fQ.vtGZtCxVGMabXUzuo6_ln_Y"
                }
            """
        }
    )
    def post(self, request, *args, **kwargs):
        '''
        身份认证获取json web token

        '''
        return super().post(request, *args, **kwargs)


class RefreshJSONWebTokenView(RefreshJSONWebToken):
    @swagger_auto_schema(
        operation_summary='刷新json web token',
        request_body=RefreshJSONWebTokenSerializer,
        responses={
            status.HTTP_200_OK: """
                {
                  "token": "eyJ0eXAiOiJKV1QiLC.eyJzc3MDgs3OTUxMzA4fQ.vtGZtCxVGMabXUzuo6_ln_Y"
                }
            """
        }
    )
    def post(self, request, *args, **kwargs):
        '''
        刷新json web token
        '''
        return super().post(request, *args, **kwargs)


obtain_jwt_token = ObtainJSONWebTokenView.as_view()
refresh_jwt_token = RefreshJSONWebTokenView.as_view()