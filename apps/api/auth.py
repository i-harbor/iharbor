from rest_framework.authtoken.views import ObtainAuthToken
from rest_framework.authtoken.models import Token
from rest_framework.authtoken.serializers import AuthTokenSerializer
from rest_framework.response import Response
from rest_framework.compat import coreapi, coreschema
from rest_framework.permissions import IsAuthenticated
from rest_framework import status

from .views import CustomAutoSchema
from .serializers import AuthTokenDumpSerializer

class CustomAuthToken(ObtainAuthToken):
    '''
    get:
    获取当前用户的token，需要通过身份认证权限(如session认证)

    put:
    刷新当前用户的token，旧token失效，需要通过身份认证权限

    post:
    身份验证并返回一个token，用于其他API验证身份

    令牌应包含在AuthorizationHTTP标头中。密钥应以字符串文字“Token”为前缀，空格分隔两个字符串。
    例如Authorization: Token 9944b09199c62bcf9418ad846dd0e4bbdfc6ee4b；
    此外，可选Path参数,“new”，?new=true用于刷新生成一个新token；
    '''
    common_manual_fields = [
        coreapi.Field(
            name='version',
            required=True,
            location='path',
            schema=coreschema.String(description='API版本（v1, v2）')
        ),
    ]

    schema = CustomAutoSchema(
        manual_fields={
            'POST': common_manual_fields + [
                coreapi.Field(
                    name="username",
                    required=True,
                    location='form',
                    schema=coreschema.String(
                        title="Username",
                        description="Valid username for authentication",
                    ),
                ),
                coreapi.Field(
                    name="password",
                    required=True,
                    location='form',
                    schema=coreschema.String(
                        title="Password",
                        description="Valid password for authentication",
                    ),
                ),
                coreapi.Field(
                    name="new",
                    required=False,
                    location='query',
                    schema=coreschema.String(description="为true时,生成一个新token"),
                ),
            ],
            'GET': common_manual_fields,
            'PUT': common_manual_fields,
        }
    )
    def get(self, request, *args, **kwargs):
        user = request.user
        if user.is_authenticated:
            token, created = Token.objects.get_or_create(user=user)
            slr = AuthTokenDumpSerializer(token)
            return Response({'token': slr.data})
        return Response({'code': 403, 'code_text': '您没有访问权限'}, status=status.HTTP_403_FORBIDDEN)

    def put(self, request, *args, **kwargs):
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
        return Response({'token': token.key})

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.request.method.upper() in ['POST']:
            return []
        return [IsAuthenticated()]


obtain_auth_token = CustomAuthToken.as_view()



