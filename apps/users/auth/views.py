from django.shortcuts import get_object_or_404

from rest_framework.response import Response
from rest_framework.compat import coreapi, coreschema
from rest_framework import viewsets, status
from rest_framework.permissions import IsAuthenticated
from rest_framework.serializers import Serializer

from api.views import CustomAutoSchema
from .serializers import AuthKeyDumpSerializer, AuthKeySerializer
from ..models import AuthKey


class ObtainAuthKey(viewsets.GenericViewSet):
    '''
    Auth Key视图集

    list:
    获取当前用户的所有访问密钥，需要通过身份认证权限(如session认证)

        返回内容：
        {
          "keys": [
            {
              "access_key": "1cc2174a30e511e9a004c800a000655d",
              "secret_key": "0cf91e146740c73a3959b9ab85195e294b0663df",
              "user": "869588058@qq.com",
              "create_time": "2019-02-15 13:46:59",
              "state": true,
              "permission": "可读可写"
            },
            {
                ...
            },
          ]
        }

    partial_update:
    停用或激活访问密钥，需要通过身份认证权限

        此外，可选Path参数,“new”，?new=true用于刷新生成一个新token；

    create:
    创建一个新的访问密钥

        需要通过身份认证权限(如session，basic认证)，或直接提交用户名和密码

    delete:
    删除一个访问密钥

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
            ],
            'GET': common_manual_fields,
            'PATCH': common_manual_fields + [
                coreapi.Field(
                    name="active",
                    required=True,
                    location='query',
                    schema=coreschema.String(description="为true时,激活key；为false时停用key"),
                ),
            ],
            'DELETE': common_manual_fields,
        }
    )

    lookup_field = 'access_key'

    def list(self, request, *args, **kwargs):
        user = request.user
        if user.is_authenticated:
            keys = AuthKey.objects.filter(user=user).all()
            slr = AuthKeyDumpSerializer(keys, many=True)
            return Response({'keys': slr.data})
        return Response({'code': 403, 'code_text': 'You do not have access permissions'}, status=status.HTTP_403_FORBIDDEN)

    def partial_update(self, request, *args, **kwargs):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 403, 'code_text': 'You do not have access permissions'}, status=status.HTTP_403_FORBIDDEN)

        active = request.query_params.get('active', '').lower()
        if active == 'true':
            active = True
        elif active == 'false':
            active = False
        else:
            return Response({'code': 400, 'code_text': "The value of the path parameter 'active' is invalid"}, status=status.HTTP_400_BAD_REQUEST)

        id = kwargs.get(self.lookup_field, '')
        key = get_object_or_404(AuthKey.objects.filter(user=user).all(), id=id)
        if key.state != active:
            key.state = active
            try:
                key.save()
            except:
                return Response({'code': 500, 'code_text': 'Failed to modify key'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({'code': 200, 'code_text': 'The key has been successfully modified'}, status=status.HTTP_200_OK)

    def create(self, request, *args, **kwargs):
        user = request.user
        if not user.is_authenticated:
            serializer = self.get_serializer(data=request.data, context={'request': request})
            if not serializer.is_valid(raise_exception=False):
                return Response({
                    'code': 400,
                    'code_text': serializer.errors.get('non_field_errors', '参数有误，验证未通过'),
                })
            user = serializer.validated_data['user']

        if AuthKey.objects.filter(user=user).count() >= 5:
            return Response({'code': 403, 'code_text': 'The key you can have has reached the upper limit.'}, status=status.HTTP_403_FORBIDDEN)

        key = AuthKey(user=user)
        try:
            key.save()
        except:
            return Response({'code': 500, 'code_text': 'Create key failed'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({'code': 201, 'code_text': 'Create key successfully',
                         'key': AuthKeyDumpSerializer(key).data}, status=status.HTTP_201_CREATED)

    def destroy(self, request, *args, **kwargs):
        # id = kwargs.get(self.lookup_field, None)
        user = request.user
        if user.is_authenticated:
            self.queryset = AuthKey.objects.filter(user=user).all()
            id = kwargs.get(self.lookup_field, '')
            key = get_object_or_404(AuthKey.objects.filter(user=user).all(), id=id)
            try:
                key.delete()
            except:
                return Response({'code': 500, 'code_text': 'Delete failed'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            return Response(status=status.HTTP_204_NO_CONTENT)

        return Response({'code': 403, 'code_text': 'You do not have access permissions'}, status=status.HTTP_403_FORBIDDEN)

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action =='create':
            return []
        return [IsAuthenticated()]

    def get_serializer_class(self):
        '''
        动态加载序列化器
        '''
        if self.action == 'create':
            return AuthKeySerializer

        return Serializer



