from django.shortcuts import get_object_or_404

from rest_framework.response import Response
from rest_framework import viewsets, status
from rest_framework.permissions import IsAuthenticated
from rest_framework.serializers import Serializer
from drf_yasg.utils import swagger_auto_schema, no_body
from drf_yasg import openapi

from .serializers import AuthKeyDumpSerializer, AuthKeySerializer
from ..models import AuthKey
from utils.doc import NoPagingAutoSchema


class ObtainAuthKey(viewsets.GenericViewSet):
    '''
    Auth Key视图集
    '''
    lookup_field = 'access_key'

    @swagger_auto_schema(
        auto_schema=NoPagingAutoSchema,
         responses={
             status.HTTP_200_OK:"""
                {
                  "keys": [
                    {
                      "access_key": "1cc2174a30e511e9a004c800a000655d",
                      "secret_key": "0cf91e146740c73a3959b9ab85195e294b0663df",
                      "user": "869588058@qq.com",
                      "create_time": "2019-02-20T13:56:25+08:00",
                      "state": true,
                      "permission": "可读可写"
                    },
                    {
                        ...
                    },
                  ]
                }
             """
         }
    )
    def list(self, request, *args, **kwargs):
        '''
        获取当前用户的所有访问密钥

        获取当前用户的所有访问密钥，需要通过身份认证权限(如session认证)
        '''
        user = request.user
        if user.is_authenticated:
            keys = AuthKey.objects.filter(user=user).all()
            slr = AuthKeyDumpSerializer(keys, many=True)
            return Response({'keys': slr.data})
        return Response({'code': 403, 'code_text': 'You do not have access permissions'}, status=status.HTTP_403_FORBIDDEN)

    @swagger_auto_schema(
        request_body=no_body,
        manual_parameters=[
            openapi.Parameter(
                name='active', in_=openapi.IN_QUERY,
                type=openapi.TYPE_BOOLEAN,
                description="为true时,激活key；为false时停用key",
                required=True
            ),
        ],
        responses={
            status.HTTP_200_OK: """
                {
                    'code': 200, 
                    'code_text': 'The key has been successfully modified'
                }
            """,
            status.HTTP_403_FORBIDDEN: """
                {
                    'code': 403, 
                    'code_text': 'You do not have access permissions'
                }
            """,
            status.HTTP_400_BAD_REQUEST: """
                {
                    'code': 400, 
                    'code_text': "The value of the path parameter 'active' is invalid"
                }
            """
        }
    )
    def partial_update(self, request, *args, **kwargs):
        '''
        停用或激活访问密钥

        停用或激活访问密钥，需要通过身份认证权限

            参数,“active”，active=true用于激活启用访问密钥，active=false用于停用访问密钥；
        '''
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

    @swagger_auto_schema(
        responses={
            status.HTTP_201_CREATED: """
                    {
                        'code': 201,
                        'code_text': 'Create key successfully',
                        'key':{
                          "access_key": "1cc2174a30e511e9a004c800a000655d",
                          "secret_key": "0cf91e146740c73a3959b9ab85195e294b0663df",
                          "user": "869588058@qq.com",
                          "create_time": "2019-02-20T13:56:25+08:00",
                          "state": true,
                          "permission": "可读可写"
                        }
                    }
                 """
        }
    )
    def create(self, request, *args, **kwargs):
        '''
        创建一个新的访问密钥

        需要通过身份认证权限(如session，basic认证)，或直接提交用户名和密码
        '''
        user = request.user
        if not user.is_authenticated:
            serializer = self.get_serializer(data=request.data, context={'request': request})
            if not serializer.is_valid(raise_exception=False):
                return Response({
                    'code': 400,
                    'code_text': serializer.errors.get('non_field_errors', '参数有误，验证未通过'),
                })
            user = serializer.validated_data['user']

        if AuthKey.objects.filter(user=user).count() >= 2:
            return Response({'code': 403, 'code_text': 'The key you can have has reached the upper limit.'}, status=status.HTTP_403_FORBIDDEN)

        key = AuthKey(user=user)
        try:
            key.save()
        except:
            return Response({'code': 500, 'code_text': 'Create key failed'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response({'code': 201, 'code_text': 'Create key successfully',
                         'key': AuthKeyDumpSerializer(key).data}, status=status.HTTP_201_CREATED)

    def destroy(self, request, *args, **kwargs):
        '''
        删除一个访问密钥

        需要通过身份认证权限(如session，basic认证)
        '''
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



