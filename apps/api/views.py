from django.shortcuts import render
from rest_framework import viewsets, status, generics, mixins
from rest_framework.response import Response

from .models import User, Bucket
from . import serializers

# Create your views here.

class UserViewSet( mixins.CreateModelMixin,
                   mixins.RetrieveModelMixin,
                   # mixins.UpdateModelMixin,
                   mixins.DestroyModelMixin,
                   mixins.ListModelMixin,
                   viewsets.GenericViewSet):
    '''
    用户类视图
    list:
    return user list.

    retrieve：
    return user infomation.

    create:
    create a user
    '''
    queryset = User.objects.all()

    def create(self, request):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.data, status=status.HTTP_201_CREATED, )

    def get_serializer_class(self):
        '''
        动态加载序列化器
        '''
        if self.action == 'create':
            return serializers.UserCreateSerializer

        return serializers.UserDeitalSerializer



class BucketViewSet(mixins.CreateModelMixin,
                   mixins.RetrieveModelMixin,
                   # mixins.UpdateModelMixin,
                   mixins.DestroyModelMixin,
                   mixins.ListModelMixin,
                   viewsets.GenericViewSet):
    '''
    存储桶视图

    list:
    return bucket list.

    retrieve:
    return bucket infomation.

    create:
    create a bucket

    delete:
    delete a bucket
    '''
    queryset = Bucket.objects.all()
    # serializer_class = serializers.BucketCreateSerializer

    def create(self, request, *args, **kwargs):
        '''create a bucket'''
        serializer = self.get_serializer(data=request.data, context={'request': request})
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.data, status=status.HTTP_201_CREATED)


    def get_serializer_class(self):
        """
        Return the class to use for the serializer.
        Defaults to using `self.serializer_class`.
        Custom serializer_class
        """
        if self.action in ['create', 'delete']:
            return serializers.BucketCreateSerializer

        return serializers.BucketSerializer


