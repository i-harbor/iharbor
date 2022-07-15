import re

from django.utils.translation import gettext_lazy, gettext as _

from rest_framework import status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.serializers import Serializer
from rest_framework.pagination import LimitOffsetPagination

from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi

from utils.view import CustomGenericViewSet
from buckets.models import Bucket, BackupBucket
from api import serializers, permissions

from api import exceptions
from api.views import check_authenticated_or_bucket_token


class BackupNodeViewSet(CustomGenericViewSet):
    """
    备份节点管理
    """
    queryset = {}
    permission_classes = [permissions.IsAuthenticatedOrBucketToken]
    lookup_field = 'id'
    pagination_class = LimitOffsetPagination

    @swagger_auto_schema(
        operation_summary=gettext_lazy('获取当前用户桶的备份信息列表'),
        responses={
            status.HTTP_200_OK: ""
        }
    )
    def list(self, request, *args, **kwargs):
        """
        获取当前用户桶的备份信息列表

            http 200:
            {
              "count": 2,
              "next": null,
              "previous": null,
              "results": [
                {
                  "endpoint_url": "http://obs1.cstcloud.cn/",
                  "bucket_name": "wang",
                  "bucket_token": "02c005c87294908b0a087a074c17ac49450423f3",
                  "backup_num": 2,
                  "remarks": "wang",
                  "id": 4,
                  "created_time": "2022-03-04T17:51:32.254758+08:00",
                  "modified_time": "2022-03-04T17:51:32.254758+08:00",
                  "status": "start",
                  "error": "null",
                  "bucket": {
                    "id": 1,
                    "name": "wang"
                  }
                }
                ...
              ]
            }

            http code 404:
            {
              "code": "Notfound",
              "message": ""
            }
        """
        backup_qs = BackupBucket.objects.filter(bucket__user_id=request.user.id).all()
        if not backup_qs:
            exc = exceptions.NotFound(message=_('资源不存在'))
            return Response(data=exc.err_data(), status=status.HTTP_404_NOT_FOUND)
        queryset = self.filter_queryset(backup_qs)
        page = self.paginate_queryset(queryset)
        serializer = self.get_serializer(page, many=True)
        return self.get_paginated_response(serializer.data)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('获取备份点的详细信息'),
        manual_parameters=[
            openapi.Parameter(
                name='id',
                in_=openapi.IN_PATH,
                type=openapi.TYPE_NUMBER,
                required=True,
                description=gettext_lazy('id为数据表中的id')
            ),
        ],
        responses={
            status.HTTP_200_OK: ""
        }
    )
    def retrieve(self, request, *args, **kwargs):
        """
        获取备份点的详细信息

            http 200:
            {
              "endpoint_url": "http://obs1.cstcloud.cn/",
              "bucket_name": "wang",
              "bucket_token": "02c005187204903b0a087a074c17ac49410423f3",
              "backup_num": 1,
              "remarks": "",
              "id": 1,
              "created_time": "2022-03-04T17:51:32.254758+08:00",
              "modified_time": "2022-03-04T17:51:32.254758+08:00",
              "status": "start",
              "error": "",
              "bucket": {
                "id": 1,
                "name": "wang"
              }
            }

            http code 403 404:
            {
              "code": "xxx",  // NoSuchBucket、AccessDenied
              "message": ""
            }
        """
        backup_id = kwargs.get(self.lookup_field, "")
        # 获取备份点id数据
        backup = BackupBucket.objects.select_related('bucket').filter(id=backup_id).first()
        if not backup:
            exc = exceptions.NotFound(message=_('资源不存在'))
            return Response(data=exc.err_data(), status=status.HTTP_404_NOT_FOUND)

        bucket = backup.bucket
        # 桶可操作权限
        try:
            check_authenticated_or_bucket_token(request, bucket_id=bucket.id, act='read', view=self)
        except exceptions.Error as exc:
            return Response(data=exc.err_data(), status=exc.status_code)

        # 用户是否拥有该桶的权限
        if bucket.user_id != request.user.id:
            exc = exceptions.AccessDenied(message=_('您没有权限操作该桶'))
            return Response(data=exc.err_data(), status=status.HTTP_403_FORBIDDEN)

        serializer = self.get_serializer(backup)
        return Response(serializer.data)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('获取桶的备份信息'),
        responses={
            status.HTTP_200_OK: ""
        }
    )
    @action(methods=['GET'], detail=False, url_path='bucket/(?P<bucket_name>.+)',
            url_name='list-bucketp-backups')
    def list_bucket_backups(self, request, *args, **kwargs):
        """
        获取桶的备份信息

            http 200:
            {
              "count": 2,
              "next": null,
              "previous": null,
              "results": [
                {
                  "endpoint_url": "http://obs1.cstcloud.cn/",
                  "bucket_name": "wang",
                  "bucket_token": "02c005c87212908b0a085a074c17ac49459423f3",
                  "backup_num": 2,
                  "remarks": "wang",
                  "id": 4,
                  "created_time": "2022-03-04T17:51:32.254758+08:00",
                  "modified_time": "2022-03-04T17:51:32.254758+08:00",
                  "status": "start",
                  "error": "null",
                  "bucket": {
                    "id": 1,
                    "name": "wang"
                  }
                }
                ...
              ]
            }

            http code 403 404:
            {
              "code": "xxx",  // NoSuchBucket、AccessDenied
              "message": ""
            }

        """
        bucket_name = kwargs.get('bucket_name', '')
        r = re.match('[a-z0-9-_]{3,64}$', bucket_name)
        if not r:
            exc = exceptions.NotFound(message=_('资源不存在'))
            return Response(data=exc.err_data(), status=exc.status_code)

        # 检测桶是否存在且是否是该用户的桶
        bucket = self.get_user_bucket(bucket_name, True, user=request.user)

        try:
            check_authenticated_or_bucket_token(request, bucket_name=bucket_name, act='read', view=self)
        except exceptions.Error as exc:
            return Response(data=exc.err_data(), status=exc.status_code)

        backup_bucket_info = BackupBucket.objects.select_related('bucket').filter(bucket__id=bucket.id)
        queryset = self.filter_queryset(backup_bucket_info)
        page = self.paginate_queryset(queryset)
        serializer = self.get_serializer(page, many=True)
        return self.get_paginated_response(serializer.data)

    def get_serializer_class(self):
        """
        Return the class to use for the serializer.
        Defaults to using `self.serializer_class`.
        Custom serializer_class
        """
        if self.action in ['list', 'retrieve', 'list_bucket_backups']:
            return serializers.BucketBackupSerializer
        return Serializer

    @staticmethod
    def get_user_bucket(id_or_name: str, by_name: bool = False, user=None):
        """
        获取存储桶对象，并检测用户访问权限

        :return:
            Bucket()

        :raises: Error
        """
        if by_name:
            bucket = Bucket.objects.select_related('user').filter(name=id_or_name).first()
        else:
            try:
                bid = int(id_or_name)
            except Exception as e:
                raise exceptions.BadRequest(message=_('无效的存储桶ID'))

            bucket = Bucket.objects.filter(id=bid).first()

        if not bucket:
            raise exceptions.NoSuchBucket(message=_('存储桶不存在'))

        if not bucket.check_user_own_bucket(user):
            raise exceptions.AccessDenied(message=_('您没有操作此存储桶的权限'))

        return bucket
