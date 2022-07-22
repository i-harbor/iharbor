import random
import re
import string
import requests

from django.utils.translation import gettext_lazy, gettext as _
from django.db.models import Q
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi

from rest_framework import status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.serializers import Serializer
from rest_framework.pagination import LimitOffsetPagination

from buckets.utils import BucketFileManagement
from api.serializers import BucketBackupCreateSerializer
from utils.view import CustomGenericViewSet
from buckets.models import Bucket, BackupBucket
from api import serializers, permissions
from api import exceptions
from api.views import check_authenticated_or_bucket_token, serializer_error_text


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
                description=gettext_lazy('通过列表查询备份id')
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
            exc = exceptions.NotFound(message=_('桶不存在'))
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

    @swagger_auto_schema(
        operation_summary=gettext_lazy('创建桶的备份信息'),
        responses={
            status.HTTP_200_OK: ""
        }
    )
    def create(self, request, *args, **kwargs):
        """
        创建备份
        """
        # 校验参数
        serializer = self.get_serializer(data=request.data)
        if not serializer.is_valid(raise_exception=False):
            code_text = serializer_error_text(serializer.errors, default='参数验证有误')
            exc = exceptions.BadRequest(message=_(code_text))
            return Response(data=exc.err_data(), status=exc.status_code)

        validated_data = serializer.validated_data
        endpoint_url = validated_data.get('endpoint_url')
        bucket_name = validated_data.get('bucket_name')
        bucket_token = validated_data.get('bucket_token')
        backup_num = validated_data.get('backup_num')
        bucket_id = validated_data.get('bucket_id')
        endpoint_url = endpoint_url.rstrip('/')

        # 桶的读写权限
        try:
            check_authenticated_or_bucket_token(request, bucket_id=bucket_id, act='write', view=self)
        except exceptions.Error as exc:
            return Response(data=exc.err_data(), status=exc.status_code)

        # 用户是否能操作该桶
        bucket = self.get_user_bucket(id_or_name=bucket_id, user=request.user)

        error = self.check_field(endpoint_url, bucket_name, bucket_token)
        if error:
            return Response(data=error.err_data(), status=error.status_code)

        # 服务器备份 backup_num 数据是否存在，存在只能修改
        backup_list = BackupBucket.objects.filter(bucket_id=bucket_id)
        if len(backup_list) == 2:
            exc = exceptions.BadRequest(message=_('备份数据已超出限制，备份点1和备份点2只能个备份1次，再次备份需要去修改页面操作。'))
            return Response(data=exc.err_data(), status=exc.status_code)
        backup_n = backup_list.filter(backup_num=backup_num)
        if backup_n:
            exc = exceptions.BadRequest(message=_('该备份点已经备份过一次，如需备份请去修改页面操作。'))
            return Response(data=exc.err_data(), status=exc.status_code)

        bfm = BucketFileManagement(collection_name=bucket.get_bucket_table_name())
        model_cls = bfm.get_obj_model_class()

        # 更新 async1、async2 时间
        try:
            if backup_num == BackupBucket.BackupNum.ONE:
                model_cls.objects.filter(~Q(async1__isnull=True)).update(async1=None)
            if backup_num == BackupBucket.BackupNum.TWO:
                model_cls.objects.filter(~Q(async2__isnull=True)).update(async2=None)
        except exceptions.Error as exc:
            return Response(data=exc.err_data(), status=exc.status_code)

        backup = serializer.save()
        serializer = serializers.BucketBackupSerializer(instance=backup)
        return Response(serializer.data, status=status.HTTP_201_CREATED)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('删除备份信息'),
        manual_parameters=[
            openapi.Parameter(
                name='id',
                in_=openapi.IN_PATH,
                type=openapi.TYPE_NUMBER,
                required=True,
                description=gettext_lazy('通过列表查询备份id')
            ),
        ],
        responses={
            status.HTTP_200_OK: ""
        }
    )
    def destroy(self, request, *args, **kwargs):
        backup_id = kwargs.get(self.lookup_field, "")
        backup = BackupBucket.objects.select_related('bucket').filter(id=backup_id).first()
        if not backup:
            exc = exceptions.NotFound(message=_('资源不存在'))
            return Response(data=exc.err_data(), status=status.HTTP_404_NOT_FOUND)
        bucket = backup.bucket

        # 用户是否拥有该桶的权限
        if bucket.user_id != request.user.id:
            exc = exceptions.AccessDenied(message=_('您没有权限。'))
            return Response(data=exc.err_data(), status=status.HTTP_403_FORBIDDEN)
        backup.delete()

        bfm = BucketFileManagement(collection_name=bucket.get_bucket_table_name())
        model_cls = bfm.get_obj_model_class()
        # 删除 async1、async2 时间
        try:
            if backup.backup_num == BackupBucket.BackupNum.ONE:
                model_cls.objects.filter(~Q(async1__isnull=True)).update(async1=None)
            elif backup.backup_num == BackupBucket.BackupNum.TWO:
                model_cls.objects.filter(~Q(async2__isnull=True)).update(async2=None)
        except Exception as e:
            pass

        return Response(status=status.HTTP_200_OK)

    def get_serializer_class(self):
        """
        Return the class to use for the serializer.
        Defaults to using `self.serializer_class`.
        Custom serializer_class
        """
        if self.action in ['list', 'retrieve', 'list_bucket_backups']:
            return serializers.BucketBackupSerializer
        if self.action == 'create':
            return BucketBackupCreateSerializer
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

    def check_field(self, endpoint_url, bucket_name, bucket_token):
        # 检查备份服务器是否存在
        url, headers, file_name, exc = self.check_server_file(self, endpoint_url, bucket_name, bucket_token)
        if exc:
            return exc
        # 上传文件校验权限
        exc = self.request_upload_file(url, headers)
        if exc:
            return exc
        # 删除文件
        exc = self.request_delete_file(endpoint_url, headers, bucket_name, file_name)
        if exc:
            return self.request_delete_file(endpoint_url, headers, bucket_name, file_name)
        return

    @staticmethod
    def check_server_file(self, endpoint_url, bucket_name, bucket_token):
        # 从a-zA-Z0-9生成指定数量的随机字符：
        file_name = ''.join(random.sample(string.ascii_letters + string.digits + '@#!&.+', 10))
        url = endpoint_url + f'/api/v1/metadata/{bucket_name}/{file_name}/'
        headers = {
            'Authorization': 'BucketToken ' + bucket_token
        }

        try:
            r = requests.get(url, headers=headers)
        except requests.exceptions.RequestException as e:
            exc = exceptions.BadRequest(message=_('无法连接服务器，请查看服务器地址是否正确或网络等问题。'))
            return '', '', '', exc
        if r.status_code == 200:
            try:
                self.check_server_file(endpoint_url, bucket_name, bucket_token)
            except Exception as e:
                exc = exceptions.BadRequest(message=_('服务异常，请重试。'))
                return '', '', '', exc

        if r.status_code == 404 and r.json()['code'] == 'NoSuchKey':

            return url, headers, file_name, ''

        if r.status_code != 404:
            exc = exceptions.BadRequest(message=_('请查看endpoint_url、bucket_name、bucket_token 填写正确及权限问题。'))
            return '', '', '', exc

    @staticmethod
    def request_upload_file(url, headers):
        try:
            r = requests.post(url, headers=headers)
        except requests.exceptions.RequestException as e:
            exc = exceptions.BadRequest(message=_('无法连接服务器，请查看服务器地址是否正确或网络等问题。'))
            return exc

        if r.status_code != 200:
            exc = exceptions.BadRequest(message=_('请查看关键参数是否填写正确。'))
            return exc
        return

    @staticmethod
    def request_delete_file(endpoint_url, headers, bucket_name, file_name):
        url = endpoint_url + f'/api/v1/obj/{bucket_name}/{file_name}/'
        try:
            r = requests.delete(url, headers=headers)
        except requests.exceptions.RequestException as e:
            exc = exceptions.BadRequest(message=_('无法连接服务器，请查看服务器地址是否正确或网络等问题。'))
            return exc
        if r.status_code != 204:
            return
        return

