import urllib.parse

from django.utils.translation import gettext
from rest_framework.response import Response

from s3 import exceptions
from s3.harbor import HarborManager
from s3 import renders
from s3 import paginations
from s3 import serializers
from s3.viewsets import S3CustomGenericViewSet


class ListObjectsHandler:
    def list_objects_v1(self, request, view: S3CustomGenericViewSet):
        """
        encoding-type == url:
            key需要url编码

        prefix一直需要url编码
        """
        delimiter = request.query_params.get('delimiter', None)
        prefix = request.query_params.get('prefix', '')
        encoding_type = request.query_params.get('encoding-type', '')
        bucket_name = view.get_bucket_name(request)

        if not delimiter:  # list所有对象和目录
            return self.list_objects_v1_list_prefix(
                view=view, request=request, prefix=prefix, bucket_name=bucket_name, encoding_type=encoding_type)

        if delimiter != '/':
            return view.exception_response(
                request, exc=exceptions.S3InvalidArgument(message=gettext('参数“delimiter”必须是“/”')))

        path = prefix.strip('/')
        if prefix and not path:  # prefix invalid, return no match data
            return self.list_objects_v1_no_match(view=view, request=request, prefix=prefix, delimiter=delimiter,
                                                 bucket_name=bucket_name)

        hm = HarborManager()
        try:
            bucket, obj = hm.get_bucket_and_obj_or_dir(bucket_name=bucket_name, path=path, user=request.user)
        except exceptions.S3Error as e:
            return view.exception_response(request, exc=e)

        if obj is None:
            return self.list_objects_v1_no_match(view=view, request=request, prefix=prefix, delimiter=delimiter,
                                                 bucket_name=bucket_name)

        # 添加prefix目录到返回结果中，目录在s3中是一个空对象，根目录是个虚拟的对象，不返回
        paginator = paginations.ListObjectsV1CursorPagination(prefix_obj=obj)
        max_keys = paginator.get_page_size(request=request)
        ret_data = {
            'IsTruncated': 'false',  # can not use bool
            'Name': bucket_name,
            'Prefix': urllib.parse.quote(prefix),
            'EncodingType': 'url',
            'MaxKeys': max_keys,
            'Delimiter': delimiter
        }

        if prefix == '' or prefix.endswith('/'):  # list dir
            if not obj.is_dir():
                return self.list_objects_v1_no_match(view=view, request=request, prefix=prefix, delimiter=delimiter,
                                                     bucket_name=bucket_name)

            objs_qs = hm.list_dir_queryset(bucket=bucket, dir_obj=obj)
            paginator.paginate_queryset(objs_qs, request=request)
            objs, _ = paginator.get_objects_and_dirs()
            serializer = serializers.ObjectListWithOwnerSerializer(
                instance=objs, many=True, context={'user': request.user}, encoding_type=encoding_type)
            data = paginator.get_paginated_data(common_prefixes=True, delimiter=delimiter, encoding_type=encoding_type)
            ret_data.update(data)
            ret_data['Contents'] = serializer.data
            view.set_renderer(request, renders.ListObjectsV1XMLRenderer())
            return Response(data=ret_data, status=200)

        # list object metadata
        if not obj.is_file():
            return self.list_objects_v1_no_match(view=view, request=request, prefix=prefix, delimiter=delimiter,
                                                 bucket_name=bucket_name)

        serializer = serializers.ObjectListWithOwnerSerializer(
            obj, context={'user': request.user}, encoding_type=encoding_type)

        ret_data['Contents'] = [serializer.data]
        ret_data['KeyCount'] = 1
        view.set_renderer(request, renders.ListObjectsV1XMLRenderer())
        return Response(data=ret_data, status=200)

    @staticmethod
    def list_objects_v1_list_prefix(view, request, prefix, bucket_name, encoding_type: str):
        """
        列举所有对象和目录
        """
        hm = HarborManager()
        try:
            bucket, objs_qs = hm.get_bucket_objects_dirs_queryset(bucket_name=bucket_name, user=request.user,
                                                                  prefix=prefix)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        paginator = paginations.ListObjectsV1CursorPagination()
        objs_dirs = paginator.paginate_queryset(objs_qs, request=request)
        serializer = serializers.ObjectListWithOwnerSerializer(
            instance=objs_dirs, many=True, context={'user': request.user}, encoding_type=encoding_type)

        data = paginator.get_paginated_data(delimiter='', encoding_type=encoding_type)
        data['Contents'] = serializer.data
        data['Name'] = bucket_name
        if encoding_type == 'url':
            data['Prefix'] = urllib.parse.quote(prefix)
        else:
            data['Prefix'] = prefix
        data['EncodingType'] = 'url'

        view.set_renderer(request, renders.ListObjectsV1XMLRenderer())
        return Response(data=data, status=200)

    @staticmethod
    def list_objects_v1_no_match(view, request, prefix, delimiter, bucket_name):
        paginator = paginations.ListObjectsV1CursorPagination()
        max_keys = paginator.get_page_size(request=request)
        ret_data = {
            'IsTruncated': 'false',     # can not use bool True, need use string
            'Name': bucket_name,
            'Prefix': urllib.parse.quote(prefix),
            'EncodingType': 'url',
            'MaxKeys': max_keys,
            'KeyCount': 0
        }
        if delimiter:
            ret_data['Delimiter'] = delimiter

        view.set_renderer(request, renders.ListObjectsV1XMLRenderer())
        return Response(data=ret_data, status=200)

    def list_objects_v2(self, request, view: S3CustomGenericViewSet):
        """
        encoding-type == url:
            key 和 prefix 需要url编码
        """
        delimiter = request.query_params.get('delimiter', None)
        prefix = request.query_params.get('prefix', '')
        fetch_owner = request.query_params.get('fetch-owner', '').lower()
        encoding_type = request.query_params.get('encoding-type', '')
        bucket_name = view.get_bucket_name(request)

        if not delimiter:    # list所有对象和目录
            return self.list_objects_v2_list_prefix(
                request=request, view=view, prefix=prefix, encoding_type=encoding_type)

        if delimiter != '/':
            return view.exception_response(request, exceptions.S3InvalidArgument(
                message=gettext('参数“delimiter”必须是“/”')))

        path = prefix.strip('/')
        if prefix and not path:     # prefix invalid, return no match data
            return self.list_objects_v2_no_match(
                request=request, view=view, prefix=prefix, delimiter=delimiter, encoding_type=encoding_type)

        hm = HarborManager()
        try:
            bucket, obj = hm.get_bucket_and_obj_or_dir(bucket_name=bucket_name, path=path, user=request.user)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        if obj is None:
            return self.list_objects_v2_no_match(
                request=request, view=view, prefix=prefix, delimiter=delimiter,
                bucket=bucket, encoding_type=encoding_type)

        # 添加prefix目录到返回结果中，目录在s3中是一个空对象
        paginator = paginations.ListObjectsV2CursorPagination(context={'bucket': bucket}, prefix_obj=obj)
        max_keys = paginator.get_page_size(request=request)

        res_prefix = prefix
        if encoding_type == 'url':
            res_prefix = urllib.parse.quote(res_prefix)

        ret_data = {
            'IsTruncated': 'false',     # can not use bool
            'Name': bucket_name,
            'Prefix': res_prefix,
            'EncodingType': 'url',
            'MaxKeys': max_keys,
            'Delimiter': delimiter
        }

        if prefix == '' or prefix.endswith('/'):  # list dir
            if not obj.is_dir():
                return self.list_objects_v2_no_match(
                    request=request, view=view, prefix=prefix, delimiter=delimiter,
                    bucket=bucket, encoding_type=encoding_type)

            objs_qs = hm.list_dir_queryset(bucket=bucket, dir_obj=obj)
            paginator.paginate_queryset(objs_qs, request=request)
            objs, _ = paginator.get_objects_and_dirs()

            if fetch_owner == 'true':
                serializer = serializers.ObjectListV2WithOwnerSerializer(
                    instance=objs, many=True, context={'user': request.user}, encoding_type=encoding_type)
            else:
                serializer = serializers.ObjectListV2Serializer(
                    instance=objs, many=True, encoding_type=encoding_type)

            data = paginator.get_paginated_data(common_prefixes=True, delimiter=delimiter, encoding_type=encoding_type)
            ret_data.update(data)
            ret_data['Contents'] = serializer.data
            view.set_renderer(request, renders.ListObjectsV2XMLRenderer())
            return Response(data=ret_data, status=200)

        # list object metadata
        if not obj.is_file():
            return self.list_objects_v2_no_match(
                request=request, view=view, prefix=prefix, delimiter=delimiter,
                bucket=bucket, encoding_type=encoding_type)

        if fetch_owner == 'true':
            serializer = serializers.ObjectListV2WithOwnerSerializer(
                instance=obj, context={'user': request.user}, encoding_type=encoding_type)
        else:
            serializer = serializers.ObjectListV2Serializer(instance=obj, encoding_type=encoding_type)

        ret_data['Contents'] = [serializer.data]
        ret_data['KeyCount'] = 1
        view.set_renderer(request, renders.ListObjectsV2XMLRenderer())
        return Response(data=ret_data, status=200)

    @staticmethod
    def list_objects_v2_list_prefix(request, view: S3CustomGenericViewSet, prefix: str, encoding_type: str):
        """
        列举所有对象和目录
        """
        fetch_owner = request.query_params.get('fetch-owner', '').lower()

        bucket_name = view.get_bucket_name(request)
        hm = HarborManager()
        try:
            bucket, objs_qs = hm.get_bucket_objects_dirs_queryset(
                bucket_name=bucket_name, user=request.user, prefix=prefix)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        paginator = paginations.ListObjectsV2CursorPagination(context={'bucket': bucket})
        objs_dirs = paginator.paginate_queryset(objs_qs, request=request)
        if fetch_owner == 'true':
            serializer = serializers.ObjectListV2WithOwnerSerializer(
                instance=objs_dirs, many=True, context={'user': request.user}, encoding_type=encoding_type)
        else:
            serializer = serializers.ObjectListV2Serializer(
                instance=objs_dirs, many=True, encoding_type=encoding_type)

        data = paginator.get_paginated_data(encoding_type=encoding_type)
        data['Contents'] = serializer.data
        data['Name'] = bucket_name
        data['Prefix'] = prefix
        data['EncodingType'] = 'url'

        view.set_renderer(request, renders.ListObjectsV2XMLRenderer())
        return Response(data=data, status=200)

    @staticmethod
    def list_objects_v2_no_match(
            request, view: S3CustomGenericViewSet, prefix, delimiter, encoding_type: str, bucket=None
    ):
        if bucket:
            bucket_name = bucket.name
            context = {'bucket': bucket}
        else:
            bucket_name = view.get_bucket_name(request)
            context = {'bucket_name': bucket_name}

        paginator = paginations.ListObjectsV2CursorPagination(context=context)
        max_keys = paginator.get_page_size(request=request)
        if encoding_type == 'url':
            prefix = urllib.parse.quote(prefix)

        ret_data = {
            'IsTruncated': 'false',     # can not use True
            'Name': bucket_name,
            'Prefix': prefix,
            'EncodingType': 'url',
            'MaxKeys': max_keys,
            'KeyCount': 0
        }
        if delimiter:
            ret_data['Delimiter'] = delimiter

        view.set_renderer(request, renders.ListObjectsV2XMLRenderer())
        return Response(data=ret_data, status=200)
