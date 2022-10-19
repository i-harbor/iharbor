from urllib import parse

from django.utils.encoding import force_str
from django.utils.translation import gettext as _
from rest_framework.pagination import CursorPagination, Cursor
from rest_framework.exceptions import NotFound

from . import exceptions
from .harbor import HarborManager


def get_query_param(url, key):
    """
    get query param form url

    :param url: url
    :param key: name of query param
    :return:
        list(str) or None
    """
    (scheme, netloc, path, query, fragment) = parse.urlsplit(force_str(url))
    query_dict = parse.parse_qs(query, keep_blank_values=True)
    return query_dict.get(key, None)


class ListObjectsV2CursorPagination(CursorPagination):
    """
    存储通文件对象分页器
    """
    cursor_query_param = 'continuation-token'
    cursor_query_description = 'The pagination continuation-token value.'
    page_size = 1000
    invalid_cursor_message = 'Invalid continuation-token'
    ordering = '-id'

    page_size_query_param = 'max-keys'
    page_size_query_description = 'Max number of results to return per page.'

    max_page_size = 1000
    offset_cutoff = 0

    start_after_query_param = 'start-after'  # used if no cursor_query_param

    def __init__(self, context):
        """
        :param context:
            {
                'bucket': bucket,
                'bucket_name': bucket_name,
                ...
            }
        """
        if 'bucket' not in context and 'bucket_name' not in context:
            raise ValueError('Invalid param "context", one of "bucket" and "bucket_name" needs to be in it.')

        self._context = context

    def paginate_queryset(self, queryset, request, view=None):
        self.request = request
        data = super().paginate_queryset(queryset=queryset, request=request, view=view)
        if data is None:
            data = []
        self._data = data
        self.key_count = len(self._data)
        return self._data

    @property
    def page_data(self):
        if hasattr(self, '_data'):
            return self._data

        raise AssertionError('You must call `.paginate_queryset()` before accessing `.data`.')

    def get_objects_and_dirs(self):
        if not hasattr(self, 'objects') or not hasattr(self, 'dirs'):
            objects = []
            dirs = []
            data = self.page_data
            for obj in data:
                if obj.is_file():
                    objects.append(obj)
                else:
                    dirs.append(obj)
            self.objects = objects
            self.dirs = dirs

        return self.objects, self.dirs

    def get_common_prefix(self, delimiter='/'):
        common_prefix = []
        for d in self.dirs:
            na = d.na
            if not na.endswith(delimiter):
                na = na + delimiter
            common_prefix.append({"Prefix": na})

        return common_prefix

    def get_paginated_data(self, common_prefixes=False, delimiter='/'):
        is_truncated = 'true' if self.has_next else 'false'
        data = {
            'IsTruncated': is_truncated,  # can not use True
            'MaxKeys': self.page_size,
            'KeyCount': self.key_count
        }
        c_token = self.get_continuation_token(request=self.request)
        if c_token:
            data['ContinuationToken'] = c_token

        nc_token = self.get_next_continuation_token()
        if nc_token:
            data['NextContinuationToken'] = nc_token

        if common_prefixes:
            data['CommonPrefixes'] = self.get_common_prefix(delimiter)

        return data

    def get_continuation_token(self, request):
        return request.query_params.get(self.cursor_query_param, None)

    def get_next_continuation_token(self):
        next_link = self.get_next_link()
        if next_link is None:
            return None

        params = get_query_param(url=next_link, key=self.cursor_query_param)
        if not params:
            return None

        return params[0]

    def decode_cursor(self, request):
        try:
            cursor = super().decode_cursor(request)
        except NotFound as e:
            raise exceptions.S3InvalidArgument(message=_('无效的参数continuation-token'))

        if cursor:
            return cursor

        start_after = request.query_params.get(self.start_after_query_param, None)
        if not start_after:
            return None

        start_after = start_after.strip('/')
        if start_after:
            cursor = self._get_start_after_cursor(start_after=start_after)
            return cursor

        return None

    def _get_start_after_cursor(self, start_after):
        """
        获取分页起始参数start_after对应的游标cursor

        :param start_after: 对象Key
        :return:
            Cursor() or None

        :raises: S3Error
        """
        hm = HarborManager()
        bucket = self._context.get('bucket', None)
        if not bucket:
            bucket_name = self._context('bucket_name')
            bucket = hm.get_bucket(bucket_name=bucket_name)

        if not bucket:
            return None

        table_name = bucket.get_bucket_table_name()
        try:
            obj = HarborManager().get_metadata_obj(table_name=table_name, path=start_after)
        except exceptions.S3Error as e:
            raise e

        if not obj:
            raise exceptions.S3NoSuchKey(_('无效的参数start_after'))

        order = self.ordering[0]
        is_reversed = order.startswith('-')
        attr = obj.id  # order by id
        if is_reversed:  # 倒序
            position = max(attr - 1, 0)
            reverse = False
        else:
            position = attr + 1
            reverse = True

        return Cursor(offset=0, reverse=reverse, position=position)


class ListObjectsV1CursorPagination(CursorPagination):
    """
    存储通文件对象分页器
    """
    cursor_query_param = 'marker'
    cursor_query_description = 'The pagination marker value.'
    page_size = 1000
    invalid_cursor_message = 'Invalid marker'
    ordering = '-id'

    page_size_query_param = 'max-keys'
    page_size_query_description = 'Max number of results to return per page.'

    max_page_size = 1000
    offset_cutoff = 0

    def __init__(self, context=None):
        """
        :param context: {}
        """
        self._context = context if context else {}

    def paginate_queryset(self, queryset, request, view=None):
        self.request = request
        data = super().paginate_queryset(queryset=queryset, request=request, view=view)
        if data is None:
            data = []
        self._data = data
        self.key_count = len(self._data)
        return self._data

    @property
    def page_data(self):
        if hasattr(self, '_data'):
            return self._data

        raise AssertionError('You must call `.paginate_queryset()` before accessing `.data`.')

    def get_objects_and_dirs(self):
        if not hasattr(self, 'objects') or not hasattr(self, 'dirs'):
            objects = []
            dirs = []
            data = self.page_data
            for obj in data:
                if obj.is_file():
                    objects.append(obj)
                else:
                    dirs.append(obj)
            self.objects = objects
            self.dirs = dirs

        return self.objects, self.dirs

    def get_common_prefix(self, delimiter='/'):
        if not delimiter:
            delimiter = '/'

        common_prefix = []
        for d in self.dirs:
            na = d.na
            if not na.endswith(delimiter):
                na = na + delimiter
            common_prefix.append({"Prefix": na})

        return common_prefix

    def get_paginated_data(self, common_prefixes=False, delimiter=None):
        is_truncated = 'true' if self.has_next else 'false'
        data = {
            'IsTruncated': is_truncated,  # can not use True
            'MaxKeys': self.page_size,
            'KeyCount': self.key_count
        }
        marker = self.get_marker(request=self.request)
        data['Marker'] = marker if marker else ''

        if delimiter:   # 请求有delimiter才有 NextMarker
            next_marker = self.get_next_marker()
            if next_marker:
                data['NextMarker'] = next_marker

        if common_prefixes:
            data['CommonPrefixes'] = self.get_common_prefix(delimiter)

        return data

    def get_marker(self, request):
        return request.query_params.get(self.cursor_query_param, None)

    def get_next_marker(self):
        next_link = self.get_next_link()
        if next_link is None:
            return None

        params = get_query_param(url=next_link, key=self.cursor_query_param)
        if not params:
            return None

        return params[0]

    def decode_cursor(self, request):
        try:
            cursor = super().decode_cursor(request)
        except NotFound as e:
            raise exceptions.S3InvalidArgument(message=_('无效的参数marker'))

        if cursor:
            return cursor

        return None
