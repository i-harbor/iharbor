from collections import OrderedDict

from rest_framework.pagination import CursorPagination, LimitOffsetPagination
from rest_framework.response import Response


class BucketFileCursorPagination(CursorPagination):
    '''
    存储通文件对象分页器
    '''
    cursor_query_param = 'cursor'
    page_size = 100
    ordering = '-ult' #['fod', '-ult'], #文档降序，最近日期靠前

    # Client can control the page size using this query parameter.
    # Default is 'None'. Set to eg 'page_size' to enable usage.
    page_size_query_param = 'size'

    # Set to an integer to limit the maximum page size the client may request.
    # Only relevant if 'page_size_query_param' has also been set.
    max_page_size = 1000

    def get_paginated_response(self, data):
        if isinstance(data, OrderedDict):
            data['next'] = self.get_next_link()
            data['previous'] = self.get_previous_link()
            return Response(data)


class BucketsLimitOffsetPagination(LimitOffsetPagination):
    '''
    存储桶分页器
    '''
    default_limit = 100
    limit_query_param = 'limit'
    offset_query_param = 'offset'
    max_limit = 1000

    def get_paginated_response(self, data):
        return Response(OrderedDict([
            ('count', self.count),
            ('next', self.get_next_link()),
            ('previous', self.get_previous_link()),
            ('buckets', data)
        ]))




