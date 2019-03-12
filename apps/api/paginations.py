from collections import OrderedDict

from rest_framework.pagination import CursorPagination, LimitOffsetPagination, _divide_with_ceil
from rest_framework.response import Response


class BucketFileCursorPagination(CursorPagination):
    '''
    存储通文件对象分页器
    '''
    cursor_query_param = 'cursor'
    page_size = 100
    ordering = ('fod', '-ult',) # 日期降序，最近日期靠前

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


class BucketFileLimitOffsetPagination(LimitOffsetPagination):
    '''
    存储桶分页器
    '''
    default_limit = 100
    limit_query_param = 'limit'
    offset_query_param = 'offset'
    max_limit = 1000

    def get_paginated_response(self, data):
        # content = self.get_html_context()
        current, final = self.get_current_and_final_page_number()
        d = OrderedDict([
            ('count', self.count),
            ('next', self.get_next_link()),
            ('page', {'current': current, 'final': final}),
            ('previous', self.get_previous_link()),
            # ('page_links', content['page_links'])
        ])
        if isinstance(data, OrderedDict):
            data.update(d)
        return Response(data)

    def get_current_and_final_page_number(self):
        if self.limit:
            current = _divide_with_ceil(self.offset, self.limit) + 1

            # The number of pages is a little bit fiddly.
            # We need to sum both the number of pages from current offset to end
            # plus the number of pages up to the current offset.
            # When offset is not strictly divisible by the limit then we may
            # end up introducing an extra page as an artifact.
            final = (
                _divide_with_ceil(self.count - self.offset, self.limit) +
                _divide_with_ceil(self.offset, self.limit)
            )

            if final < 1:
                final = 1
        else:
            current = 1
            final = 1

        if current > final:
            current = final

        return (current, final)


class BucketsLimitOffsetPagination(LimitOffsetPagination):
    '''
    存储桶分页器
    '''
    default_limit = 100
    limit_query_param = 'limit'
    offset_query_param = 'offset'
    max_limit = 1000

    def get_paginated_response(self, data):
        # content = self.get_html_context()
        current, final = self.get_current_and_final_page_number()
        return Response(OrderedDict([
            ('count', self.count),
            ('next', self.get_next_link()),
            ('page', {'current': current, 'final': final}),
            ('previous', self.get_previous_link()),
            ('buckets', data),
            # ('page_links', content['page_links'])
        ]))

    def get_current_and_final_page_number(self):
        if self.limit:
            current = _divide_with_ceil(self.offset, self.limit) + 1

            # The number of pages is a little bit fiddly.
            # We need to sum both the number of pages from current offset to end
            # plus the number of pages up to the current offset.
            # When offset is not strictly divisible by the limit then we may
            # end up introducing an extra page as an artifact.
            final = (
                _divide_with_ceil(self.count - self.offset, self.limit) +
                _divide_with_ceil(self.offset, self.limit)
            )

            if final < 1:
                final = 1
        else:
            current = 1
            final = 1

        if current > final:
            current = final

        return (current, final)
