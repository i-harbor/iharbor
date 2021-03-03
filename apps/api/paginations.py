from collections import OrderedDict

from rest_framework.pagination import CursorPagination, LimitOffsetPagination, _divide_with_ceil
from rest_framework.response import Response


class BucketFileCursorPagination(CursorPagination):
    '''
    存储通文件对象分页器
    '''
    cursor_query_param = 'cursor'
    page_size = 200
    ordering = ('id', )

    # Client can control the page size using this query parameter.
    # Default is 'None'. Set to eg 'page_size' to enable usage.
    page_size_query_param = 'size'

    # Set to an integer to limit the maximum page size the client may request.
    # Only relevant if 'page_size_query_param' has also been set.
    max_page_size = 1000
    offset_cutoff = None

    def get_paginated_response(self, data):
        if isinstance(data, OrderedDict):
            data['next'] = self.get_next_link()
            data['previous'] = self.get_previous_link()
            return Response(data)


class BucketFileLimitOffsetPagination(LimitOffsetPagination):
    '''
    存储桶分页器
    '''
    default_limit = 200
    limit_query_param = 'limit'
    offset_query_param = 'offset'
    max_limit = 2000

    def paginate_queryset(self, queryset, request, view=None):
        limit = self.get_limit(request)
        offset = self.get_offset(request)
        return self.pagenate_to_list(queryset, request=request, offset=offset, limit=limit)

    def pagenate_to_list(self, queryset, offset, limit, request=None):
        if request:
            self.request = request

        self.offset = offset
        self.limit = limit

        if not hasattr(self, 'count'):      # 避免多次调用时重复查询
            self.count = self.get_count(queryset)

        count = self.count
        if count > limit and self.template is not None:
            self.display_page_controls = True

        if count == 0 or offset > count:
            self.offset = self.count
            return []

        # 当数据少时
        if count <= 10000:
            return list(queryset[offset:offset+limit])

        # 当数据很多时，目录和对象分开考虑
        dirs_queryset = queryset.filter(fod=False).order_by('-id')
        dirs_count = dirs_queryset.count()
        # 分页数据只有目录
        if (offset + limit) <= dirs_count:
            return list(dirs_queryset[offset:offset+limit])

        objs_queryset = queryset.filter(fod=True).order_by('-id')
        # 分页数据只有对象
        if offset >= dirs_count:
            oft = offset - dirs_count
            # 偏移量offset较小时
            if oft <= 10000:
                return list(objs_queryset[oft:oft+limit])

            # 偏移量offset较大时
            id = objs_queryset.values_list('id').order_by('-id')[oft:oft+1].first()
            if not id:
                return []
            return list(objs_queryset.filter(id__lte=id[0])[0:limit])

        # 分页数据包含目录和对象
        dirs = list(dirs_queryset[offset:offset + limit])
        dir_len = len(dirs)
        objs = list(objs_queryset[0:limit - dir_len])
        return dirs + objs

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

        return current, final


class SearchBucketFileLimitOffsetPagination(BucketFileLimitOffsetPagination):
    pass


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

