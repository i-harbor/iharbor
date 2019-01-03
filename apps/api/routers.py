from rest_framework.routers import SimpleRouter, DefaultRouter, Route


class DetailPostRouter(SimpleRouter):
    '''
    自定义路由器，增加detail POST 方法url
    '''
    def __init__(self, trailing_slash=True, *args, **kwargs):
        # Detail route.
        detail = Route(
            url=r'^{prefix}/{lookup}{trailing_slash}$',
            mapping={
                'post': 'create',
            },
            name='{basename}-detail',
            detail=True,
            initkwargs={'suffix': 'Instance'}
        )
        self.routes.append(detail)
        super().__init__(trailing_slash=trailing_slash)

