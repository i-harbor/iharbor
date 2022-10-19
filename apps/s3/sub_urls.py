from django.urls import path, include

from .routers import NoDetailRouter
from .views import bucket_views, obj_views


router = NoDetailRouter(trailing_slash=False)
router.register(r'', bucket_views.BucketViewSet, basename='bucket')
router.register(r'.+', obj_views.ObjViewSet, basename='obj')


urlpatterns = [
    path('', include(router.urls)),
]
