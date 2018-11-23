from django.conf.urls import url, include
from rest_framework.routers import DefaultRouter
from rest_framework_jwt.views import obtain_jwt_token, refresh_jwt_token
# from rest_framework_swagger.views import get_swagger_view

from .auth import obtain_auth_token
from . import views


# Create a router and register our viewsets with it.
router = DefaultRouter()
router.register(r'(?P<version>(v1|v2))/users', views.UserViewSet, base_name='user')
router.register(r'(?P<version>(v1|v2))/buckets', views.BucketViewSet, base_name='buckets')
router.register(r'(?P<version>(v1|v2))/obj', views.ObjViewSet, base_name='obj')
router.register(r'(?P<version>(v1|v2))/download', views.DownloadFileViewSet, base_name='download')
router.register(r'(?P<version>(v1|v2))/dir', views.DirectoryViewSet, base_name='dir')


urlpatterns = [
    url(r'^', include(router.urls)), # The API URLs are now determined automatically by the router.
    url(r'^jwt-token/', obtain_jwt_token),
    url(r'^jwt-token-refresh/', refresh_jwt_token),
    url(r'^auth-token/', obtain_auth_token),
    # url(r'docs/', get_swagger_view(title='EVHarbor API')),
]

