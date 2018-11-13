from django.conf.urls import url, include
from rest_framework.routers import DefaultRouter
from rest_framework_jwt.views import obtain_jwt_token, refresh_jwt_token

from . import views

# Create a router and register our viewsets with it.
router = DefaultRouter()
router.register(r'(?P<version>(v1|v2))/users', views.UserViewSet, base_name='user')
router.register(r'(?P<version>(v1|v2))/buckets', views.BucketViewSet, base_name='buckets')
router.register(r'(?P<version>(v1|v2))/upload', views.UploadFileViewSet, base_name='upload')
router.register(r'(?P<version>(v1|v2))/delete', views.DeleteFileViewSet, base_name='delete')
router.register(r'(?P<version>(v1|v2))/download', views.DownloadFileViewSet, base_name='download')
router.register(r'(?P<version>(v1|v2))/directory', views.DirectoryViewSet, base_name='directory')
router.register(r'(?P<version>(v1|v2))/bucket', views.BucketFileViewSet, base_name='bucket')


urlpatterns = [
    # url(r'^bucket/(?P<bucket_name>[\w-]{1,50})/(?P<path>.*)', views.file_list, name='file_list'),
    url(r'^', include(router.urls)), # The API URLs are now determined automatically by the router.
    url(r'^auth/', obtain_jwt_token),
    url(r'^token-refresh/', refresh_jwt_token),
]
