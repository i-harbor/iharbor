from django.conf.urls import url, include
from rest_framework.routers import DefaultRouter

from . import views

# Create a router and register our viewsets with it.
router = DefaultRouter()
router.register(r'(?P<version>(v1|v2))/users', views.UserViewSet, base_name='user')
router.register(r'(?P<version>(v1|v2))/bucket', views.BucketViewSet, base_name='bucket')
router.register(r'(?P<version>(v1|v2))/upload', views.UploadFileViewSet, base_name='upload')
router.register(r'(?P<version>(v1|v2))/delete', views.DeleteFileViewSet, base_name='delete')
router.register(r'(?P<version>(v1|v2))/download', views.DownloadFileViewSet, base_name='download')
router.register(r'(?P<version>(v1|v2))/directory', views.DirectoryViewSet, base_name='directory')


urlpatterns = [
    # url(r'^bucket/(?P<bucket_name>[\w-]{1,50})/(?P<path>.*)', views.file_list, name='file_list'),
    url(r'^', include(router.urls)), # The API URLs are now determined automatically by the router.
]
