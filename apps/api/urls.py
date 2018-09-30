from django.conf.urls import url, include
from rest_framework.routers import DefaultRouter

from . import views

# Create a router and register our viewsets with it.
router = DefaultRouter()
router.register(r'users', views.UserViewSet, base_name='user')
router.register(r'buckets', views.BucketViewSet, base_name='bucket')
router.register(r'upload', views.UploadFileViewSet, base_name='upload')
router.register(r'delete', views.DeleteFileViewSet, base_name='delete')
router.register(r'download', views.DownloadFileViewSet, base_name='download')

urlpatterns = [
    # url(r'^bucket/(?P<bucket_name>[\w-]{1,50})/(?P<path>.*)', views.file_list, name='file_list'),
    url(r'^', include(router.urls)), # The API URLs are now determined automatically by the router.
]
