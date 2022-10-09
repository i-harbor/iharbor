from django.urls import path, include, re_path
from rest_framework.routers import DefaultRouter

from . import auth
from . import views
from .views_v1.bucketbackup import BackupNodeViewSet
from .views_v1.admin_bucket_views import AdminBucketViewSet
from .routers import DetailPostRouter, DetailListPostRouter
from users.auth.views import ObtainAuthKey
from . import v2views

app_name = "api"

# Create a router and register our viewsets with it.
router = DefaultRouter()
router.register(r'users', views.UserViewSet, basename='user')
router.register(r'buckets', views.BucketViewSet, basename='buckets')
router.register(r'auth-key', ObtainAuthKey, basename='auth-key')
router.register(r'stats/bucket', views.BucketStatsViewSet, basename='stats_bucket')
router.register(r'stats/ceph', views.CephStatsViewSet, basename='stats_ceph')
router.register(r'stats/user', views.UserStatsViewSet, basename='stats_user')
router.register(r'stats/visit', views.VisitStatsViewSet, basename='stats_visit')
router.register(r'security', views.SecurityViewSet, basename='security')
router.register(r'ceph/perf', views.CephPerformanceViewSet, basename='ceph_performance')
router.register(r'usercount', views.UserCountViewSet, basename='usercount')
router.register(r'test', views.TestViewSet, basename='test')
router.register(r'ftp', views.FtpViewSet, basename='ftp')
router.register(r'obj-rados/(?P<bucket_name>[a-z0-9-_]{3,64})', views.ObjKeyViewSet, basename='obj-rados')
router.register(r'bucket-token', auth.BucketTokenView, basename='bucket-token')
router.register(r'search/object', views.SearchObjectViewSet, basename='search-object')
router.register(r'list/bucket',
                views.ListBucketObjectViewSet, basename='list-bucket')

no_slash_router = DefaultRouter(trailing_slash=False)
no_slash_router.register(r'backup', BackupNodeViewSet, basename='backup_bucket')
no_slash_router.register(r'admin/bucket', AdminBucketViewSet, basename='admin-bucket')

dlp_router = DetailListPostRouter()
dlp_router.register(r'dir/(?P<bucket_name>[a-z0-9-_]{3,64})', views.DirectoryViewSet, basename='dir')

detail_router = DetailPostRouter()
detail_router.register(r'obj/(?P<bucket_name>[a-z0-9-_]{3,64})', views.ObjViewSet, basename='obj')
detail_router.register(r'move/(?P<bucket_name>[a-z0-9-_]{3,64})', views.MoveViewSet, basename='move')
detail_router.register(r'metadata/(?P<bucket_name>[a-z0-9-_]{3,64})', views.MetadataViewSet, basename='metadata')
detail_router.register(r'refresh-meta/(?P<bucket_name>[a-z0-9-_]{3,64})', views.RefreshMetadataViewSet,
                       basename='refresh-meta')

no_slash_detail_router = DetailPostRouter(trailing_slash=False)
no_slash_detail_router.register(r'share/(?P<bucket_name>[a-z0-9-_]{3,64})', views.ShareViewSet, basename='share')


v2_detail_router = DetailPostRouter(trailing_slash=False)
v2_detail_router.register(r'obj/(?P<bucket_name>[a-z0-9-_]{3,64})', v2views.V2ObjViewSet, basename='v2-obj')


urlpatterns = [
    path('v1/', include(router.urls)),
    path('v1/', include(detail_router.urls)),
    path('v1/', include(no_slash_detail_router.urls)),
    path('v1/', include(no_slash_router.urls)),
    path('v1/', include(dlp_router.urls)),
    path('v2/', include(v2_detail_router.urls)),
    path('v1/auth-token/', auth.obtain_auth_token, name='auth-token'),
    path('v1/jwt/', auth.JWTObtainPairView.as_view(), name='jwt-token'),
    path('v1/jwt-refresh/', auth.JWTRefreshView.as_view(), name='jwt-refresh'),
    path('v1/jwt-verify/', auth.JWTVerifyView.as_view(), name='jwt-verify'),
]

