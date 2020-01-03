from django.urls import path, include, re_path
from rest_framework.routers import DefaultRouter

from .auth import obtain_auth_token, obtain_jwt_token, refresh_jwt_token
from . import views
from .routers import DetailPostRouter, DetailListPostRouter
from users.auth.views import ObtainAuthKey

app_name = "api"

# Create a router and register our viewsets with it.
router = DefaultRouter()
router.register(r'users', views.UserViewSet, base_name='user')
router.register(r'buckets', views.BucketViewSet, base_name='buckets')
router.register(r'auth-key', ObtainAuthKey, base_name='auth-key')
router.register(r'stats/bucket', views.BucketStatsViewSet, base_name='stats_bucket')
router.register(r'stats/ceph', views.CephStatsViewSet, base_name='stats_ceph')
router.register(r'stats/user', views.UserStatsViewSet, base_name='stats_user')
router.register(r'stats/visit', views.VisitStatsViewSet, base_name='stats_visit')
router.register(r'security', views.SecurityViewSet, base_name='security')
router.register(r'metadata/(?P<bucket_name>[a-z0-9-]{3,64})', views.MetadataViewSet,
                base_name='metadata')
router.register(r'ceph/comp', views.CephComponentsViewSet, base_name='ceph_components')
router.register(r'ceph/perf', views.CephPerformanceViewSet, base_name='ceph_performance')
router.register(r'ceph/errors', views.CephErrorViewSet, base_name='ceph_errors')
router.register(r'usercount', views.UserCountViewSet, base_name='usercount')
router.register(r'availability', views.AvailabilityViewSet, base_name='availability')
router.register(r'test', views.TestViewSet, base_name='test')
router.register(r'ftp', views.FtpViewSet, base_name='ftp')
router.register(r'vpn', views.VPNViewSet, base_name='vpn')
router.register(r'obj-rados/(?P<bucket_name>[a-z0-9-]{3,64})', views.ObjKeyViewSet, base_name='obj-rados')


dlp_router = DetailListPostRouter()
dlp_router.register(r'dir/(?P<bucket_name>[a-z0-9-]{3,64})', views.DirectoryViewSet, base_name='dir')

detail_router = DetailPostRouter()
detail_router.register(r'obj/(?P<bucket_name>[a-z0-9-]{3,64})', views.ObjViewSet, base_name='obj')
detail_router.register(r'move/(?P<bucket_name>[a-z0-9-]{3,64})', views.MoveViewSet, base_name='move')


urlpatterns = [
    path('', include(router.urls)), # The API URLs are now determined automatically by the router.
    path('', include(detail_router.urls)),
    path('', include(dlp_router.urls)),
    path('jwt-token/', obtain_jwt_token),
    path('jwt-token-refresh/', refresh_jwt_token),
    path('auth-token/', obtain_auth_token),
]

