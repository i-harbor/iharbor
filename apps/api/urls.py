from django.conf.urls import url, include
from rest_framework.routers import DefaultRouter
from rest_framework_jwt.views import obtain_jwt_token, refresh_jwt_token

from .auth import obtain_auth_token
from . import views
from .routers import DetailPostRouter, DetailListPostRouter
from users.auth.views import ObtainAuthKey

app_name = "api"

# Create a router and register our viewsets with it.
router = DefaultRouter()
router.register(r'(?P<version>(v1|v2))/users', views.UserViewSet, base_name='user')
router.register(r'(?P<version>(v1|v2))/buckets', views.BucketViewSet, base_name='buckets')
# router.register(r'(?P<version>(v1|v2))/obj/(?P<bucket_name>[a-z0-9-]{3,64})', views.ObjViewSet, base_name='obj')
router.register(r'(?P<version>(v1|v2))/auth-key', ObtainAuthKey, base_name='auth-key')
router.register(r'(?P<version>(v1|v2))/stats/bucket', views.BucketStatsViewSet, base_name='stats_bucket')
router.register(r'(?P<version>(v1|v2))/stats/ceph', views.CephStatsViewSet, base_name='stats_ceph')
router.register(r'(?P<version>(v1|v2))/stats/user', views.UserStatsViewSet, base_name='stats_user')
router.register(r'(?P<version>(v1|v2))/stats/visit', views.VisitStatsViewSet, base_name='stats_visit')
router.register(r'(?P<version>(v1|v2))/security', views.SecurityViewSet, base_name='security')
router.register(r'(?P<version>(v1|v2))/metadata/(?P<bucket_name>[a-z0-9-]{3,64})', views.MetadataViewSet,
                base_name='metadata')
router.register(r'(?P<version>(v1|v2))/ceph/comp', views.CephComponentsViewSet, base_name='ceph_components')
router.register(r'(?P<version>(v1|v2))/ceph/perf', views.CephPerformanceViewSet, base_name='ceph_performance')
router.register(r'(?P<version>(v1|v2))/ceph/errors', views.CephErrorViewSet, base_name='ceph_errors')
router.register(r'(?P<version>(v1|v2))/usercount', views.UserCountViewSet, base_name='usercount')
router.register(r'(?P<version>(v1|v2))/availability', views.AvailabilityViewSet, base_name='availability')
router.register(r'(?P<version>(v1|v2))/test', views.TestViewSet, base_name='test')
router.register(r'(?P<version>(v1|v2))/ftp', views.FtpViewSet, base_name='ftp')


dlp_router = DetailListPostRouter()
dlp_router.register(r'(?P<version>(v1|v2))/dir/(?P<bucket_name>[a-z0-9-]{3,64})', views.DirectoryViewSet,
                       base_name='dir')

detail_router = DetailPostRouter()
detail_router.register(r'(?P<version>(v1|v2))/obj/(?P<bucket_name>[a-z0-9-]{3,64})', views.ObjViewSet, base_name='obj')
detail_router.register(r'(?P<version>(v1|v2))/move/(?P<bucket_name>[a-z0-9-]{3,64})', views.MoveViewSet,
                       base_name='move')


urlpatterns = [
    url(r'^', include(router.urls)), # The API URLs are now determined automatically by the router.
    url(r'^', include(detail_router.urls)),
    url(r'^', include(dlp_router.urls)),
    url(r'^(?P<version>(v1|v2))/jwt-token/', obtain_jwt_token),
    url(r'^(?P<version>(v1|v2))/jwt-token-refresh/', refresh_jwt_token),
    url(r'^(?P<version>(v1|v2))/auth-token/', obtain_auth_token),
]

