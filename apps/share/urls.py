from django.urls import path, include, re_path
from rest_framework.routers import DefaultRouter

from . import views


app_name = "share"
# Create a router and register our viewsets with it.
router = DefaultRouter(trailing_slash=False)
router.register(r'obs', views.ObsViewSet, basename='obs')
router.register(r'list', views.ShareDirViewSet, basename='list')
router.register(r'sd', views.ShareDownloadViewSet, basename='download')


urlpatterns = [
    path('', include(router.urls)),     # The API URLs are now determined automatically by the router.
    re_path(r'^s/(?P<share_base>.+)', views.ShareView.as_view(), name='share-view'),
]
