from django.conf.urls import url, include
from rest_framework.routers import DefaultRouter

from . import views

# Create a router and register our viewsets with it.
router = DefaultRouter(trailing_slash=False)
router.register(r'obs', views.ObsViewSet, base_name='obs')


urlpatterns = [
    url(r'', include(router.urls)), # The API URLs are now determined automatically by the router.
]

