from django.conf.urls import url, include
from rest_framework.routers import DefaultRouter

from . import views

# Create a router and register our viewsets with it.
router = DefaultRouter()
router.register(r'(?P<version>(v1|v2))/users', views.ShareViewSet, base_name='user')


urlpatterns = [
    # url(r'^', include(router.urls)), # The API URLs are now determined automatically by the router.

]

