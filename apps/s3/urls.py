from django.urls import path, include

from s3.views import MainHostViewSet, LocationHostViewSet
from .routers import NoDetailRouter


router = NoDetailRouter(trailing_slash=False)
router.register(r'', MainHostViewSet, basename='main-host')
router.register(r'.+', LocationHostViewSet, basename='location-host')


urlpatterns = [
    path('', include(router.urls)),
]
