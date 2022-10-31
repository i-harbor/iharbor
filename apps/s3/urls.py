from django.urls import path, include

from s3.views import MainHostViewSet
from .routers import NoDetailRouter


router = NoDetailRouter(trailing_slash=False)
router.register(r'.+', MainHostViewSet, basename='main-host')


urlpatterns = [
    path('', include(router.urls)),
]
