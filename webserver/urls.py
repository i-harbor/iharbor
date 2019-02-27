"""webserver URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.11/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf import settings
from django.contrib import admin
from django.conf.urls import url, include
from django.contrib.staticfiles.views import serve
from rest_framework.routers import DefaultRouter
from rest_framework_swagger.views import get_swagger_view

from apps.share.views import ObsViewSet

# Create a router and register our viewsets with it.
router = DefaultRouter(trailing_slash=False)
router.register(r'obs', ObsViewSet, base_name='obs')


urlpatterns = [
    url(r'api/', include('api.urls', namespace='api')),
    url(r'evcloud/', include('evcloud.urls', namespace='evcloud')),
    url(r'share/', include('share.urls', namespace='share')),
    url(r'', include('buckets.urls', namespace='buckets')), # 注意顺序
    url(r'', include(router.urls, namespace='obs')),        # 注意顺序
    url(r'^users/', include('users.urls', namespace='users')),
    url(r'vpn/', include('vpn.urls', namespace='vpn')),
    url(r'^admin/', admin.site.urls),
    url(r'favicon.ico', view=serve, kwargs={'path': 'images/icon/favicon.ico'}),
    url(r'apidocs/', get_swagger_view(title='EVHarbor API'), name='apidocs'),
    url(r'^docs/', include('docs.urls', namespace='docs')),
]

if settings.DEBUG:
    import debug_toolbar
    urlpatterns = [
        # For django versions before 2.0:
        url(r'^__debug__/', include(debug_toolbar.urls)),

    ] + urlpatterns
