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
from django.urls import path, include
from rest_framework_swagger.views import get_swagger_view

from .views import kjy_login_callback


urlpatterns = [
    path('api/', include('api.urls', namespace='api')),
    path('evcloud/', include('evcloud.urls', namespace='evcloud')),
    path('obs/', include('share.urls', namespace='obs')),
    path('', include('buckets.urls', namespace='buckets')),
    path('users/', include('users.urls', namespace='users')),
    path('vpn/', include('vpn.urls', namespace='vpn')),
    path('admin/', admin.site.urls),
    path('apidocs/', get_swagger_view(title='EVHarbor API'), name='apidocs'),
    path('docs/', include('docs.urls', namespace='docs')),
    path('callback/', kjy_login_callback, name='callback'),
]


if settings.DEBUG:
    import debug_toolbar
    urlpatterns = [
        path('__debug__/', include(debug_toolbar.urls)),

    ] + urlpatterns
