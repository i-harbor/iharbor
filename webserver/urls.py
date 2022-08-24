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
from django.conf.urls import i18n
from django.shortcuts import render
from django.views.i18n import JavaScriptCatalog
from rest_framework import permissions
from drf_yasg.views import get_schema_view
from drf_yasg import openapi

from .views import kjy_login_callback, home
from version import __version__, __version_timestamp__
from . import admin_site    # admin后台一些设置


def about(request):
    print(__version_timestamp__)
    return render(request, 'about.html', context={'version': __version__, 'version_timestamp':__version_timestamp__})

schema_view = get_schema_view(
   openapi.Info(
      title="iHarbor API",
      default_version='v1',
   ),
   public=False,
   permission_classes=(permissions.AllowAny,),
)


urlpatterns = [
    path('api/', include('api.urls', namespace='api')),
    path('share/', include('share.urls', namespace='share')),
    path('', home, name='home'),
    path('', include('buckets.urls', namespace='buckets')),
    path('users/', include('users.urls', namespace='users')),
    path('admin/', admin.site.urls),
    path('apidocs/', schema_view.with_ui('swagger', cache_timeout=0), name='apidocs'),
    path('redoc/', schema_view.with_ui('redoc', cache_timeout=0), name='redoc'),
    path('docs/', include('docs.urls', namespace='docs')),
    path('callback/', kjy_login_callback, name='callback'),
    path('i18n/', include(i18n)),
    path('jsi18n/', JavaScriptCatalog.as_view(), name='javascript-catalog'),
    path('about/', about, name="about"),
]


if settings.DEBUG:
    import debug_toolbar
    urlpatterns = [
        path('__debug__/', include(debug_toolbar.urls)),

    ] + urlpatterns
