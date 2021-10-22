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
import os

from django.conf import settings
from django.contrib import admin
from django.urls import path, include
from django.conf.urls import i18n
from django.views.i18n import JavaScriptCatalog
from rest_framework import permissions
from drf_yasg.views import get_schema_view
from drf_yasg import openapi

from utils.oss.pyrados import build_harbor_object
from .views import kjy_login_callback

schema_view = get_schema_view(
   openapi.Info(
      title="iHarbor API",
      default_version='v1',
   ),
   public=False,
   permission_classes=(permissions.AllowAny,),
)

def check_ceph_settins():
    def raise_msg(msg):
        print(msg)
        raise Exception(msg)

    cephs = getattr(settings, 'CEPH_RADOS', None)
    if not cephs:
        raise_msg('未配置CEPH集群信息，配置文件中配置“CEPH_RADOS”')

    if 'default' not in cephs:
        raise_msg('配置文件中CEPH集群信息配置“CEPH_RADOS”中必须存在一个别名“default”')

    enable_choices = []
    for using in cephs:
        if len(using) >= 16:
            raise_msg(f'CEPH集群配置“CEPH_RADOS”中，别名"{using}"太长，不能超过16字符')

        ceph = cephs[using]
        conf_file = ceph['CONF_FILE_PATH']
        if not os.path.exists(conf_file):
            raise_msg(f'别名为“{using}”的CEPH集群配置文件“{conf_file}”不存在')

        keyring_file = ceph['KEYRING_FILE_PATH']
        if not os.path.exists(keyring_file):
            raise_msg(f'别名为“{using}”的CEPH集群keyring配置文件“{keyring_file}”不存在')

        if 'USER_NAME' not in ceph:
            raise_msg(f'别名为“{using}”的CEPH集群配置信息未设置“USER_NAME”')

        if 'POOL_NAME' not in ceph:
            raise_msg(f'别名为“{using}”的CEPH集群配置信息未设置“POOL_NAME”')

        if not (isinstance(ceph['POOL_NAME'], str) or isinstance(ceph['POOL_NAME'], tuple)):
            raise_msg(f'别名为“{using}”的CEPH集群配置信息“POOL_NAME”必须是str或者tuple')

        ho = build_harbor_object(using=using, pool_name='', obj_id='')
        try:
            with ho.rados:
                pass
        except Exception as e:
            raise_msg(f'别名为“{using}”的CEPH集群连接错误，{str(e)}')

        if ('DISABLE_CHOICE' in ceph) and (ceph['DISABLE_CHOICE'] is True):
            continue

        enable_choices.append(using)

    if not enable_choices:
        raise_msg('没有可供选择的CEPH集群配置，创建bucket时没有可供选择的CEPH集群，'
                  '请至少确保有一个CEPH集群配置“DISABLE_CHOICE”为False')


check_ceph_settins()


urlpatterns = [
    path('api/', include('api.urls', namespace='api')),
    path('share/', include('share.urls', namespace='share')),
    path('', include('buckets.urls', namespace='buckets')),
    path('users/', include('users.urls', namespace='users')),
    path('admin/', admin.site.urls),
    path('apidocs/', schema_view.with_ui('swagger', cache_timeout=0), name='apidocs'),
    path('redoc/', schema_view.with_ui('redoc', cache_timeout=0), name='redoc'),
    path('docs/', include('docs.urls', namespace='docs')),
    path('callback/', kjy_login_callback, name='callback'),
    path('i18n/', include(i18n)),
    path('jsi18n/', JavaScriptCatalog.as_view(), name='javascript-catalog'),
]


if settings.DEBUG:
    import debug_toolbar
    urlpatterns = [
        path('__debug__/', include(debug_toolbar.urls)),

    ] + urlpatterns
