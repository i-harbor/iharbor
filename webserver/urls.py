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
from django.contrib import admin
from django.conf.urls import url, include
from django.contrib.staticfiles.views import serve
# from rest_framework.documentation import include_docs_urls
# from rest_framework_swagger.views import get_swagger_view


urlpatterns = [
    url(r'api/', include('api.urls', namespace='api')),
    url(r'share/', include('share.urls', namespace='share')),
    url(r'', include('buckets.urls', namespace='buckets')),
    url(r'^users/', include('users.urls', namespace='users')),
    url(r'^admin/', admin.site.urls),
    url(r'favicon.ico', view=serve, kwargs={'path': 'images/icon/favicon.ico'}),
    # url(r'docs/', include_docs_urls(title='EVHarbor API Docs')),
    # url(r'docs/', get_swagger_view(title='EVHarbor API')),
]
