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
from rest_framework.documentation import include_docs_urls
from django.contrib.staticfiles.views import serve

urlpatterns = [
    url(r'favicon.ico', view=serve, kwargs={'path': 'images/icon/favicon.ico'}),
    url(r'^admin/', admin.site.urls),
    url(r'', include('buckets.urls', namespace='buckets')),
    url(r'^users/', include('users.urls', namespace='users')),
    url(r'api/', include('api.urls', namespace='api')),
    url(r'v1/docs/', include_docs_urls(title='EVOBCloud API Docs')),
]
