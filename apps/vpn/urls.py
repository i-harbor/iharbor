from django.conf.urls import url

from . import views

app_name = "vpn"

urlpatterns = [
    url(r'^usage/$', views.usage, name='usage'),
]
