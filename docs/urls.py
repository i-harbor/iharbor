from django.conf.urls import url

from . import views

app_name = 'docs'

urlpatterns = [
    url(r'^$', views.docs, name='docs'),
]