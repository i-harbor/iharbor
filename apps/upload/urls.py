from django.conf.urls import url
from django.contrib.auth.decorators import login_required

from . import views

urlpatterns = [
    url(r'^bucket/(?P<bucket_name>[\w-]{3,50})', views.file_list, name='file_list'),
    url(r'^$', login_required(views.BucketView.as_view()), name='bucket_view'),
    url(r'^download/(?P<uuid>[\w-]{32,36})/', views.download, name='download'),
    url(r'^delete/(?P<uuid>[\w-]{32,36})/', views.delete, name='delete'),
]


