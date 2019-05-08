from django.conf.urls import url
from django.contrib.auth.decorators import login_required

from . import views

app_name = "buckets"

urlpatterns = [
    url(r'^$', login_required(views.BucketView.as_view()), name='bucket_view'),
    url(r'^usage/', views.UsageView.as_view(), name='usage'),
]


