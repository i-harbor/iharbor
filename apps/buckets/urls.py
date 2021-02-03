from django.urls import path
from django.contrib.auth.decorators import login_required

from . import views

app_name = "buckets"

urlpatterns = [
    path('', login_required(views.BucketView.as_view()), name='bucket_view'),
    path('bucket/detail/<bucket_name>/', login_required(views.BucketDetailView.as_view()), name='bucket-detail'),
    path('usage/', views.UsageView.as_view(), name='api-usage'),
    path('ftp-usage/', views.FTPUsageView.as_view(), name='ftp-usage'),
    path('s3-api-usage/', views.S3ApiUsageView.as_view(), name='s3-api-usage'),
]


