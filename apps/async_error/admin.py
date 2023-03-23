from django.contrib import admin
from .models import BucketAsyncError


# Register your models here.


@admin.register(BucketAsyncError)
class BucketAsyncErrorAdmin(admin.ModelAdmin):
    list_display_links = ('id', 'node_ip')
    list_display = ('id', 'node_ip', 'bucket_id', 'bucket_name', 'object_id', 'object_name', 'async_error', 'backup_ip',
                    'backup_bucket', 'error_time', 'node_num', 'node_count', 'bucketlist', 'thread_num')
    search_fields = ['node_ip']
