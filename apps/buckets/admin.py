from django.contrib import admin

from .models import (Bucket, BucketLimitConfig, ApiUsageDescription)
# Register your models here.

@admin.register(Bucket)
class BucketAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'get_collection_name', 'created_time', 'user', 'objs_count',
                    'size', 'soft_delete', 'ftp_enable', 'ftp_password', 'modified_time')
    list_display_links = ('id', 'name')

    list_filter = ('user', 'created_time', 'soft_delete')
    search_fields = ('name', 'user__username')  # 搜索字段

    def get_collection_name(self, obj):
        return obj.get_bucket_table_name()

    get_collection_name.short_description = '桶的表名'


@admin.register(BucketLimitConfig)
class BucketLimitConfigAdmin(admin.ModelAdmin):
    list_display = ('id', 'user', 'limit')
    list_display_links = ('id', 'user')

    list_editable = ('limit',)  # 列表可编辑字段
    search_fields = ('user__username',)  # 搜索字段


@admin.register(ApiUsageDescription)
class UsageDescAdmin(admin.ModelAdmin):
    list_display = ('id', 'title', 'desc_for', 'modified_time')
    list_display_links = ('id', 'title')
    search_fields = ('title', 'content')  # 搜索字段

    def get_desc_for(self, obj):
        return obj.get_desc_for_display()
