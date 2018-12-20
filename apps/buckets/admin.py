from django.contrib import admin

from .models import Bucket, BucketLimitConfig, BucketFileInfoBase
# Register your models here.

@admin.register(Bucket)
class BucketAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'collection_name', 'created_time', 'user', 'soft_delete')
    list_display_links = ('id', 'name')

    list_filter = ('user', 'created_time')
    search_fields = ('name', 'user__username')  # 搜索字段


@admin.register(BucketLimitConfig)
class BucketLimitConfigAdmin(admin.ModelAdmin):
    list_display = ('id', 'user', 'limit')
    list_display_links = ('id', 'user')

    list_editable = ('limit',)  # 列表可编辑字段
    search_fields = ('user__username',)  # 搜索字段


