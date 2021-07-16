from django.contrib import admin, messages

from .models import (Bucket, BucketLimitConfig, ApiUsageDescription, Archive, BucketToken)


def bucket_stats(modeladmin, request, queryset):
    exc = None
    failed_count = 0
    success_count = 0
    for obj in queryset:
        try:
            obj.update_stats()
            success_count = success_count + 1
        except Exception as e:
            failed_count = failed_count + 1
            exc = e
            continue

    if exc is not None:
        msg = f'更新存储桶统计信息，{success_count}个成功，{failed_count}个失败，error: {exc}'
        modeladmin.message_user(request=request, message=msg, level=messages.ERROR)
    else:
        msg = f'成功更新{success_count}个存储桶统计信息'
        modeladmin.message_user(request=request, message=msg, level=messages.SUCCESS)


bucket_stats.short_description = "更新存储桶统计信息"


class NoDeleteSelectModelAdmin(admin.ModelAdmin):
    def get_actions(self, request):
        actions = super(NoDeleteSelectModelAdmin, self).get_actions(request)
        if 'delete_selected' in actions:
            del actions['delete_selected']

        return actions


@admin.register(Bucket)
class BucketAdmin(NoDeleteSelectModelAdmin):
    list_display = ('id', 'name', 'get_collection_name', 'type', 'created_time', 'lock', 'user', 'objs_count',
                    'size', 'ftp_enable', 'ftp_password', 'ftp_ro_password', 'modified_time')
    list_display_links = ('id', 'name')
    list_editable = ('lock',)
    list_filter = ('created_time',)
    search_fields = ('name', 'user__username')  # 搜索字段
    readonly_fields = ('collection_name', )
    raw_id_fields = ('user',)
    actions = [bucket_stats]

    def get_collection_name(self, obj):
        return obj.get_bucket_table_name()

    get_collection_name.short_description = '桶的表名'


@admin.register(Archive)
class BucketArchiveAdmin(NoDeleteSelectModelAdmin):
    list_display = ('id', 'original_id', 'name', 'type', 'table_name', 'archive_time', 'created_time', 'user', 'objs_count',
                    'size', 'ftp_enable', 'ftp_password', 'modified_time')
    list_display_links = ('id', 'name')

    list_filter = ('created_time',)
    search_fields = ('name', 'user__username')  # 搜索字段
    readonly_fields = ('table_name', 'original_id')
    actions = [bucket_stats]


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

    @staticmethod
    def get_desc_for(obj):
        return obj.get_desc_for_display()


@admin.register(BucketToken)
class BucketTokenAdmin(admin.ModelAdmin):
    list_display = ('key', 'permission', 'created', 'bucket')
    list_display_links = ('key', )
    search_fields = ('key', 'bucket__name')
