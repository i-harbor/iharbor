from django.contrib import admin, messages

from .models import (
    Bucket, BucketLimitConfig, ApiUsageDescription, Archive, BucketToken, get_encryptor,
    BackupBucket
)
from .forms import BackupBUcketModelForm


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


def bucket_ftp_password_encrypt(modeladmin, request, queryset):
    def try_encrypt_password(bucket):
        try:
            need_save = False
            encryptor = get_encryptor()
            if not encryptor.is_encrypted(bucket.ftp_password):       # 不是有效的加密字符串
                if not bucket.ftp_password.startswith(encryptor.prefix):
                    bucket.set_ftp_password(bucket.ftp_password)
                    need_save = True

            if not encryptor.is_encrypted(bucket.ftp_ro_password):
                if not bucket.ftp_ro_password.startswith(encryptor.prefix):
                    bucket.set_ftp_ro_password(bucket.ftp_ro_password)
                    need_save = True

            if need_save:
                bucket.save(update_fields=['ftp_password', 'ftp_ro_password'])
        except Exception as e:
            return e

        return None

    exc = None
    failed_count = 0
    success_count = 0
    for bkt in queryset:
        exc = try_encrypt_password(bkt)
        if exc is None:
            success_count = success_count + 1
        else:
            failed_count = failed_count + 1
            continue

    if failed_count > 0:
        msg = f'更新存储桶统计信息，{success_count}个成功，{failed_count}个失败; error: {exc}'
        modeladmin.message_user(request=request, message=msg, level=messages.ERROR)
    else:
        msg = f'成功更新{success_count}个存储桶统计信息'
        modeladmin.message_user(request=request, message=msg, level=messages.SUCCESS)


bucket_ftp_password_encrypt.short_description = "加密ftp密码"


class NoDeleteSelectModelAdmin(admin.ModelAdmin):
    def get_actions(self, request):
        actions = super(NoDeleteSelectModelAdmin, self).get_actions(request)
        if 'delete_selected' in actions:
            del actions['delete_selected']

        return actions


@admin.register(Bucket)
class BucketAdmin(NoDeleteSelectModelAdmin):
    list_display = ('id', 'name', 'get_collection_name', 'type', 'created_time', 'lock', 'user', 'objs_count',
                    'size', 'stats_time', 'ftp_enable', 'raw_ftp_password', 'ftp_password', 'raw_ftp_ro_password',
                    'ftp_ro_password', 'modified_time', 'ceph_using')
    list_display_links = ('id', 'name')
    list_editable = ('lock',)
    list_filter = ('created_time',)
    search_fields = ('name', 'user__username')  # 搜索字段
    readonly_fields = ('collection_name', 'ceph_using')
    raw_id_fields = ('user',)
    actions = [bucket_stats, bucket_ftp_password_encrypt]

    def get_collection_name(self, obj):
        return obj.get_bucket_table_name()

    get_collection_name.short_description = '桶的表名'

    def delete_model(self, request, obj):
        if not obj.delete_and_archive():  # 删除归档
            self.message_user(request=request, message='删除归档操作失败', level=messages.ERROR)

    def delete_queryset(self, request, queryset):
        """Given a queryset, delete it from the database."""
        self.message_user(request=request, message='不允许删除操作', level=messages.ERROR)


@admin.register(Archive)
class BucketArchiveAdmin(NoDeleteSelectModelAdmin):
    list_display = ('id', 'original_id', 'name', 'type', 'table_name', 'archive_time', 'created_time', 'user', 'objs_count',
                    'size', 'ftp_enable', 'ftp_password', 'modified_time', 'ceph_using')
    list_display_links = ('id', 'name')

    list_filter = ('created_time',)
    search_fields = ('name', 'user__username')  # 搜索字段
    readonly_fields = ('table_name', 'original_id', 'ceph_using')
    actions = [bucket_stats, 'resume_archive_to_bucket']

    def delete_model(self, request, obj):
        self.message_user(request=request, message='不允许删除归档记录', level=messages.ERROR)

    def delete_queryset(self, request, queryset):
        """Given a queryset, delete it from the database."""
        self.message_user(request=request, message='不允许删除操作', level=messages.ERROR)

    @admin.action(description="从归档恢复存储桶")
    def resume_archive_to_bucket(self, request, queryset):
        length = len(queryset)
        if length == 0:
            self.message_user(request=request, message='你没有选中任何一个桶归档记录', level=messages.ERROR)
            return
        if length > 1:
            self.message_user(request=request, message='每次只能选中一个桶归档记录恢复', level=messages.ERROR)
            return

        obj = queryset[0]
        try:
            obj.resume_archive()
        except Exception as e:
            self.message_user(request=request, message=f'恢复存储桶失败，{str(e)}', level=messages.ERROR)
            return

        self.message_user(request=request, message='恢复存储桶成功', level=messages.SUCCESS)


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


@admin.register(BackupBucket)
class BackupBucketAdmin(admin.ModelAdmin):
    form = BackupBUcketModelForm

    list_display = ('id', 'bucket', 'endpoint_url', 'bucket_name', 'bucket_token', 'backup_num', 'status',
                    'created_time', 'remarks')
    list_display_links = ('id', )
    list_select_related = ('bucket',)
    raw_id_fields = ('bucket',)
    search_fields = ('key', 'bucket__name')
