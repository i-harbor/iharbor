from django.contrib import admin

from .models import VPNUsageDescription, VPNAuth


@admin.register(VPNUsageDescription)
class VPNUsageDescriptionAdmin(admin.ModelAdmin):
    list_display = ('id', 'title', 'modified_time')
    list_display_links = ('id', 'title')

    list_editable = ()  # 列表可编辑字段
    search_fields = ('title', 'content')  # 搜索字段


@admin.register(VPNAuth)
class VPNAuthAdmin(admin.ModelAdmin):
    list_display = ('id', 'user', 'password', 'created_time', 'modified_time')
    list_display_links = ('id',)

    # list_editable = ('password',)  # 列表可编辑字段
    search_fields = ('user__username',)  # 搜索字段
    raw_id_fields = ('user',)
