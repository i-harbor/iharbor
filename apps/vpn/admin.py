from django.contrib import admin

from .models import VPNUsageDescription
# Register your models here.
@admin.register(VPNUsageDescription)
class BucketLimitConfigAdmin(admin.ModelAdmin):
    list_display = ('id', 'title', 'modified_time')
    list_display_links = ('id', 'title')

    list_editable = ()  # 列表可编辑字段
    search_fields = ('title', 'content')  # 搜索字段

