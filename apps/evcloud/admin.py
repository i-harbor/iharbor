from django.contrib import admin
from .models import VMLimit, VMConfig, APIAuth, EvcloudVM
# Register your models here.

@admin.register(VMLimit)
class VMLimitAdmin(admin.ModelAdmin):
    list_display = ('id', 'user', 'limit')
    list_display_links = ('id', 'user', 'limit')
    list_filter = ('user', 'limit')

@admin.register(VMConfig)
class VMConfigAdmin(admin.ModelAdmin):
    list_display = ('id', 'cpu', 'mem', 'time')
    list_display_links = ('id', 'cpu', 'mem', 'time')
    list_filter = ('cpu', 'mem', 'time')

@admin.register(APIAuth)
class APIAuthAdmin(admin.ModelAdmin):
    list_display = ('id', 'url', 'name', 'pwd', 'flag')
    list_display_links = ('id', 'url', 'name', 'pwd', 'flag')
    list_filter = ('url', 'name', 'pwd', 'flag')

@admin.register(EvcloudVM)
class EvcloudVMAdmin(admin.ModelAdmin):
    list_display = ('vm_id', 'user', 'created_time', 'end_time')
    list_display_links = ('vm_id', 'user', 'created_time', 'end_time')
    list_filter = ('vm_id', 'user', 'created_time', 'end_time')