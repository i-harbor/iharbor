from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from django.utils.translation import ugettext_lazy as _

from .models import UserProfile, Email, AuthKey
# Register your models here.

admin.site.site_header = 'EVHarbor管理'
admin.site.site_title = 'EVHarbor站点管理'


@admin.register(UserProfile)
class UserProfileAdmin(UserAdmin):
    list_display = ('id', 'username', 'fullname', 'company', 'telephone', 'is_active', 'is_staff')
    list_display_links = ('id', 'username')
    # list_filter = ('date_joined', 'is_superuser', 'is_staff')
    search_fields = ('username', 'company', 'first_name', 'last_name')  # 搜索字段

    fieldsets = (
        (None, {'fields': ('username', 'password')}),
        (_('Personal info'), {'fields': ('first_name', 'last_name', 'email', 'company', 'telephone','secret_key')}),
        (_('Permissions'), {'fields': ('is_active', 'is_staff', 'is_superuser',
                                       'groups', 'user_permissions')}),
        (_('Important dates'), {'fields': ('last_login', 'date_joined')}),
    )

    add_fieldsets = (
        (None, {
            'classes': ('wide',),
            'fields': ('username', 'password1', 'password2', 'company', 'first_name', 'last_name', 'telephone'),
        }),
    )

    def fullname(self, obj):
        return obj.get_full_name()
    fullname.short_description = '姓名'

@admin.register(Email)
class UserProfileAdmin(admin.ModelAdmin):
    list_display = ('id', 'sender', 'receiver', 'send_time')
    list_display_links = ('id',)
    list_filter = ('send_time',)
    search_fields = ('receiver',)  # 搜索字段


@admin.register(AuthKey)
class AuthKeyAdmin(admin.ModelAdmin):
    list_display = ('id', 'secret_key', 'create_time', 'state', 'permission', 'user')
    list_display_links = ('id',)
    list_filter = ('state', 'create_time')
    list_editable = ('state', 'permission')  # 列表可编辑字段
    search_fields = ('id', 'secret_key', 'user__username')  # 搜索字段

