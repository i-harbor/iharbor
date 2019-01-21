from django.contrib import admin
from django.contrib.auth.admin import UserAdmin

from .models import UserProfile, Email
# Register your models here.

admin.site.site_header = 'EVHarbor管理'
admin.site.site_title = 'EVHarbor站点管理'


@admin.register(UserProfile)
class UserProfileAdmin(UserAdmin):
    list_display = ('id', 'username', 'fullname', 'company', 'telephone', 'is_active', 'is_staff')
    list_display_links = ('id', 'username')
    # list_filter = ('date_joined', 'is_superuser', 'is_staff')
    search_fields = ('username', 'company', 'first_name', 'last_name')  # 搜索字段

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
