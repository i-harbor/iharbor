from django.contrib import admin

from .models import UserProfile
# Register your models here.

admin.site.site_header = 'EVOBcloud管理'
admin.site.site_title = 'EVOBcloud站点管理'


@admin.register(UserProfile)
class UserProfileAdmin(admin.ModelAdmin):
    list_display = ('id', 'username', 'email')
    list_display_links = ('id', 'username')



