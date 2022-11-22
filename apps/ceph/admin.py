from django.contrib import admin
from .models import CephCluster


# Register your models here.


@admin.register(CephCluster)
class CephClusterAdmin(admin.ModelAdmin):
    list_display_links = ('name',)
    list_display = ('id', 'name', 'alias', 'cluster_name', 'user_name', 'disable_choice', 'pool_names', 'config_file',
                    'keyring_file', 'modified_time', 'remarks')
    search_fields = ['name']
