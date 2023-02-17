from django.contrib import admin
from .models import CephCluster


# Register your models here.


@admin.register(CephCluster)
class CephClusterAdmin(admin.ModelAdmin):
    list_display_links = ('name',)
    list_display = ('id', 'name', 'disable_choice', 'priority_stored_value', 'cluster_name', 'user_name', 'pool_names',
                    'config_file', 'keyring_file', 'modified_time', 'remarks')
    search_fields = ['name']
