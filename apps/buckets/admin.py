from django.contrib import admin

from .models import Bucket
# Register your models here.

@admin.register(Bucket)
class BucketAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'collection_name', 'created_time', 'user', 'soft_delete')
    list_display_links = ('id', 'name')

    list_filter = ('user', 'created_time')

