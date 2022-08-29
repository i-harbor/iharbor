from django.apps import AppConfig
from django.core.checks import register

from webserver import checks


class BucketsConfig(AppConfig):
    name = 'buckets'
    verbose_name = '存储桶管理'

    # def ready(self):
    #     register(checks.check_ceph_settins)
