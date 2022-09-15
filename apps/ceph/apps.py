from django.apps import AppConfig
from django.core.checks import register, Tags

from webserver.checks import check_ceph_settings


class CephConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'ceph'

    def ready(self):
        # 服务启动后的ceph初始化的操作
        from ceph import ceph_settings
        ceph_settings.ceph_settings_update()
        register(check_ceph_settings, Tags.security, deploy=True)
