from django.apps import AppConfig


class CephConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'ceph'

    def ready(self):
        # 服务启动后的ceph初始化的操作
        from ceph import ceph_settings
        ceph_settings.ceph_settings_update()

