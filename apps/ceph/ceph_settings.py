from django.conf import settings
from ceph.models import CephCluster
from django.core.checks import Warning
from webserver.checks import check_ceph_settins


# 根据数据库信息配置settings

def ceph_settings_update():
    ceph_cluster = {}
    errors = []
    # 在数据迁移的时候会出错，要将一下内容注释后在迁移
    ceph_cluster_list = CephCluster.objects.all()
    if not ceph_cluster_list:
        errors.append(Warning('未配置CEPH集群信息，服务启动后需要先登录后端配置ceph集群信息。'))
        return errors

    for ceph_cluster_info in ceph_cluster_list:
        # 将配置文先保存到本地
        ceph_cluster_info.save()
        # 默认是最新的配置文件
        ceph_pool_conf = {
            'CLUSTER_NAME': ceph_cluster_info.cluster_name,
            'USER_NAME': ceph_cluster_info.user_name,
            'DISABLE_CHOICE': ceph_cluster_info.disable_choice,
            'CONF_FILE_PATH': ceph_cluster_info.config_file,
            'KEYRING_FILE_PATH': ceph_cluster_info.keyring_file,
            'POOL_NAME': tuple(ceph_cluster_info.pool_names),
        }
        ceph_cluster[ceph_cluster_info.alias] = ceph_pool_conf
    settings.CEPH_RADOS = ceph_cluster
    # 校验配置
    error = check_ceph_settins()
    if error:
        return error
    return errors

