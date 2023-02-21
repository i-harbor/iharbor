from django.conf import settings

from ceph.ceph_settings import ceph_settings_update
from ceph.models import CephCluster
from s3.models import MultipartUpload
from buckets.utils import is_model_table_exists, create_table_for_model_class


def get_or_create_ceph_cluster():
    ceph_configs = settings.TEST_CASE.get('CEPH_CLUSTER', None)
    if not ceph_configs:
        raise ValueError('test配置文件中未配置”TEST_CASE.CEPH_CLUSTER“')

    for _id, ceph_config in ceph_configs.items():

        with open(ceph_config['config_filename'], 'rt') as f:
            config_text = f.read()

        with open(ceph_config['keyring_filename'], 'rt') as f:
            keyring_text = f.read()

        cluster = CephCluster(
            id=int(_id),
            name=ceph_config['name'],
            cluster_name=ceph_config['cluster_name'],
            user_name=ceph_config['username'],
            pool_names=ceph_config['pool_names'],
            disable_choice=ceph_config['disable_choice'],
            config=config_text,
            keyring=keyring_text,
            priority_stored_value=ceph_config['priority_stored_value']
        )
        cluster.save(force_insert=True)


def config_ceph_clustar_settings():
    get_or_create_ceph_cluster()
    ceph_settings_update()


def ensure_s3_multipart_table_exists():
    ok = is_model_table_exists(model=MultipartUpload)
    if ok:
        return

    create_table_for_model_class(model=MultipartUpload)
