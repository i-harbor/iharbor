from django.conf import settings

from ceph.ceph_settings import ceph_settings_update
from ceph.models import CephCluster
from s3.models import MultipartUpload
from buckets.utils import is_model_table_exists, create_table_for_model_class


def get_or_create_ceph_cluster():
    cluster = CephCluster.objects.filter(alias='default').first()
    if cluster:
        return cluster

    ceph = settings.TEST_CASE.get('CEPH_CLUSTER', None)
    if not ceph:
        raise ValueError('test配置文件中未配置”TEST_CASE.CEPH_CLUSTER“')

    with open(ceph['config_filename'], 'rt') as f:
        config_text = f.read()

    with open(ceph['keyring_filename'], 'rt') as f:
        keyring_text = f.read()

    cluster = CephCluster(
        name='test ceph',
        alias=ceph['alias'],
        cluster_name=ceph['cluster_name'],
        user_name=ceph['username'],
        pool_names=ceph['pool_names'],
        disable_choice=ceph['disable_choice'],
        config=config_text,
        keyring=keyring_text
    )
    cluster.save(force_insert=True)
    return cluster


def config_ceph_clustar_settings():
    get_or_create_ceph_cluster()
    ceph_settings_update()


def ensure_s3_multipart_table_exists():
    ok = is_model_table_exists(model=MultipartUpload)
    if ok:
        return

    create_table_for_model_class(model=MultipartUpload)
