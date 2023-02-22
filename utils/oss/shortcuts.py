from django.conf import settings

from .pyrados import HarborObject, RadosError


def build_harbor_object(using: str, pool_name: str, obj_id: str, obj_size: int = 0) -> HarborObject:
    """
    构建iharbor对象对应的ceph读写接口

    :param using: ceph集群配置别名，对应对象数据所在ceph集群
    :param pool_name: ceph存储池名称，对应对象数据所在存储池名称; 当值为None时，pool name从django settings中获取
    :param obj_id: 对象在ceph存储池中对应的rados名称
    :param obj_size: 对象的大小
    """
    cephs = settings.CEPH_RADOS

    if using not in cephs:
        raise RadosError(f'别名为"{using}"的CEPH集群信息未配置，请确认配置文件中的“CEPH_RADOS”配置内容')

    ceph = cephs[using]
    cluster_name = ceph['CLUSTER_NAME']
    user_name = ceph['USER_NAME']
    conf_file = ceph['CONF_FILE_PATH']
    keyring_file = ceph['KEYRING_FILE_PATH']
    if pool_name is None:
        pool_name = ceph['POOL_NAME'][0]

    return HarborObject(pool_name=pool_name, obj_id=obj_id, obj_size=obj_size, cluster_name=cluster_name,
                        user_name=user_name, conf_file=conf_file, keyring_file=keyring_file, alise_cluster=using)


def build_rados_harbor_object(
        obj, obj_rados_key: str, obj_size: int = None, use_settings: bool = True
) -> HarborObject:
    """
    构建iharbor对象对应的ceph读写接口

    :param obj: ceph集群配置别名，对应对象数据所在ceph集群; type: BucketFileBase
    :param obj_rados_key: 对象在ceph存储池中对应的rados名称
    :param obj_size: 对象的大小，默认从obj获取；
    :param use_settings: 默认True(ceph配置和pool name从django settings中获取); False(实时查询数据库获取)
    """
    cephs = settings.CEPH_RADOS
    using = str(obj.get_pool_id())
    if using not in cephs:
        raise RadosError(f'别名为"{using}"的CEPH集群信息未配置，请确认配置文件中的“CEPH_RADOS”配置内容')

    if use_settings:
        pool_name = None
    else:
        pool_name = obj.get_pool_name()

    if obj_size is None:
        obj_size = obj.obj_size

    return build_harbor_object(
        using=using, pool_name=pool_name, obj_id=obj_rados_key, obj_size=obj_size
    )
