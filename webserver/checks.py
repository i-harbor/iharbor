import os

from django.core.checks import Warning
from django.conf import settings

from utils.oss.shortcuts import build_harbor_object


def check_ceph_settings(**kwargs):
    errors = []
    cephs = getattr(settings, 'CEPH_RADOS', None)
    if not cephs:
        errors.append(Warning('未配置CEPH集群信息，配置文件中配置“CEPH_RADOS”', hint='CEPH_RADOS'))

    # if 'default' not in cephs:
    #     errors.append(Warning('配置文件中CEPH集群信息配置“CEPH_RADOS”中必须存在一个别名“default”', hint='CEPH_RADOS.default'))

    enable_choices = []
    for using in cephs:
        if len(using) >= 16:
            errors.append(Warning(f'CEPH集群配置“CEPH_RADOS”中，别名"{using}"太长，不能超过16字符'))

        ceph = cephs[using]
        conf_file = ceph['CONF_FILE_PATH']
        if not os.path.exists(conf_file):
            errors.append(Warning(f'别名为“{using}”的CEPH集群配置文件“{conf_file}”不存在'))

        keyring_file = ceph['KEYRING_FILE_PATH']
        if not os.path.exists(keyring_file):
            errors.append(Warning(f'别名为“{using}”的CEPH集群keyring配置文件“{keyring_file}”不存在'))

        if 'USER_NAME' not in ceph:
            errors.append(Warning(f'别名为“{using}”的CEPH集群配置信息未设置“USER_NAME”'))

        if 'POOL_NAME' not in ceph:
            errors.append(Warning(f'别名为“{using}”的CEPH集群配置信息未设置“POOL_NAME”'))

        if not (isinstance(ceph['POOL_NAME'], str) or isinstance(ceph['POOL_NAME'], tuple)):
            errors.append(Warning(f'别名为“{using}”的CEPH集群配置信息“POOL_NAME”必须是str或者tuple'))

        ho = build_harbor_object(using=using, pool_name='', obj_id='')
        try:
            with ho.rados:
                pass
        except Exception as e:
            errors.append(Warning(f'无法连接ceph，别名为“{using}”的CEPH集群连接错误，{str(e)}'))
        ho.rados.close_cluster_connect()
        if ('DISABLE_CHOICE' in ceph) and (ceph['DISABLE_CHOICE'] is True):
            continue

        enable_choices.append(using)

    if not enable_choices:
        errors.append(Warning('没有可供选择的CEPH集群配置，创建bucket时没有可供选择的CEPH集群，'
                  '请至少确保有一个CEPH集群配置“DISABLE_CHOICE”为False'))

    return errors
