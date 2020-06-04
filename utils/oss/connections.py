import threading

import rados
from django.conf import settings


class ConnectionHandler:
    def __init__(self):
        self._connections = threading.local()

    def __getitem__(self, alias):
        if hasattr(self._connections, alias):
            return getattr(self._connections, alias)

        return None

    def __setitem__(self, key, value):
        setattr(self._connections, key, value)

    def __delitem__(self, key):
        delattr(self._connections, key)

    def all(self):
        return [self[alias] for alias in self]

    def close_all(self):
        for alias in self:
            try:
                connection = getattr(self._connections, alias)
            except AttributeError:
                continue
            connection.shutdown()


connection_pools = ConnectionHandler()


def get_ceph_setting(alias:str='default'):
    d = {}
    d['cluster_name'] = settings.CEPH_RADOS.get('CLUSTER_NAME', 'ceph')
    d['username'] = settings.CEPH_RADOS.get('USER_NAME', '')
    d['conf_file'] = settings.CEPH_RADOS.get('CONF_FILE_PATH', '')
    d['keyring_file'] = settings.CEPH_RADOS.get('KEYRING_FILE_PATH', '')
    return d


def get_connection(alias:str='default'):
    '''
    获取指定ceph集群的链接

    :return:
        failed: None
        success: Rados()
    :raises: class:`RadosError`
    '''
    conn = connection_pools[alias]
    if conn and conn.state == 'connected':
        return conn

    s = get_ceph_setting()
    conn = new_connection(**s)
    connection_pools[alias] = conn
    return connection_pools[alias]


def new_connection(cluster_name:str, username: str, conf_file:str, keyring_file:str, **kwargs):
    '''
    创建一个ceph集群的链接

    :param cluster_name:
    :param username:
    :param conf_file:
    :param keyring_file:
    :return:
        rados.Rados()
    :raises: rados.Error
    '''
    conf = dict(keyring=keyring_file) if keyring_file else None
    cluster = rados.Rados(name=username, clustername=cluster_name, conffile=conf_file, conf=conf)
    try:
        cluster.connect(timeout=5)
    except rados.Error as e:
        msg = e.args[0] if e.args else 'error connecting to the cluster'
        raise rados.Error(message=msg, errno=e.errno)

    return cluster
