import ctypes
import os

from django.conf import settings


rados_dll = ctypes.CDLL(settings.CEPH_RADOS.get('RADOS_DLL_PATH', ''))

class CephRadosObject():
    '''
    文件对象读写接口
    '''
    def __init__(self, obj_id, cluster_name=None , user_name=None, conf_file=None, pool_name=None, *args, **kwargs):
        self._cluster_name = cluster_name if cluster_name else settings.CEPH_RADOS.get('CLUSTER_NAME', 'ceph')
        self._user_name = user_name if user_name else settings.CEPH_RADOS.get('USER_NAME', 'client.objstore')
        self._conf_file = conf_file if os.path.exists(conf_file) else settings.CEPH_RADOS.get(
                                                                            'CONF_FILE_PATH', '/etc/ceph/ceph.conf')
        self._pool_name = pool_name if pool_name else settings.CEPH_RADOS.get('POOL_NAME', 'objstore')
        self._obj_id = obj_id
        self._rados_dll = rados_dll

    def read(self, offset, size):
        '''
        从指定字节偏移位置读取指定长度的数据块

        :param offset: 偏移位置
        :param size: 读取长度
        :return:
            正常时：bytes
            错误时：None
        '''
        if offset < 0 or size < 0:
            return None

        bytes_read, ret = self._rados_dll.FromObj(self._cluster_name,
                                          self._user_name,
                                          self._conf_file,
                                          self._pool_name,
                                          size,
                                          self._obj_id,
                                          offset)  # return. type:bytes
        if ret < 0:
            return None
        return bytes_read

    def write(self, offset, data_block):
        '''
        从指定字节偏移量写入数据块

        :param offset: 偏移量
        :param data_block: 数据块
        :return:
            正常时：写入的数据长度
            错误时：None
        '''
        if offset < 0 or not isinstance(data_block, bytes):
            return None

        result = self._rados_dll.WriteToObj(self._cluster_name,
                                           self._user_name,
                                           self._conf_file,
                                           self._pool_name,
                                           self._obj_id, data_block, offset)  # return. type:bytes
        if result < 0:
            return None
        return result

    def overwrite(self, data_block):
        '''
        重写对象，创建新对象并写入数据块，如果对象已存在，会删除就对象

        :param data_block: 数据块
        :return:
            正常时：写入的数据长度
            错误时：None
        '''
        if not isinstance(data_block, bytes):
            return None

        result = self._rados_dll.WriteFullToObj(self._cluster_name,
                                               self._user_name,
                                               self._conf_file,
                                               self._pool_name,
                                               self._obj_id, data_block)  # return. type:bytes
        # if not result:
        #     return None
        return result

    def append(self, data_block):
        '''
        向对象追加数据

        :param data_block: 数据块
        :return:
            正常时：写入的数据长度
            错误时：-1
        '''
        if not isinstance(data_block, bytes):
            return None

        result = self._rados_dll.AppendToObj(self._cluster_name,
                                           self._user_name,
                                           self._conf_file,
                                           self._pool_name,
                                           self._obj_id, data_block)  # return. type:bytes
        if result < 0:
            return -1
        return result

    def delete(self):
        '''
        删除对象
        :return:
        '''
        result = self._rados_dll.DelObj(self._cluster_name,
                                       self._user_name,
                                       self._conf_file,
                                       self._pool_name,
                                       self._obj_id)  # return. type:bytes
        if result < 0:
            return False
        return True

    def read_iterator(self, offset=0, block_size=2*1024**2):
        '''
        读取对象生成器
        '''
        offset = offset
        while True:
            data_block = self.read(offset=offset, size=block_size)
            if data_block:
                offset = offset + data_block.size
                yield data_block
            else:
                break


