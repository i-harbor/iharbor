import ctypes
import os

from django.conf import settings


# rados_dll = ctypes.CDLL(settings.CEPH_RADOS.get('RADOS_DLL_PATH', ''))
rados_dll = ctypes.CDLL("utils/oss/rados.so")


# Return type for rados_dll interfaces.
class BaseReturnType(ctypes.Structure):
    _fields_ = [('ok', ctypes.c_bool), ('data_ptr', ctypes.c_void_p), ('data_len', ctypes.c_int)]


class CephRadosObject():
    '''
    文件对象读写接口
    '''
    def __init__(self, obj_id, cluster_name=None , user_name=None, conf_file='', pool_name=None, *args, **kwargs):
        self._cluster_name = cluster_name if cluster_name else settings.CEPH_RADOS.get('CLUSTER_NAME', '')
        self._user_name = user_name if user_name else settings.CEPH_RADOS.get('USER_NAME', '')
        self._conf_file = conf_file if os.path.exists(conf_file) else settings.CEPH_RADOS.get(
                                                                            'CONF_FILE_PATH', '')
        self._pool_name = pool_name if pool_name else settings.CEPH_RADOS.get('POOL_NAME', '')
        self._obj_id = obj_id
        self._rados_dll = rados_dll

    def read(self, offset, size):
        '''
        从指定字节偏移位置读取指定长度的数据块

        :param offset: 偏移位置
        :param size: 读取长度
        :return: Tuple
            正常时：(True, bytes) bytes是读取的数据
            错误时：(False, bytes) bytes是错误描述
        '''
        if offset < 0 or size < 0:
            return None

        self._rados_dll.FromObj.restype = BaseReturnType  # declare the expected type returned
        result = self._rados_dll.FromObj(self._cluster_name.encode('utf-8'),
                                          self._user_name.encode('utf-8'),
                                          self._conf_file.encode('utf-8'),
                                          self._pool_name.encode('utf-8'),
                                          size,
                                          self._obj_id.encode('utf-8'),
                                         ctypes.c_ulonglong(offset))
        data = ctypes.string_at(result.data_ptr, result.data_len)
        return (result.ok, data)

    def write(self, offset, data_block):
        '''
        从指定字节偏移量写入数据块

        :param offset: 偏移量
        :param data_block: 数据块
        :return: Tuple
            正常时：(True, bytes) bytes是正常结果描述
            错误时：(False, bytes) bytes是错误描述
        '''
        if offset < 0 or not isinstance(data_block, bytes):
            return None

        return self.write_by_chunk(data_block=data_block, offset=offset, mode='w')

    def overwrite(self, data_block):
        '''
        重写对象，创建新对象并写入数据块，如果对象已存在，会删除旧对象

        :param data_block: 数据块
        :return: Tuple
            正常时：(True, bytes) bytes是正常结果描述
            错误时：(False, bytes) bytes是错误描述
        '''
        if not isinstance(data_block, bytes):
            return None

        return self.write_by_chunk(data_block=data_block, mode='wf')

    def append(self, data_block):
        '''
        向对象追加数据

        :param data_block: 数据块
        :return: Tuple
            正常时：(True, bytes) bytes是正常结果描述
            错误时：(False, bytes) bytes是错误描述
        '''
        if not isinstance(data_block, bytes):
            return None

        return self.write_by_chunk(data_block=data_block, mode='wa')

    def delete(self):
        '''
        删除对象
        :return: Tuple
            成功时：(True, bytes) bytes是成功结果描述
            错误时：(False, bytes) bytes是错误描述
        '''
        self._rados_dll.DelObj.restype = BaseReturnType  # declare the expected type returned
        result = self._rados_dll.DelObj(self._cluster_name.encode('utf-8'),
                                       self._user_name.encode('utf-8'),
                                       self._conf_file.encode('utf-8'),
                                       self._pool_name.encode('utf-8'),
                                       self._obj_id.encode('utf-8'))
        data = ctypes.string_at(result.data_ptr, result.data_len)
        return (result.ok, data)

    def read_obj_generator(self, offset=0, block_size=10*1024**2):
        '''
        读取对象生成器
        :param offset: 读起始偏移量；type: int
        :param block_size: 每次读取数据块长度；type: int
        :return:
        '''
        offset = offset
        while True:
            ok, data_block = self.read(offset=offset, size=block_size)
            if ok and data_block:
                l = len(data_block)
                offset = offset + l
                yield data_block
            else:
                break

    def write_by_chunk(self, data_block, mode, offset=0, chunk_size=10*1024**2):
        '''
        分片写入一个数据块，默认分片大小10MB
        :param data_block: 要写入的数据块; type: bytes
        :param offset: 写入起始偏移量; type: int
        :param mode: 写入模式; type: str; value: 'w', 'wf', 'wa'
        :return:
            正常时：(True, bytes) bytes是正常结果描述
            错误时：(False, bytes) bytes是错误描述
        '''
        if offset < 0 or chunk_size < 0:
            return (False, "error:offset or chunk_size don't less than 0".encode('utf-8'))

        if mode not in ('w', 'wf', 'wa'):
            return (False, "error:write mode not in ('w', 'wf', 'wa')".encode('utf-8'))

        block_size = len(data_block)
        start = 0
        end = start + chunk_size
        self._rados_dll.ToObj.restype = BaseReturnType  # declare the expected type returned
        while True:
            if start >= block_size:
                break
            chunk = data_block[start:end]
            if chunk:
                result = self._rados_dll.ToObj(self._cluster_name.encode('utf-8'),
                                               self._user_name.encode('utf-8'),
                                               self._conf_file.encode('utf-8'),
                                               self._pool_name.encode('utf-8'),
                                               self._obj_id.encode('utf-8'),
                                               chunk,
                                               len(chunk),
                                               mode.encode('utf-8'),
                                               ctypes.c_ulonglong(offset))
                data = ctypes.string_at(result.data_ptr, result.data_len)
                if not result.ok:
                    return (False, data)
                start += len(chunk)
                end = start + chunk_size

        return (True, 'writed successfull')
