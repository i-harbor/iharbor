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
    ERROR_CODE_NO_FILE_OR_DIR = 2

    def __init__(self, obj_id, cluster_name=None , user_name=None, conf_file='', pool_name=None, *args, **kwargs):
        self._cluster_name = cluster_name if cluster_name else settings.CEPH_RADOS.get('CLUSTER_NAME', '')
        self._user_name = user_name if user_name else settings.CEPH_RADOS.get('USER_NAME', '')
        self._conf_file = conf_file if os.path.exists(conf_file) else settings.CEPH_RADOS.get(
                                                                            'CONF_FILE_PATH', '')
        self._pool_name = pool_name if pool_name else settings.CEPH_RADOS.get('POOL_NAME', '')
        self._obj_id = obj_id
        self._rados_dll = rados_dll

    def reset_obj_id(self, obj_id):
        self._obj_id = obj_id

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
                                         ctypes.c_int(size),
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
            正常时：(True, str) str是正常结果描述
            错误时：(False, str) str是错误描述
        '''
        if offset < 0 or not isinstance(data_block, bytes):
            return None

        return self.write_by_chunk(data_block=data_block, offset=offset, mode='w')

    def overwrite(self, data_block):
        '''
        重写对象，创建新对象并写入数据块，如果对象已存在，会删除旧对象

        :param data_block: 数据块
        :return: Tuple
            正常时：(True, str) str是正常结果描述
            错误时：(False, str) str是错误描述
        '''
        if not isinstance(data_block, bytes):
            return None

        return self.write_by_chunk(data_block=data_block, mode='wf')

    def append(self, data_block):
        '''
        向对象追加数据

        :param data_block: 数据块
        :return: Tuple
            正常时：(True, str) str是正常结果描述
            错误时：(False, str) str是错误描述
        '''
        if not isinstance(data_block, bytes):
            return None

        return self.write_by_chunk(data_block=data_block, mode='wa')

    def delete(self):
        '''
        删除对象
        :return: Tuple
            成功时：(True, str) str是成功结果描述
            错误时：(False, str) str是错误描述
        '''
        self._rados_dll.DelObj.restype = BaseReturnType  # declare the expected type returned
        result = self._rados_dll.DelObj(self._cluster_name.encode('utf-8'),
                                       self._user_name.encode('utf-8'),
                                       self._conf_file.encode('utf-8'),
                                       self._pool_name.encode('utf-8'),
                                       self._obj_id.encode('utf-8'))
        data = ctypes.string_at(result.data_ptr, result.data_len)
        # 有错误
        if not result.ok:
            code, error = self.parse_error_bytes(data)
            if code is None:
                return  (False, error)

            # 对象不存在导致的错误，等价于删除成功
            if code == self.ERROR_CODE_NO_FILE_OR_DIR:
                return (True, error)

        return (True, data.decode())

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
            # 读取发生错误，尝试再读一次
            if not ok:
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
            正常时：(True, str) str是正常结果描述
            错误时：(False, str) str是错误描述
        '''
        if offset < 0 or chunk_size < 0:
            return (False, "error:offset or chunk_size don't less than 0")

        if mode not in ('w', 'wf', 'wa'):
            return (False, "error:write mode not in ('w', 'wf', 'wa')")

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
                                               ctypes.c_int(len(chunk)),
                                               mode.encode('utf-8'),
                                               ctypes.c_ulonglong(offset))
                data = ctypes.string_at(result.data_ptr, result.data_len)
                if not result.ok:
                    _, err = self.parse_error_bytes(data)
                    return (False, err)
                start += len(chunk)
                end = start + chunk_size

        return (True, 'writed successfull')

    def parse_error_bytes(self, error: bytes):
        '''
        解析错误数据

        :param error: 错误数据, type: bytes；转为字符串后格式为以','分隔的错误码和错误描述：‘2,xxxxx’
        :return: type: tuple, (code:int, description:str)
        '''
        s = error.decode()
        l = s.split(',', maxsplit=1)
        if len(l) < 2:
            return (None, s)
        try:
            code = int(l[0])
            desc = l[-1]
        except:
            code = None
            desc = s

        return (code, desc)

    def get_error_code_dscription(self, errorcode: int):
        '''
        获得错误码的文字描述信息

        :param errorcode: 错误码，type: str
        :return: 错误信息；type: str
        '''
        return self.ERROR_INFO.get(errorcode, '未知的错误')


    ERROR_INFO = {
        1: "Operation not permitted",
        2: "No such file or directory",
        3: "No such process",
        4: "Interrupted system call",
        5: "Input/output error",
        6: "No such device or address",
        7: "Argument list too long",
        8: "Exec format error",
        9: "Bad file descriptor",
        10: "No child processes",
        11: "Resource temporarily unavailable",
        12: "Cannot allocate memory",
        13: "Permission denied",
        14: "Bad address",
        15: "Block device required",
        16: "Device or resource busy",
        17: "File exists",
        18: "Invalid cross-device link",
        19: "No such device",
        20: "Not a directory",
        21: "Is a directory",
        22: "Invalid argument",
        23: "Too many open files in system",
        24: "Too many open files",
        25: "Inappropriate ioctl for device",
        26: "Text file busy",
        27: "File too large",
        28: "No space left on device",
        29: "Illegal seek",
        30: "Read-only file system",
        31: "Too many links",
        32: "Broken pipe",
        33: "Numerical argument out of domain",
        34: "Numerical result out of range",
        35: "Resource deadlock avoided",
        36: "File name too long",
        37: "No locks available",
        38: "Function not implemented",
        39: "Directory not empty",
        40: "Too many levels of symbolic links",
        41: "Unknown error 41",
        42: "No message of desired type",
        43: "Identifier removed",
        44: "Channel number out of range",
        45: "Level 2 not synchronized",
        46: "Level 3 halted",
        47: "Level 3 reset",
        48: "Link number out of range",
        49: "Protocol driver not attached",
        50: "No CSI structure available",
        51: "Level 2 halted",
        52: "Invalid exchange",
        53: "Invalid request descriptor",
        54: "Exchange full",
        55: "No anode",
        56: "Invalid request code",
        57: "Invalid slot",
        58: "Unknown error 58",
        59: "Bad font file format",
        60: "Device not a stream",
        61: "No data available",
        62: "Timer expired",
        63: "Out of streams resources",
        64: "Machine is not on the network",
        65: "Package not installed",
        66: "Object is remote",
        67: "Link has been severed",
        68: "Advertise error",
        69: "Srmount error",
        70: "Communication error on send",
        71: "Protocol error",
        72: "Multihop attempted",
        73: "RFS specific error",
        74: "Bad message",
        75: "Value too large for defined data type",
        76: "Name not unique on network",
        77: "File descriptor in bad state",
        78: "Remote address changed",
        79: "Can not access a needed shared library",
        80: "Accessing a corrupted shared library",
        81: ".lib section in a.out corrupted",
        82: "Attempting to link in too many shared libraries",
        83: "Cannot exec a shared library directly",
        84: "Invalid or incomplete multibyte or wide character",
        85: "Interrupted system call should be restarted",
        86: "Streams pipe error",
        87: "Too many users",
        88: "Socket operation on non-socket",
        89: "Destination address required",
        90: "Message too long",
        91: "Protocol wrong type for socket",
        92: "Protocol not available",
        93: "Protocol not supported",
        94: "Socket type not supported",
        95: "Operation not supported",
        96: "Protocol family not supported",
        97: "Address family not supported by protocol",
        98: "Address already in use",
        99: "Cannot assign requested address",
        100: "Network is down",
        101: "Network is unreachable",
        102: "Network dropped connection on reset",
        103: "Software caused connection abort",
        104: "Connection reset by peer",
        105: "No buffer space available",
        106: "Transport endpoint is already connected",
        107: "Transport endpoint is not connected",
        108: "Cannot send after transport endpoint shutdown",
        109: "Too many references: cannot splice",
        110: "Connection timed out",
        111: "Connection refused",
        112: "Host is down",
        113: "No route to host",
        114: "Operation already in progress",
        115: "Operation now in progress",
        116: "Stale file handle",
        117: "Structure needs cleaning",
        118: "Not a XENIX named type file",
        119: "No XENIX semaphores available",
        120: "Is a named type file",
        121: "Remote I/O error",
        122: "Disk quota exceeded",
        123: "No medium found",
        124: "Wrong medium type",
        125: "Operation canceled",
        126: "Required key not available",
        127: "Key has expired",
        128: "Key has been revoked",
        129: "Key was rejected by service",
        130: "Owner died",
        131: "State not recoverable",
        132: "Operation not possible due to RF-kill",
        133: "Memory page has hardware error",
    }