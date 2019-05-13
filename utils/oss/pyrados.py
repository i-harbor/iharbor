import os
import math

import rados

from django.conf import settings


class RadosError(rados.Error):
    '''def __init__(self, message, errno=None)'''
    pass

class RadosWriteError(rados.Error):
    pass


MAXSIZE_PER_RADOS_OBJ = 2147483648 # 每个rados object 最大2Gb


def build_part_id(obj_id, part_num):
    '''
    构造对象part对应的id
    :param obj_id: 对象id
    :param part_num: 对象part的编号
    :return: string
    '''
    # 第一个part id等于对象id
    if part_num == 0:
        return obj_id

    return f'{obj_id}_{part_num}'

def write_part_tasks(obj_id, offset, bytes_len):
    '''
    分析对象写入操作具体写入任务, 即对象part的写入操作

    注：此函数实现基于下面情况考虑，当不满足以下情况时，请重新实现此函数:
        由于每个part(rados对象)比较大，每次写入数据相对较小，即写入最多涉及到两个part。

    :param obj_id: 对象id
    :param offset: 数据写入的偏移量
    :param bytes_len: 要写入的bytes数组长度
    :return:
        [(part_id, offset, slice_start, slice_end), ]
        列表每项为一个元组，依次为涉及到的对象part的id，数据写入part的偏移量，数据切片的前索引，数据切片的后索引;
    '''
    if offset < 0 or bytes_len < 0:
        raise ValueError('“offset”和“rd_wr_size”不能小于0')

    if bytes_len > MAXSIZE_PER_RADOS_OBJ:
        raise ValueError('写入或读取长度不能大于一个rados对象的最大长度')

    start_part_num = int(offset / MAXSIZE_PER_RADOS_OBJ)
    end_part_num = int(math.ceil((offset + bytes_len) / MAXSIZE_PER_RADOS_OBJ)) - 1 # 向上取整数-1
    start_part_offset = offset % MAXSIZE_PER_RADOS_OBJ

    # 要写入的数据在1个part上
    if start_part_num == end_part_num:
        part_id = build_part_id(obj_id=obj_id, part_num=start_part_num)
        return [(part_id, start_part_offset,  0, bytes_len)]

    # 要写入的数据在2个part上
    start_part_id = build_part_id(obj_id=obj_id, part_num=start_part_num)
    end_part_id = build_part_id(obj_id=obj_id, part_num=end_part_num)
    slice_index = MAXSIZE_PER_RADOS_OBJ - start_part_offset
    return [(start_part_id, start_part_offset, 0, slice_index), (end_part_id, 0, slice_index, bytes_len)]

def read_part_tasks(obj_id, offset, bytes_len):
    '''
    :param obj_id: 对象id
    :param offset: 读取对象的偏移量
    :param bytes_len: 读取字节长度
    :return:
        [(part_id, offset, read_len), ]
        列表每项为一个元组，依次为涉及到的对象part的id，从part读取数据的偏移量，读取数据长度
    '''
    tasks = write_part_tasks(obj_id=obj_id, offset=offset, bytes_len=bytes_len)
    read_tasks = [(obj_key, offset, end - start) for obj_key, offset, start, end in tasks]
    return read_tasks

class HarborObjectStructure():
    '''
    每个EVHarbor对象可能有多个部分part(rados对象)组成
    OBJ(part0, part1, part2, ...)
    part0 id == obj_id;  partN id == f'{obj_id}_{N}'
    '''
    def __init__(self, obj_id, obj_size):
        self._obj_id = obj_id
        self._obj_size = obj_size
        self._parts_id = []

    @property
    def parts_id(self):
        if not self._parts_id:
            self._build()

        return self._parts_id

    def _build(self):
        last_part_num = int(math.ceil(self._obj_size / MAXSIZE_PER_RADOS_OBJ)) - 1
        self.build_parts_id(last_part_num=last_part_num)

    def build_parts_id(self, last_part_num):
        '''
        part0 id == obj_id;  partN id == f'{obj_id}_{N}'
        '''
        last_part_num = last_part_num if last_part_num >= 0 else 0
        obj_id = self._obj_id
        for num in range(last_part_num + 1):
            self._parts_id.append(build_part_id(obj_id=obj_id, part_num=num))

    def how_many_parts(self):
        '''
        对象有多少个part
        :return: int
        '''
        return len(self.parts_id)

    def first_part_id(self):
        return self.parts_id[0]

    def last_part_id(self):
        return self.parts_id[-1]


class RadosAPI():
    '''
    ceph cluster rados对象接口封装
    '''
    def __init__(self, cluster_name, user_name, pool_name, conf_file, keyring_file='', *args, **kwargs):
        ''':raises: class:`RadosError`'''
        self._cluster_name = cluster_name
        self._user_name = user_name
        self._pool_name = pool_name
        self._cluster = None  # rados.Rados()

        if not os.path.exists(conf_file):
            raise RadosError("参数有误，配置文件路径不存在")
        self._conf_file = conf_file

        if keyring_file and not os.path.exists(keyring_file):
            raise RadosError("参数有误，keyring配置文件路径不存在")
        self._keyring_file = keyring_file

    def __enter__(self):
        self.get_cluster()
        return self

    def __exit__(self, type_, value, traceback):
        self.get_cluster().shutdown()
        self._cluster = None
        return False    #__exit__返回的是False，有异常不被忽略会向上抛出。

    def __del__(self):
        if self._cluster:
            self._cluster.shutdown()

    def get_cluster(self):
        '''
        获取已连接到ceph集群的句柄handle
        :return:
            failed: None
            success: Rados()
        :raises: class:`RadosError`
        '''
        if not self._cluster:
            conf = dict(keyring=self._keyring_file) if self._keyring_file else None
            self._cluster = rados.Rados(conffile=self._conf_file, conf=conf)
            try:
                self._cluster.connect()
            except rados.Error as e:
                msg = e.args[0] if e.args else 'error connecting to the cluster'
                raise RadosError(msg, errno=e.errno)

        return self._cluster

    def write(self, obj_id, offset, data:bytes):
        '''
        向对象写入数据

        :param obj_id: 对象id
        :param offset: 数据写入偏移量
        :param data: 数据，bytes
        :return:
            success: True
        :raises: class:`RadosError`
        '''
        tasks = write_part_tasks(obj_id, offset=offset, bytes_len=len(data))

        cluster = self.get_cluster()
        try:
            with cluster.open_ioctx(self._pool_name) as ioctx:
                for obj_key, off, start, end in tasks:
                    try:
                        r = ioctx.write(obj_key, data, offset=off)
                    except rados.Error as e:
                        msg = e.args[0] if e.args else 'Failed to write bytes to rados object'
                        raise RadosError(msg, errno=e.errno)
                    if r != 0:
                        raise RadosError('Failed to write bytes to rados object')
        except rados.Error as e:
            msg = e.args[0] if e.args else f'Failed to open_ioctx({self._pool_name})'
            raise RadosError(msg, errno=e.errno)
        except Exception as e:
            raise RadosError(str(e))

        return True

    def _rados_read(self, ioctx, obj_id, offset, read_size):
        '''
        从rados对象指定偏移量开始读取指定长度的字节数据
        :param ioctx: 输入/输出上下文
        :param obj_id: 对象id
        :param offset: 对象偏移量
        :param read_size: 要读取的字节长度
        :return:
            success; bytes
        :raises: class:`RadosError`
        '''
        try:
            data = ioctx.read(obj_id, length=read_size, offset=offset)
        except rados.ObjectNotFound as e:
            return bytes(read_size) # rados对象不存在，构造一个指定长度的bytes
        except rados.Error as e:
            msg = e.args[0] if e.args else 'Failed to read bytes from rados object'
            raise RadosError(msg, errno=e.errno)

        # 读取数据不足，补足
        read_len = len(data)
        if read_len < read_size:
            data += bytes(read_size - read_len)

        return data

    def read(self, obj_id,  offset, read_size):
        '''
        读对象数据

        :param obj_id: 对象id
        :param offset: 数据读取偏移量
        :param read_size: 读取数据byte大小
        :return:
            success; bytes
        :raises: class:`RadosError`
        '''
        tasks = read_part_tasks(obj_id, offset=offset, bytes_len=read_size)
        cluster = self.get_cluster()

        try:
            with cluster.open_ioctx(self._pool_name) as ioctx:
                # 要读取的数据在一个rados对象上
                if len(tasks) == 1:
                    obj_key, off, size = tasks[0]
                    return self._rados_read(ioctx=ioctx, obj_id=obj_key, read_size=size, offset=off)

                ret_data = bytes()
                for obj_key, off, size in tasks:
                    data = self._rados_read(ioctx=ioctx, obj_id=obj_key, read_size=size, offset=off)
                    ret_data += data

                return ret_data

        except rados.Error as e:
            msg = e.args[0] if e.args else f'Failed to open_ioctx({self._pool_name})'
            raise RadosError(msg, errno=e.errno)
        except Exception as e:
            raise RadosError(str(e))

    def delete(self, obj_id, obj_size):
        '''
        删除对象

        :param obj_id: 对象id
        :param obj_size: 对象大小
        :return:
            success: True
        :raises: class:`RadosError`
        '''
        cluster = self.get_cluster()

        try:
            with cluster.open_ioctx(self._pool_name) as ioctx:
                hos = HarborObjectStructure(obj_id=obj_id, obj_size=obj_size)
                for part_id in hos.parts_id:
                    try:
                        ok = ioctx.remove_object(part_id)
                        if ok is True:
                            continue
                    except rados.ObjectNotFound:
                        continue
                    except rados.Error as e:
                        msg = e.args[0] if e.args else f'Failed to remove rados object {part_id}'
                        raise RadosError(msg, errno=e.errno)

                return True

        except rados.Error as e:
            msg = e.args[0] if e.args else f'Failed to delete object {obj_id})'
            raise RadosError(msg, errno=e.errno)
        except Exception as e:
            raise RadosError(str(e))


class HarborObject():
    '''
    EVHarbor对象操作接口封装
    '''

    def __init__(self, obj_id, obj_size=0, cluster_name=None, pool_name=None, user_name=None, conf_file='',
                 keyring_file='', *args, **kwargs):
        self._cluster_name = cluster_name if cluster_name else settings.CEPH_RADOS.get('CLUSTER_NAME', '')
        self._user_name = user_name if user_name else settings.CEPH_RADOS.get('USER_NAME', '')
        self._conf_file = conf_file if os.path.exists(conf_file) else settings.CEPH_RADOS.get('CONF_FILE_PATH', '')
        self._keyring_file = keyring_file if os.path.exists(keyring_file) else settings.CEPH_RADOS.get('KEYRING_FILE_PATH', '')
        self._pool_name = pool_name if pool_name else settings.CEPH_RADOS.get('POOL_NAME', '')
        self._obj_id = obj_id
        self._obj_size = obj_size
        self._rados = RadosAPI(cluster_name=self._cluster_name, user_name=self._user_name, pool_name=self._pool_name,
                               conf_file=self._conf_file, keyring_file=self._keyring_file)

    def reset_obj_id_and_size(self, obj_id, obj_size):
        self._obj_id = obj_id
        self._obj_size = obj_size

    def get_obj_size(self):
        '''获取对象大小'''
        return self._obj_size

    def read(self, offset, size):
        '''
        从指定字节偏移位置读取指定长度的数据块

        :param offset: 偏移位置
        :param size: 读取长度
        :return: Tuple
            正常时：(True, bytes) bytes是读取的数据
            错误时：(False, error_msg) error_msg是错误描述
        '''
        if offset < 0 or size < 0:
            return False, 'offset or size param is invalid'

        try:
            data = self._rados.read(obj_id=self._obj_id, offset=offset, read_size=size)
        except RadosError as e:
            return False, str(e)

        return True, data

    def write(self, data_block, offset=0, chunk_size=20*1024**2):
        '''
        分片写入一个数据块，默认分片大小20MB
        :param data_block: 要写入的数据块; type: bytes
        :param offset: 写入起始偏移量; type: int
        :return:
            正常时：(True, str) str是正常结果描述
            错误时：(False, str) str是错误描述
        '''
        if offset < 0 or not isinstance(data_block, bytes):
            return False, 'offset must be >=0 and data input must be bytes'

        if offset < 0 or chunk_size < 0:
            return False, "offset or chunk_size don't less than 0"

        block_size = len(data_block)
        start = 0
        end = start + chunk_size

        while True:
            if start >= block_size:
                break
            chunk = data_block[start:end]
            if chunk:
                try:
                    self._rados.write(obj_id=self._obj_id, offset=offset, data=chunk)
                except RadosError as e:
                    return False, str(e)

                start += len(chunk)
                end = start + chunk_size

        self._obj_size = max(offset + block_size, self._obj_size)
        return True, 'write success'

    def write_file(self, offset, file, per_size=20*1024**2):
        '''
        向对象写入一个类文件数据

        :param offset: 文件数据写入对象偏移量
        :param file: 类文件
        :param per_size: 每次从文件读取数据的大小,默认20MB
        :return:
                （True, msg）无误
                 (False msg) 错误
        '''
        try:
            size = file.size
        except AttributeError:
            return False, 'input is not a file'

        file_offset = 0 # 文件已写入的偏移量
        while True:
            # 文件是否已完全写入
            if file_offset == size:
                return True, 'write success'

            file.seek(file_offset)
            chunk = file.read(per_size)
            if chunk:
                ok, msg = self.write(offset=offset + file_offset, data_block=chunk)
                if not ok:
                    # 写入失败再尝试一次
                    ok, msg = self.write(offset=offset + file_offset, data_block=chunk)
                    if not ok:
                        return False, msg

                file_offset += len(chunk) # 更新已写入大小
            else:
                return False, 'read error'

    def delete(self, obj_size=None):
        '''
        删除对象
        :return: Tuple
            成功时：(True, str) str是成功结果描述
            错误时：(False, str) str是错误描述
        '''
        size = self.get_obj_size()
        if isinstance(obj_size, int):
            size = obj_size

        try:
            self._rados.delete(obj_id=self._obj_id, obj_size=size)
        except RadosError as e:
            return False, str(e)

        return True, 'delete success'

    def read_obj_generator(self, offset=0, block_size=10*1024**2):
        '''
        读取对象生成器
        :param offset: 读起始偏移量；type: int
        :param block_size: 每次读取数据块长度；type: int
        :return:
        '''
        obj_size = self.get_obj_size()
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

                # 下载完成
                if offset >= obj_size:
                    break
            else:
                break

