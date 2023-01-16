import os
import math
import json
import datetime
import pytz

import rados

from django.conf import settings

from utils.oss.connection_pool import RadosConnectionPoolManager

class RadosError(rados.Error):
    """def __init__(self, message, errno=None)"""
    pass


class RadosNotFound(RadosError):
    pass


class RadosWriteError(RadosError):
    pass


MAXSIZE_PER_RADOS_OBJ = 2147483648  # 每个rados object 最大2Gb


def build_part_id(obj_id, part_num):
    """
    构造对象part对应的id
    :param obj_id: 对象id
    :param part_num: 对象part的编号
    :return: string
    """
    # 第一个part id等于对象id
    if part_num == 0:
        return obj_id

    return f'{obj_id}_{part_num}'


def write_part_tasks(obj_id, offset, bytes_len):
    """
    分析对象写入操作具体写入任务, 即对象part的写入操作

    注：此函数实现基于下面情况考虑，当不满足以下情况时，请重新实现此函数:
        由于每个part(rados对象)比较大，每次写入数据相对较小，即写入最多涉及到两个part。

    :param obj_id: 对象id
    :param offset: 数据写入的偏移量
    :param bytes_len: 要写入的bytes数组长度
    :return:
        [(part_id, offset, slice_start, slice_end), ]
        列表每项为一个元组，依次为涉及到的对象part的id，数据写入part的偏移量，数据切片的前索引，数据切片的后索引;
    """
    if offset < 0 or bytes_len < 0:
        raise ValueError('“offset”和“rd_wr_size”不能小于0')

    if bytes_len > MAXSIZE_PER_RADOS_OBJ:
        raise ValueError('写入或读取长度不能大于一个rados对象的最大长度')

    start_part_num = int(offset / MAXSIZE_PER_RADOS_OBJ)
    end_part_num = int(math.ceil((offset + bytes_len) / MAXSIZE_PER_RADOS_OBJ)) - 1  # 向上取整数-1
    start_part_offset = offset % MAXSIZE_PER_RADOS_OBJ

    # 要写入的数据在1个part上
    if start_part_num == end_part_num:
        part_id = build_part_id(obj_id=obj_id, part_num=start_part_num)
        return [(part_id, start_part_offset, 0, bytes_len)]

    # 要写入的数据在2个part上
    start_part_id = build_part_id(obj_id=obj_id, part_num=start_part_num)
    end_part_id = build_part_id(obj_id=obj_id, part_num=end_part_num)
    slice_index = MAXSIZE_PER_RADOS_OBJ - start_part_offset
    return [(start_part_id, start_part_offset, 0, slice_index), (end_part_id, 0, slice_index, bytes_len)]


def read_part_tasks(obj_id, offset, bytes_len):
    """
    :param obj_id: 对象id
    :param offset: 读取对象的偏移量
    :param bytes_len: 读取字节长度
    :return:
        [(part_id, offset, read_len), ]
        列表每项为一个元组，依次为涉及到的对象part的id，从part读取数据的偏移量，读取数据长度
    """
    tasks = write_part_tasks(obj_id=obj_id, offset=offset, bytes_len=bytes_len)
    read_tasks = [(obj_key, offset, end - start) for obj_key, offset, start, end in tasks]
    return read_tasks


def get_size(fd):
    """
    获取文件大小
    :param fd: 文件描述符(file descriptor)
    :return:
        size:int

    :raise AttributeError
    """
    if hasattr(fd, 'size'):
        return fd.size
    if hasattr(fd, 'name'):
        try:
            return os.path.getsize(fd.name)
        except (OSError, TypeError):
            pass
    if hasattr(fd, 'tell') and hasattr(fd, 'seek'):
        pos = fd.tell()
        fd.seek(0, os.SEEK_END)
        size = fd.tell()
        fd.seek(pos)
        return size
    raise AttributeError("Unable to determine the file's size.")


class HarborObjectStructure:
    """
    每个EVHarbor对象可能有多个部分part(rados对象)组成
    OBJ(part0, part1, part2, ...)
    part0 id == obj_id;  partN id == f'{obj_id}_{N}'
    """

    def __init__(self, obj_id, obj_size):
        self._obj_id = obj_id
        self._obj_size = obj_size
        self._parts_id = []

    @property
    def parts_id(self):
        if not self._parts_id:
            self._build()

        return self._parts_id

    @property
    def size_part_by(self):
        return MAXSIZE_PER_RADOS_OBJ

    def _build(self):
        last_part_num = int(math.ceil(self._obj_size / MAXSIZE_PER_RADOS_OBJ)) - 1
        self.build_parts_id(last_part_num=last_part_num)

    def build_parts_id(self, last_part_num):
        """
        part0 id == obj_id;  partN id == f'{obj_id}_{N}'
        """
        last_part_num = last_part_num if last_part_num >= 0 else 0
        obj_id = self._obj_id
        for num in range(last_part_num + 1):
            self._parts_id.append(build_part_id(obj_id=obj_id, part_num=num))

    def parts_count(self):
        """
        对象有多少个part
        :return: int
        """
        return len(self.parts_id)

    def first_part_id(self):
        return self.parts_id[0]

    def last_part_id(self):
        return self.parts_id[-1]


class CephClusterCommand(dict):
    """
    执行ceph 命令
    """

    def __init__(self, cluster, prefix, format_='json', **kwargs):
        dict.__init__(self)
        kwargs['prefix'] = prefix
        kwargs['format'] = format_
        try:
            ret, buf, err = cluster.mon_command(json.dumps(kwargs), '', timeout=5)
        except rados.Error as e:
            self['err'] = str(e)
        else:
            if ret != 0:
                self['err'] = err
            else:
                self.update(json.loads(buf))


class RadosAPI:
    """
    ceph cluster rados对象接口封装
    """

    def __init__(self, cluster_name, user_name, pool_name, conf_file, keyring_file='', alise_cluster="default", *args, **kwargs):
        """:raises: class:`RadosError`"""
        self._cluster_name = cluster_name
        self._user_name = user_name
        self._pool_name = pool_name
        self._cluster = None  # rados.Rados()
        self.alise_cluster = alise_cluster

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
        self.clear_cluster()
        return False  # __exit__返回的是False，有异常不被忽略会向上抛出。

    def __del__(self):
        self.clear_cluster()

    def get_cluster(self):
        """
        获取已连接到ceph集群的句柄handle
        :return:
            failed: None
            success: Rados()
        :raises: class:`RadosError`
        """
        if not self._cluster:
            rados_connect = RadosConnectionPoolManager()
            conf = dict(keyring=self._keyring_file) if self._keyring_file else None
            try:
                self._cluster = rados_connect.connection(ceph_cluster_alias=self.alise_cluster, cluster_name=self._cluster_name,
                                     user_name=self._user_name, conf_file=self._conf_file, conf=conf)
            except rados.Error as e:
                raise e
        return self._cluster

    def clear_cluster(self, cluster=None):
        rados_connect = RadosConnectionPoolManager()
        if cluster:
            # 释放连接
            rados_connect.put_connection(conn=cluster, ceph_cluster_alias=self.alise_cluster)

        if self._cluster is not None:
            rados_connect.put_connection(conn=self._cluster, ceph_cluster_alias=self.alise_cluster)
            self._cluster = None

    def _open_ioctx(self, pool_name: str, try_times: int = 0):
        """
        open pool

        :param pool_name: ceph pool名
        :param try_times: 打开失败尝试的次数计数
        :return:
            rados.Ioctx()

        :raises: class:`RadosError`
        """
        cluster = self.get_cluster()
        try:
            return cluster.open_ioctx(pool_name)
        except rados.Error as e:
            self.clear_cluster(cluster)
            if try_times >= 2:
                raise RadosError(f'Failed to open_ioctx, pool={pool_name},{str(e)}')
        except Exception as e:
            raise RadosError(f'Failed to open_ioctx, pool={pool_name},{str(e)}')

        return self._open_ioctx(pool_name=pool_name, try_times=(try_times + 1))

    @staticmethod
    def _io_write(ioctx, obj_id, offset, data: bytes):
        """
        向对象写入数据

        :param obj_id: 对象id
        :param offset: 数据写入偏移量
        :param data: 数据，bytes
        :return:
            success: True
        :raises: class:`RadosError`
        """
        tasks = write_part_tasks(obj_id, offset=offset, bytes_len=len(data))

        for obj_key, off, start, end in tasks:
            try:
                r = ioctx.write(obj_key, data[start:end], offset=off)
            except rados.Error as e:
                msg = e.args[0] if e.args else 'Failed to write bytes to rados object'
                raise RadosError(msg, errno=e.errno)
            if r != 0:
                raise RadosError('Failed to write bytes to rados object')

        return True

    def write(self, obj_id, offset, data: bytes):
        """
        向对象写入数据

        :param obj_id: 对象id
        :param offset: 数据写入偏移量
        :param data: 数据，bytes
        :return:
            success: True
        :raises: class:`RadosError`
        """
        with self._open_ioctx(self._pool_name) as ioctx:
            try:
                self._io_write(ioctx=ioctx, obj_id=obj_id, offset=offset, data=data)
            except rados.Error as e:
                msg = e.args[0] if e.args else f'Failed to open_ioctx({self._pool_name})'
                raise RadosError(msg, errno=e.errno)

        return True

    def _io_write_file(self, ioctx, obj_id, offset, file, per_size=20 * 1024 ** 2):
        """
        向对象写入一个类文件数据

        :param obj_id: 对象id
        :param offset: 文件数据写入对象偏移量
        :param file: 类文件
        :param per_size: 每次从文件读取数据的大小,默认20MB
        :return:
            success: True
        :raises: class:`RadosError`
        """
        try:
            size = get_size(file)
        except AttributeError:
            raise RadosError('input is not a file')

        file_offset = 0  # 文件已写入的偏移量
        while True:
            # 文件是否已完全写入
            if file_offset >= size:
                return True

            file.seek(file_offset)
            chunk = file.read(per_size)
            if chunk:
                try:
                    self._io_write(ioctx=ioctx, obj_id=obj_id, offset=offset + file_offset, data=chunk)
                except RadosError:
                    # 写入失败再尝试一次
                    self._io_write(ioctx=ioctx, obj_id=obj_id, offset=offset + file_offset, data=chunk)

                file_offset += len(chunk)  # 更新已写入大小
            else:
                raise RadosError('read error when write a file to rados')

    def write_file(self, obj_id, offset, file, per_size=20 * 1024 ** 2):
        """
        向对象写入一个类文件数据

        :param obj_id: 对象id
        :param offset: 文件数据写入对象偏移量
        :param file: 类文件
        :param per_size: 每次从文件读取数据的大小,默认20MB
        :return:
            success: True
        :raises: class:`RadosError`
        """
        with self._open_ioctx(self._pool_name) as ioctx:
            try:
                self._io_write_file(ioctx=ioctx, obj_id=obj_id, offset=offset, file=file, per_size=per_size)
            except rados.Error as e:
                msg = e.args[0] if e.args else f'Failed to open_ioctx({self._pool_name})'
                raise RadosError(msg, errno=e.errno)

        return True

    @staticmethod
    def _rados_read(ioctx, obj_id, offset, read_size):
        """
        从rados对象指定偏移量开始读取指定长度的字节数据
        :param ioctx: 输入/输出上下文
        :param obj_id: 对象id
        :param offset: 对象偏移量
        :param read_size: 要读取的字节长度
        :return:
            success; bytes
        :raises: class:`RadosError`
        """
        try:
            data = ioctx.read(obj_id, length=read_size, offset=offset)
        except rados.ObjectNotFound as e:
            return bytes(read_size)  # rados对象不存在，构造一个指定长度的bytes
        except rados.Error as e:
            msg = e.args[0] if e.args else 'Failed to read bytes from rados object'
            raise RadosError(msg, errno=e.errno)

        # 读取数据不足，补足
        read_len = len(data)
        if read_len < read_size:
            data += bytes(read_size - read_len)

        return data

    def read(self, obj_id, offset, read_size):
        """
        读对象数据

        :param obj_id: 对象id
        :param offset: 数据读取偏移量
        :param read_size: 读取数据byte大小
        :return:
            success; bytes
        :raises: class:`RadosError`
        """
        if offset < 0 or read_size <= 0:
            return bytes()

        tasks = read_part_tasks(obj_id, offset=offset, bytes_len=read_size)
        with self._open_ioctx(self._pool_name) as ioctx:
            try:
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
        """
        删除对象

        :param obj_id: 对象id
        :param obj_size: 对象大小
        :return:
            success: True
        :raises: class:`RadosError`
        """
        try:
            with self._open_ioctx(self._pool_name) as ioctx:
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

    def rados_stat(self, obj_id):
        """
        获取rados对象大小和修改时间

        :param obj_id: 对象id
        :return:
                (int, datetime())   # size, mtime
        :raises: class:`RadosError`, `RadosNotFound`
        """
        try:
            with self._open_ioctx(self._pool_name) as ioctx:
                size, t = ioctx.stat(obj_id)
        except rados.ObjectNotFound:
            raise RadosNotFound('rados对象不存在')
        except rados.Error as e:
            msg = e.args[0] if e.args else f'Failed to get rados size({self._pool_name})'
            raise RadosError(msg, errno=e.errno)

        cn_zone = pytz.timezone('Asia/Shanghai')
        mtime = datetime.datetime(year=t.tm_year, month=t.tm_mon, day=t.tm_mday, hour=t.tm_hour,
                                  minute=t.tm_min, second=t.tm_sec).astimezone(cn_zone)
        return size, mtime

    def get_cluster_stats(self):
        """
        获取ceph集群总容量和已使用容量

        :returns: dict - contains the following keys:
            - ``kb`` (int) - total space
            - ``kb_used`` (int) - space used
            - ``kb_avail`` (int) - free space available
            - ``num_objects`` (int) - number of objects
        """
        cluster = self.get_cluster()
        try:
            stats = cluster.get_cluster_stats()
        except rados.Error as e:
            msg = e.args[0] if e.args else f'Failed to get ceph cluster stats'
            raise RadosError(msg, errno=e.errno)
        return stats

    def command(self, prefix, **kwargs):
        cluster = self.get_cluster()
        return CephClusterCommand(cluster, prefix=prefix)

    def mgr_command(self, prefix, format_='json', **kwargs):
        """
        执行ceph的mgr命令

        :prefix: 命令， 例如'iostat'
        :return:
            (string outbuf, string outs)
        :raises: class:`RadosError`
        """
        cluster = self.get_cluster()
        kwargs['prefix'] = prefix
        kwargs['format'] = format
        try:
            ret, buf, outs = cluster.mgr_command(json.dumps(kwargs), b'', timeout=5)
        except rados.Error as e:
            raise RadosError(str(e), errno=e.errno)
        else:
            if ret != 0:
                raise RadosError(outs)
            else:
                return buf, outs

    def mon_command(self, prefix, format_='json', **kwargs):
        """
        执行ceph的mon命令

        :return:
            (string outbuf, string outs)
        :raises: class:`RadosError`
        """
        cluster = self.get_cluster()
        kwargs['prefix'] = prefix
        kwargs['format'] = format
        try:
            ret, buf, outs = cluster.mon_command(json.dumps(kwargs), b'', timeout=5)
        except rados.Error as e:
            raise RadosError(str(e), errno=e.errno)
        else:
            if ret != 0:
                raise RadosError(outs)
            else:
                return buf, outs

    def get_ceph_io_status(self):
        """
        :return: {
                'bw_rd': 0.0,   # Kb/s ,float
                'bw_wr': 0.0,   # Kb/s ,float
                'bw': 0.0       # Kb/s ,float
                'op_rd': 0,     # op/s, int
                'op_wr': 0,     # op/s, int
                'op': 0,        # op/s, int
            }
        :raises: class:`RadosError`
        """
        try:
            b, s = self.mgr_command(prefix='iostat')
        except RadosError as e:
            raise e
        return self.parse_io_str(io_str=s)

    @staticmethod
    def parse_io_str(io_str: str):
        """
        :param io_str:  '  | 1623 KiB/s |   20 KiB/s | 1643 KiB/s |          2 |          1 |          4 |  '
        :return: {
                'bw_rd': 0.0,   # Kb/s ,float
                'bw_wr': 0.0,   # Kb/s ,float
                'bw': 0.0       # Kb/s ,float
                'op_rd': 0,     # op/s, int
                'op_wr': 0,     # op/s, int
                'op': 0,        # op/s, int
            }
        """
        def to_kb(value, unit):
            u = unit[0]
            if u == 'b':
                value = value / 1024
            elif u == 'k':
                value = value
            elif u == 'm':
                value = value * 1024
            elif u == 'g':
                value = value * 1024 * 1024

            return value

        keys = ['bw_rd', 'bw_wr', 'bw', 'op_rd', 'op_wr', 'op']
        data = {}
        items = io_str.strip(' |').split('|')
        for i, item in enumerate(items):
            item = item.strip()
            item = item.lower()
            units = item.split(' ')
            try:
                val = float(units[0])
            except:
                data[keys[i]] = 0
                continue

            # iobw
            if item.endswith('b/s'):
                val = to_kb(val, units[-1])
            # iops
            else:
                val = int(val)

            data[keys[i]] = val

        return data


class HarborObjectBase:
    """
    HarborObject读写相关的封装类，要实现此基类的方法
    """

    def read(self, offset, size):
        """从指定字节偏移位置读取指定长度的数据块"""
        raise NotImplementedError('`read()` must be implemented.')

    def write(self, data_block, offset=0, chunk_size=20 * 1024 ** 2):
        """分片写入一个数据块，默认分片大小20MB"""
        raise NotImplementedError('`write()` must be implemented.')

    def write_file(self, offset, file, per_size=20 * 1024 ** 2):
        """向对象写入一个类文件数据"""
        raise NotImplementedError('`write_file()` must be implemented.')

    def delete(self, obj_size=None):
        """删除对象"""
        raise NotImplementedError('`delete()` must be implemented.')

    def read_obj_generator(self, offset=0, end=None, block_size=10 * 1024 ** 2):
        """读取对象生成器"""
        raise NotImplementedError('`read_obj_generator()` must be implemented.')

    def get_cluster_stats(self):
        """
        获取ceph集群总容量和已使用容量

        :returns: dict - contains the following keys:
            - ``kb`` (int) - total space
            - ``kb_used`` (int) - space used
            - ``kb_avail`` (int) - free space available
            - ``num_objects`` (int) - number of objects
        """
        raise NotImplementedError('`get_cluster_stats()` must be implemented.')

    def get_ceph_io_status(self):
        """
        :return: {
                'bw_rd': 0.0,   # Kb/s ,float
                'bw_wr': 0.0,   # Kb/s ,float
                'bw': 0.0       # Kb/s ,float
                'op_rd': 0,     # op/s, int
                'op_wr': 0,     # op/s, int
                'op': 0,        # op/s, int
            }
        :raises: class:`RadosError`
        """
        raise NotImplementedError('`get_ceph_io_status()` must be implemented.')

    def reset_obj_id_and_size(self, obj_id, obj_size):
        raise NotImplementedError('`reset_obj_id_and_size()` must be implemented.')

    def get_rados_key_info(self):
        raise NotImplementedError('`reset_obj_id_and_size()` must be implemented.')


class HarborObject:
    """
    iHarbor对象操作接口封装，
    """
    def __init__(self, pool_name: str, obj_id: str, obj_size: int, cluster_name: str,  user_name: str, conf_file: str,
                 keyring_file: str, alise_cluster:str,*args, **kwargs):
        self._cluster_name = cluster_name
        self._user_name = user_name
        self._conf_file = conf_file
        self._keyring_file = keyring_file
        self._pool_name = pool_name
        self._obj_id = obj_id
        self._obj_size = obj_size
        self._rados = None
        self.alise_cluster = alise_cluster

    def reset_obj_id_and_size(self, obj_id=None, obj_size=None):
        if obj_id is not None:
            self._obj_id = obj_id
        if obj_size is not None:
            self._obj_size = obj_size

    def get_obj_size(self):
        """获取对象大小"""
        return self._obj_size

    @property
    def rados(self):
        """
        :raises: class:`RadosError`
        """
        return self.get_rados_api()

    def get_rados_api(self):
        """
        获取RadosAPI对象

        :return:
            RadosAPI()  # success

        :raises: class:`RadosError`
        """
        if not self._rados:
            try:
                self._rados = RadosAPI(cluster_name=self._cluster_name, user_name=self._user_name,
                                       pool_name=self._pool_name,
                                       conf_file=self._conf_file, keyring_file=self._keyring_file,
                                       alise_cluster=self.alise_cluster)
            except RadosError as e:
                raise e

        return self._rados

    def read(self, offset, size):
        """
        从指定字节偏移位置读取指定长度的数据块

        :param offset: 偏移位置
        :param size: 读取长度
        :return: Tuple
            正常时：(True, bytes) bytes是读取的数据
            错误时：(False, error_msg) error_msg是错误描述
        """
        if offset < 0 or size < 0:
            return False, 'offset or size param is invalid'

        # 读取偏移量超出对象大小，直接返回空bytes
        obj_size = self.get_obj_size()
        if offset >= obj_size:
            return True, bytes()
        # 读取数据超出对象大小，计算可读取大小
        read_size = (obj_size - offset) if (offset + size) > obj_size else size

        try:
            rados_ = self.get_rados_api()
            data = rados_.read(obj_id=self._obj_id, offset=offset, read_size=read_size)
        except RadosError as e:
            return False, str(e)

        return True, data

    def write(self, data_block: bytes, offset: int = 0, chunk_size: int = None):
        """
        分片写入一个数据块，默认不分片

        :param data_block: 要写入的数据块; type: bytes
        :param offset: 写入起始偏移量; type: int
        :param chunk_size: 默认不分片
        :return:
            正常时：(True, str) str是正常结果描述
            错误时：(False, str) str是错误描述
        """
        if offset < 0 or not isinstance(data_block, bytes):
            return False, 'offset must be >=0 and data input must be bytes'

        block_size = len(data_block)
        if chunk_size is None:  # 未明确指定分片写入，则一次输入
            chunk_size = block_size

        if offset < 0 or chunk_size <= 0:
            return False, "offset or chunk_size don't less than 0"

        start = 0
        end = start + chunk_size

        while True:
            if start >= block_size:
                break
            chunk = data_block[start:end]
            if chunk:
                try:
                    rados_ = self.get_rados_api()
                    rados_.write(obj_id=self._obj_id, offset=offset, data=chunk)
                except (RadosError, Exception) as e:
                    return False, str(e)

                start += len(chunk)
                end = start + chunk_size

        self._obj_size = max(offset + block_size, self._obj_size)
        return True, 'write success'

    def write_file(self, offset, file, per_size=20 * 1024 ** 2):
        """
        向对象写入一个类文件数据

        :param offset: 文件数据写入对象偏移量
        :param file: 类文件
        :param per_size: 每次从文件读取数据的大小,默认20MB
        :return:
                （True, msg）无误
                 (False msg) 错误
        """
        try:
            rados_ = self.get_rados_api()
            rados_.write_file(obj_id=self._obj_id, offset=offset, file=file)
        except (RadosError, Exception) as e:
            return False, str(e)

        file_size = get_size(file)
        self._obj_size = max(offset + file_size, self._obj_size)
        return True, 'success to write file'

    def delete(self, obj_size=None):
        """
        删除对象
        :return: Tuple
            成功时：(True, str) str是成功结果描述
            错误时：(False, str) str是错误描述
        """
        size = obj_size if isinstance(obj_size, int) else self.get_obj_size()

        try:
            rados_ = self.get_rados_api()
            rados_.delete(obj_id=self._obj_id, obj_size=size)
        except (RadosError, Exception) as e:
            return False, str(e)

        self._obj_size = 0
        return True, 'delete success'

    def read_obj_generator(self, offset=0, end=None, block_size=10 * 1024 ** 2):
        """
        读取对象生成器
        :param offset: 读起始偏移量；type: int
        :param end: 读结束偏移量(包含)；type: int；None:表示对象结尾；
        :param block_size: 每次读取数据块长度；type: int
        :return:
        """
        obj_size = self.get_obj_size()
        if isinstance(end, int):
            end_oft = min(end + 1, obj_size)  # 包括end,不大于对象大小
        else:
            end_oft = obj_size

        oft = max(offset, 0)
        while True:
            # 下载完成
            if oft >= end_oft:
                break

            size = min(end_oft - oft, block_size)
            ok, data_block = self.read(offset=oft, size=size)
            # 读取发生错误，尝试再读一次
            if not ok:
                ok, data_block = self.read(offset=oft, size=size)

            if ok and data_block:
                l = len(data_block)
                oft = oft + l
                yield data_block
            else:
                break

    def write_obj_generator(self):
        """
        写入对象生成器

        :return:
            generator

        :usage:
            ok = next(generator)
            ok = generator.send((offset, bytes))    # ok = True写入成功， ok=False写入失败
        """
        ok = True
        while True:
            offset, data = yield ok
            ok, _ = self.write(offset=offset, data_block=data)

    def get_cluster_stats(self):
        """
        获取ceph集群总容量和已使用容量

        :returns: dict - contains the following keys:
            - ``kb`` (int) - total space
            - ``kb_used`` (int) - space used
            - ``kb_avail`` (int) - free space available
            - ``num_objects`` (int) - number of objects
        """
        rados_ = self.get_rados_api()
        return rados_.get_cluster_stats()

    def get_ceph_io_status(self):
        """
        :return:
            success: True, {
                    'bw_rd': 0.0,   # Kb/s ,float
                    'bw_wr': 0.0,   # Kb/s ,float
                    'bw': 0.0       # Kb/s ,float
                    'op_rd': 0,     # op/s, int
                    'op_wr': 0,     # op/s, int
                    'op': 0,        # op/s, int
                }
            error: False, err:tsr
        """
        try:
            rados_ = self.get_rados_api()
            status = rados_.get_ceph_io_status()
        except RadosError as e:
            return False, str(e)

        return True, status

    def get_rados_key_info(self):
        """
        获取对象在ceph rados中的存储信息

        :return:
               int, [item, ...]   # item: str; format = iharbor:{cluster_name}/{pool_name}/{rados-key}
        """
        hos = HarborObjectStructure(obj_id=self._obj_id, obj_size=self._obj_size)
        parts = hos.parts_id
        cn = self._cluster_name
        pn = self._pool_name
        info = [f'iharbor:{cn}/{pn}/{k}' for k in parts]
        return hos.size_part_by, info

    def get_rados_stat(self, obj_id: str):
        """
        :return:
            (True, (int, datetime))   # success, (size, mtime)
            (True, (0, None))         # rados object not found
            (False, str)              # error
        """
        try:
            rados_ = self.get_rados_api()
            r = rados_.rados_stat(obj_id=obj_id)
            return True, r
        except RadosNotFound as e:
            return True, (0, None)
        except RadosError as e:
            return False, str(e)


class FileWrapper:
    def __init__(self, ho: HarborObject):
        self._ho = ho
        self.offset = 0
        self.closed = True

    def open(self):
        try:
            self._ho.get_rados_api().get_cluster()
        except Exception as e:
            raise ValueError("The file cannot be reopened.")

        self.closed = False
        return self

    def close(self):
        self.closed = True
        self._ho.get_rados_api().clear_cluster()

    @property
    def size(self):
        return self._ho.get_obj_size()

    @size.setter
    def size(self, value):
        self._ho.reset_obj_id_and_size(obj_size=value)

    def read(self, size):
        ok, data = self._ho.read(offset=self.offset, size=size)
        if not ok:
            ok, data = self._ho.read(offset=self.offset, size=size)
            if not ok:
                raise IOError('failed to read from harbor object')

        rl = len(data)
        self.offset += rl
        return data

    def write(self, data, offset=None):
        offset = offset if offset is not None else self.offset
        ok, msg = self._ho.write(data, offset=offset)
        if not ok:
            ok, msg = self._ho.write(data, offset=offset)
            if not ok:
                raise IOError('failed write data to harbor object')

        wl = len(data)
        self.offset += wl
        return wl

    def seek(self, offset):
        size = self.size
        if size <= 0:
            self.offset = 0

        if 0 <= offset <= size:
            self.offset = offset
        elif offset < 0:
            self.offset = 0
        else:
            self.offset = size

    def __del__(self):
        self.close()


def build_harbor_object(using: str, pool_name: str, obj_id: str, obj_size: int = 0):
    """
    构建iharbor对象对应的ceph读写接口

    :param using: ceph集群配置别名，对应对象数据所在ceph集群
    :param pool_name: ceph存储池名称，对应对象数据所在存储池名称
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
    return HarborObject(pool_name=pool_name, obj_id=obj_id, obj_size=obj_size, cluster_name=cluster_name,
                        user_name=user_name, conf_file=conf_file, keyring_file=keyring_file, alise_cluster=using)
