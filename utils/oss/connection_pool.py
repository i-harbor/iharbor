import os
import queue
import rados
from django.conf import settings
import func_timeout
from func_timeout import func_set_timeout


class RadosConnectionPool:
    """
    rados连接池
    Queue() -- >Rados()...,

    uwsgi 中 每个进程对应独立的 连接池
    """
    def __init__(self):
        # 队列最大的空间数量
        self.max_connect_num = getattr(settings, 'RADOS_POOL_MAX_CONNECT_NUM', 4)
        self.pool_queue = queue.Queue(maxsize=self.max_connect_num)
        # # rados 最大的连接数量上限
        # self.rados_pool_upper_limit = getattr(settings, 'RADOS_POOL_UPPER_LIMIT', 0.8 * self.max_connect_num)
        # # rados 最小的连接数量下限
        # self.rados_pool_lower_limit = getattr(settings, 'RADOS_POOL_LOWER_LIMIT', 0.2 * self.max_connect_num)

    @func_set_timeout(10)
    def create_new_connect(self, user_name, cluster_name, conf_file, conf):
        """
        创建新的Rados连接
        :param ceph配置
        :return: Rados()
        """
        rados_conncet = rados.Rados(name=user_name, clustername=cluster_name, conffile=conf_file,
                                    conf=conf)
        try:
            rados_conncet.connect(timeout=5)
        except rados.Error as e:
            msg = e.args[0] if e.args else 'error connecting to the cluster'
            raise rados.Error(msg, errno=e.errno)

        return rados_conncet

    def get_connection(self, user_name, cluster_name, conf_file, conf):
        """
        获取连接
        :return: Rados(), bool rados连接是否可用
        """

        try:
            rados_conn = self.pool_queue.get(timeout=1)
        except queue.Empty as e:
            rados_conn = self.create_new_connect(user_name=user_name, cluster_name=cluster_name, conf_file=conf_file,
                                                 conf=conf)

        # 检查rados连接状态
        flag = self.connect_state_check(rados_conncet=rados_conn)
        return rados_conn, flag

    def put_connection(self, conn):
        """"释放rados连接到队列中"""
        try:
            self.release_rados_connect(connect=conn)
        except func_timeout.exceptions.FunctionTimedOut as e:
            pass

        return

    def release_rados_connect(self, connect):
        """
        向队列中添加Rados连接
        :return:Queue() --> Rados().....
        """
        try:
            # 超时3s 无法向队列中添加报错
            self.pool_queue.put(item=connect, timeout=3)
        except queue.Full as e:
            self.close(conn=connect)

        # #  队列中的数量 大于 预期值 关闭部分数量的raods连接     暂不使用
        # if self.pool_queue.qsize() >= self.rados_pool_upper_limit:
        #
        #     self.close_queue_part_connect()

        return self.pool_queue

    def close_queue_part_connect(self):
        """关闭部分连接， 降低队列的峰值"""

        while True:
            # 队列目前数量 小于 rados连接的下限数量 （将峰值降下）
            if self.pool_queue.qsize() <= self.rados_pool_lower_limit:
                break
            self.close_rados_connection()

    def close_rados_connection(self):
        try:
            conn = self.pool_queue.get(timeout=3)
            if conn:
                self.close(conn=conn)
        except queue.Empty as e:
            pass
        except func_timeout.exceptions.FunctionTimedOut as e:
            pass

    @staticmethod
    def connect_state_check(rados_conncet):
        """检查rados的连接状态"""
        if rados_conncet.state != "connected":
            return False
        return True

    @func_set_timeout(10)
    def close(self, conn):
        """关闭一个连接"""
        conn.shutdown()

    def close_all(self):
        """关闭所有连接"""
        while True:
            self.close_rados_connection()


# 单例模式
class RadosConnectionPoolManager:
    """
    管理多个 pool
    """

    _instance = None
    _pool = {}

    def __new__(cls, *args, **kw):
        if not cls._instance:
            cls._instance = object.__new__(cls)
        return cls._instance

    def __init__(self, ceph_cluster_alias):
        if ceph_cluster_alias in self._pool:
            return
        self._pool[ceph_cluster_alias] = RadosConnectionPool()

    def connection(self, ceph_cluster_alias, user_name, cluster_name, conf_file, conf):

        for i in range(3):
            # flag 为 rados 连接是的参数验证标记，该连接是否可用
            try:
                rados_connect, flag = self._pool[ceph_cluster_alias].get_connection(user_name=user_name,
                                                                                    cluster_name=cluster_name,
                                                                                    conf_file=conf_file, conf=conf)
            except rados.Error as e:
                raise e
            except func_timeout.exceptions.FunctionTimedOut as e:
                raise ValueError("ceph 连接或关闭连接超时。")
            except Exception as e:
                raise e

            if flag:
                return rados_connect
        raise ValueError("无法获取连接")

    def put_connection(self, conn, ceph_cluster_alias):
        """释放连接"""

        try:
            self._pool[ceph_cluster_alias].put_connection(conn=conn)
        except func_timeout.exceptions.FunctionTimedOut as e:
            pass
        return

    def close(self, conn, ceph_cluster_alias):
        """关闭一个连接"""
        try:
            self._pool[ceph_cluster_alias].close(conn=conn)
        except func_timeout.exceptions.FunctionTimedOut as e:
            pass

    def close_connect_ceph_cluster(self, ceph_cluster_alias):
        """关闭一个集群连接"""
        self._pool[ceph_cluster_alias].close_all()

    def close_all(self):
        """关闭所有连接"""
        for ceph_cluster_alias in self._pool:
            self._pool[ceph_cluster_alias].close_all()

