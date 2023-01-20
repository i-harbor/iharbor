import queue
import rados
from django.conf import settings
import func_timeout
from func_timeout import func_set_timeout


class RadosConnectionPool:
    """
    rados连接池
    {   cluster_alise: Queue() -- >Rados()...,
        cluster_alise2: Queue()
    }
    uwsgi 中 每个进程对应独立的 连接池
    """

    def __init__(self):
        self.max_connect_num = getattr(settings, 'RADOS_POOL_MAX_CONNECT_NUM', 100)  # 队列最大的空间数量
        self.rados_pool_upper_limit = getattr(settings, 'RADOS_POOL_UPPER_LIMIT', 0.8 * self.max_connect_num) # rados 最大的连接数量上限
        self.rados_pool_lower_limit = getattr(settings, 'RADOS_POOL_LOWER_LIMIT', 0.2 * self.max_connect_num) # rados 最小的连接数量下限
        self.alias_queue = {}  # {cluster_alise: queue}

    @func_set_timeout(10)
    def create_new_connect(self, cluster_name, user_name, conf_file, conf):
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

    def add_rados_to_queue(self, connect, ceph_cluster_alias):
        """
        向队列中添加Rados连接
        {cluster_alise: queue}
        :return:Queue() --> Rados().....
        """
        try:
            # 超时3s 无法向队列中添加报错
            self.alias_queue[ceph_cluster_alias].put(item=connect, timeout=3)
        except queue.Full as e:
            self.close(conn=connect)

        #  队列中的数量 大于 预期值 关闭部分数量的raods连接
        if self.alias_queue[ceph_cluster_alias].qsize() > self.rados_pool_upper_limit:

            self.close_queue_part_connect(ceph_cluster_alias=ceph_cluster_alias)

        return self.alias_queue[ceph_cluster_alias]

    def get_connection(self, ceph_cluster_alias, cluster_name, user_name, conf_file, conf):
        """
        获取连接
        :param ceph配置参数
        :return: Rados(), bool rados连接是否可用
        """

        try:
            conn = self.alias_queue[ceph_cluster_alias]
        except KeyError as e:
            self.alias_queue[ceph_cluster_alias] = queue.Queue(maxsize=self.max_connect_num)
            rados_connect = self.create_new_connect(cluster_name, user_name, conf_file, conf)
            conn = self.add_rados_to_queue(rados_connect, ceph_cluster_alias)

        try:
            rados_conn = conn.get(timeout=3)
        except queue.Empty as e:
            rados_connect = self.create_new_connect(cluster_name, user_name, conf_file, conf)
            conn = self.add_rados_to_queue(rados_connect, ceph_cluster_alias)
            rados_conn = conn.get(timeout=3)

        # 检查rados连接状态
        flag = self.connect_state_check(rados_conncet=rados_conn)
        return rados_conn, flag

    def put_connection(self, conn, ceph_cluster_alias):
        """"释放rados连接到队列中"""

        alias_queue = self.add_rados_to_queue(connect=conn, ceph_cluster_alias=ceph_cluster_alias)
        return

    def get_rados_pool_num(self, ceph_cluster_alias):
        """获取缓存池连接数量"""
        return self.alias_queue[ceph_cluster_alias].qsize()

    @staticmethod
    def connect_state_check(rados_conncet):
        """检查rados的连接状态"""
        if rados_conncet.state != "connected":
            return False
        return True

    def close(self, conn):
        """关闭一个连接"""
        conn.shutdown()

    def close_queue_part_connect(self, ceph_cluster_alias):
        """关闭部分连接， 降低队列的峰值"""

        while True:
            # 队列目前数量 小于 rados连接的下限数量 （将峰值降下）
            if self.alias_queue[ceph_cluster_alias].qsize() < self.rados_pool_lower_limit:
                break
            self.close_rados_connection(ceph_cluster_alias)

    def close_all(self):
        """关闭所有连接"""
        for ceph_cluster_alias in self.alias_queue:
            while True:
                if self.alias_queue[ceph_cluster_alias].qsize() == 0:
                    break
                self.close_rados_connection(ceph_cluster_alias=ceph_cluster_alias)

    def close_rados_connection(self, ceph_cluster_alias):
        try:
            conn = self.alias_queue[ceph_cluster_alias].get(timeout=3)
            if conn:
                self.close(conn=conn)
        except queue.Empty as e:
            pass


# 单例模式
class RadosConnectionPoolManager:
    _instance = None
    _init_flag = False
    _pool = None

    def __new__(cls, *args, **kw):
        if not cls._instance:
            cls._instance = object.__new__(cls)
        return cls._instance

    def __init__(self):
        if self.__class__._init_flag:
            return

        self._pool = RadosConnectionPool()
        self.__class__._init_flag = True

    def connection(self, ceph_cluster_alias, cluster_name, user_name, conf_file, conf):

        while True:
            try:
                rados_connect, flag = self._pool.get_connection(ceph_cluster_alias=ceph_cluster_alias,
                                                                cluster_name=cluster_name, user_name=user_name,
                                                                conf_file=conf_file, conf=conf)
            except (rados.Error, Exception) as e:
                raise e
            except func_timeout.exceptions.FunctionTimedOut as e:
                raise ValueError("ceph 连接超时。")

            if flag:
                return rados_connect

    def put_connection(self, conn, ceph_cluster_alias):
        """释放连接"""
        return self._pool.put_connection(conn=conn, ceph_cluster_alias=ceph_cluster_alias)

    def close(self, conn):
        """关闭一个连接"""
        self._pool.close(conn=conn)

    def close_all(self):
        """关闭所有连接"""
        self._pool.close_all()

    def get_rados_pool_num(self, ceph_cluster_alias):
        """获取缓存池连接数量"""
        return self._pool.get_rados_pool_num(ceph_cluster_alias=ceph_cluster_alias)
