import queue
import rados
from django.conf import settings


class RadosConnectionPool:
    """
    rados连接池
    {   cluster_alise: Queue() -- >Rados()...,
        cluster_alise2: Queue()
    }
    """

    def __init__(self):
        self.max_connect_num = getattr(settings, 'RADOS_POOL_MAX_CONNECT_NUM', 100)
        self.alias_queue = {}  # {cluster_alise: queue}

    @staticmethod
    def create_new_connect(cluster_name, user_name, conf_file, conf):
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
            # 超时10s 无法向队列中添加报错
            self.alias_queue[ceph_cluster_alias].put(item=connect, timeout=5)
        except queue.Full as e:
            self.close(conn=connect)

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
        except rados.Error as e:
            raise e

        try:
            rados_conn = conn.get(timeout=3)
        except queue.Empty as e:
            rados_connect = self.create_new_connect(cluster_name, user_name, conf_file, conf)
            conn = self.add_rados_to_queue(rados_connect, ceph_cluster_alias)
            rados_conn = conn.get(timeout=3)
        except rados.Error as e:
            raise e

        # 检查rados连接状态
        flag = self.connect_state_check(rados_conncet=rados_conn)

        return rados_conn, flag

    def put_connection(self, conn, ceph_cluster_alias):
        """"释放rados连接到队列中"""

        alias_queue = self.add_rados_to_queue(connect=conn, ceph_cluster_alias=ceph_cluster_alias)
        # 连接数在池中的占比
        pool_num_proportion = round(alias_queue.qsize() / self.max_connect_num, 2)
        rados_pool_upper_limit = getattr(settings, 'RADOS_POOL_UPPER_LIMIT', 0.8)
        if pool_num_proportion > rados_pool_upper_limit:
            try:
                self.close_num(ceph_cluster_alias=ceph_cluster_alias)
            except rados.Error as e:
                raise e
        # print(f"连接池数量 {alias_queue.qsize()}")
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

    def close_num(self, ceph_cluster_alias=None):
        """关闭部分连接"""
        if ceph_cluster_alias:
            rados_pool_lower_limit = getattr(settings, 'RADOS_POOL_LOWER_LIMIT', 0.2)
            while True:
                if self.alias_queue[ceph_cluster_alias].qsize() <= rados_pool_lower_limit:
                    break
                try:
                    conn = self.alias_queue[ceph_cluster_alias].get_nowait()
                    if conn:
                        conn.shutdown()
                except queue.Empty as e:
                    break
                except rados.Error as e:
                    raise e

    def close_all(self):
        """关闭所有连接"""
        for ceph_cluster_alias in self.alias_queue:
            try:
                while True:
                    conn = self.alias_queue[ceph_cluster_alias].get_nowait()
                    if conn:
                        conn.shutdown()
            except queue.Empty:
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
            except rados.Error as e:
                raise e

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
