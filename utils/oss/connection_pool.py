import queue
import rados
from webserver import settings as django_settings
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
        self.max_connect_num = getattr(django_settings, 'RADOS_POOL_MAX_CONNECT_NUM', 4)
        self.pool_queue = queue.Queue(maxsize=self.max_connect_num)

    @func_set_timeout(10)
    def create_new_connect(self, user_name, cluster_name, conf_file, conf):
        """
        创建新的Rados连接

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
        :return: Rados()

        :raises: func_timeout.exceptions.FunctionTimedOut
        """

        try:
            rados_conn = self.pool_queue.get_nowait()
        except queue.Empty:
            return self.create_new_connect(
                user_name=user_name, cluster_name=cluster_name, conf_file=conf_file, conf=conf)

        # 检查rados连接状态
        if self.connect_state_check(rados_conncet=rados_conn):
            return rados_conn

        return self.get_connection(user_name=user_name, cluster_name=cluster_name, conf_file=conf_file, conf=conf)

    def put_connection(self, conn):
        """"释放rados连接到队列中"""
        try:
            self.release_rados_connect(connect=conn)
        except func_timeout.exceptions.FunctionTimedOut:
            pass

    def release_rados_connect(self, connect):
        """
        向队列中添加Rados连接
        :return:Queue() --> Rados().....

        :raises: func_timeout.exceptions.FunctionTimedOut
        """
        try:
            # 队列中没有可以存放的卡槽，直接 full错误
            self.pool_queue.put_nowait(item=connect)
        except queue.Full:
            self.close(conn=connect)

    @staticmethod
    def connect_state_check(rados_conncet):
        """检查rados的连接状态"""
        if rados_conncet.state != "connected":
            return False

        return True

    @func_set_timeout(10)
    def close(self, conn):
        """
        关闭一个连接

        :raises: func_timeout.exceptions.FunctionTimedOut
        """
        conn.shutdown()

    def close_all(self):
        """关闭所有连接"""
        while True:
            try:
                conn = self.pool_queue.get(timeout=3)
                if conn:
                    self.close(conn=conn)
            except queue.Empty:
                break
            except func_timeout.exceptions.FunctionTimedOut:
                pass


class Singleton(type):
    def __call__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super().__call__(*args, **kwargs)

        return cls._instance


class RadosConnectionPoolManager(metaclass=Singleton):
    """
    管理多个 pool
    """
    def __init__(self):
        self._pools = {}

    def __del__(self):
        self.close_all()

    def _get_pool(self, ceph_cluster_alias) -> RadosConnectionPool:
        if ceph_cluster_alias not in self._pools:
            self._pools[ceph_cluster_alias] = RadosConnectionPool()

        return self._pools[ceph_cluster_alias]

    def connection(self, ceph_cluster_alias, user_name, cluster_name, conf_file, conf):
        pool = self._get_pool(ceph_cluster_alias)
        try:
            return pool.get_connection(user_name=user_name, cluster_name=cluster_name, conf_file=conf_file, conf=conf)
        except rados.Error as e:
            raise rados.Error(e)
        except func_timeout.exceptions.FunctionTimedOut:
            raise rados.Error('ceph连接获取超时')

    def put_connection(self, conn, ceph_cluster_alias):
        """释放连接"""
        pool = self._get_pool(ceph_cluster_alias)
        pool.put_connection(conn=conn)

    def close(self, conn, ceph_cluster_alias):
        """关闭一个连接"""
        pool = self._get_pool(ceph_cluster_alias)
        try:
            pool.close(conn=conn)
        except func_timeout.exceptions.FunctionTimedOut:
            pass

    def close_connect_ceph_cluster(self, ceph_cluster_alias):
        """关闭一个集群连接"""
        pool = self._get_pool(ceph_cluster_alias)
        pool.close_all()

    def close_all(self):
        """关闭所有连接"""
        for alias in self._pools:
            self._get_pool(alias).close_all()


conn_pool_manager = RadosConnectionPoolManager()    # 模块是单例模式
