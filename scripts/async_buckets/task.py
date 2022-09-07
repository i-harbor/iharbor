import os
import time
import threading
import random

from .managers import AsyncBucketManager
from .querys import QueryHandler, BackupNum
from .databases import CanNotConnection


class AsyncTask:
    def __init__(self, node_num: int = None, node_count: int = 100,
                 in_multi_thread: bool = False, max_threads: int = 10,
                 test: bool = False, logger=None, buckets: list = None,
                 small_size_first: bool = False
                 ):
        """
        :param node_num: 当前工作节点编号，不指定尝试从hostname获取
        :param node_count: 一共多少个节点，用于id求余，只同步 余数 == node_num的对象
        :param in_multi_thread: True(开启多线程模式)， False(单线程)
        :param max_threads: 多线程工作模式时，最大线程数
        :param test: 不同步对象，打印一些参数
        :param logger:
        :param buckets: 只同步指定桶
        :param small_size_first: True(对象小的先同步)
        """
        if logger is None:
            raise ValueError(f"No logger config")

        self.logger = logger
        self.test = test
        self.in_multi_thread = in_multi_thread
        self.max_threads = max_threads
        self.node_count = node_count
        self.node_num = node_num
        self.buckets = buckets
        self.small_size_first = small_size_first

        try:
            self.validate_params()
        except ValueError as e:
            self.logger.error(str(e))
            exit(1)     # exit error

        self.pool_sem = threading.Semaphore(self.max_threads)  # 定义最多同时启用多少个线程
        self.in_exiting = False         # 多线程时标记是否正在退出

    def validate_params(self):
        """
        :raises: ValueError
        """
        if self.node_num is None:
            self.node_num = self.get_current_node_num_from_hostname()

        if not isinstance(self.node_num, int):
            raise ValueError(f"Invalid node_num {self.node_num}")

        if self.node_num <= 0:
            raise ValueError(f'node_num({self.node_num}) must be greater than 0')

        if self.node_count <= 0:
            raise ValueError(f'node_count({self.node_count}) must be greater than 0')

        if self.node_num > self.node_count:
            raise ValueError(f'node_num({self.node_num}) cannot be greater than node_count({self.node_count})')

        if not isinstance(self.max_threads, int):
            raise ValueError(f"Invalid max_threads {self.max_threads}")

        if self.max_threads > 100:
            raise ValueError(f"The value of 'max_threads' ({self.max_threads}) set is too large")

    def run(self):
        if self.in_multi_thread:
            mode_str = f'Will Starting in mode multi-threading, max_threads={self.max_threads}'
        else:
            mode_str = 'Will Starting in mode single-threading'

        self.logger.warning(f'{mode_str}, node_num={self.node_num}, node_count={self.node_count}')
        if self.buckets:
            self.logger.warning(f'Only async buckets: {self.buckets}')

        if self.test:
            self.logger.warning('Test Mode.')

        if self.small_size_first:
            self.logger.warning('Small object size first.')

        while True:
            try:
                # self.dev_test()
                for num in [1, 2]:
                    self.logger.debug(f"Start Backup number {num} async loop.")
                    self.loop_buckets(backup_num=num, names=self.buckets)
            except KeyboardInterrupt:
                self.in_exiting = True
                self.logger.error('Quit soon, because KeyboardInterrupt')

            key_ipt_count = 0
            while self.in_multi_thread:     # 多线程模式下，等待所有线程结束
                try:
                    c = threading.active_count()
                    if c <= 1:
                        break

                    self.logger.debug(f'There are {c} threads left to end.')
                    time.sleep(5)
                except KeyboardInterrupt:
                    self.in_exiting = True
                    key_ipt_count += 1
                    self.logger.debug(f'You need to enter CTL + C {3 - key_ipt_count} times, will be forced to exit.')
                    if key_ipt_count >= 3:
                        break

            break

        self.logger.warning('Exit')

    @staticmethod
    def get_current_node_num_from_hostname():
        f = os.popen("hostname")
        hostname = f.readline().strip()
        host_node_num = hostname.replace("ip", "")
        f.close()

        try:
            host_node_num = int(host_node_num)
        except ValueError as e:
            msg = f'Get host node number error, Hostname is {hostname}, ' \
                  f'host node number is {host_node_num}, can not to int'
            raise ValueError(f'{msg}, {str(e)}')

        return host_node_num

    def is_object_should_be_handled_by_me(self, object_id: int):
        yu = object_id % self.node_count
        if yu == 0:
            yu = self.node_count

        if yu == self.node_num:
            return True

        return False

    def loop_buckets(self, backup_num: int, names: list = None):
        """
        loop all buckets one times
        """
        last_bucket_id = 0
        error_count = 0
        can_not_connection = 0
        while True:
            try:
                buckets = QueryHandler().get_need_async_buckets(id_gt=last_bucket_id, limit=10, names=names)
                if len(buckets) == 0:  # 所有桶循环一遍完成
                    break
                for bucket in buckets:
                    bucket_id = bucket['id']
                    backup = QueryHandler().get_bucket_backup(bucket_id=bucket_id, backup_num=backup_num)
                    if backup is not None and backup['status'] == 'start':
                        if self.in_multi_thread:
                            self.create_async_bucket_thread(bucket=bucket, backup=backup)
                        else:
                            self.async_one_bucket(bucket=bucket, backup=backup)

                    last_bucket_id = bucket_id
                    error_count = 0
                can_not_connection = max(can_not_connection - 1, 0)
            except CanNotConnection as exc:
                can_not_connection += 1
                if can_not_connection > 6:
                    break

                self.logger.error(f"Error, loop_buckets, CanNotConnection db, sleep {can_not_connection} s,{str(exc)}")
                time.sleep(can_not_connection)
            except Exception as err:
                self.logger.error(f'{str(err)}')
                error_count += 1
                if error_count > 5:
                    break

                continue

    def create_async_bucket_thread(self, bucket: dict, last_object_id: int = 0, limit: int = 1000, backup: dict = None):
        if self.pool_sem.acquire():  # 可用线程数-1，控制线程数量，当前正在运行线程数量达到上限会阻塞等待
            try:
                worker = threading.Thread(
                    target=self.thread_async_one_bucket,
                    kwargs={'bucket': bucket, 'last_object_id': last_object_id, 'limit': limit, 'backup': backup}
                )
                worker.start()
            except Exception as e:
                self.pool_sem.release()  # 可用线程数+1

    def thread_async_one_bucket(self, bucket: dict, last_object_id: int = 0, limit: int = 100, backup: dict = None):
        try:
            self.async_one_bucket(bucket=bucket, last_object_id=last_object_id, limit=limit, backup=backup)
        except Exception as e:
            pass
        finally:
            self.pool_sem.release()  # 可用线程数+1

    def async_one_bucket(self, bucket: dict, last_object_id: int = 0, limit: int = 100, backup: dict = None):
        """
        :param bucket: Bucket instance
        :param last_object_id: 同步id大于last_object_id的对象
        :param limit: select objects number per times
        :param backup: 要同步的备份点
        """
        backup_num = backup["backup_num"]
        bucket_id = bucket["id"]
        bucket_name = bucket["name"]
        self.logger.debug(f'Start async Bucket(id={bucket_id}, name={bucket_name}), Backup number {backup_num}.')
        can_not_connection = 0
        failed_count = 0
        ok_count = 0
        query_hand = QueryHandler()
        last_object_id = last_object_id
        last_object_size = 0
        while True:
            try:
                if self.in_exiting:     # 退出中
                    break

                backup = query_hand.get_bucket_backup(bucket_id=bucket_id, backup_num=backup_num)
                if backup is None:
                    raise Exception(f'Bucket backup number {backup_num} not exists')
                elif backup['status'] != 'start':
                    raise Exception(f'Bucket backup number {backup_num} not start')

                if self.small_size_first:
                    kwargs = {'size_gte': last_object_size}
                else:
                    kwargs = {'id_gt': last_object_id}
                objs = query_hand.get_need_async_objects(
                    bucket_id=bucket_id, limit=limit,
                    backup_nums=[backup_num, ], **kwargs
                )
                if len(objs) == 0:
                    break

                ok_num, l_id, l_size, err = self.handle_async_objects(bucket=bucket, objs=objs, backup=backup)
                ok_count += ok_num
                if l_id is not None:
                    last_object_id = l_id
                if l_size is not None:
                    last_object_size = l_size
                if err is not None:
                    raise err
                if len(objs) < limit:
                    break

                can_not_connection = max(can_not_connection - 1, 0)
            except CanNotConnection as exc:
                can_not_connection += 1
                if can_not_connection > 6:
                    break

                self.logger.error(f"Error, async Bucket(id={bucket_id}, name={bucket_name}), "
                                  f"CanNotConnection db, sleep {can_not_connection} s,{str(exc)}")
                time.sleep(can_not_connection)
            except Exception as err:
                failed_count += 1
                if self.is_unusual_async_failed(failed_count=failed_count, ok_count=ok_count):
                    self.logger.debug(f"Skip bukcet(id={bucket_id}, name={bucket_name}), async unusual, "
                                      f"failed: {failed_count}, ok: {ok_count}.")
                    break

                self.logger.error(f"Error, async_bucket,{str(err)}")
                continue

        self.logger.debug(f'Exit async Bucket(id={bucket_id}, name={bucket_name}), Backup number {backup_num}, '
                          f'ok {ok_count}, failed {failed_count}.')

    def handle_async_objects(self, bucket, objs: list, backup: dict):
        """
        :return:
            (
                int                 # 成功同步对象数
                last_object_id,     # int or None, 已同步完成的最后一个对象的id
                last_object_size,   # int or None, 已同步完成的最后一个对象的size
                error               # None or Exception, AsyncError, CanNotConnection
            )
        """
        ok_count = 0
        last_object_id = None
        last_object_size = None
        for obj in objs:
            if self.in_exiting:
                break

            obj_id = obj['id']
            if self.is_object_should_be_handled_by_me(obj_id):
                if self.is_meet_async_to_backup(obj=obj, backup=backup):
                    r = self.async_one_object(bucket=bucket, obj=obj, backup=backup)
                    if r is not None:
                        return ok_count, last_object_id, last_object_size, r

                    ok_count += 1

            last_object_id = obj_id
            last_object_size = obj['si']

        return ok_count, last_object_id, last_object_size, None

    def async_one_object(self, bucket: dict, obj: dict, backup: dict):
        """
        :return:
            None    # success
            Error   # failed Exception, AsyncError, CanNotConnection
        """
        ret = None
        backup_num = backup['backup_num']
        msg = f"backup num {backup_num}, [bucket(id={bucket['id']}, name={bucket['name']})]," \
              f"[object(id={obj['id']}, key={obj['na']}, size={obj['si']})];"

        self.logger.debug(f"Start Async, {msg}")
        start_timestamp = time.time()
        try:
            if self.test:
                self.logger.debug(f"Test async {msg}")
                time.sleep(1)
            else:
                AsyncBucketManager().async_bucket_object(bucket=bucket, obj=obj, backup=backup)
        except Exception as e:
            ret = e
            self.logger.error(f"Failed Async, {msg}, {str(e)}")
        else:
            delta_seconds = time.time() - start_timestamp
            self.logger.debug(f"OK Async, Use {delta_seconds} s, {msg}")

        return ret

    @staticmethod
    def is_meet_async_to_backup(obj, backup):
        """
        对象是否满足条件同步到备份点

        :return:
            True    #
            False   #
        """
        # 对象修改时间超过now足够时间段后才允许同步, 尽量保证对象上传完成后再同步
        neet_time = QueryHandler().get_meet_time()
        async1 = obj['async1']
        async2 = obj['async2']
        upt = obj['upt']
        backup_num = backup['backup_num']

        if backup_num == BackupNum.ONE:
            if async1 is None or (async1 <= upt < neet_time):
                return True
        elif backup_num == BackupNum.TWO:
            if async2 is None or (async2 <= upt < neet_time):
                return True

        return False

    @staticmethod
    def is_unusual_async_failed(failed_count: int, ok_count: int):
        """
        是否同步失败的次数不寻常
        :return:
            True    # unusual
            False   # usual
        """
        count_failed = failed_count
        if count_failed < 10:   # 失败次数太少，避免偶然
            return False

        if count_failed > 1000:   # 失败次数太多
            return True

        count_ok = ok_count
        ratio = count_ok / count_failed     # 成功失败比例
        if ratio > 3:
            return False

        return True

    def dev_test(self):
        if self.in_multi_thread:
            self.test_working_multi_thread()
        else:
            self.test_working(forever=True)

    @staticmethod
    def test_working(forever: bool = False):
        while True:
            print('Running test')
            time.sleep(5)
            if not forever:
                break

    def test_working_multi_thread(self):
        from .databases import db_readwrite_lock, DEFAULT, METADATA

        @db_readwrite_lock
        def test_do_something(seconds: int, using):
            print(f'using {using}')
            time.sleep(seconds)
            pass

        def do_something(seconds: int, _self, in_multithread: bool = True):
            ident = threading.current_thread().ident
            print(f"Threading ident: {ident}, Start.")
            using = random.choice([DEFAULT, METADATA])
            test_do_something(seconds, using=using)
            # time.sleep(seconds)
            if in_multithread:
                _self.pool_sem.release()  # 可用线程数+1

            print(f"Threading ident: {ident}, End.")

        for i in range(self.max_threads + 10):
            if self.pool_sem.acquire():  # 可用线程数-1，控制线程数量，当前正在运行线程数量达到上限会阻塞等待
                worker = threading.Thread(
                    target=do_something,
                    kwargs={'seconds': random.randint(1, 10), '_self': self, 'in_multithread': True}
                )
                worker.start()
