import sys
import os
import time
import threading
import random

import django
from django.db import connections
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# 设置项目的配置文件 不做修改的话就是 settings 文件
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webserver.settings")
django.setup()

from api.backup import AsyncBucketManager
from buckets.models import BackupBucket


class AsyncTask:
    def __init__(self, node_num: int = None, node_count: int = 100,
                 in_multi_thread: bool = False, max_threads: int = 10,
                 test: bool = False, logger=None, buckets: list = None
                 ):
        """
        :param node_num: 当前工作节点编号，不指定尝试从hostname获取
        :param node_count: 一共多少个节点，用于id求余，只同步 余数 == node_num的对象
        :param in_multi_thread: True(开启多线程模式)， False(单线程)
        :param max_threads: 多线程工作模式时，最大线程数
        :param test: 不同步对象，打印一些参数
        :param logger:
        :param buckets: 只同步指定桶
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

        try:
            self.validate_params()
        except ValueError as e:
            self.logger.error(str(e))
            exit(1)     # exit error

        self.pool_sem = threading.Semaphore(self.max_threads)  # 定义最多同时启用多少个线程
        self.ok_count = 0           # 同步对象成功次数
        self.failed_count = 0       # 同步对象失败次数

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

        while True:
            try:
                # self.dev_test()
                for num in BackupBucket.BackupNum.values:
                    self.logger.debug(f"Start Backup number {num} async loop.")
                    self.loop_buckets(backup_num=num, names=self.buckets)
            except KeyboardInterrupt:
                self.logger.error('Quit soon, because KeyboardInterrupt')

            while self.in_multi_thread:     # 多线程模式下，等待所有线程结束
                c = threading.active_count()
                if c <= 1:
                    break

                self.logger.debug(f'There are {c} threads left to end.')
                time.sleep(2)

            break

        self.logger.warning(f'Exit, async ok: {self.ok_count}, failed: {self.failed_count}')

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
        manager = AsyncBucketManager()
        last_bucket_id = 0
        while True:
            try:
                buckets = manager.get_need_async_bucket_queryset(id_gt=last_bucket_id, limit=10, names=names)
                if len(buckets) == 0:  # 所有桶循环一遍完成
                    break
                for bucket in buckets:
                    backup = bucket.backup_buckets.filter(backup_num=backup_num).first()
                    if backup is not None and backup.status == BackupBucket.Status.START:
                        try:
                            self.async_bucket(bucket, backup_num=backup_num)       # 同步桶
                        except Exception as err:
                            continue

                    last_bucket_id = bucket.id
            except Exception as err:
                self.logger.error(f'{str(err)}')
                continue

    def async_bucket(self, bucket, last_object_id: int = 0, limit: int = 100, is_loop: bool = True,
                     backup_num: int = None
                     ):
        """
        :param bucket: Bucket instance
        :param last_object_id: 同步id大于last_object_id的对象
        :param limit: select objects number per times
        :param is_loop: True(循环遍历完桶内所有对象)；False(只查询一次，最多limit个对象，然后结束)
        :param backup_num: 要同步的备份点编号

        :raises: Exception, AsyncError
        """
        self.logger.debug(f'Start async Bucket(id={bucket.id}, name={bucket.name}), Backup number {backup_num}.')
        id_mod_div = self.node_count
        if self.node_count == self.node_num:
            id_mod_equal = 0
        else:
            id_mod_equal = self.node_num

        start_failed = self.failed_count     # 失败次数起始值
        start_ok = self.ok_count
        manager = AsyncBucketManager()
        last_object_id = last_object_id
        err_count = 0
        while True:
            try:
                backup = bucket.backup_buckets.filter(backup_num=backup_num).first()
                if backup is None:
                    raise Exception(f'Bucket backup number {backup_num} not exists')
                elif backup.status != BackupBucket.Status.START:
                    raise Exception(f'Bucket backup number {backup_num} not start')

                objs = manager.get_need_async_objects_queryset(
                    bucket, id_gt=last_object_id, limit=limit,
                    id_mod_div=id_mod_div, id_mod_equal=id_mod_equal,
                    backup_num=backup_num
                )
                if len(objs) == 0:
                    break

                l_id, err = self.handle_async_objects(bucket=bucket, objs=objs, backup=backup)
                if l_id is not None:
                    last_object_id = l_id
                if err is not None:
                    raise err

                if not is_loop:
                    break

                if self.is_unusual_async_failed(failed_start=start_failed, ok_start=start_ok):
                    self.logger.debug(f"Skip bukcet(id={bucket.id}, name={bucket.name}), async unusual, "
                                      f"failed: {self.failed_count - start_failed}, ok: {self.ok_count - start_ok}.")
                    break
                err_count = 0
            except Exception as err:
                err_count += 1
                if err_count >= 3:
                    break

                self.logger.error(f"Error, async_bucket,{str(err)}")
                continue

        self.logger.debug(f'Exit async Bucket(id={bucket.id}, name={bucket.name}), Backup number {backup_num}.')

    def handle_async_objects(self, bucket, objs, backup):
        """
        :return:
            (
                last_object_id,     # int or None, 已同步完成的最后一个对象的id
                error               # None or Exception, AsyncError
            )
        """
        last_object_id = None
        for obj in objs:
            if self.is_object_should_be_handled_by_me(obj.id):
                if self.in_multi_thread:
                    self.create_async_thread(bucket=bucket, obj=obj, backup=backup)
                else:
                    r = self.async_one_object(bucket=bucket, obj=obj, backup=backup)
                    if r is not None:
                        return last_object_id, r

            last_object_id = obj.id

        return last_object_id, None

    def create_async_thread(self, bucket, obj, backup):
        if self.pool_sem.acquire():  # 可用线程数-1，控制线程数量，当前正在运行线程数量达到上限会阻塞等待
            worker = threading.Thread(
                target=self.thread_async_one_object,
                kwargs={'bucket': bucket, 'obj': obj, 'backup': backup}
            )
            worker.start()

    def thread_async_one_object(self, bucket, obj, backup):
        try:
            self.async_one_object(bucket=bucket, obj=obj, backup=backup)
        except Exception as e:
            pass
        finally:
            self.pool_sem.release()  # 可用线程数+1
            connections.close_all()

    def async_one_object(self, bucket, obj, backup):
        """
        :return:
            None    # success
            Error   # failed

        :raises: Exception, AsyncError
        """
        ret = None
        msg = f"backup num {backup.backup_num}, [bucket(id={bucket.id}, name={bucket.name})]," \
              f"[object(id={obj.id}, key={obj.na})];"

        self.logger.debug(f"Start Async, {msg}")
        start_timestamp = time.time()
        try:
            if self.test:
                self.logger.debug(f"Test async {msg}")
                time.sleep(1)
            else:
                ok, err = AsyncBucketManager().async_bucket_object(bucket=bucket, obj=obj, backup=backup)
                if not ok:
                    raise err
        except Exception as e:
            self.failed_count_plus()
            ret = e
            self.logger.error(f"Failed Async, {msg}, {str(e)}")
        else:
            self.ok_count_plus()
            delta_seconds = time.time() - start_timestamp
            self.logger.debug(f"OK Async, Use {delta_seconds} s, {msg}")

        return ret

    def failed_count_plus(self):
        self.failed_count += 1      # 多线程并发时可能有误差，为效率不加互斥锁

    def ok_count_plus(self):
        self.ok_count += 1      # 多线程并发时可能有误差，为效率不加互斥锁

    def is_unusual_async_failed(self, failed_start: int, ok_start: int):
        """
        是否同步失败的次数不寻常
        :return:
            True    # unusual
            False   # usual
        """
        count_failed = self.failed_count - failed_start
        if count_failed < 10:   # 失败次数太少，避免偶然
            return False

        if count_failed > 1000:   # 失败次数太多
            return True

        count_ok = self.ok_count - ok_start
        ratio = count_ok / count_failed     # 成功失败比例
        if ratio > 3:
            return False

        return True

    def dev_test(self):
        if self.in_multi_thread:
            self.test_working_multi_thread()
        else:
            self.test_working(forever=True)

    def test_working(self, forever: bool = False):
        while True:
            print('Running test')
            time.sleep(5)
            if not forever:
                break

    def test_working_multi_thread(self):
        def do_something(seconds: int, self, in_multithread: bool = True):
            ident = threading.current_thread().ident
            print(f"Threading ident: {ident}, Start.")
            time.sleep(seconds)
            if in_multithread:
                self.pool_sem.release()  # 可用线程数+1

            print(f"Threading ident: {ident}, End.")

        for i in range(self.max_threads + 10):
            if self.pool_sem.acquire():  # 可用线程数-1，控制线程数量，当前正在运行线程数量达到上限会阻塞等待
                worker = threading.Thread(
                    target=do_something,
                    kwargs={'seconds': random.randint(1, 10), 'self': self, 'in_multithread': True}
                )
                worker.start()
