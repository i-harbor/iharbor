import os
import random
import sys
import threading
import logging
import time

import django
import psutil

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# 设置项目的配置文件 不做修改的话就是 settings 文件
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webserver.settings")
django.setup()

from api.backup import AsyncBucketManager
from buckets.models import BackupBucket


LOGGER_NAME = 'async-logger'


def config_logger(name: str = LOGGER_NAME, level=logging.INFO):
    # 日志配置
    log_files_dir = '/var/log/iharbor'
    if not os.path.exists(log_files_dir):
        os.makedirs(log_files_dir, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(message)s ",  # 配置输出日志格式
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    std_handler = logging.StreamHandler(stream=sys.stdout)
    std_handler.setFormatter(formatter)
    logger.addHandler(std_handler)

    file_handler = logging.FileHandler(filename=f"{log_files_dir}/async_bucket.log")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    return logger


class AsyncTask:
    def __init__(self, node_num: int = None, node_count: int = 100,
                 in_multi_thread: bool = False, max_threads: int = 10,
                 test: bool = False, logger=None
                 ):
        """
        :param node_num: 当前工作节点编号，不指定尝试从hostname获取
        :param node_count: 一共多少个节点，用于id求余，只同步 余数 == node_num的对象
        :param in_multi_thread: True(开启多线程模式)， False(单线程)
        :param max_threads: 多线程工作模式时，最大线程数
        :param test: 不同步对象，打印一些参数
        :param logger:
        """
        if logger is None:
            logger = logging.getLogger(LOGGER_NAME)

        self.logger = logger
        self.test = test
        self.in_multi_thread = in_multi_thread
        self.max_threads = max_threads
        self.node_count = node_count
        self.node_num = node_num

        try:
            self.validate_params()
        except ValueError as e:
            self.logger.error(str(e))
            exit(1)     # exit error

        self.pool_sem = threading.Semaphore(self.max_threads)  # 定义最多同时启用多少个线程

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
        if self.test:
            self.logger.warning('Test Mode.')

        try:
            self.check_same_task_run()
        except Exception as e:
            self.logger.error(str(e))
            exit(1)  # exit error

        while True:
            try:
                # self.dev_test()
                for num in BackupBucket.BackupNum.values:
                    self.logger.debug(f"Start Backup number {num} async loop.")
                    self.loop_buckets(backup_num=num)
            except KeyboardInterrupt:
                self.logger.error('Quit soon, because KeyboardInterrupt')

            while self.in_multi_thread:     # 多线程模式下，等待所有线程结束
                c = threading.active_count()
                if c <= 1:
                    break

                self.logger.debug(f'There are {c} threads left to end.')
                time.sleep(2)

            break

        self.logger.warning('Exit')

    @staticmethod
    def check_same_task_run():
        """
        检测系统中是否存在该程序进程

        :raises: Exception
        """
        pid, process_line = check_cmd_run()
        if pid is not None:
            pid_self = os.getpid()
            cmd_self = psutil.Process(pid_self).cmdline()
            msg = f"检测到可能有该程序进程正在运行, PID={pid}, Process cmd:{process_line}; 当前Process cmd：{cmd_self}"
            raise Exception(msg)

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

    def loop_buckets(self, backup_num: int):
        """
        loop all buckets one times
        """
        manager = AsyncBucketManager()
        last_bucket_id = 0
        while True:
            try:
                buckets = manager.get_need_async_bucket_queryset(id_gt=last_bucket_id, limit=10)
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

        manager = AsyncBucketManager()
        last_object_id = last_object_id
        while True:
            try:
                objs = manager.get_need_async_objects_queryset(
                    bucket, id_gt=last_object_id, limit=limit,
                    id_mod_div=id_mod_div, id_mod_equal=id_mod_equal,
                    backup_num=backup_num
                )
                # self.logger.debug(f'objects count:{len(objs)}')
                if len(objs) == 0:
                    break
                for obj in objs:
                    if self.is_object_should_be_handled_by_me(obj.id):
                        if self.in_multi_thread:
                            self.create_async_thread(bucket=bucket, obj=obj, backup_num=backup_num)
                        else:
                            r = self.async_one_object(bucket=bucket, obj=obj, in_multithread=False,
                                                      backup_num=backup_num)
                            if r is not None:
                                raise r

                    last_object_id = obj.id

                if not is_loop:
                    break
            except Exception as err:
                self.logger.error(f"Error, async_bucket,{str(err)}")
                raise err  # continue

        self.logger.debug(f'Exit async Bucket(id={bucket.id}, name={bucket.name}), Backup number {backup_num}.')

    def create_async_thread(self, bucket, obj, backup_num: int = None):
        if self.pool_sem.acquire():  # 可用线程数-1，控制线程数量，当前正在运行线程数量达到上限会阻塞等待
            worker = threading.Thread(
                target=self.async_one_object,
                kwargs={'bucket': bucket, 'obj': obj, 'in_multithread': True, 'backup_num': backup_num}
            )
            worker.start()

    def async_one_object(self, bucket, obj, in_multithread: bool = True, backup_num: int = None):
        """
        :return:
            None    # success
            Error   # failed

        :raises: Exception, AsyncError
        """
        ret = None
        msg = f"backup num {backup_num}, [bucket(id={bucket.id}, name={bucket.name})]," \
              f"[object(id={obj.id}, key={obj.na})];"

        self.logger.debug(f"Start Async, {msg}")
        start_timestamp = time.time()
        do_async_num_list = []
        try:
            if self.test:
                self.logger.debug(f"Test async {msg}")
                time.sleep(1)
            else:
                r = AsyncBucketManager().async_bucket_object(bucket=bucket, obj=obj, backup_num=backup_num)
                for num, val in r.items():
                    do_async_num_list.append(num)
                    ok, err = val
                    if not ok:
                        raise err
        except Exception as e:
            ret = e
            self.logger.error(f"Failed Async, {msg}, {str(e)}")
        else:
            delta_seconds = time.time() - start_timestamp
            self.logger.debug(f"OK Async, Use {delta_seconds} s, Async actually done num {do_async_num_list}, {msg}")

        if in_multithread:
            self.pool_sem.release()  # 可用线程数+1

        return ret

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


PARAM_DEBUG = 'debug'
PARAM_HELP = 'help'
PARAM_TEST = 'test'
PARAM_NODE_NUM = 'node-num'
PARAM_NODE_COUNT = 'node-count'
PARAM_MULTI_THREAD = 'multi-thread'
PARAM_MAX_THREADS = 'max-threads'
PARAM_STOP = 'stop'
PARAM_STATUS = 'status'
PARAM_NAME_LIST = [
    PARAM_DEBUG, PARAM_HELP, PARAM_TEST, PARAM_NODE_NUM, PARAM_NODE_COUNT, PARAM_MULTI_THREAD, PARAM_MAX_THREADS,
    PARAM_STOP, PARAM_STATUS
]


def print_help():
    print(
        f"""
    python scripts/bucket_async_worker.py [{PARAM_DEBUG}] [{PARAM_HELP}] [{PARAM_NODE_NUM}=1] [{PARAM_NODE_COUNT}=100]
        
    {PARAM_DEBUG}:          Print debug message, No value is required.
    {PARAM_HELP}:           Print help message, No value is required.
    {PARAM_TEST}:           Run cmd but not async objects, No value is required.
    {PARAM_NODE_NUM}:       Specifies the node number of the command, Required value int; Default try get from hostname. 
    {PARAM_NODE_COUNT}:     Total number of nodes, Required value int.
    {PARAM_MULTI_THREAD}:   Work in multi-threading mode, No value is required.
    {PARAM_MAX_THREADS}:    Maximum number of threads when work in multi-threading mode, Required value int.
    {PARAM_STOP}:           Stop process
    {PARAM_STATUS}:         Is it running
    
    * daemon mode run cmd:
        nohup cmd >/dev/null 2>&1 &
        """
    )


def parse_params():
    """
    :return: dict
    :raises: ValueError
    """
    params = {}
    argv = sys.argv
    for arg in argv[1:]:
        name_val = arg.split('=', maxsplit=1)
        le = len(name_val)
        if le == 1:
            params[name_val[0]] = None
        elif le == 2:
            name, val = name_val
            params[name] = val
        else:
            raise ValueError(f"Invalid param {arg}")

    for name in params:
        if name not in PARAM_NAME_LIST:
            raise ValueError(f'Got an unexpected keyword argument "{name}"')

    return params


def validate_params(params):
    """
    :return: dict
    :raises: ValueError
    """
    if PARAM_NODE_NUM in params:
        node_num = params[PARAM_NODE_NUM]
        try:
            node_num = int(node_num)
        except ValueError as e:
            raise ValueError(f'Invalid value({node_num}) of param "{PARAM_NODE_NUM}", must be int, {str(e)}')

        if node_num <= 0:
            raise ValueError(f'"{PARAM_NODE_NUM}"({node_num}) must be greater than 0')

        params[PARAM_NODE_NUM] = node_num

    if PARAM_NODE_COUNT in params:
        node_count = params[PARAM_NODE_COUNT]
        try:
            node_count = int(node_count)
        except ValueError as e:
            raise ValueError(f'Invalid value({node_count}) of param "{PARAM_NODE_COUNT}", must be int, {str(e)}')

        if node_count <= 0:
            raise ValueError(f'"{PARAM_NODE_COUNT}"({node_count}) must be greater than 0')

        params[PARAM_NODE_COUNT] = node_count

        if PARAM_NODE_NUM in params:
            node_num = params[PARAM_NODE_NUM]
            if node_num > node_count:
                raise ValueError(
                    f'{PARAM_NODE_NUM}({node_num}) cannot be greater than {PARAM_NODE_COUNT}({node_count})')

    if PARAM_MAX_THREADS in params:
        max_threads = params[PARAM_MAX_THREADS]
        try:
            max_threads = int(max_threads)
        except ValueError as e:
            raise ValueError(f'Invalid value({max_threads}) of param "{PARAM_MAX_THREADS}", must be int, {str(e)}')

        if max_threads <= 0:
            raise ValueError(f'"{PARAM_MAX_THREADS}"({max_threads}) must be greater than 0')

        params[PARAM_MAX_THREADS] = max_threads

    return params


def get_cmd_name():
    cmd_name = sys.argv[0]
    li = cmd_name.rsplit('/', maxsplit=1)
    if len(li) == 1:
        cmd_name = li[0]
    else:
        cmd_name = li[1]

    return cmd_name


def do_stop(name: str):
    cmd = f"ps aux | grep {name} | grep -v grep |awk '{{print $2}}' |xargs -r kill -9"
    os.system(cmd)


def check_cmd_run():
    """
    检测系统中是否存在该程序进程

    :return:
        int, list

    :raises: Exception
    """
    pid_self = os.getpid()
    # cmd_self = psutil.Process(pid_self).cmdline()
    pid_list = psutil.pids()
    if pid_self in pid_list:    # remove self pid
        pid_list.remove(pid_self)

    cmd_name = get_cmd_name()
    for pid in pid_list:
        # 进程命令
        try:
            process_line = psutil.Process(pid).cmdline()
        except Exception as e:
            continue
        if not process_line:
            continue

        if "python" in process_line[0]:
            if cmd_name in process_line[1]:
                return pid, process_line

    return None, None


def do_status():
    pid, cmd_line = check_cmd_run()
    if pid is None:
        print('No running commands were found.')
    else:
        print(f"Found running commands, PID={pid}, {cmd_line}")


def main():
    try:
        params = parse_params()
        params = validate_params(params)
    except ValueError as e:
        print(str(e))
        exit(1)
        return

    if PARAM_HELP in params:
        print_help()
        exit(0)

    if PARAM_STATUS in params:
        do_status()
        exit(0)

    if PARAM_STOP in params:
        cmd_name = get_cmd_name()
        do_stop(cmd_name)
        exit(0)

    kwargs = {}
    if PARAM_TEST in params:
        kwargs['test'] = True

    if PARAM_NODE_NUM in params:
        kwargs['node_num'] = params[PARAM_NODE_NUM]

    if PARAM_NODE_COUNT in params:
        kwargs['node_count'] = params[PARAM_NODE_COUNT]

    if PARAM_MULTI_THREAD in params:
        kwargs['in_multi_thread'] = True

    if PARAM_MAX_THREADS in params:
        kwargs['max_threads'] = params[PARAM_MAX_THREADS]

    if PARAM_DEBUG in params:
        logger = config_logger(level=logging.DEBUG)
    else:
        logger = config_logger(level=logging.DEBUG)

    task = AsyncTask(logger=logger, **kwargs)
    task.run()


if __name__ == '__main__':
    main()
