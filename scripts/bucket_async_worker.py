import os
import sys
import logging

import psutil

from async_task import AsyncTask


def config_logger(name: str = 'async-logger', level=logging.INFO):
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


PARAM_DEBUG = 'debug'
PARAM_HELP = 'help'
PARAM_TEST = 'test'
PARAM_NODE_NUM = 'node-num'
PARAM_NODE_COUNT = 'node-count'
PARAM_MULTI_THREAD = 'multi-thread'
PARAM_MAX_THREADS = 'max-threads'
PARAM_STOP = 'stop'
PARAM_STATUS = 'status'
PARAM_BUCKETS = 'buckets'
PARAM_NAME_LIST = [
    PARAM_DEBUG, PARAM_HELP, PARAM_TEST, PARAM_NODE_NUM, PARAM_NODE_COUNT, PARAM_MULTI_THREAD, PARAM_MAX_THREADS,
    PARAM_STOP, PARAM_STATUS, PARAM_BUCKETS
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
    {PARAM_BUCKETS}:        Only bucket to async, '["name1","name2"]'
    
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

    if PARAM_BUCKETS in params:
        buckets = params[PARAM_BUCKETS]
        import json
        try:
            b = json.loads(buckets)
        except Exception as e:
            raise ValueError(f'"{PARAM_BUCKETS}"({buckets}) must be json list, No spaces allowed, {str(e)}')

        if not isinstance(b, list):
            raise ValueError(f'"{PARAM_BUCKETS}"({buckets}) must be json list')

        params[PARAM_BUCKETS] = b

    return params


def get_cmd_name():
    cmd_name = sys.argv[0]
    li = cmd_name.rsplit('/', maxsplit=1)
    if len(li) == 1:
        cmd_name = li[0]
    else:
        cmd_name = li[1]

    return cmd_name


def do_stop():
    # name = get_cmd_name()
    # cmd = f"ps aux | grep {name} | grep -v grep |awk '{{print $2}}' |xargs -r kill -9"
    # os.system(cmd)
    pid = do_status()
    if pid is not None:
        os.system(f"kill -9 {pid}")


def check_cmd_run():
    """
    检测系统中是否存在该程序进程

    :return:
        (
            pid: int,        # None(不存在)， int(存在)
            list
        )

    :raises: Exception
    """
    pid_self = os.getpid()
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


def do_status():
    pid, cmd_line = check_cmd_run()
    if pid is None:
        print('No running commands were found.')
    else:
        print(f"Found running commands, PID={pid}, {cmd_line}")

    return pid


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
        do_stop()
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

    if PARAM_BUCKETS in params:
        kwargs['buckets'] = params[PARAM_BUCKETS]

    try:
        check_same_task_run()
    except Exception as e:
        logger.error(str(e))
        exit(1)  # exit error

    task = AsyncTask(logger=logger, **kwargs)
    task.run()


if __name__ == '__main__':
    main()
