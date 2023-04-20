import time
import config
import os
import sys
import logging
from node_connect_cline import NodeClient
from functools import wraps


def async_monitor_logger(name: str = 'async-monitor-logger', level=logging.INFO):
    # 日志配置
    log_files_dir = '/var/log/nginx'
    if not os.path.exists(log_files_dir):
        os.makedirs(log_files_dir, exist_ok=True)

    logger = logging.getLogger(name)
    file_handler = logging.FileHandler(filename=f"{log_files_dir}/async_monitor.log")
    std_handler = logging.StreamHandler(stream=sys.stdout)

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(message)s ",  # 配置输出日志格式
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger.setLevel(level)
    file_handler.setLevel(level)
    std_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    logger.addHandler(std_handler)
    logger.addHandler(file_handler)
    return logger


def delay_time(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        out = func(self, *args, **kwargs)
        time.sleep(self.delay_t)
        return out

    return wrapper


class ServiceCommandHandle:
    """
    生成ip和命令 字典
    """

    def __init__(self):
        self.ip_start = getattr(config, 'IP_START', None)
        self.ip_end = getattr(config, 'IP_END', None)
        self.python_alise = getattr(config, 'PYTHON', 'python')
        self.loacl_script = getattr(config, 'LOCALSCRIPT', '/home/uwsgi/iharbor/scripts/bucket_async_worker.py')

    def get_ip_range(self):
        try:
            ip_s = int(self.ip_start.split('.')[3])
            ip_e = int(self.ip_end.split('.')[3])
        except Exception as e:
            raise ValueError(f"请查看config.py配置：error {str(e)}")
        ip_rang = ip_e - ip_s
        return ip_rang + 1, ip_s, ip_e

    def get_ip_prefix(self):
        if self.ip_start:
            return self.ip_start.rsplit(".", 1)[0]
        raise ValueError("您没有在config.py配置ip地址")

    def get_ip_range_list(self):
        """
        获取 ip 范围
        :return:
        """
        ip_list = []
        ip_rang, ip_s, ip_e = self.get_ip_range()
        ip_prefix = self.get_ip_prefix()
        for i in range(ip_s, ip_e + 1):
            ip_ = ip_prefix + "." + str(i)
            ip_list.append(ip_)

        if ip_rang != len(ip_list):
            raise ValueError(f"ip范围与生成的ip列表不匹配{ip_rang} != {len(ip_list)} : 生成的ip列表 {ip_list}")

        return ip_list

    def get_node_count(self):
        """
        获取节点范围
        :return:
        """
        if self.ip_end and self.ip_start:
            try:
                self.node_num, _, _ = self.get_ip_range()
            except Exception as e:
                raise e
            return self.node_num
        else:
            raise ValueError("请在config.py文件中填写ip")

    def get_ip_dict(self):
        """
        ip 序号字典
        {1：[ip]}
        :return:
        """
        ip_dict = {}
        node_num = self.get_node_count()
        ip_list = self.get_ip_range_list()
        for i in range(1, node_num + 1):
            ip_dict[i] = [ip_list[i - 1]]

        return ip_dict

    def command_template(self, state=None, bucket=None, thread=None):
        """
        state: stop status
        :return:
        """
        command_base = f"{self.python_alise} {self.loacl_script}"
        if state:
            command_base = command_base + f" {state}"
            return command_base

        if bucket and thread:
            command = command_base + f" buckets='{bucket}' multi-thread  max-threads={thread}"
            return command
        elif bucket:
            command = command_base + f" buckets='{bucket}' "
            return command
        elif thread:
            command = command_base + f" multi-thread  max-threads={thread}"
            return command
        else:
            # start
            return command_base

    def generate_command(self, state, bucket, thread, nodenum):
        """
        生成 命令
        :param state:
        :param bucket:
        :param thread:
        :param nodenum:
        :return:
        """
        ip_range, ip_s, ip_e = self.get_ip_range()
        command_base = self.command_template(state=state, bucket=bucket, thread=thread)
        if not state:
            command = command_base + f" node-num={nodenum} node-count={ip_range}"
            command = f'nohup {command} >/dev/null 2>&1 &'  # 后端运行
            return command
        return command_base

    # def ip_command(self, state):
    #     """
    #     ip 批量命令
    #     :param state:
    #     :return:
    #     """
    #     ip_dict = self.get_ip_dict()
    #     # ip_range, ip_s, ip_e = self.get_ip_range()
    #     command_base = self.command_template(state=state)
    #     for num in ip_dict:
    #         ip_dict[num].append(command_base)
    #
    #     return ip_dict
    #


class NodeMonitor:
    """
    节点监控
    1. 查看服务是否正在运行、
    2. 正在运行 退出 ，停止运行 需要连接客户端发送命令
    """

    def __init__(self):
        self.username = getattr(config, 'USERNAME', None)
        self.passwoed = getattr(config, 'PASSWARD', None)
        self.thread_num = getattr(config, 'THREAD_NUM', None)
        self.bucket_list = getattr(config, 'BUCKETLIST', None)
        self.logger = async_monitor_logger(level=logging.DEBUG)
        self.error_list = {}  # 有异常的节点列表
        self.status_list = {}  # 正在运行的节点列表
        self.stop_list = {}  # 停止运行的节点列表
        self.restart_list = {} # 重新启动的节点列表
        self.ip_command_handle = ServiceCommandHandle()
        self.delay_t = 0

    def connect(self, hostname, command):
        """
        连接客户端
        :param hostname:
        :param command:
        :return:
        """
        hostname = "223.193.36.121"
        node_client = NodeClient()
        try:
            if self.username and self.passwoed:
                node_client.connect(hostname, username=self.username, password=self.passwoed)
            else:
                node_client.connect(hostname)
            stdin, stdout, stderr = node_client.exec_command(command)
        except Exception as e:
            raise e
        err = stderr.read().decode('utf8')
        if err:
            raise ValueError(f"执行{command}命令错误：err = {err}")

        node_client.close()

        return stdout

    def run_start(self, hostname, nodenum):
        """
        启动服务

        :param command:
        :return:
        """

        out = self.task(nodenum=nodenum, hostname=hostname, state=None, bucket=self.bucket_list,
                        thread=self.thread_num)
        if self.run_status(hostname=hostname, nodenum=nodenum) is False:
            self.error_list[nodenum] = hostname
            self.logger.error(f"节点：id = {nodenum}, hostname = {hostname} 异常未启动成功")
        else:
            self.status_list[nodenum] = hostname
            self.logger.debug(f"节点：id = {nodenum}, hostname = {hostname} 启动成功")
            return True

        return False

    def run_stop(self, hostname, nodenum):
        """
        停止服务
        :param command:
        :return:
        """
        count = 0
        while True:

            out = self.task(nodenum=nodenum, hostname=hostname, state="stop", bucket=self.bucket_list,
                            thread=self.thread_num)
            if self.run_status(hostname=hostname, nodenum=nodenum) is True and count <= 3:
                # 服务未关闭成功
                count += 1
                if count > 3:
                    self.error_list[nodenum] = hostname
                    self.logger.error(f"节点：id = {nodenum} , hostname = {hostname} 服务未关闭成功")
                    break
            else:
                # 服务正常关闭
                self.stop_list[nodenum] = hostname
                self.logger.debug(f"节点：id = {nodenum} , hostname = {hostname} 服务关闭成功")
                return True
        return False

    def run_status(self, hostname, nodenum, deplay_logger=None):
        """
        查看服务状态
        :param command:
        :return:
        """
        out = self.task(nodenum=nodenum, hostname=hostname, state="status", bucket=self.bucket_list,
                        thread=self.thread_num)

        if out.startswith('No running commands'):
            if deplay_logger:
            # 程序没有执行
                self.stop_list[nodenum] = hostname
                self.logger.debug(f"节点：id = {nodenum} , hostname = {hostname} 服务处于未执行状态")
            return False

        if out.startswith('Found running commands'):
            # 程序正在运行/停止时显示
            if deplay_logger:
                self.status_list[nodenum] = hostname
                self.logger.debug(f"节点：id = {nodenum} , hostname = {hostname} 服务处于执行状态")
            return True
        raise ValueError("命令返回内容有误请检查代码。")

    def checkrestart(self, hostname, nodenum):
        """检查是否有未能启动成功的节点，将服务启动"""

        run_status_bool = self.run_status(hostname=hostname, nodenum=nodenum, deplay_logger=True)
        if run_status_bool is False:
            # 先执行下停止服务命令
            run_stop_bool = self.run_stop(hostname=hostname, nodenum=nodenum)
            if run_stop_bool is False:
                self.error_list[nodenum] = hostname
                self.logger.error(f"节点：id = {nodenum} , hostname = {hostname} 重启：停止服务异常")
                return run_stop_bool  # False
            # 服务没有运行 拉起服务
            run_start_bool = self.run_start(hostname=hostname, nodenum=nodenum)
            if run_start_bool:
                self.restart_list[nodenum] = hostname
                self.logger.debug(f"节点：id = {nodenum} , hostname = {hostname} 重启：服务重启成功")
                return True
            else:
                self.error_list[nodenum] = hostname
                self.logger.error(f"节点：id = {nodenum} , hostname = {hostname} 重启：服务无法重启")
                return run_start_bool  # False

        else:
            self.status_list[nodenum] = hostname
            self.logger.debug(f"节点：id = {nodenum} , hostname = {hostname} 重启：服务正在运行")

    @delay_time
    def task(self, nodenum, hostname, state, bucket=None, thread=None):
        """
        命令发送任务
        :param nodenum:
        :param hostname:
        :param command:
        :param test:
        :param bucket:
        :param thread:
        :return:
        """

        command = self.ip_command_handle.generate_command(state=state, bucket=bucket, thread=thread,
                                                          nodenum=nodenum)
        self.logger.debug(f"节点执行命令：id = {nodenum} , hostname = {hostname}, command = {command}。")
        stdout = self.connect(hostname=hostname, command=command)
        out = stdout.readline()
        return out

    def release_command(self, command):
        """
        发送命令
        command: start stop status
        :return:
        """
        self.ip_command_handle = ServiceCommandHandle()
        # ip_dict = self.ip_command_handle.ip_command(state=command)

        for num, value in self.ip_command_handle.get_ip_dict().items():
            # 连接客户端
            try:
                if command == "start":
                    self.run_start(hostname=value[0], nodenum=num)
                elif command == "stop":
                    self.run_stop(hostname=value[0], nodenum=num)
                elif command == "status":
                    self.run_status(hostname=value[0], nodenum=num, deplay_logger=True)
                elif command == "checkrestart":
                    self.checkrestart(hostname=value[0], nodenum=num)
                else:
                    self.help()
            except Exception as e:
                self.error_list[num] = value[0]
                self.logger.error(f"节点：id = {num} , hostname = {value[0]} 执行命令错误：{str(e)}")


    def help(self):
        """

        :return:
        """
        print(f"""
            python scripts/async_distribution/node_monitoring.py start/stop/status/checkrestart/help delay-time
            arg1:
                start               启动服务
                stop                停止服务
                status              服务运行状态
                checkrestart        start 未启动成功,再次拉起服务
                help                提供帮助信息
            arg2:    
                delay-time          延时参数
            
            执行主控制端需要先使用 python scripts/bucket_async_worker.py 脚本测试;
            没有问题请先配置 scripts/async_distribution/config.py 内容；
        """)

    def arguments(self):
        """终端输入参数"""


        try:
            run_status = sys.argv[1]  # start stop status
        except Exception as e:
            return None, 0

        try:
            run_status2 = int(sys.argv[2])  # 延时时间
        except Exception as e:
            return run_status, 0

        if isinstance(run_status2, int):
            return run_status, run_status2

        else:

            return 'help', 0

    def run(self):
        """
        参数： start/stop/status
        :return:
        """
        arg1, arg2 = self.arguments()

        if not arg1 or arg1 == 'help':
            self.help()
            return

        if arg1 not in ["start", "stop", "status", "checkrestart"]:
            self.help()

        if arg2:
            self.delay_t = arg2

        self.logger.debug(f"监控程序服务启动。")

        try:
            self.release_command(command=arg1)
        except Exception as e:
            self.logger.error(f"监控程序服务启动失败， 错误为：{str(e)}")

        if self.error_list:
            self.logger.error(f"列出服务执行命令异常的节点: {self.error_list}")
        if self.status_list:
            self.logger.debug(f"列出服务启动成功/正在运行的节点: {self.status_list}")
        if self.stop_list:
            self.logger.debug(f"列出服务正常关闭的节点: {self.stop_list}")
        if self.restart_list:
            self.logger.debug(f"列出服务重启正常的节点：{self.restart_list}")


if __name__ == '__main__':
    n = NodeMonitor()
    n.run()
