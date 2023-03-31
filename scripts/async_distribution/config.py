# -------------------------------
# 远程连接修改部分
# -------------------------------
IP_START = ""   # 节点起始ip
IP_END = ""  # 节点结束ip

USERNAME = ""
PASSWARD = ""
# -------------------------------
# 配置修改部分
# -------------------------------
THREAD_NUM = None  # 并发线程数 None/int
BUCKETLIST = None  # 指定的桶列表 '["bucket1", "bucket2"]' / None
PYTHON = 'python'  # python3 指定python的运行名称
LOCALSCRIPT = '/home/uwsgi/iharbor/scripts/bucket_async_worker.py'
DELAYTIME = 0 # 延时执行命令 单位 s
