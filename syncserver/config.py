broker_url = 'amqp://guest@localhost//'
task_acks_late = False  # 接受消息后立即确认，防止同一任务同时执行
worker_prefetch_multiplier = 1  # 设置预取数量为1
task_serializer = 'json'
accept_content = ['json']
result_serializer = 'json'
worker_max_tasks_per_child = 1000  # 每1000个任务之后重建worker
timezone = 'Asia/Shanghai'  # 设置时区，应该与RabbitMQ时区同步
enable_utc = False
broker_pool_limit = 20
task_time_limit = 7200  # 软超时时间为2h
imports = ['syncserver.tasks']
task_ignore_result = True
