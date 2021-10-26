broker_url = 'amqp://guest@localhost//'
task_acks_late = True  # 任务完成后确认
worker_prefetch_multiplier = 1  # 设置预取数量为1
task_serializer = 'json'
accept_content = ['json']
result_serializer = 'json'
worker_max_tasks_per_child = 1000  # 每1000个任务之后重建worker
task_track_started = True  # 监控队列任务, 获得细粒度的监控
timezone = 'Asia/Shanghai'  # 设置时区，应该与RabbitMQ时区同步
enable_utc = True
broker_pool_limit = 20
worker_pool = "gevent"
worker_concurrency = 30
