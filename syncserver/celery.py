import os
import sys

import django
from celery import Celery

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# # 设置项目的配置文件 不做修改的话就是 settings 文件
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webserver.settings")
django.setup()

app = Celery('sync')
app.config_from_object("syncserver.config")
app.autodiscover_tasks()
