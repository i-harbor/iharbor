import logging
import os
import sys
import time
from logging.handlers import RotatingFileHandler

import django
from celery import Task
from celery.utils.log import get_task_logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# 设置项目的配置文件 不做修改的话就是 settings 文件
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webserver.settings")
django.setup()

from api.backup import AsyncBucketManager
from syncserver import celery_app

manager = AsyncBucketManager()

logger = get_task_logger(__name__)
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(process)d - %(thread)d - %(threadName)s - %(message)s"
DATE_FORMAT = "%Y/%m/%d - %H:%M:%S"
fh = RotatingFileHandler('syncserver/log/worker.log', maxBytes=1024 * 50, backupCount=10)
fh.setLevel(logging.INFO)
formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
fh.setFormatter(formatter)
logger.addHandler(fh)


class BaseTask(Task):
    def run(self, *args, **kwargs):
        pass

    def on_success(self, retval, task_id, args, kwargs):
        if retval < 60:
            logger.info('args:{}|spend:{}'.format(args, retval))
        else:
            logger.warning('slow sync: {}|spend:{}'.format(args, retval))

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error('args:{}|einfo:{}|exc:{}'.format(args, einfo, exc))


@celery_app.task(
    bind=True,
    base=BaseTask
)
def sync_object(self, bucket_id, object_id, bucket_name: str, object_key: str):
    start = time.perf_counter()
    manager.async_object(bucket_id, object_id, bucket_name, object_key)
    return round(time.perf_counter() - start, 3)
