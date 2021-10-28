import os
import sys

import django

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# 设置项目的配置文件 不做修改的话就是 settings 文件
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webserver.settings")
django.setup()

from api.backup import AsyncBucketManager
from syncserver import celery_app


@celery_app.task(
    bind=True,
)
def sync_object(self, bucket_id, object_id, bucket_name: str, object_key: str):
    manager = AsyncBucketManager()
    manager.async_object(bucket_id, object_id, bucket_name, object_key)
    return
