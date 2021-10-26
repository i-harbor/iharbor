import os
import sys
from multiprocessing import Pool

import django

from syncserver import celery_app

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# 设置项目的配置文件 不做修改的话就是 settings 文件
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webserver.settings")
django.setup()

from api.backup import AsyncBucketManager

manager = AsyncBucketManager()
pool = Pool(os.cpu_count())


@celery_app.task(bind=True)
def sync_object(self, bucket_id, object_id, bucket_name: str, object_key: str):
    manager.async_object(bucket_id, object_id, bucket_name, object_key)


def main():
    def sync(bucket_id, object_id, bucket_name, object_key):
        sync_object.delay(bucket_id, object_id, bucket_name, object_key)

    def gen_sync_args():
        for i in range(1000):
            buckets = manager.get_need_async_bucket_queryset(i * 1000)
            if not buckets:
                break
            for bucket in buckets:
                for j in range(1000):
                    objs = manager.get_need_async_objects_queryset(i * 1000)
                    if not objs:
                        break
                    for obj in objs:
                        yield bucket.id, obj.id, bucket.name, obj.key

    pool.starmap_async(sync, gen_sync_args())


if __name__ == '__main__':
    main()
