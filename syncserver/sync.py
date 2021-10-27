import os
import sys
from multiprocessing import Pool

import django
from celery.exceptions import SoftTimeLimitExceeded
from tqdm import tqdm

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# 设置项目的配置文件 不做修改的话就是 settings 文件
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webserver.settings")
django.setup()

from api.backup import AsyncBucketManager
from syncserver import celery_app

manager = AsyncBucketManager()
pool = Pool(os.cpu_count())


@celery_app.task(
    bind=True,
    autoretry_for=(SoftTimeLimitExceeded,),
    retry_backoff=True,  # 避免重试时出现大量重复请求
    retry_kwargs={'max_retries': 2}
)
def sync_object(self, bucket_id, object_id, bucket_name: str, object_key: str):
    manager.async_object(bucket_id, object_id, bucket_name, object_key)


def main():
    def sync(bucket_id, object_id, bucket_name, object_key):
        sync_object.delay(bucket_id, object_id, bucket_name, object_key)

    def safe_gen_sync_args():
        bucket_id = 0
        last_bucket_id = 0
        while True:
            try:
                buckets = manager.get_need_async_bucket_queryset(bucket_id)
                if not buckets:
                    break
                bucket_id = buckets[-1].id
                for bucket in tqdm(buckets, desc="{}-{}".format(buckets[0].id, buckets[-1].id)):
                    obj_id = 0
                    last_obj_id = 0
                    while True:
                        try:
                            objs = manager.get_need_async_objects_queryset(bucket.id, obj_id)
                            if not objs:
                                break
                            obj_id = objs[-1].id
                            for obj in tqdm(objs, desc="bucket: {}".format(str(bucket.id)), leave=False):
                                sync_object.delay(bucket.id, obj.id, bucket.name, obj.na)
                        except:
                            if last_obj_id != obj_id:
                                last_obj_id = obj_id
                                continue
                            else:
                                print("object sync error! bucket: {}, obj: {}".format(bucket.id, obj_id))
                                break
            except:
                if last_bucket_id != bucket_id:
                    last_bucket_id = bucket_id
                    continue
                else:
                    print("bucket sync error! bucket: {}".format(bucket_id))
                    break


if __name__ == '__main__':
    main()
