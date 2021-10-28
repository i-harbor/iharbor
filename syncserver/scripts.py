import os
import sys
from time import sleep

import django
from tqdm import tqdm

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# 设置项目的配置文件 不做修改的话就是 settings 文件
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webserver.settings")
django.setup()
from syncserver.sync import sync_object
from syncserver.ratelimit import RabbitMQTool
from api.backup import AsyncBucketManager

manager = AsyncBucketManager()
controller = RabbitMQTool(host='http://localhost:15672', queue='celery', user='guest', passwd='guest')


def main():
    bucket_id = 0
    last_bucket_id = 0
    while True:
        try:
            buckets = manager.get_need_async_bucket_queryset(bucket_id)
            if not buckets:
                break
            for bucket in tqdm(buckets, desc="buckets from: {}".format(buckets[0].id)):
                bucket_id = bucket.id
                obj_id = 0
                last_obj_id = 0
                while True:
                    try:
                        objs = manager.get_need_async_objects_queryset(bucket, obj_id)
                        sleep(controller.refresh())
                        if not objs:
                            break
                        for obj in tqdm(objs, desc="bucket: {}".format(str(bucket.id)), leave=False):
                            obj_id = obj.id
                            sync_object.delay(bucket.id, obj.id, bucket.name, obj.na)
                    except Exception as err:
                        if last_obj_id != obj_id:
                            last_obj_id = obj_id
                            continue
                        else:
                            print("object sync error! bucket: {}, obj: {} with {}".format(bucket.id, obj_id, err))
                            break
        except Exception as err:
            if last_bucket_id != bucket_id:
                last_bucket_id = bucket_id
                continue
            else:
                print("bucket sync error! bucket: {} with {}".format(bucket_id, err))
                break


if __name__ == '__main__':
    main()
