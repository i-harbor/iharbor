from django.db import close_old_connections
from django.db.models import Q, F

from buckets.models import Bucket, BackupBucket
from buckets.utils import BucketFileManagement
from utils.oss.pyrados import build_harbor_object


def async_close_old_connections(func):
    def wrapper(*args, **kwargs):
        close_old_connections()
        return func(*args, **kwargs)

    return wrapper


class AsyncBucketManager:
    @async_close_old_connections
    def get_need_async_bucket_queryset(self, id_gt: int = 0, limit: int = 1000):
        """
        获取设置了备份点并开启了备份的所有桶, id正序排序

        :param id_gt: 查询id大于id_gt的数据，实现分页续读
        :param limit: 获取数据的数量
        :return:
            QuerySet
        """
        return Bucket.objects.filter(
            id__gt=id_gt,
            backup_buckets__status=BackupBucket.Status.START
        ).all().order_by('id')[0:limit]

    @async_close_old_connections
    def get_need_async_objects_queryset(self, bucket, id_gt: int = 0, limit: int = 1000):
        """
        获取需要同步的对象的查询集, id正序排序

        :param bucket: bucket instance
        :param id_gt: 查询id大于id_gt的数据，实现分页续读
        :param limit: 获取数据的数量
        :return:
            QuerySet
        """
        table_name = bucket.get_bucket_table_name()
        bfm = BucketFileManagement(collection_name=table_name)
        object_class = bfm.get_obj_model_class()

        backup_nums = []
        for backup in bucket.backup_buckets.all():
            if backup.status == BackupBucket.Status.START:
                if backup.backup_num in [1, 2]:
                    backup_nums.append(backup.backup_num)

        if not backup_nums:
            return object_class.objects.none()

        queryset = object_class.objects.filter(fod=True, id__gt=id_gt).all()
        if backup_nums == [1, ]:
            queryset = queryset.filter(
                Q(async1__isnull=True) | Q(upt__gte=F('async1'))
            ).order_by('id')
        elif backup_nums == [2, ]:
            queryset = queryset.filter(
                Q(async2__isnull=True) | Q(upt__gte=F('async2'))
            ).order_by('id')
        else:
            queryset = queryset.filter(
                Q(async1__isnull=True) | Q(upt__gte=F('async1')) | Q(async2__isnull=True) | Q(upt__gte=F('async2'))
            ).order_by('id')

        return queryset[0:limit]

    def get_bucket_by_id(self, bucket_id):
        """
        :return:
            Bucket()    # exist
            None        # not exist
        """
        return Bucket.objects.filter(id=bucket_id).first()

    def get_object_by_id(self, bucket, object_id):
        """
        查村对象

        :param bucket: bucket instance
        :param object_id: object id
        :return:
            BucketFileBase()    # exist
            None                # not exist
        """
        table_name = bucket.get_bucket_table_name()
        bfm = BucketFileManagement(collection_name=table_name)
        object_class = bfm.get_obj_model_class()
        obj = object_class.objects.filter(id=object_id).first()
        if obj is None:
            return None

        if obj.is_file():
            return obj

        return None

    def get_object_ceph_rados(self, bucket, object):
        """
        获取对象对应ceph读写接口

        :param bucket: bucket instance
        :param object: object instance
        :return:
            HarborObject()
        """
        obj_key = object.get_obj_key(bucket.id)
        pool_name = bucket.get_pool_name()
        return build_harbor_object(using=bucket.ceph_using, pool_name=pool_name,
                                    obj_id=obj_key, obj_size=object.si)


