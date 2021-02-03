import os
import logging
from datetime import datetime

from django.core.management.base import BaseCommand, CommandError
from django.db import close_old_connections

from buckets.utils import BucketFileManagement
from buckets.models import Bucket
from api.harbor import HarborManager, HarborError


logger = logging.getLogger('error')
logger.addHandler(logging.FileHandler(filename='/var/log/iharbor/copydir.log', mode='a'))
logger.setLevel(level=logging.ERROR)


def decorator_close_old_connections(func):
    def wrapper(*args, **kwargs):
        close_old_connections()
        return func(*args, **kwargs)

    return wrapper


class Command(BaseCommand):
    """
    为bucket对应的表，清理满足彻底删除条件的对象和目录
    """
    help = """manage.py copydir --from-bucket="xxx" --from-path="/a/b" --to-bucket="xxx" --to-path="/" """

    def add_arguments(self, parser):
        parser.add_argument(
            '--from-bucket', default=None, dest='from-bucket',
            help='Name of bucket copy from',
        )
        parser.add_argument(
            '--from-path', default=None, dest='from-path',
            help='copy from path',
        )
        parser.add_argument(
            '--to-bucket', default=None, dest='to-bucket',
            help='Name of bucket copy to',
        )
        parser.add_argument(
            '--to-path', default=None, dest='to-path',
            help='copy to path',
        )
        parser.add_argument(
            '--overwrite', default=False, nargs='?', dest='overwrite', const=True,   # 当命令行有此参数时取值const, 否则取值default
            help='是否覆盖已存在',
        )

    def handle(self, *args, **options):
        from_bucket_name = options['from-bucket']
        from_path = options['from-path']
        to_bucket_name = options['to-bucket']
        to_path = options['to-path']

        if not from_bucket_name:
            raise CommandError("cancelled, param --from-bucket is required")

        if not from_path:
            raise CommandError("cancelled, param --from-path is required")

        from_path = from_path.strip('/')

        if not to_bucket_name:
            raise CommandError("cancelled, param --to-bucket is required")

        if not to_path:
            raise CommandError("cancelled, param --to-path is required")

        to_path = to_path.strip('/')

        from_bucket = Bucket.objects.filter(name=from_bucket_name).first()
        if not from_bucket:
            raise CommandError("cancelled, from bucket is not exists.")

        to_bucket = Bucket.objects.filter(name=to_bucket_name).first()
        if not to_bucket:
            raise CommandError("cancelled, to bucket is not exists.")

        # 源
        from_table_name = from_bucket.get_bucket_table_name()
        from_bfm = BucketFileManagement(collection_name=from_table_name)
        from_modelclass = from_bfm.get_obj_model_class()

        hm = HarborManager()
        try:
            r = hm.is_dir(bucket_name=from_bucket_name, path_name=from_path)
        except HarborError as e:
            raise CommandError(f"cancelled, from path `{from_path}` is not exists, error: {e}.")

        if not r:
            raise CommandError(f"cancelled, from path `{from_path}` is not dir.")

        # 目标
        to_table_name = to_bucket.get_bucket_table_name()
        to_modelclass = BucketFileManagement(collection_name=to_table_name).get_obj_model_class()

        self.from_bucket = from_bucket
        self.from_path = from_path
        self.from_momelclass = from_modelclass
        self.to_bucket = to_bucket
        self.to_path = to_path
        self.to_modelclass = to_modelclass
        self.hm = hm

        if input('Are you sure you want to do this?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("Clearing buckets cancelled.")

        logger.error(msg=f'#### Start a new copy, time: {datetime.now()}. ####')
        self.update(from_path)

    @decorator_close_old_connections
    def update(self, path):
        path = path.lstrip('/')
        # 目标路径是否存在，不存在创建
        target_path = self.build_target_path(path)
        if target_path:
            if not self.make_target_dir(target_path):   # 目录创建失败，跳过
                return

        self.stdout.write(self.style.SUCCESS(f'Start copy path: "{ path }"'))
        from_qs = self.list_from_dir(path=path)
        if from_qs is None:
            logger.error(f'#result=failed# #type=dir# #path={path}#')
            self.stdout.write(self.style.ERROR(f'Failed copy path: "{path}"'))
            return

        for f_obj in from_qs:
            if f_obj.is_dir():
                current_path = path + '/' + f_obj.name
                self.update(current_path)
            elif f_obj.is_file():
                filepath = f_obj.na
                target_obj_name = self.build_target_path(path=filepath)
                if not self.copy_object(obj=f_obj, target_obj_path=target_obj_name):
                    logger.error(f'#result=failed# #type=obj# #path={filepath}#')
                    self.stdout.write(self.style.ERROR(f'Failed copy object: "{filepath}"'))
            else:
                print(f"[????] *{path}* is not file, not dir")

        self.stdout.write(self.style.SUCCESS(f'Success copy path: "{path}"'))

    def build_target_path(self, path):
        """
        :param path: 源目录路径, or 源object路径
        """
        from_ab_path = path.lstrip(self.from_path)  # 相对路径
        from_ab_path = from_ab_path.lstrip('/')

        if from_ab_path:
            target_path = os.path.join(self.to_path, from_ab_path)
        else:
            target_path = self.to_path

        return target_path

    @decorator_close_old_connections
    def make_target_dir(self, target_path):
        """
        :param target_path: 源目录路径

        :return:
            True
            False
        """
        for i in range(10):
            b, obj = self.hm.get_bucket_and_obj_or_dir(bucket_name=self.to_bucket.name, path=target_path)
            if not obj:
                try:
                    self.hm.mkdir(bucket_name=self.to_bucket.name, path=target_path)
                    return True
                except Exception as e:
                    continue

            if obj.is_dir():
                return True

            self.stdout.write(self.style.ERROR(f'无法复制目录，跳过目录: {target_path}, 目标bucket中已存在同名的对象'))
            return False

        self.stdout.write(self.style.WARNING(f'mak dir failed in to bucket: "{target_path}", try times 10.'))
        return False

    @decorator_close_old_connections
    def copy_object(self, obj, target_obj_path):
        # 是否存在
        for i in range(10):
            try:
                b, target_obj = self.hm.get_bucket_and_obj_or_dir(bucket_name=self.to_bucket.name, path=target_obj_path)
            except Exception as e:
                continue

            if target_obj:
                if target_obj.is_file():
                    if target_obj.obj_size > 0:
                        self.stdout.write(self.style.WARNING(f'skip copy object path: "{target_obj.na}"'))
                        return True
                else:
                    self.stdout.write(self.style.ERROR(f'无法复制对象，跳过对象: {target_obj_path}, 目标bucket中已存在同名的目录'))
            else:
                break

        # 复制对象
        for i in range(6):
            try:
                from_obj_generator = self.hm._get_obj_generator(bucket=self.from_bucket, obj=obj, per_size=32*1024**2)
                to_obj_generator = self.hm.get_write_generator(bucket_name=self.to_bucket.name, obj_path=target_obj_path)
                next(to_obj_generator)
                offset = 0
                for data in from_obj_generator:
                    ok = to_obj_generator.send((offset, data))  # ok = True写入成功， ok=False写入失败
                    if not ok:
                        ok = to_obj_generator.send((offset, data))  # ok = True写入成功， ok=False写入失败
                        if not ok:
                            raise Exception('write object error')

                    offset = offset + len(data)
                return True
            except Exception as e:
                continue

        return False

    @decorator_close_old_connections
    def list_from_dir(self, path):
        table_name = self.from_bucket.get_bucket_table_name()
        bfm = BucketFileManagement(path=path, collection_name=table_name)
        ok, qs = bfm.get_cur_dir_files()
        if not ok:
            ok, qs = bfm.get_cur_dir_files()
            if not ok:
                return None

        return qs
