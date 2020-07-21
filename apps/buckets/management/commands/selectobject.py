from django.core.management.base import BaseCommand, CommandError

from buckets.utils import get_bfmanager
from buckets.models import Bucket
from utils.storagers import PathParser
from rest_framework import serializers


class MetadataSerializer(serializers.Serializer):
    '''
    目录下文件列表序列化器
    '''
    id = serializers.IntegerField()
    na = serializers.CharField()  # 全路径的文件名或目录名
    name = serializers.CharField()  # 非全路径目录名
    fod = serializers.BooleanField(required=True)  # file_or_dir; True==文件，False==目录
    did = serializers.IntegerField()  # 父节点ID
    si = serializers.IntegerField()  # 文件大小,字节数
    ult = serializers.DateTimeField(format='%Y-%m-%d %H:%M:%S')  # 文件的上传时间，或目录的创建时间
    # ult = serializers.SerializerMethodField()  # 自定义字段序列化方法
    upt = serializers.DateTimeField(format='%Y-%m-%d %H:%M:%S')  # 文件的最近修改时间，目录，则upt为空
    # upt = serializers.SerializerMethodField()  # 自定义字段序列化方法
    dlc = serializers.SerializerMethodField()  # IntegerField()  # 该文件的下载次数，目录时dlc为空
    # bac = serializers.ListField(child = serializers.CharField(required=True))  # backup，该文件的备份地址，目录时为空
    # arc = serializers.ListField(child = serializers.CharField(required=True))  # archive，该文件的归档地址，目录时arc为空
    # sh = serializers.BooleanField()  # shared，若sh为True，则文件可共享，若sh为False，则文件不能共享
    # shp = serializers.CharField()  # 该文件的共享密码，目录时为空
    # stl = serializers.BooleanField()  # True: 文件有共享时间限制; False: 则文件无共享时间限制
    # sst = serializers.DateTimeField()  # share_start_time, 该文件的共享起始时间
    # set = serializers.SerializerMethodField()  # share_end_time,该文件的共享终止时间
    # sds = serializers.SerializerMethodField() # 自定义“软删除”字段序列化方法
    access_permission = serializers.SerializerMethodField()  # 公共读权限

    def get_dlc(self, obj):
        return obj.dlc if obj.dlc else 0

    def get_sds(self, obj):
        return obj.get_sds_display()

    def get_access_permission(self, obj):
        return obj.get_share_display()


class Command(BaseCommand):
    '''
    从指定bucket查询一个对象和目录
    '''
    help = """manage.py selectobject --bucket-name=xxx --path=/a/b/c"""

    def add_arguments(self, parser):
        parser.add_argument(
            '--bucket-name', default=None, dest='bucketname',
            help='Name of bucket will',
        )
        parser.add_argument(
            '--path', default='', dest='path', type=str,
            help='path of object or dir.',
        )

    def handle(self, *args, **options):
        path = options['path']   # sql模板
        path = path.strip('/')
        if not path:
            self.stdout.write(self.style.ERROR("invalid param 'path'"))
            raise CommandError("invalid param.")

        bucket = self.get_bucket(**options)
        if not bucket:
            self.stdout.write(self.style.ERROR("bucket not exists"))
            raise CommandError("cancelled.")

        if input('Are you sure you want to do this?\n\n' + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("cancelled.")

        self.select_object(bucket=bucket, path=path)

    def get_bucket(self, **options):
        '''
        获取给定的bucket

        :return:
            Bucket() or None
        '''
        bucketname = options['bucketname']

        # 指定名字的桶
        if bucketname:
            return Bucket.objects.filter(name=bucketname).first()

        # 未给出参数
        if not bucketname:
            bucketname = input('Please input a bucket name:')

        return Bucket.objects.filter(name=bucketname).first()

    def select_object(self, bucket, path):
        '''

        :param bucket: Bucket obj
        :param path: path of obj or dir
        :return:
        '''
        dir_path, name = PathParser(filepath=path).get_path_and_filename()
        table_name = bucket.get_bucket_table_name()
        bfm = get_bfmanager(path=dir_path, table_name=table_name)
        try:
            obj = bfm.get_dir_or_obj_exists(name=name)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"error when select object,bucket='{bucket.name}', path={path},: {str(e)}"))
            raise CommandError("error")

        if not obj:
            self.stdout.write(self.style.WARNING("路径未找到"))
        else:
            data = MetadataSerializer(obj).data
            if obj.is_file():
                msg = f"bucket='{bucket.name}', path={path},文件对象: {data}"
            else:
                msg = f"bucket='{bucket.name}', path={path},目录: {data}"

            self.stdout.write(self.style.SUCCESS(msg))




