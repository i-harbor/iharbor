from django.utils.http import urlquote
from rest_framework import serializers
from rest_framework.serializers import Serializer
from rest_framework.reverse import reverse

from buckets.models import Bucket


class SharedPostSerializer(Serializer):
    '''
    分享序列化器
    '''
    bucket_name = serializers.CharField(label='存储桶名称', help_text='要分享的资源所在的存储桶名称')
    path = serializers.CharField(label='分享路径', help_text='要分享的文件或文件夹存储桶下的绝对路径，为空时分享整个存储桶')
    days = serializers.IntegerField(label='分享有效期', required=True, min_value =0, max_value=365*100,
                               help_text='分享有效期天数，0为永久有效')

    def validate(self, data):
        # bucket是否属于当前用户,检测存储桶名称是否存在
        request = self.context.get('request')
        bucket_name = data.get('bucket_name')
        if not Bucket.objects.filter(name=bucket_name).exists():
            raise serializers.ValidationError(detail={'bucket_name': '存储桶不存在'})

        return data

    def create(self, validated_data):
        # create必须要返回一个instance
        return True


class ShareObjInfoSerializer(serializers.Serializer):
    '''
    目录下文件列表序列化器
    '''
    na = serializers.CharField() # 全路径的文件名或目录名
    name = serializers.CharField()  # 非全路径目录名
    fod = serializers.BooleanField(required=True)  # file_or_dir; True==文件，False==目录
    did = serializers.IntegerField()  # 父节点ID
    si = serializers.IntegerField()  # 文件大小,字节数
    ult = serializers.DateTimeField(format='%Y-%m-%d %H:%M:%S')  # 文件的上传时间，或目录的创建时间
    # ult = serializers.SerializerMethodField()  # 自定义字段序列化方法
    upt = serializers.DateTimeField(format='%Y-%m-%d %H:%M:%S')  # 文件的最近修改时间，目录，则upt为空
    # upt = serializers.SerializerMethodField()  # 自定义字段序列化方法
    dlc = serializers.SerializerMethodField() #IntegerField()  # 该文件的下载次数，目录时dlc为空
    # bac = serializers.ListField(child = serializers.CharField(required=True))  # backup，该文件的备份地址，目录时为空
    # arc = serializers.ListField(child = serializers.CharField(required=True))  # archive，该文件的归档地址，目录时arc为空
    # sh = serializers.BooleanField()  # shared，若sh为True，则文件可共享，若sh为False，则文件不能共享
    # shp = serializers.CharField()  # 该文件的共享密码，目录时为空
    # stl = serializers.BooleanField()  # True: 文件有共享时间限制; False: 则文件无共享时间限制
    # sst = serializers.DateTimeField()  # share_start_time, 该文件的共享起始时间
    # set = serializers.SerializerMethodField()  # share_end_time,该文件的共享终止时间
    # sds = serializers.SerializerMethodField() # 自定义“软删除”字段序列化方法
    download_url = serializers.SerializerMethodField()
    # access_permission = serializers.SerializerMethodField() # 公共读权限


    def get_dlc(self, obj):
        return obj.dlc if obj.dlc else 0

    def get_sds(self, obj):
        return obj.get_sds_display()

    def get_download_url(self, obj):
        # 目录
        if not obj.fod:
            return  ''
        request = self.context.get('request', None)
        share_base = self._context.get('share_base', '')
        subpath = self._context.get('subpath', '')
        filepath = f'{subpath}/{obj.name}' if subpath else obj.name
        filepath = urlquote(filepath)
        download_url = reverse('share:download-detail', kwargs={'share_base': share_base})
        download_url = f'{download_url}?subpath={filepath}'
        if request:
            download_url = request.build_absolute_uri(download_url)
        return download_url

    def get_access_permission(self, obj):
        # 目录
        if not obj.fod:
            return ''

        # 桶公有权限，对象都为公有权限
        bucket = self._context.get('bucket')
        if bucket and bucket.is_public_permission():
            return '公有'

        try:
            if obj.is_shared_and_in_shared_time():
                return '公有'
        except Exception as e:
            pass
        return '私有'

