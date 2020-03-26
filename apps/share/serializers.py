from django.utils.http import urlquote
from rest_framework import serializers
from rest_framework.reverse import reverse


class ShareObjInfoSerializer(serializers.Serializer):
    '''
    目录下文件列表序列化器
    '''
    na = serializers.CharField() # 全路径的文件名或目录名
    name = serializers.CharField()  # 非全路径目录名
    fod = serializers.BooleanField(required=True)  # file_or_dir; True==文件，False==目录
    did = serializers.IntegerField()  # 父节点ID
    si = serializers.IntegerField()  # 文件大小,字节数
    ult = serializers.DateTimeField()  # 文件的上传时间，或目录的创建时间
    upt = serializers.DateTimeField()  # 文件的最近修改时间，目录，则upt为空
    dlc = serializers.SerializerMethodField() #IntegerField()  # 该文件的下载次数，目录时dlc为空
    download_url = serializers.SerializerMethodField()


    def get_dlc(self, obj):
        return obj.dlc if obj.dlc else 0

    def get_download_url(self, obj):
        # 目录
        if not obj.fod:
            return  ''
        request = self.context.get('request', None)
        share_base = self._context.get('share_base', '')
        share_code = self._context.get('share_code', '')
        subpath = self._context.get('subpath', '')

        filepath = f'{subpath}/{obj.name}' if subpath else obj.name
        filepath = urlquote(filepath)
        download_url = reverse('share:download-detail', kwargs={'share_base': share_base})
        if share_code:
            download_url = f'{download_url}?subpath={filepath}&p={share_code}'
        else:
            download_url = f'{download_url}?subpath={filepath}'
        if request:
            download_url = request.build_absolute_uri(download_url)
        return download_url


