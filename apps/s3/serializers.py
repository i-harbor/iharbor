from django.utils.timezone import utc
from rest_framework import serializers

from utils.time import GMT_FORMAT


def time_to_gmt(value):
    """
    :param value: datetime()
    :return:
        GMT time string
    """
    try:
        return serializers.DateTimeField(format=GMT_FORMAT, default_timezone=utc).to_representation(value)
    except Exception as e:
        return ''


class BucketListSerializer(serializers.Serializer):
    """
    桶列表序列化器
    """
    CreationDate = serializers.SerializerMethodField(method_name='get_creation_date')
    Name = serializers.SerializerMethodField(method_name='get_name')

    @staticmethod
    def get_creation_date(obj):
        return serializers.DateTimeField(default_timezone=utc).to_representation(obj.created_time)

    @staticmethod
    def get_name(obj):
        return obj.name


class ObjectListSerializer(serializers.Serializer):
    """
    对象序列化器
    """
    Key = serializers.SerializerMethodField(method_name='get_key')
    LastModified = serializers.SerializerMethodField(method_name='get_last_modified')
    ETag = serializers.SerializerMethodField(method_name='get_etag')
    Size = serializers.SerializerMethodField(method_name='get_size')
    StorageClass = serializers.SerializerMethodField(method_name='get_storage_class')

    @staticmethod
    def get_key(obj):
        if obj.is_dir():
            return obj.na + '/'
        return obj.na

    @staticmethod
    def get_last_modified(obj):
        t = obj.upt if obj.upt else obj.ult
        return serializers.DateTimeField(default_timezone=utc).to_representation(t)

    @staticmethod
    def get_etag(obj):
        return obj.hex_md5

    @staticmethod
    def get_size(obj):
        return obj.si

    @staticmethod
    def get_storage_class(obj):
        return 'STANDARD'


class ObjectListWithOwnerSerializer(ObjectListSerializer):
    """
    带owner信息的对象序列化器
    """
    Owner = serializers.SerializerMethodField(method_name='get_owner')

    def get_owner(self, obj):
        owner = self.context.get('owner', None)
        if owner is not None:
            return owner

        user = self.context.get('user', None)
        owner = {}
        if user:
            owner = {'ID': user.id, "DisplayName": user.username}

        self.context['owner'] = owner
        return owner


class ObjectListV2Serializer(ObjectListSerializer):
    pass


class ObjectListV2WithOwnerSerializer(ObjectListWithOwnerSerializer):
    pass


class ListMultipartUploadsSerializer(serializers.Serializer):
    """
    多部分上传列化器
    """
    UploadId = serializers.SerializerMethodField(method_name='get_upload_id')
    Key = serializers.SerializerMethodField(method_name='get_key')
    Initiated = serializers.SerializerMethodField(method_name='get_initiated')
    Initiator = serializers.SerializerMethodField(method_name='get_owner')
    Owner = serializers.SerializerMethodField(method_name='get_owner')
    StorageClass = serializers.SerializerMethodField(method_name='get_storage_class')

    @staticmethod
    def get_upload_id(up):
        return up.id

    @staticmethod
    def get_key(up):
        return up.obj_key

    @staticmethod
    def get_initiated(up):
        t = up.create_time
        return serializers.DateTimeField(default_timezone=utc).to_representation(t)

    @staticmethod
    def get_storage_class(up):
        return 'STANDARD'

    def get_owner(self, up):
        owner = self.context.get('owner', None)
        if owner is not None:
            return owner

        user = self.context.get('user', None)
        owner = {}
        if user:
            owner = {'ID': user.id, "DisplayName": user.username}

        self.context['owner'] = owner
        return owner
