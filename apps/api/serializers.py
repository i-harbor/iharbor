import logging

from django.utils.translation import gettext, gettext_lazy as _
from django.utils.timezone import utc
from rest_framework import serializers
from rest_framework.reverse import reverse, replace_query_param

from .models import User, Bucket
from .validators import DNSStringValidator, bucket_limit_validator
from buckets.utils import get_ceph_poolname_rand, get_ceph_alias_rand
from buckets.models import get_next_bucket_max_id


debug_logger = logging.getLogger('debug')#这里的日志记录器要和setting中的loggers选项对应，不能随意给参


class UserDeitalSerializer(serializers.ModelSerializer):
    '''
    用户信息序列化器
    '''
    # id = serializers.IntegerField(label='用户ID', help_text='用户的Id号')
    class Meta:
        model = User
        fields = ('id', 'username', 'email', 'date_joined', 'last_login', 'first_name', 'last_name', 'is_active',
                  'telephone', 'company')
        read_only_fields = ('id', 'username', 'email', 'date_joined', 'last_login')


class UserUpdateSerializer(serializers.ModelSerializer):
    '''
    用户信息更新序列化器
    '''
    password = serializers.CharField(label=_('密码'), min_length=8, max_length=128, help_text=_('至少8位密码'))

    class Meta:
        model = User
        fields = ('is_active', 'password', 'first_name', 'last_name',  'telephone', 'company')

    def update(self, instance, validated_data):
        '''
        修改用户信息
        '''
        instance.first_name = validated_data.get('first_name', instance.first_name)
        instance.last_name = validated_data.get('last_name', instance.last_name)
        instance.is_active = validated_data.get('is_active', instance.is_active)
        instance.telephone = validated_data.get('telephone', instance.telephone)
        instance.company = validated_data.get('company', instance.company)
        password = validated_data.get('password', None)
        if password:
            instance.set_password(password)
        instance.save()
        return instance


class UserCreateSerializer(serializers.Serializer):
    '''
    用户创建序列化器
    '''
    username = serializers.EmailField(label=_('用户名(邮箱)'), required=True, max_length=150, help_text=_('邮箱'))
    password = serializers.CharField(label=_('密码'), required=True, min_length=8, max_length=128, help_text=_('至少8位密码'))
    last_name = serializers.CharField(label=_('姓氏'), max_length=30, default='')
    first_name = serializers.CharField(label=_('名字'), max_length=30, default='')
    telephone = serializers.CharField(label=_('电话'), max_length=11, default='')
    company = serializers.CharField(label=_('公司/单位'), max_length=255, default='')

    def validate(self, data):
        username = data.get('username')
        password = data.get('password')

        if not username or not password:
            raise serializers.ValidationError(detail={'code_text': gettext('用户名或密码不能为空')})
        return data

    def create(self, validated_data):
        """
        Create and return a new `Snippet` instance, given the validated data.
        """
        email = username = validated_data.get('username')
        password = validated_data.get('password')
        last_name = validated_data.get('last_name')
        first_name = validated_data.get('first_name')
        telephone = validated_data.get('telephone')
        company = validated_data.get('company')

        user = User.objects.filter(username=username).first()
        if user:
            if user.is_active:
                raise serializers.ValidationError(detail={'code_text': gettext('用户名已存在'), 'existing': True})
            else:
                user.email = email
                user.set_password(password)
                user.last_name = last_name
                user.first_name = first_name
                user.telephone = telephone
                user.company = company
                user.save()
                return user # 未激活用户

        # 创建非激活状态新用户并保存
        return User.objects.create_user(username=username, password=password, email=email, is_active=False,
                                        last_name=last_name, first_name=first_name, telephone=telephone, company=company)


class BucketSerializer(serializers.ModelSerializer):
    '''
    存储桶序列化器
    '''
    user = serializers.SerializerMethodField() # 自定义user字段内容
    access_permission = serializers.SerializerMethodField()
    ftp_password = serializers.SerializerMethodField()
    ftp_ro_password = serializers.SerializerMethodField()

    class Meta:
        model = Bucket
        fields = ('id', 'name', 'user', 'created_time', 'access_permission', 'ftp_enable',
                  'ftp_password', 'ftp_ro_password', 'remarks')
        # depth = 1

    @staticmethod
    def get_user(obj):
        return {'id': obj.user.id, 'username': obj.user.username}

    @staticmethod
    def get_access_permission(obj):
        return obj.get_access_permission_display()

    @staticmethod
    def get_ftp_password(obj):
        return obj.raw_ftp_password

    @staticmethod
    def get_ftp_ro_password(obj):
        return obj.raw_ftp_ro_password


class BucketCreateSerializer(serializers.Serializer):
    '''
    创建存储桶序列化器
    '''
    name = serializers.CharField(label=_('存储桶名称'), min_length=3, max_length=63,
                                 help_text=_('存储桶名称，名称唯一，不可使用已存在的名称，符合DNS标准的存储桶名称，英文字母、数字和-组成，3-63个字符'))

    def validate(self, data):
        """
        复杂验证
        """
        bucket_name = data.get('name')
        request = self.context.get('request')
        user = request.user

        if not bucket_name:
            raise serializers.ValidationError(gettext('存储桶bucket名称不能为空'))

        if bucket_name.startswith('-') or bucket_name.endswith('-'):
            raise serializers.ValidationError(gettext('存储桶bucket名称不能以“-”开头或结尾'))

        DNSStringValidator(bucket_name)
        bucket_name = bucket_name.lower()
        data['name'] = bucket_name

        # 用户存储桶限制数量检测
        bucket_limit_validator(user=user)
        return data

    def create(self, validated_data):
        """
        Create and return a new `Bucket` instance, given the validated data.
        """
        request = self.context.get('request')
        if not request:
            return None
        user = request.user
        using = get_ceph_alias_rand()
        pool_name = get_ceph_poolname_rand(using)
        bucket_id = get_next_bucket_max_id()
        bucket = Bucket.objects.create(id=bucket_id, ceph_using=using, pool_name=pool_name,
                                       user=user, **validated_data) # 创建并保存
        return bucket


class ObjPutSerializer(serializers.Serializer):
    '''
    文件分块上传序列化器
    '''
    chunk_offset = serializers.IntegerField(label=_('文件块偏移量'), required=True, min_value=0, max_value=5*1024**4, # 5TB
                                            help_text=_('上传文件块在整个文件中的起始位置（bytes偏移量)，类型int'))
    chunk = serializers.FileField(label=_('文件块'), required=False, help_text=_('文件分片的二进制数据块,文件或类文件对象，如JS的Blob对象'))
    chunk_size = serializers.IntegerField(label=_('文件块大小'), required=True, min_value=0,
                                          help_text=_('上传文件块的字节大小，类型int'))

    def validate(self, data):
        """
        复杂验证
        """
        chunk = data.get('chunk')
        chunk_size = data.get('chunk_size')

        if not chunk:
            raise serializers.ValidationError(detail={'chunk': gettext('无效的空文件块')})
        elif chunk.size != chunk_size:
            raise serializers.ValidationError(detail={'chunk_size': gettext('chunk_size与文件块大小不一致')})

        return data

    def is_valid(self, raise_exception=False):
        return super(ObjPutSerializer, self).is_valid(raise_exception)


class ObjPutFileSerializer(serializers.Serializer):
    '''
    完整文件上传序列化器
    '''
    file = serializers.FileField(label=_('文件'), required=False, help_text=_('一个完整的文件'))

    def validate(self, data):
        """
        复杂验证
        """
        file = data.get('file')

        if not file or file.size == 0:
            raise serializers.ValidationError(detail={'file': gettext('无效的空文件')})

        return data


class ObjInfoSerializer(serializers.Serializer):
    """
    目录下文件列表序列化器
    """
    na = serializers.CharField()    # 全路径的文件名或目录名
    name = serializers.CharField()  # 非全路径目录名
    fod = serializers.BooleanField(required=True)  # file_or_dir; True==文件，False==目录
    did = serializers.IntegerField()  # 父节点ID
    si = serializers.IntegerField()  # 文件大小,字节数
    ult = serializers.DateTimeField()  # 文件的上传时间，或目录的创建时间
    upt = serializers.DateTimeField()  # 文件的最近修改时间，目录，则upt为空，format='%Y-%m-%d %H:%M:%S'
    dlc = serializers.SerializerMethodField()   # 该文件的下载次数，目录时dlc为空
    download_url = serializers.SerializerMethodField()
    access_permission = serializers.SerializerMethodField()     # 公共读权限
    access_code = serializers.SerializerMethodField()  # 公共读权限
    md5 = serializers.SerializerMethodField(method_name='get_md5')

    def get_md5(self, obj):
        return obj.hex_md5

    def get_dlc(self, obj):
        return obj.dlc if obj.dlc else 0

    def get_sds(self, obj):
        return obj.get_sds_display()

    def get_download_url(self, obj):
        # 目录
        if not obj.fod:
            return ''
        request = self.context.get('request', None)
        bucket_name = self._context.get('bucket_name', '')
        filepath = '/'.join((bucket_name, obj.na))
        download_url = reverse('share:obs-detail', kwargs={'objpath': filepath})
        if obj.has_share_password():
            download_url = replace_query_param(url=download_url, key='p', val=obj.get_share_password())
        if request:
            download_url = request.build_absolute_uri(download_url)
        return download_url

    def get_access_code(self, obj):
        if hasattr(obj, 'access_permission_code'):
            return obj.access_permission_code

        bucket = self._context.get('bucket')
        c = obj.get_access_permission_code(bucket)
        obj.access_permission_code = c
        return c

    def get_access_permission(self, obj):
        c = self.get_access_code(obj)
        if c == obj.SHARE_ACCESS_READONLY:
            return gettext('公有')
        elif c == obj.SHARE_ACCESS_READWRITE:
            return gettext('公有（读写）')
        else:
            return gettext('私有')


class AuthTokenDumpSerializer(serializers.Serializer):
    key = serializers.CharField()
    user = serializers.SerializerMethodField()
    created = serializers.DateTimeField()

    def get_user(self, obj):
        return obj.user.username


class VPNSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    password = serializers.CharField()
    created_time = serializers.DateTimeField()  # format='%Y-%m-%d %H:%M:%S'
    modified_time = serializers.DateTimeField()
    user = serializers.SerializerMethodField()

    def get_user(self, obj):
        u = obj.user
        return {'id': u.id, 'username': u.username}


class VPNPostSerializer(serializers.Serializer):
    password = serializers.CharField(max_length=20, min_length=6, help_text=_('新的VPN口令密码'))


class BucketTokenSerializer(serializers.Serializer):
    key = serializers.CharField()
    bucket = serializers.SerializerMethodField(method_name='get_bucket')
    permission = serializers.CharField()
    created = serializers.DateTimeField()

    def get_bucket(self, obj):
        try:
            return {'id': obj.bucket.id, 'name': obj.bucket.name}
        except:
            return {}


class ListBucketObjectsSerializer(serializers.Serializer):
    """
    对象序列化器
    """
    Key = serializers.SerializerMethodField(method_name='get_key')
    LastModified = serializers.SerializerMethodField(method_name='get_last_modified')
    Creation = serializers.SerializerMethodField(method_name='get_creation')
    ETag = serializers.SerializerMethodField(method_name='get_etag')
    Size = serializers.SerializerMethodField(method_name='get_size')
    IsObject = serializers.SerializerMethodField(method_name='get_is_object')

    @staticmethod
    def get_key(obj):
        return obj.na

    @staticmethod
    def get_creation(obj):
        return serializers.DateTimeField(default_timezone=utc).to_representation(obj.ult)

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
    def get_is_object(obj):
        return obj.fod
