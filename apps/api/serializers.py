import os
from bson import ObjectId

from rest_framework import serializers
from rest_framework.reverse import reverse

from buckets.utils import BucketFileManagement
from .models import User, Bucket
from utils.storagers import PathParser
from utils.time import to_localtime_string_naive_by_utc
from .validators import DNSStringValidator, bucket_limit_validator



def get_bucket_collection_name_or_ValidationError(bucket_name, request):
    '''
    获取存储通对应集合名称，或者ValidationError对象
    :param bucket_name: 存储通名称
    :return: (collection_name, ValidationError)
            collection_name=None时，存储通不存在，ValidationError有效；
            collection_name!=''时，存储通存在，ValidationError=None；
    '''
    bucket = Bucket.get_bucket_by_name(bucket_name)
    if not bucket:
        vali_error = serializers.ValidationError(detail={'error_text': 'bucket_name参数有误,存储桶不存在'})
        return (None, vali_error)
    if not bucket.check_user_own_bucket(request):
        vali_error = serializers.ValidationError(detail={'error_text': 'bucket_name参数有误,存储桶不存在'})
        return (None, vali_error)

    collection_name = bucket.get_bucket_mongo_collection_name()
    return (collection_name, None)


class UserDeitalSerializer(serializers.ModelSerializer):
    '''
    用户信息序列化器
    '''
    # id = serializers.IntegerField(label='用户ID', help_text='用户的Id号')
    class Meta:
        model = User
        fields = ('id', 'username', 'email', 'date_joined', 'last_login',)


class UserCreateSerializer(serializers.Serializer):
    '''
    用户创建序列化器
    '''
    username = serializers.EmailField(label='用户名(邮箱)', required=True, max_length=150, help_text='邮箱')
    password = serializers.CharField(label='密码', required=True, max_length=128, help_text='至少8位密码')
    last_name = serializers.CharField(label='姓氏', max_length=30, default='')
    first_name = serializers.CharField(label='名字', max_length=30, default='')
    telephone = serializers.CharField(label='电话', max_length=11, default='')
    company = serializers.CharField(label='公司/单位', max_length=255, default='')

    def validate(self, data):
        username = data.get('username')
        password = data.get('password')

        if not username or not password:
            raise serializers.ValidationError(detail={'code_text': '用户名或密码不能为空'})
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
                raise serializers.ValidationError(detail={'code_text': '用户名已存在', 'existing': True})
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
    created_time = serializers.SerializerMethodField()  # 自定义字段内容
    access_permission = serializers.SerializerMethodField()

    class Meta:
        model = Bucket
        fields = ('id', 'name', 'user', 'created_time', 'access_permission')
        # depth = 1

    def get_user(selfself, obj):
        return {'id': obj.user.id, 'username': obj.user.username}

    def get_created_time(self, obj):
        if not obj.created_time:
            return ''
        return to_localtime_string_naive_by_utc(obj.created_time)

    def get_access_permission(self, obj):
        return obj.get_access_permission_display()


class BucketCreateSerializer(serializers.Serializer):
    '''
    创建存储桶序列化器
    '''
    name = serializers.CharField(label='存储桶名称', min_length=3, max_length=63,
                                 help_text='存储桶名称，名称唯一，不可使用已存在的名称，符合DNS标准的存储桶名称，英文字母、数字和-组成，3-63个字符')
    # user = serializers.CharField(label='所属用户', help_text='所创建的存储桶的所属用户，可输入用户名或用户id')

    def validate(self, data):
        """
        复杂验证
        """
        bucket_name = data.get('name')
        request = self.context.get('request')
        user = request.user

        if not bucket_name:
            raise serializers.ValidationError('存储桶bucket名称不能为空')

        if bucket_name.startswith('-') or bucket_name.endswith('-'):
            raise serializers.ValidationError('存储桶bucket名称不能以“-”开头或结尾')

        DNSStringValidator(bucket_name)
        bucket_name = bucket_name.lower()
        data['name'] = bucket_name

        # 用户存储桶限制数量检测
        bucket_limit_validator(user=user)

        if Bucket.get_bucket_by_name(bucket_name):
            raise serializers.ValidationError("存储桶名称已存在", code='existing')
        return data

    def create(self, validated_data):
        """
        Create and return a new `Bucket` instance, given the validated data.
        """
        request = self.context.get('request')
        if not request:
            return None
        user = request.user
        bucket = Bucket.objects.create(user=user, **validated_data) # 创建并保存
        return bucket


class ObjPostSerializer(serializers.Serializer):
    '''
    文件分块上传序列化器
    '''
    bucket_name = serializers.CharField(label='存储桶名称', required=True,
                                        help_text='文件上传到的存储桶名称，类型string')
    dir_path = serializers.CharField(label='文件上传路径', required=False,
                                     help_text='存储桶下的目录路径，指定文件上传到那个目录下，为空或者‘/’表示在存储桶下，类型string')
    file_name = serializers.CharField(label='文件名', required=True, help_text='上传文件的文件名，类型string')
    # file_size = serializers.IntegerField(label='文件大小', required=True, min_value=1, help_text='上传文件的字节大小')
    # file_md5 = serializers.CharField(label='文件MD5码', required=False, min_length=32, max_length=32,
    #                                  help_text='由文件内容生成的MD5码字符串')
    overwrite = serializers.BooleanField(label='是否覆盖',
                                         help_text='存在同名文件时是否覆盖，类型boolean,True(覆盖)，False(不覆盖)')

    def create(self, validated_data):
        file_name = validated_data.get('file_name')
        # file_size = validated_data.get('file_size')
        # file_md5 = validated_data.get('file_md5')

        did = validated_data.get('_did')
        old_bfinfo = validated_data.get('finfo')
        _collection_name = validated_data.get('_collection_name')
        BucketFileClass = validated_data.get('BucketFileClass')

        # 存在同名文件对象，覆盖上传删除原文件
        if old_bfinfo:
            old_bfinfo.do_soft_delete()

        # 创建文件对象
        bfinfo = BucketFileClass(na=file_name,# 文件名
                                fod = True, # 文件
                                si = 0 )# 文件大小
        # 有父节点
        if did:
            bfinfo.did = ObjectId(did)
        bfinfo.save()

        # 构造返回数据
        res = {}
        res['data'] = self.data
        res['id'] = str(bfinfo.id)
        self.context['_res'] = res

        return bfinfo

    @property
    def response_data(self):
        return self.context.get('_res')

    def validate(self, data):
        """
        复杂验证
        """
        request = self.context.get('request')
        bucket_name = data.get('bucket_name')
        dir_path = data.get('dir_path')
        file_name = data.get('file_name')
        overwrite = data.get('overwrite', False)

        if '/' in file_name:
            raise serializers.ValidationError(detail={'error_text': 'file_name有误，文件名不能包含/'})

        # bucket是否属于当前用户,检测存储桶名称是否存在
        _collection_name, vali_error = get_bucket_collection_name_or_ValidationError(bucket_name, request)
        if not _collection_name and vali_error:
            raise vali_error

        bfm = BucketFileManagement(path=dir_path, collection_name=_collection_name)
        # 当前目录下是否已存在同文件名文件
        finfo = bfm.get_file_exists(file_name)
        #
        # if not ok:
        #     raise serializers.ValidationError(detail={'error_text': 'dir_path路径有误，路径不存在'})

        if finfo:
            # 同名文件覆盖上传
            if overwrite:
                # 文件记录删除动作在create中
                data['finfo'] = finfo
            else:
                raise serializers.ValidationError(detail={'error_text': 'file_name参数有误，已存在同名文件'})

        _, did = bfm.get_cur_dir_id()
        data['_did'] = did
        data['_collection_name'] = _collection_name
        data['BucketFileClass'] = bfm.get_bucket_file_class()
        return data


class ObjPutSerializer(serializers.Serializer):
    '''
    文件分块上传序列化器
    '''
    chunk_offset = serializers.IntegerField(label='文件块偏移量', required=True, min_value=0,
                                            help_text='上传文件块在整个文件中的起始位置（bytes偏移量)，类型int')
    chunk = serializers.FileField(label='文件块', required=False, help_text='文件分片的二进制数据块,文件或类文件对象，如JS的Blob对象')
    chunk_size = serializers.IntegerField(label='文件块大小', required=True, min_value=0,
                                          help_text='上传文件块的字节大小，类型int')
    # overwrite = serializers.BooleanField(label='是否覆盖', required=False,
    #                                      help_text='存在同名文件时是否覆盖(chunk_offset=0时有效)，True(覆盖)，其他默认False(不覆盖)')

    def validate(self, data):
        """
        复杂验证
        """
        chunk = data.get('chunk')
        chunk_size = data.get('chunk_size')
        # overwrite = data.get('overwrite', None)

        if not chunk:
            # chunk_size != 0时，此时却获得一个空文件块
            if 0 != chunk_size:
                raise serializers.ValidationError(detail={'chunk_size': 'chunk_size与文件块大小(0)不一致'})
            # 如果上传确实是一个空文件块不做处理
            return data
        elif chunk.size != chunk_size:
            raise serializers.ValidationError(detail={'chunk_size': 'chunk_size与文件块大小不一致'})

        # if overwrite is not True :
        #     overwrite = False

        return data


class DirectoryCreateSerializer(serializers.Serializer):
    '''
    创建目录序列化器
    '''
    bucket_name = serializers.CharField(label='存储桶名称', required=True, help_text='文件上传到的存储桶名称')
    dir_path = serializers.CharField(label='目录路径', required=False, help_text='存储桶下的目录路径，指定创建目录的父目录')
    dir_name = serializers.CharField(label='目录名', required=True, help_text='要创建的目录名称')

    def validate(self, data):
        '''校验参数'''
        request = self.context.get('request')
        bucket_name = data.get('bucket_name', '')
        dir_path = data.get('dir_path', '')
        dir_name = data.get('dir_name', '')

        if not bucket_name or not dir_name:
            raise serializers.ValidationError(detail={'error_text': 'bucket_name or dir_name不能为空'})

        if '/' in dir_name:
            raise serializers.ValidationError(detail={'error_text': 'dir_name不能包含‘/’'})

        # bucket是否属于当前用户,检测存储桶名称是否存在
        _collection_name, vali_error = get_bucket_collection_name_or_ValidationError(bucket_name, request)
        if not _collection_name and vali_error:
            raise vali_error

        data['collection_name'] = _collection_name

        bfm = BucketFileManagement(path=dir_path, collection_name=_collection_name)
        ok, dir = bfm.get_dir_exists(dir_name=dir_name)
        if not ok:
            raise serializers.ValidationError(detail={'error_text': 'dir_path参数有误，对应目录不存在'})
        # 目录已存在
        if dir:
            raise serializers.ValidationError(detail={'error_text': f'"{dir_name}"目录已存在'}, code='existing')
        data['did'] = bfm.cur_dir_id if bfm.cur_dir_id else bfm.get_cur_dir_id()[-1]

        return data

    @property
    def data(self):
        self.instance = None
        # data = self.validated_data
        data = super(DirectoryCreateSerializer, self).data
        return data

class ObjInfoSerializer(serializers.Serializer):
    '''
    目录下文件列表序列化器
    '''
    na = serializers.SerializerMethodField() # 文件名或目录名
    dir_name = serializers.SerializerMethodField()  # 非全路径目录名
    fod = serializers.BooleanField(required=True)  # file_or_dir; True==文件，False==目录
    did = serializers.CharField()  # 父节点objectID
    si = serializers.IntegerField()  # 文件大小,字节数
    # ult = serializers.DateTimeField(default=datetime.utcnow)  # 文件的上传时间，或目录的创建时间
    ult = serializers.SerializerMethodField()  # 自定义字段序列化方法
    # upt = serializers.DateTimeField()  # 文件的最近修改时间，目录，则upt为空
    upt = serializers.SerializerMethodField()  # 自定义字段序列化方法
    dlc = serializers.SerializerMethodField() #IntegerField()  # 该文件的下载次数，目录时dlc为空
    # bac = serializers.ListField(child = serializers.CharField(required=True))  # backup，该文件的备份地址，目录时为空
    # arc = serializers.ListField(child = serializers.CharField(required=True))  # archive，该文件的归档地址，目录时arc为空
    sh = serializers.BooleanField()  # shared，若sh为True，则文件可共享，若sh为False，则文件不能共享
    # shp = serializers.CharField()  # 该文件的共享密码，目录时为空
    stl = serializers.BooleanField()  # True: 文件有共享时间限制; False: 则文件无共享时间限制
    # sst = serializers.DateTimeField()  # share_start_time, 该文件的共享起始时间
    set = serializers.SerializerMethodField()  # share_end_time,该文件的共享终止时间
    sds = serializers.SerializerMethodField() # 自定义“软删除”字段序列化方法
    download_url = serializers.SerializerMethodField()
    access_permission = serializers.SerializerMethodField() # 公共读权限

    def get_na(self, obj):
        # 文件
        if obj.fod:
            pp = PathParser(obj.na)
            _, name = pp.get_path_and_filename()
            return name

        return obj.na

    def get_dlc(self, obj):
        return obj.dlc if obj.dlc else 0

    def get_dir_name(self, obj):
        # 文件
        if obj.fod:
            return ''

        pp = PathParser(obj.na)
        _, name = pp.get_path_and_filename()
        return name

    def get_sds(self, obj):
        return obj.get_sds_display()

    def get_ult(self, obj):
        if not obj.ult:
            return ''
        return to_localtime_string_naive_by_utc(obj.ult)

    def get_upt(self, obj):
        if not obj.upt:
            return ''
        return to_localtime_string_naive_by_utc(obj.upt)

    def get_set(self, obj):
        if not obj.set:
            return ''
        return to_localtime_string_naive_by_utc(obj.set)

    def get_download_url(self, obj):
        # 目录
        if not obj.fod:
            return  ''
        request = self.context.get('request', None)
        bucket_name = self._context.get('bucket_name', '')
        filepath = '/'.join((bucket_name, obj.na))
        download_url = reverse('obs:obs-detail', kwargs={'objpath': filepath})
        if request:
            download_url = request.build_absolute_uri(download_url)
        return download_url

    def get_access_permission(self, obj):
        # 目录
        if not obj.fod:
            return ''

        try:
            if obj.is_shared_and_in_shared_time():
                return '公有'
        except Exception as e:
            pass
        return '私有'


class AuthTokenDumpSerializer(serializers.Serializer):
    key = serializers.CharField()
    user = serializers.SerializerMethodField()
    created = serializers.SerializerMethodField()

    def get_user(self, obj):
        return obj.user.username

    def get_created(self, obj):
        return to_localtime_string_naive_by_utc(obj.created)


