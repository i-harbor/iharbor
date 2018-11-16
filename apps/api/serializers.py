import os

from datetime import datetime
from bson import ObjectId

from django.db.models import Q as dQ
from rest_framework import serializers
from rest_framework.reverse import reverse
from mongoengine.context_managers import switch_collection

from buckets.utils import BucketFileManagement
# from buckets.models import get_bucket_by_name
from .models import User, Bucket, BucketFileInfo
from utils.storagers import FileStorage, PathParser
from utils.oss.rados_interfaces import CephRadosObject
from .validators import DNSStringValidator



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
    # id = serializers.IntegerField(read_only=True)
    username = serializers.CharField(label='用户名', required=True, max_length=150, help_text='至少5位字母或数字')
    password = serializers.CharField(label='密码', required=True, max_length=128, help_text='至少8位密码')
    email = serializers.EmailField(label='邮箱', required=False, help_text='邮箱')
    # date_joined = serializers.DateTimeField()
    # last_login = serializers.DateTimeField()

    def create(self, validated_data):
        """
        Create and return a new `Snippet` instance, given the validated data.
        """
        username = validated_data['username']
        if User.objects.filter(username=username).exists():
            raise serializers.ValidationError(detail={'error_text': '用户名已存在'})
        return User.objects.create(**validated_data)


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
        return obj.created_time.strftime("%Y-%m-%d %H:%M:%S")

    def get_access_permission(self, obj):
        return obj.get_access_permission_display()


class BucketCreateSerializer(serializers.Serializer):
    '''
    创建存储桶序列化器
    '''
    name = serializers.CharField(label='存储桶名称', max_length=63,
                                 help_text='存储桶名称，名称唯一，不可使用已存在的名称，符合DNS标准的存储桶名称，英文字母、数字和-组成，不超过63个字符')
    # user = serializers.CharField(label='所属用户', help_text='所创建的存储桶的所属用户，可输入用户名或用户id')

    def validate(self, data):
        """
        复杂验证
        """
        bucket_name = data['name']

        if not bucket_name:
            raise serializers.ValidationError('存储桶bucket名称不能为空')

        if bucket_name.startswith('-') or bucket_name.endswith('-'):
            raise serializers.ValidationError('存储桶bucket名称不能以“-”开头或结尾')

        DNSStringValidator(bucket_name)
        bucket_name = bucket_name.lower()
        data['name'] = bucket_name

        if Bucket.get_bucket_by_name(bucket_name):
            raise serializers.ValidationError("存储桶名称已存在")
        return data

    # def validate_name(self, value):
    #     '''验证字段'''
    #     return value

    def create(self, validated_data):
        """
        Create and return a new `Bucket` instance, given the validated data.
        """
        request = self.context.get('request')
        user = request.user
        bucket = Bucket.objects.create(user=user, **validated_data) # 创建并保存
        return bucket


class ChunkedUploadCreateSerializer(serializers.Serializer):
    '''
    文件分块上传序列化器
    '''
    bucket_name = serializers.CharField(label='存储桶名称', required=True,
                                        help_text='文件上传到的存储桶名称，类型string')
    dir_path = serializers.CharField(label='文件上传路径', required=False,
                                     help_text='存储桶下的目录路径，指定文件上传到那个目录下，类型string')
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
        bfinfo = validated_data.get('finfo')
        _collection_name = validated_data.get('_collection_name')

        with switch_collection(BucketFileInfo, _collection_name):
            if bfinfo:
                bfinfo.si = 0 # 文件大小
            else:
                bfinfo = BucketFileInfo(na=file_name,# 文件名
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

        # bucket是否属于当前用户,检测存储桶名称是否存在
        _collection_name, vali_error = get_bucket_collection_name_or_ValidationError(bucket_name, request)
        if not _collection_name and vali_error:
            raise vali_error

        with switch_collection(BucketFileInfo, _collection_name):
            bfm = BucketFileManagement(path=dir_path)
            # 当前目录下是否已存在同文件名文件
            ok, finfo = bfm.get_file_exists(file_name)
            #
            if not ok:
                raise serializers.ValidationError(detail={'error_text': 'dir_path路径有误，路径不存在'})

            if finfo:
                # 同名文件覆盖上传
                if overwrite:
                    # 删除文件记录
                    # finfo.delete()
                    data['finfo'] = finfo
                    # 删除文件对象
                    fs = FileStorage(str(finfo.id))
                    fs.delete()
                else:
                    raise serializers.ValidationError(detail={'error_text': 'file_name参数有误，已存在同名文件'})

            _, did = bfm.get_cur_dir_id()
            data['_did'] = did
            data['_collection_name'] = _collection_name
        return data


class ChunkedUploadUpdateSerializer(serializers.Serializer):
    '''
    文件分块上传序列化器
    '''
    #id = serializers.CharField(label='文件ID', required=True, min_length=24, max_length=24,
    #                           help_text='请求上传文件服务器返回的文件id')
    bucket_name = serializers.CharField(label='存储桶名称', required=True, help_text='文件上传到的存储桶名称，类型string')
    chunk_offset = serializers.IntegerField(label='文件块偏移量', required=True, min_value=0,
                                            help_text='上传文件块在整个文件中的起始位置（bytes偏移量)，类型int')
    chunk = serializers.FileField(label='文件块', required=False, help_text='文件分片的块数据,如Blob对象')
    chunk_size = serializers.IntegerField(label='文件块大小', required=True, min_value=0,
                                          help_text='上传文件块的字节大小，类型int')

    def validate(self, data):
        """
        复杂验证
        """
        request = self.context.get('request')
        kwargs = self.context.get('kwargs')
        file_id = kwargs.get('pk')
        bucket_name = data.get('bucket_name')
        chunk_offset = data.get('chunk_offset')
        chunk = data.get('chunk')
        chunk_size = data.get('chunk_size')

        if not chunk:
            if 0 != chunk_size:
                raise serializers.ValidationError(detail={'chunk_size': 'chunk_size与文件块大小不一致'})
            return data
        elif chunk.size != chunk_size:
            raise serializers.ValidationError(detail={'chunk_size': 'chunk_size与文件块大小不一致'})

        # bucket是否属于当前用户,检测存储桶名称是否存在
        _collection_name, vali_error = get_bucket_collection_name_or_ValidationError(bucket_name, request)
        if not _collection_name and vali_error:
            raise vali_error

        with switch_collection(BucketFileInfo, _collection_name):
            bfi = BucketFileInfo.objects(id=file_id).first()
            if not bfi:
                raise serializers.ValidationError(detail={'id': '文件id有误，未找到文件'})

             # 存储文件块
            # fstorage = FileStorage(str(bfi.id))
            # if fstorage.write(chunk, chunk_size, offset=chunk_offset):
            rados = CephRadosObject(str(bfi.id))
            ok, bytes = rados.write(offset=chunk_offset, data_block=chunk.read())
            if ok:
                # 更新文件修改时间
                bfi.upt = datetime.utcnow()
                bfi.si = max(chunk_offset+chunk.size, bfi.si if bfi.si else 0) # 更新文件大小（只增不减）
                bfi.save()
            else:
                raise serializers.ValidationError(detail={'error': 'server error,文件块写入失败'})
        return data

    @property
    def response_data(self):
        res = {}
        self.instance = None # 如果self.instance != None, 调用self.data时会使用self.instance（这里真的的instance），会报错
        return res


class FileDeleteSerializer(serializers.Serializer):
    '''
    文件取消上传或删除序列化器
    '''
    id = serializers.CharField(label='文件ID', required=True, min_length=24, max_length=24,
                               help_text='文件唯一标识id,类型string')
    bucket_name = serializers.CharField(label='存储桶名称', required=True,
                                        help_text='文件所属的存储桶名称,类型string')

    def validate(self, data):
        # bucket是否属于当前用户,检测存储桶名称是否存在
        request = self.context.get('request')
        bucket_name = data.get('bucket_name')

        # bucket是否属于当前用户,检测存储桶名称是否存在
        _collection_name, vali_error = get_bucket_collection_name_or_ValidationError(bucket_name, request)
        if not _collection_name and vali_error:
            raise vali_error

        data['_collection_name'] = _collection_name
        return data

    def create(self, validated_data):
        '''删除文件或取消文件上传'''
        id = validated_data.get('id')
        collection_name = validated_data.get('_collection_name')
        with switch_collection(BucketFileInfo, collection_name):
            bfi = BucketFileInfo.objects(id=id).first()
            if bfi:
                bfi.do_soft_delete()
                return bfi
        # create必须要返回一个instance
        return True

    @property
    def response_data(self):
        # 如果self.instance != None, 调用self.data时会使用self.instance取提取字段数据，会报错；
        # self.instance为False时, self.data会通过上传的数据字段从校验后的validated_data中提取数据
        self.instance = None

        return self.data


class FileDownloadSerializer(serializers.Serializer):
    '''
    文件下载序列化器
    '''
    id = serializers.CharField(label='文件ID', required=True, min_length=24, max_length=24,
                               help_text='文件唯一标识id, 类型string')
    bucket_name = serializers.CharField(label='存储桶名称', required=True, help_text='文件所属的存储桶名称, 类型string')
    chunk_offset = serializers.IntegerField(label='文件块偏移量', required=True, min_value=0,
                                            help_text='要读取的文件块在整个文件中的起始位置（bytes偏移量), 类型int')
    chunk_size = serializers.IntegerField(label='文件块大小', required=True, min_value=0,
                                          help_text='要读取的文件块的字节大小, 类型int')

    def validate(self, data):
        request = self.context.get('request')
        bucket_name = data.get('bucket_name')

        # bucket是否属于当前用户,检测存储桶名称是否存在
        _collection_name, vali_error = get_bucket_collection_name_or_ValidationError(bucket_name, request)
        if not _collection_name and vali_error:
            raise vali_error

        data['_collection_name'] = _collection_name
        return data

    @property
    def data(self):
        self.instance = None
        return super(FileDownloadSerializer, self).data


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

        # bucket是否属于当前用户,检测存储桶名称是否存在
        _collection_name, vali_error = get_bucket_collection_name_or_ValidationError(bucket_name, request)
        if not _collection_name and vali_error:
            raise vali_error

        data['collection_name'] = _collection_name

        with switch_collection(BucketFileInfo, _collection_name):
            bfm = BucketFileManagement(path=dir_path)
            ok, dir = bfm.get_dir_exists(dir_name=dir_name)
            if not ok:
                raise serializers.ValidationError(detail={'error_text': 'dir_path参数有误，对应目录不存在'})
            # 目录已存在
            if dir:
                raise serializers.ValidationError(detail={'error_text': f'{dir_name}目录已存在'})
            data['did'] = bfm.cur_dir_id if bfm.cur_dir_id else bfm.get_cur_dir_id()[-1]

        return data

    @property
    def data(self):
        self.instance = None
        # data = self.validated_data
        data = super(DirectoryCreateSerializer, self).data
        return data

class DirectoryListSerializer(serializers.Serializer):
    '''
    目录下文件列表序列化器
    '''
    na = serializers.CharField(required=True, help_text='文件名或目录名')
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
    # sh = serializers.BooleanField()  # shared，若sh为True，则文件可共享，若sh为False，则文件不能共享
    # shp = serializers.CharField()  # 该文件的共享密码，目录时为空
    # stl = serializers.BooleanField()  # True: 文件有共享时间限制; False: 则文件无共享时间限制
    # sst = serializers.DateTimeField()  # share_start_time, 该文件的共享起始时间
    # set = serializers.DateTimeField()  # share_end_time,该文件的共享终止时间
    sds = serializers.SerializerMethodField() # 自定义“软删除”字段序列化方法
    download_url = serializers.SerializerMethodField()

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
        return obj.ult.strftime("%Y-%m-%d %H:%M:%S")

    def get_upt(self, obj):
        if not obj.upt:
            return ''
        return obj.upt.strftime("%Y-%m-%d %H:%M:%S")

    def get_download_url(self, obj):
        # 目录
        if not obj.fod:
            return  ''
        request = self.context.get('request', None)
        bucket_name = self._context.get('bucket_name', '')
        dir_path = self._context.get('dir_path', '')
        filepath = os.path.join(bucket_name, dir_path, obj.na)
        download_url = reverse('api:bucket-detail', kwargs={'version': 'v1', 'filepath': filepath})
        if request:
            download_url = request.build_absolute_uri(download_url)
        return download_url

