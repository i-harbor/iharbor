# import math
from datetime import datetime
from bson import ObjectId

from django.db.models import Q as dQ
from rest_framework import serializers
from mongoengine.context_managers import switch_collection
from rest_framework.exceptions import APIException

from buckets.utils import BucketFileManagement, get_collection_name
from .models import User, Bucket, BucketFileInfo
from utils.storagers import FileStorage


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
            raise serializers.ValidationError('用户名已存在')
        return User.objects.create(**validated_data)


class BucketSerializer(serializers.ModelSerializer):
    '''
    存储桶序列化器
    '''
    user = serializers.SerializerMethodField() # 自定义user字段内容
    class Meta:
        model = Bucket
        fields = ('id', 'name', 'user', 'created_time')
        # depth = 1

    def get_user(selfself, obj):
        return {'id': obj.user.id, 'username': obj.user.username}


class BucketCreateSerializer(serializers.Serializer):
    '''
    创建存储桶序列化器
    '''
    name = serializers.CharField(label='存储桶名称', max_length=50,
                                 help_text='存储桶名称，名称唯一，不可使用已存在的名称，由最多50个字母或数字组成')
    # user = serializers.CharField(label='所属用户', help_text='所创建的存储桶的所属用户，可输入用户名或用户id')

    def validate(self, data):
        """
        复杂验证
        """
        bucket_name = data['name']
        if Bucket.objects.filter(name=bucket_name).exists():
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
    bucket_name = serializers.CharField(label='存储桶名称', required=True, help_text='文件上传到的存储桶名称')
    dir_path = serializers.CharField(label='文件上传路径', required=False, help_text='存储桶下的目录路径，指定文件上传到那个目录下')
    file_name = serializers.CharField(label='文件名', required=True, help_text='上传文件的文件名')
    file_size = serializers.IntegerField(label='文件大小', required=True, min_value=1, help_text='上传文件的字节大小')
    file_md5 = serializers.CharField(label='文件MD5码', required=False, min_length=32, max_length=32,
                                     help_text='由文件内容生成的MD5码字符串')
    overwrite = serializers.BooleanField(label='是否覆盖', help_text='存在同名文件时是否覆盖')

    def create(self, validated_data):
        file_name = validated_data.get('file_name')
        file_size = validated_data.get('file_size')
        file_md5 = validated_data.get('file_md5')

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
        if not Bucket.objects.filter(dQ(user=request.user) & dQ(name=bucket_name)).exists():
            raise serializers.ValidationError(detail={'bucket_name': '存储桶不存在'})

        _collection_name = get_collection_name(username=request.user.username, bucket_name=bucket_name)
        with switch_collection(BucketFileInfo, _collection_name):
            bfm = BucketFileManagement(path=dir_path)
            # 当前目录下是否已存在同文件名文件
            ok, finfo = bfm.get_file_exists(file_name)
            #
            if not ok:
                raise serializers.ValidationError(detail={'dir_path': '路径有误，路径不存在'})

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
                    raise serializers.ValidationError(detail={'file_name': '已存在同名文件'})

            _, did = bfm.get_cur_dir_id()
            data['_did'] = did
            data['_collection_name'] = _collection_name
        return data


class ChunkedUploadUpdateSerializer(serializers.Serializer):
    '''
    文件分块上传序列化器
    '''
    #id = serializers.CharField(label='文件ID', required=True, min_length=24, max_length=24, help_text='请求上传文件服务器返回的文件id')
    bucket_name = serializers.CharField(label='存储桶名称', required=True, help_text='文件上传到的存储桶名称')
    chunk_offset = serializers.IntegerField(label='文件块偏移量', required=True, min_value=0,
                                            help_text='上传文件块在整个文件中的起始位置（bytes)')
    chunk = serializers.FileField(label='文件块', required=False, help_text='文件分片的块数据')
    chunk_size = serializers.IntegerField(label='文件块大小', required=True, min_value=0, help_text='上传文件块的字节大小')

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
        if not Bucket.objects.filter(dQ(user=request.user) & dQ(name=bucket_name)).exists():
            raise serializers.ValidationError(detail={'bucket_name': '存储桶不存在'})

        _collection_name = get_collection_name(username=request.user.username, bucket_name=bucket_name)
        with switch_collection(BucketFileInfo, _collection_name):
            bfi = BucketFileInfo.objects(id=file_id).first()
            if not bfi:
                raise serializers.ValidationError(detail={'id': '文件id有误，未找到文件'})

             # 存储文件块
            fstorage = FileStorage(str(bfi.id))
            if fstorage.write(chunk, chunk_size, offset=chunk_offset):
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
    id = serializers.CharField(label='文件ID', required=True, min_length=24, max_length=24, help_text='文件唯一标识id')
    bucket_name = serializers.CharField(label='存储桶名称', required=True, help_text='文件所属的存储桶名称')

    def validate(self, data):
        # bucket是否属于当前用户,检测存储桶名称是否存在
        request = self.context.get('request')
        bucket_name = data.get('bucket_name')
        if not Bucket.objects.filter(dQ(user=request.user) & dQ(name=bucket_name)).exists():
            raise serializers.ValidationError(detail={'bucket_name': '存储桶不存在'})

        _collection_name = get_collection_name(username=request.user.username, bucket_name=bucket_name)
        data['_collection_name'] = _collection_name

        return data

    def create(self, validated_data):
        '''删除文件或取消文件上传'''
        id = validated_data.get('id')
        collection_name = validated_data.get('_collection_name')
        with switch_collection(BucketFileInfo, collection_name):
            bfi = BucketFileInfo.objects(id=id).first()
            if bfi:
                bfi.delete()
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
    id = serializers.CharField(label='文件ID', required=True, min_length=24, max_length=24, help_text='文件唯一标识id')
    bucket_name = serializers.CharField(label='存储桶名称', required=True, help_text='文件所属的存储桶名称')
    chunk_offset = serializers.IntegerField(label='文件块偏移量', required=True, min_value=0,
                                            help_text='要读取的文件块在整个文件中的起始位置（bytes)')
    chunk_size = serializers.IntegerField(label='文件块大小', required=True, min_value=0,
                                          help_text='要读取的文件块的字节大小, 服务器返回最大数据2MB(2*1024*1024)')

    def validate(self, data):
        # bucket是否属于当前用户,检测存储桶名称是否存在
        request = self.context.get('request')
        bucket_name = data.get('bucket_name')
        if not Bucket.objects.filter(dQ(user=request.user) & dQ(name=bucket_name)).exists():
            raise serializers.ValidationError(detail={'bucket_name': '存储桶不存在'})

        _collection_name = get_collection_name(username=request.user.username, bucket_name=bucket_name)
        data['_collection_name'] = _collection_name

        return data

    @property
    def data(self):
        self.instance = None
        return super(FileDownloadSerializer, self).data




