from rest_framework import serializers

from .models import User, Bucket


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

    # def update(self, instance, validated_data):
    #     """
    #     Update and return an existing `Snippet` instance, given the validated data.
    #     """
    #     instance.title = validated_data.get('title', instance.title)
    #     instance.code = validated_data.get('code', instance.code)
    #     instance.linenos = validated_data.get('linenos', instance.linenos)
    #     instance.language = validated_data.get('language', instance.language)
    #     instance.style = validated_data.get('style', instance.style)
    #     instance.save()
    #     return instance


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

    def validate_name(self, value):
        '''验证字段'''
        return value

    def create(self, validated_data):
        """
        Create and return a new `Bucket` instance, given the validated data.
        """
        request = self.context.get('request')
        user = request.user
        bucket = Bucket.objects.create(user=user, **validated_data) # 创建并保存
        return bucket
