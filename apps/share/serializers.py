from django.db.models import Q as dQ
from rest_framework import serializers
from rest_framework.reverse import reverse

from buckets.models import Bucket


class SharedPostSerializer(serializers.Serializer):
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



