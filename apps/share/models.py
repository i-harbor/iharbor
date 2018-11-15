import uuid

from django.db import models
from django.contrib.auth import get_user_model


User = get_user_model()

# Create your models here.

class SharedLink(models.Model):
    '''
    分享链接模型
    '''
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False, verbose_name='分享码')
    user = models.ForeignKey(to=User, on_delete=models.CASCADE, verbose_name='所属用户')
    bucket_name = models.CharField(verbose_name='存储桶名称')
    path = models.CharField(verbose_name='分享路径', blank=True, help_text='文件或文件夹分享路径，为空时分享整个存储桶')
    shared_time = models.DateTimeField(verbose_name='分享时间', auto_now_add=True, auto_now=True)
    limit_time = models.DateTimeField(verbose_name='有效时限', blank=True, null=True, help_text='分享连接失效时间，为null时表示永久有效')


