import binascii
import os
from uuid import uuid1

from django.db import models
from django.contrib.auth.models import AbstractUser
from django.core.mail import send_mail, EmailMessage
from django.conf import settings

# Create your models here.

class UserProfile(AbstractUser):
    '''
    自定义用户模型
    '''
    NON_THIRD_APP = 0
    LOCAL_USER = NON_THIRD_APP
    THIRD_APP_KJY = 1       # 第三方科技云通行证

    THIRD_APP_CHOICES = (
        (NON_THIRD_APP, 'Local user.'),
        (THIRD_APP_KJY, '科技云通行证')
    )

    telephone = models.CharField(verbose_name='电话', max_length=11, default='')
    company = models.CharField(verbose_name='公司/单位', max_length=255, default='')
    third_app = models.SmallIntegerField(verbose_name='第三方应用登录', choices=THIRD_APP_CHOICES, default=NON_THIRD_APP)
    secret_key = models.CharField(verbose_name='个人密钥', max_length=20, blank=True, default='') # jwt加密解密需要

    def get_full_name(self):
        if self.last_name.encode('UTF-8').isalpha() and self.first_name.encode('UTF-8').isalpha():
            return f'{self.first_name} {self.last_name}'

        return f'{self.last_name}{self.first_name}'

    def get_user_secret_key(self):
        if not self.secret_key:
            self.secret_key = binascii.hexlify(os.urandom(10)).decode()
            self.save()

        return self.secret_key


class Email(models.Model):
    '''
    邮箱
    '''
    email_host = models.CharField(max_length=255)
    sender = models.EmailField(verbose_name='发送者')
    receiver = models.EmailField(verbose_name='接收者')
    message = models.CharField(verbose_name='邮件内容', max_length=1000)
    send_time = models.DateTimeField(verbose_name='发送时间', auto_now_add=True)

    class Meta:
        verbose_name = '邮件'
        verbose_name_plural = verbose_name

    def send_email(self, subject='EVHarbor',receiver=None, message=None):
        '''
        发送用户激活邮件

        :param receiver: 接收者邮箱
        :param message: 邮件内容
        :return: True(发送成功)；False(发送失败)
        '''
        if receiver:
            self.receiver = receiver
        # if message:
        #     self.message = message
        self.sender = settings.EMAIL_HOST_USER
        self.email_host = settings.EMAIL_HOST

        ok = send_mail(
            subject=subject,         # 标题
            message=message,                    # 内容
            from_email=self.sender,             # 发送者
            recipient_list=[self.receiver],     # 接收者
            # html_message=self.message,        # 内容
            fail_silently=True,                 # 不抛出异常
        )
        if ok == 0:
            return False

        self.save() # 邮件记录
        return True


class AuthKey(models.Model):

    STATUS_CHOICES = (
        (True, '正常'),
        (False, '停用'),
    )

    READ_WRITE = 0
    READ_ONLY = 1
    READ_WRITE_CHOICES = (
        (READ_WRITE, '可读可写'),
        (READ_ONLY, '只读'),
    )

    id = models.CharField(verbose_name='access_key', max_length=50, primary_key=True)
    secret_key = models.CharField(verbose_name='secret_key', max_length=50, default='')
    user = models.ForeignKey(to=UserProfile, on_delete=models.CASCADE, verbose_name='所属用户')
    state = models.BooleanField(verbose_name='状态', default=True, choices=STATUS_CHOICES, help_text='正常或者停用')
    create_time = models.DateTimeField(verbose_name='创建时间', auto_now_add=True)
    permission = models.IntegerField(verbose_name='读写权限', default=READ_WRITE, choices=READ_WRITE_CHOICES)

    class Meta:
        verbose_name = '访问密钥'
        verbose_name_plural = '访问密钥'
        ordering = ['-create_time']

    def _get_access_key_val(self):
        return self.id

    def _set_access_key_val(self, value):
        self.id = value

    access_key = property(_get_access_key_val, _set_access_key_val)

    def save(self, *args, **kwargs):
        # access_key
        if not self.id:
            self.id = self.uuid1_hex_key()

        if not self.secret_key:
            self.secret_key = self.generate_key()
        return super(AuthKey, self).save(*args, **kwargs)

    def generate_key(self):
        '''生成一个随机字串'''
        return binascii.hexlify(os.urandom(20)).decode()

    def uuid1_hex_key(self):
        '''唯一uuid1'''
        return uuid1().hex

    def is_key_active(self):
        '''
        密钥是否是激活有效的
        :return:
            有效：True
            停用：False
        '''
        return self.state

    def __str__(self):
        return self.secret_key

