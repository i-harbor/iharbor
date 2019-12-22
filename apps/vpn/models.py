import binascii, os

from django.db import models
from django.contrib.auth import get_user_model
from ckeditor.fields import RichTextField


#获取用户模型
User = get_user_model()

def rand_hex_string(len=10):
    return binascii.hexlify(os.urandom(len//2)).decode()

class VPNUsageDescription(models.Model):
    '''
    VPN使用说明
    '''
    title = models.CharField(verbose_name='标题', default='VPN使用说明', max_length=255)
    content = RichTextField(verbose_name='说明内容', default='')
    created_time = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    modified_time = models.DateTimeField(auto_now=True, verbose_name='修改时间')

    class Meta:
        verbose_name = 'VPN使用说明'
        verbose_name_plural = verbose_name

    def __str__(self):
        return f'<VPNUsageDescription>{self.title}'

    def __repr__(self):
        return f'<VPNUsageDescription>{self.title}'


class VPNAuth(models.Model):
    '''
    VPN登录认证model
    '''
    id = models.AutoField(verbose_name='ID', primary_key=True)
    user = models.OneToOneField(to=User, on_delete=models.CASCADE, related_name='vpn_auth', verbose_name='用户')
    password = models.CharField(verbose_name='VPN口令', max_length=20, default='')
    created_time = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    modified_time = models.DateTimeField(auto_now=True, verbose_name='修改时间')

    class Meta:
        db_table = 'vpn_authentication' # 数据库表名
        ordering = ('-id',)
        verbose_name = 'VPN口令'
        verbose_name_plural = verbose_name

    def __str__(self):
        return f'<VPNAuth>{self.password}'

    def save(self, force_insert=False, force_update=False, using=None, update_fields=None):
        if not self.password or len(self.password) < 6:
            self.password = rand_hex_string()

        super().save(force_insert=force_insert, force_update=force_update, using=using, update_fields=update_fields)

    def reset_password(self, password):
        if self.password == password:
            return True

        self.password = password
        try:
            self.save()
        except Exception as e:
            return False

        return True

    def check_password(self, password):
        return self.password == password

