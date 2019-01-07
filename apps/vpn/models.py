from django.db import models
from ckeditor.fields import RichTextField

# Create your models here.

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

