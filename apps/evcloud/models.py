from django.db import models
from django.contrib.auth import get_user_model
from ckeditor.fields import RichTextField
# Create your models here.

User = get_user_model()


class VMConfig(models.Model):
    id = models.BigAutoField(primary_key=True)
    cpu = models.IntegerField(verbose_name='cpu数量')
    mem = models.IntegerField(verbose_name='内存容量')
    time = models.IntegerField(verbose_name='使用期限（月）')

    class Meta:
        verbose_name = '虚拟机配置下拉菜单'
        verbose_name_plural = verbose_name

class APIAuth(models.Model):
    id = models.BigAutoField(primary_key=True)
    url = models.CharField(max_length=150, verbose_name='api_url')
    name = models.CharField(max_length=50, verbose_name='用户名')
    pwd = models.CharField(max_length=50, verbose_name='密码')
    group_id = models.IntegerField()
    vlan_id = models.IntegerField()
    pool_id = models.IntegerField()
    description = models.CharField(max_length=50, default='', verbose_name='描述')
    limit = models.IntegerField(verbose_name='虚拟机限制')
    flag = models.BooleanField(verbose_name='是否当前使用')

    class Meta:
        verbose_name = 'api调用配置表'
        verbose_name_plural = verbose_name

    def __str__(self):
        return self.description

class VMLimit(models.Model):
    id = models.BigAutoField(primary_key=True)
    api = models.ForeignKey(APIAuth, on_delete=models.CASCADE, verbose_name='api_id')
    user = models.ForeignKey(to=User, on_delete=models.CASCADE, verbose_name='所属用户')
    limit = models.IntegerField(verbose_name='限制数量')

    class Meta:
        verbose_name = '虚拟机限制'
        verbose_name_plural = verbose_name
        unique_together = ('api', 'user',)

class EvcloudVM(models.Model):

    vm_id = models.CharField(max_length=100, db_index=True, primary_key=True, verbose_name='虚拟机uuid')
    api = models.ForeignKey(APIAuth, on_delete=models.CASCADE, verbose_name='api_id')
    user = models.ForeignKey(to=User, on_delete=models.CASCADE, verbose_name='所属用户')
    created_time = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    end_time = models.DateTimeField(verbose_name='结束时间')
    vm_image = models.CharField(max_length=200, verbose_name='镜像id')
    vm_image_name = models.CharField(max_length=50, verbose_name='镜像名')
    vm_cpu = models.CharField(max_length=50, verbose_name='CPU数量')
    vm_mem = models.CharField(max_length=50, verbose_name='内存容量')
    vm_ip = models.GenericIPAddressField()
    remarks = models.TextField(null=True, blank=True)
    deleted = models.BooleanField(default=False)
    group_id = models.IntegerField()

    class Meta:
        verbose_name = '虚拟机列表'
        verbose_name_plural = verbose_name
        ordering = ['-created_time']

class VMUsageDescription(models.Model):
    '''
    虚拟云主机使用说明
    '''
    title = models.CharField(verbose_name='标题', default='虚拟云主机使用说明', max_length=255)
    content = RichTextField(verbose_name='说明内容', default='')
    created_time = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    modified_time = models.DateTimeField(auto_now=True, verbose_name='修改时间')

    class Meta:
        verbose_name = '虚拟云主机使用说明'
        verbose_name_plural = verbose_name

    def __str__(self):
        return f'<VMUsageDescription>{self.title}'

    def __repr__(self):
        return f'<VMUsageDescription>{self.title}'

