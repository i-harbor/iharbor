from django.db import models
from django.contrib.auth import get_user_model
# Create your models here.

User = get_user_model()

class EvcloudVM(models.Model):

    vm_id = models.CharField(max_length=100, db_index=True, primary_key=True, verbose_name='虚拟机uuid')
    user = models.ForeignKey(to=User, on_delete=models.CASCADE, verbose_name='所属用户')
    created_time = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    end_time = models.DateTimeField(verbose_name='结束时间')
    vm_image = models.CharField(max_length=200, verbose_name='镜像')
    vm_cpu = models.CharField(max_length=50, verbose_name='CPU数量')
    vm_mem = models.CharField(max_length=50, verbose_name='内存容量')
    vm_ip = models.GenericIPAddressField()
    remarks = models.TextField(null=True, blank=True)
    deleted = models.BooleanField(default=False)
    group_id = models.IntegerField()

    class Meta:
        verbose_name = '虚拟机列表'
        verbose_name_plural = verbose_name

class VMLimit(models.Model):
    id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(to=User, on_delete=models.CASCADE, verbose_name='所属用户')
    limit = models.IntegerField(default=3, verbose_name='限制数量')

    class Meta:
        verbose_name = '虚拟机限制'
        verbose_name_plural = verbose_name

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
    flag = models.BooleanField(verbose_name='是否当前使用')

    class Meta:
        verbose_name = 'api调用配置表'
        verbose_name_plural = verbose_name

