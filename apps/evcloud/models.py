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
