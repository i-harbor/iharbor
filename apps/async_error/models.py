from django.db import models
from django.core.validators import MinValueValidator


# Create your models here.

class BucketAsyncError(models.Model):
    """
    数据同步错误记录
    """

    id = models.AutoField(primary_key=True)
    node_ip = models.CharField(verbose_name='节点ip地址', max_length=255)
    bucket_id = models.IntegerField(verbose_name='存储桶id', validators=[MinValueValidator(1)])
    bucket_name = models.CharField(verbose_name='存储桶名称', max_length=63)
    object_id = models.IntegerField(verbose_name='对象id', validators=[MinValueValidator(1)])
    object_name = models.CharField(verbose_name='对象名称', max_length=255)
    async_error = models.TextField(verbose_name='同步错误')
    error_time = models.DateTimeField(auto_now=True, verbose_name='时间')
    backup_ip = models.CharField(verbose_name='备份地址', max_length=255)
    backup_bucket = models.CharField(verbose_name='备份存储桶名称', max_length=63)
    # 记录节点命令参数
    node_num = models.IntegerField(verbose_name='节点编号', validators=[MinValueValidator(1)])
    node_count = models.IntegerField(verbose_name='节点总数', validators=[MinValueValidator(1)])
    thread_num = models.IntegerField(verbose_name='并发线程数', validators=[MinValueValidator(0)], blank=True, null=True)
    bucketlist = models.CharField(verbose_name='指定备份的桶', max_length=255, blank=True, null=True)

    class Meta:
        ordering = ('id',)
        verbose_name = '数据同步错误日志'
        verbose_name_plural = '数据同步错误日志'

    def __str__(self):
        return str(self.id)

