import base64
import uuid

from django.db import models, connections, router
from django.db.backends.mysql.schema import DatabaseSchemaEditor
from django.utils.translation import gettext_lazy as _
# Create your models here.
from django.utils import timezone


def uuid1_time_hex_string(t):
    f = t.timestamp()
    h = uuid.uuid1().hex
    s = f'{f:.6f}'
    s += '0' * (len(s) % 4)
    bs = base64.b64encode(s.encode(encoding='utf-8')).decode('ascii')
    return f'{h}_{bs}'


class MultipartUpload(models.Model):
    """
    多部分上传
    """

    class UploadStatus(models.TextChoices):
        UPLOADING = 'uploading', _('上传中')
        COMPOSING = 'composing', _('组合中')
        COMPLETED = 'completed', _('上传完成')

    id = models.CharField(verbose_name='ID', primary_key=True, max_length=64, help_text='uuid+time')
    bucket_id = models.BigIntegerField(verbose_name='bucket id')
    bucket_name = models.CharField(verbose_name='bucket name', max_length=63, default='')
    obj_id = models.BigIntegerField(verbose_name='object id', default=0, help_text='组合对象后为对象id, 默认为0表示还未组合对象')
    obj_key = models.CharField(verbose_name='object key', max_length=1024, default='')
    key_md5 = models.CharField(max_length=32, verbose_name='object key MD5')
    status = models.CharField(max_length=64, verbose_name='状态', choices=UploadStatus.choices,
                              default=UploadStatus.UPLOADING)
    obj_perms_code = models.SmallIntegerField(verbose_name='对象访问权限', default=0)
    part_num = models.IntegerField(verbose_name='块总数', blank=True, default=0)
    part_json = models.JSONField(verbose_name='块信息', default=dict)
    create_time = models.DateTimeField(verbose_name='创建时间', auto_now_add=True)
    expire_time = models.DateTimeField(verbose_name='对象过期时间', null=True, default=None, help_text='上传过程终止时间')
    last_modified = models.DateTimeField(verbose_name='修改时间', auto_now_add=True)

    class Meta:
        # managed = True
        db_table = 'multipart_upload'
        indexes = [
            models.Index(fields=('key_md5',), name='key_md5_idx'),
            models.Index(fields=('bucket_name',), name='bucket_name_idx')
        ]
        app_label = 'metadata'  # 用于db路由指定此模型对应的数据库
        verbose_name = '对象多部分上传'
        verbose_name_plural = verbose_name

    @staticmethod
    def is_exists(table_name, using):
        return table_name in connections[using].introspection.table_names()

    @classmethod
    def create_table(cls):
        table_name = cls._meta.db_table
        using = router.db_for_write(cls)
        if not cls.is_exists(table_name, using):
            with DatabaseSchemaEditor(connection=connections[using]) as schema_editor:
                schema_editor.create_model(cls)
            return True
        else:
            return True

    def save(self, force_insert=False, force_update=False, using=None,
             update_fields=None):
        t = timezone.now()
        if not self.id:
            self.id = uuid1_time_hex_string(t)
            self.create_time = t
            if update_fields:
                update_fields.append('id')
                update_fields.append('create_time')
        super().save(force_insert=force_insert, force_update=force_update, using=using, update_fields=update_fields)

    # def get_table_name(self):
    #
    #     return self.Meta.db_table
    # def is_completed(self):
    #     """
    #     是否是已完成的多部分上传任务
    #     :return:
    #         True
    #         False
    #     """
    #     return self.status == self.UploadStatus.COMPLETED
