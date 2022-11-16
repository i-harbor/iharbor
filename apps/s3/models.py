import base64
import hashlib
import json
import uuid

from django.db import models
from django.utils.translation import gettext_lazy as _
from django.utils import timezone
from _datetime import datetime
from .multiparts import MultipartPartsManager


def uuid1_time_hex_string(t):
    f = t.timestamp()
    h = uuid.uuid1().hex
    s = f'{f:.6f}'
    s += '0' * (len(s) % 4)
    bs = base64.b64encode(s.encode(encoding='utf-8')).decode('ascii')
    return f'{h}_{bs}'

def get_str_hexMD5(s: str):
    """
    求字符串MD5
    """
    return hashlib.md5(s.encode(encoding='utf-8')).hexdigest()

def get_datetime_from_upload_id(upload_id: str):
    """
    :return:
        datetime()
        None
    """
    l = upload_id.split('_', maxsplit=1)
    if len(l) != 2:
        return None

    s = base64.b64decode(l[-1]).decode("utf-8")
    try:
        f = float(s)
    except ValueError:
        return None

    return datetime.fromtimestamp(f, tz=timezone.utc)


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        # print(f'DateEncoder = {obj}')
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return json.JSONEncoder.default(self, obj)


class MultipartUpload(models.Model):
    """
    多部分上传
    part_json：
    {
    ObjectCreateTime:"",
    Parts:[


            {"ETag": "", "Size": 5242880, "PartNumber": 1, "lastModified": "2022-11-10 07:52:09"},
            {"ETag": "", "Size": 5242880, "PartNumber": 2, "lastModified": "2022-11-10 07:53“}
            。。。

        ]
    }
    """

    class UploadStatus(models.TextChoices):
        UPLOADING = 'uploading', _('上传中')
        COMPOSING = 'composing', _('组合中')
        COMPLETED = 'completed', _('上传完成')

    id = models.CharField(verbose_name='ID', primary_key=True, max_length=64, help_text='uuid+time')
    bucket_id = models.BigIntegerField(verbose_name='bucket id')
    bucket_name = models.CharField(verbose_name='bucket name', max_length=63, default='')
    obj_id = models.BigIntegerField(verbose_name='object id', default=0, help_text='组合对象后为对象id, 默认为0表示还未组合对象')
    obj_key = models.CharField(verbose_name='object key', max_length=1024, db_collation='utf8mb4_bin', default='')
    key_md5 = models.CharField(max_length=32, verbose_name='object key MD5')
    obj_etag = models.CharField(max_length=64, verbose_name='object MD5 Etag', default='')
    obj_perms_code = models.SmallIntegerField(verbose_name='对象访问权限', default=0)
    part_num = models.IntegerField(verbose_name='块总数', blank=True, default=0)
    part_json = models.JSONField(verbose_name='块信息', default=dict, encoder=DateEncoder)
    chunk_size = models.BigIntegerField(verbose_name='块大小', blank=True, default=0)
    status = models.CharField(max_length=64, verbose_name='状态', choices=UploadStatus.choices,
                              default=UploadStatus.UPLOADING)
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

    def belong_to_bucket(self, bucket):
        """
        此多部分上传是否属于bucket, 因为这条记录可能属于已删除的桶名相同的桶

        :param bucket: Bucket()
        :return:
            True        # 属于
            False       # 不属于桶， 无效的多部分上传记录，需删除
        """
        if (self.bucket_name == bucket.name) and (self.bucket_id == bucket.id):
            return True

        return False

    # 取所有的块
    def parts_object(self):
        return self.part_json['Parts']

    def search_by_index_part(self, index):
        parts = self.parts_object() # list
        if index > len(parts)-1:
            return None
        return parts[index]

    # 增加块
    def insert_part(self, num, part_info, chunk_size=None):
        """
        增加块信息
        :param num: 块id
        :param part_info: 块信息
        :return:
        """
        parts_arr = self.parts_object()
        flag, arr = MultipartPartsManager().list_insert_part(part=part_info, parts_arr=parts_arr)

        if self.chunk_size:
            # 替换（第一块） 最后一块不修改
            if self.chunk_size == chunk_size:
                self.chunk_size = chunk_size
        else:
            # 第一块
            self.chunk_size = chunk_size

        self.save(update_fields=['part_json', 'chunk_size'])
        if not flag:
            # flog 未false 表示替换 对象不会修改大小
            return False
        return True

    # 检查第一块的上传
    def check_first_part(self, num):
        part = self.parts_object()
        if not part:
            if num == 1:
                return True
            return False
        return True

    # 获取块的数量
    def part_number(self):
        parts = self.parts_object()
        return len(parts)