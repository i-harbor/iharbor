import base64
import hashlib
import json
import uuid
import copy
from _datetime import datetime

from django.db import models
from django.utils.translation import gettext_lazy as _
from django.utils import timezone

from . import exceptions
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
    parts_count = models.IntegerField(verbose_name='块总数', blank=True, default=0)
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

    def get_parts(self) -> list:
        return self.part_json['Parts']

    def get_part_by_index(self, index):
        parts = self.get_parts()     # list
        if index >= len(parts):
            return None

        return parts[index]

    def get_part_by_number(self, number: int):
        """
        查询指定编号的part

        :return:(
            part,       # part信息; None(不存在)
            int         # part在列表的索引; None(不存在)
        )
        """
        part, index = MultipartPartsManager().query_part_info(num=number, parts=self.get_parts())
        return part, index

    def insert_part(self, part_info: dict):
        """
        增加块信息

        :param part_info: 块信息
        :return:
            bool       # True(插入)；False(替换)
        """
        parts_arr = self.get_parts()
        is_insert, arr = MultipartPartsManager().insert_part_into_list(part=part_info, parts_arr=parts_arr)

        update_fields = ['part_json']
        # 第一块 并发会修正偏移量，导致后续合并无法找到位置
        # if part_info['PartNumber'] == 1:
        #     self.chunk_size = part_info['Size']
        #     update_fields.append('chunk_size')

        self.save(update_fields=update_fields)
        return is_insert

    @property
    def is_part1_uploaded(self):
        """
        编号为1的块是否已上传

        :return:
            True    # 已上传
            False   # 未上传
        """
        parts = self.get_parts()
        if not parts:
            return False

        if parts[0]['PartNumber'] == 1:
            return True

        return False

    # 获取块的数量
    def get_parts_length(self):
        parts = self.get_parts()
        return len(parts)

    @staticmethod
    def generate_key_hex_md5(key: str):
        return get_str_hexMD5(key)

    @staticmethod
    def build_part_item(part_number: int, last_modified: datetime, etag: str, size: int) -> dict:
        """
        构建一个part信息结构
        :param part_number: part编号
        :param last_modified: part修改日期，
        :param etag: part md5
        :param size: part大小
        """
        return {
            'PartNumber': part_number,
            'lastModified': last_modified,
            'ETag': etag,
            'Size': size
        }

    @classmethod
    def create_multipart(
            cls, bucket_id: int, bucket_name: str,
            obj_id: int, obj_key: str, obj_upload_time: datetime, obj_perms_code: int
    ):
        key_md5 = cls.generate_key_hex_md5(obj_key)
        part_json = {'Parts': [], 'ObjectCreateTime': int(obj_upload_time.timestamp())}
        upload = MultipartUpload(bucket_id=bucket_id, bucket_name=bucket_name, obj_id=obj_id,
                                 obj_key=obj_key, key_md5=key_md5, obj_perms_code=obj_perms_code, part_json=part_json)
        upload.save(force_insert=True)
        return upload

    def get_obj_upload_timestamp(self) -> int:
        """
        获取多部分上传对应的对象上传时间戳
        """
        ts = self.part_json.get('ObjectCreateTime', None)
        if ts:
            return ts

        return 0

    def set_completed(self, obj_etag: str, obj=None, chunk_size=None):
        self.obj_etag = obj_etag
        self.last_modified = timezone.now()
        self.status = MultipartUpload.UploadStatus.COMPLETED.value
        update_fields = []
        try:
            if obj:
                self.obj_id = obj.id
                self.key_md5 = obj.na_md5
                self.chunk_size = chunk_size
                update_fields.extend(['obj_id', 'key_md5', 'chunk_size'])

            self.parts_count = self.get_parts_length()
            update_fields.extend(['obj_etag', 'last_modified', 'status', 'parts_count'])
            self.save(update_fields=update_fields)
        except Exception as e:
            raise exceptions.S3Error(message=f'更新多部分上传记录错误, {str(e)}')

    def set_uploading(self):
        """
        正在多部分上传
        :return:
            True or False
        """
        if self.status != MultipartUpload.UploadStatus.UPLOADING.value:
            self.status = MultipartUpload.UploadStatus.UPLOADING.value
            try:
                self.save(update_fields=['status'])
            except Exception as e:
                return False

        return True

    def get_range_parts(self, part_number_marker: int, max_parts: int):
        """
        前提条件：part列表编号从1开始，并且连续

        编号大于part_number_marker开始往后最多max_parts个part
        :return: (
                list,
                bool
            )
        """
        parts = self.get_parts()
        if part_number_marker < len(parts):
            marker_parts = parts[part_number_marker:]
        else:
            marker_parts = []

        if max_parts < len(marker_parts):
            ret_parts = marker_parts[0:max_parts]
            is_truncated = True
        else:
            ret_parts = marker_parts
            is_truncated = False

        # 深拷贝，防止外部修改返回的列表时修改多部分上传对象
        return copy.deepcopy(ret_parts), is_truncated

    def get_part_offset(self, part_number: int):
        """
        part在对象中的偏移量
        """
        if part_number == 1:
            return 0

        # if self.chunk_size > 0:
        #     chunk_size = self.chunk_size
        # else:
        #     part = self.get_part_by_index(index=0)
        #     if isinstance(part, dict) and part['PartNumber'] == 1:
        #         chunk_size = part['Size']
        #     else:
        #         raise exceptions.S3Error(message='必须先上传第一个part。')
        if not self.chunk_size:
            raise exceptions.S3Error(message='上传过程记录块大小失败。')

        return (part_number - 1) * self.chunk_size

    def set_upload_first_part_and_chunk_size(self, part_number: int, chunk_size: int):
        """
        第一块上传设置块参数 chunk_size
        标记第一块上传，用以合并是检查
        """
        self.part_json['UploadFirstPart'] = part_number
        self.chunk_size = chunk_size
        try:
            self.save(update_fields=['chunk_size', 'part_json'])
        except Exception as e:
            raise exceptions.S3Error(message=f'更新多部分上传第一块上传记录错误, {str(e)}')

    def get_upload_first_part_info(self):
        """
        获取上传第一块的信息，用于合并使的判断（文件的最后一块是不是第一个上传）
        """
        parts_length = self.get_parts_length()
        last_part = self.get_part_by_index(parts_length - 1)  # 获取最后一块的信息

        if last_part['Size'] != self.chunk_size:
            return None
        return last_part

