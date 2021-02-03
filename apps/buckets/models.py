import uuid
import binascii
import os
import hashlib
from datetime import timedelta, datetime

from django.db import models
from django.db.models import F
from django.contrib.auth import get_user_model
from django.utils import timezone
from django.utils.translation import gettext_lazy, gettext as _
from ckeditor.fields import RichTextField

from utils.storagers import PathParser
from utils.md5 import EMPTY_HEX_MD5

# debug_logger = logging.getLogger('debug')#这里的日志记录器要和setting中的loggers选项对应，不能随意给参


# 获取用户模型
User = get_user_model()


def get_uuid1_hex_string():
    return uuid.uuid1().hex


def rand_hex_string(len=10):
    return binascii.hexlify(os.urandom(len//2)).decode()


def get_str_hexMD5(s:str):
    '''
    求字符串MD5
    '''
    return hashlib.md5(s.encode(encoding='utf-8')).hexdigest()


class BucketBase(models.Model):
    """
    存储桶bucket类，bucket名称必须唯一（不包括软删除记录）
    """
    PUBLIC = 1
    PRIVATE = 2
    PUBLIC_READWRITE = 3
    ACCESS_PERMISSION_CHOICES = (
        (PUBLIC, gettext_lazy('公有')),
        (PRIVATE, gettext_lazy('私有')),
        (PUBLIC_READWRITE, gettext_lazy('公有（可读写）')),
    )

    TYPE_COMMON = 0
    TYPE_S3 = 1
    TYPE_CHOICES = (
        (TYPE_COMMON, '普通'),
        (TYPE_S3, 'S3')
    )

    access_permission = models.SmallIntegerField(choices=ACCESS_PERMISSION_CHOICES, default=PRIVATE,
                                                 verbose_name='访问权限')
    user = models.ForeignKey(to=User, null=True, on_delete=models.SET_NULL, verbose_name='所属用户')
    objs_count = models.IntegerField(verbose_name='对象数量', default=0)    # 桶内对象的数量
    size = models.BigIntegerField(verbose_name='桶大小', default=0)    # 桶内对象的总大小
    stats_time = models.DateTimeField(verbose_name='统计时间', default=timezone.now)
    ftp_enable = models.BooleanField(verbose_name='FTP可用状态', default=False)  # 桶是否开启FTP访问功能
    ftp_password = models.CharField(verbose_name='FTP访问密码', max_length=20, blank=True)
    ftp_ro_password = models.CharField(verbose_name='FTP只读访问密码', max_length=20, blank=True)
    pool_name = models.CharField(verbose_name='PoolName', max_length=32, default='obs')
    type = models.SmallIntegerField(choices=TYPE_CHOICES, default=TYPE_COMMON, verbose_name='桶类型')

    class Meta:
        abstract = True


class Bucket(BucketBase):
    """
    存储桶bucket类，bucket名称必须唯一（不包括软删除记录）
    """
    LOCK_READWRITE = 0
    LOCK_READONLY = 1
    LOCK_NO_READWRITE = 2
    CHOICES_LOCK = (
        (LOCK_READWRITE, _("正常读写")),
        (LOCK_READONLY, _("锁定写")),
        (LOCK_NO_READWRITE, _("锁定读写")),
    )

    name = models.CharField(max_length=63, db_index=True, unique=True, verbose_name='bucket名称')
    created_time = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    collection_name = models.CharField(max_length=50, default='', blank=True, verbose_name='存储桶对应的表名')
    modified_time = models.DateTimeField(auto_now=True, verbose_name='修改时间')
    remarks = models.CharField(verbose_name='备注', max_length=255, default='')
    lock = models.SmallIntegerField(verbose_name=_('读写锁'), default=LOCK_READWRITE, choices=CHOICES_LOCK,
                                    help_text=_('锁定存储桶，控制存储桶的读写。'))

    class Meta:
        ordering = ['-id']
        verbose_name = '存储桶'
        verbose_name_plural = verbose_name

    def __str__(self):
        return self.name if isinstance(self.name, str) else str(self.name)

    def __repr__(self):
        return f'<Bucket>{self.name}'

    def get_pool_name(self):
        return self.pool_name

    @classmethod
    def get_user_valid_bucket_count(cls, user):
        '''获取用户有效的存储桶数量'''
        return cls.objects.filter(user=user).count()

    @classmethod
    def get_bucket_by_name(cls, bucket_name):
        '''
        获取存储通对象
        :param bucket_name: 存储通名称
        :return: Bucket对象; None(不存在)
        '''
        return Bucket.objects.select_related('user').filter(name=bucket_name).first()

    def save(self, *args, **kwargs):
        if not self.ftp_password or len(self.ftp_password) < 6:
            self.ftp_password = rand_hex_string()
        if not self.ftp_ro_password or len(self.ftp_ro_password) < 6:
            self.ftp_ro_password = rand_hex_string()
        super().save(**kwargs)

    def delete_and_archive(self):
        """
        删除bucket,并归档

        :return:
            True    # success
            False   # failed
        """
        try:
            a = Archive()
            a.original_id = self.id
            a.name = self.name
            a.user_id = self.user_id
            a.created_time = self.created_time
            a.table_name = self.get_bucket_table_name()
            a.access_permission = self.access_permission
            a.modified_time = self.modified_time
            a.objs_count = self.objs_count
            a.size = self.size
            a.stats_time = self.stats_time
            a.ftp_enable = self.ftp_enable
            a.ftp_password = self.ftp_password
            a.ftp_ro_password = self.ftp_ro_password
            a.pool_name = self.pool_name
            a.type = self.type
            a.save()
        except Exception as e:
            return False

        try:
            self.delete()
        except Exception:
            a.delete()
            return False

        return True

    def check_user_own_bucket(self, user):
        # bucket是否属于当前用户
        if not user or not user.id:
            return False

        return user.id == self.user_id

    def get_bucket_table_name(self):
        '''
        获得bucket对应的数据库表名
        :return: 表名
        '''
        if not self.collection_name:
            name = f'bucket_{self.id}'
            self.collection_name = name
            self.save()

        return self.collection_name

    def set_permission(self, public:int=2):
        '''
        设置存储桶公有或私有访问权限

        :param public:
        :return: True(success); False(error)
        '''
        if public not in [self.PUBLIC, self.PRIVATE, self.PUBLIC_READWRITE]:
            return False

        if self.access_permission == public:
            return True

        self.access_permission = public
        try:
            self.save(update_fields=['access_permission'])
        except:
            return False

        return True

    def is_public_permission(self):
        '''
        存储桶是否是公共读访问权限

        :return: True(是公共); False(私有权限)
        '''
        if self.access_permission in [self.PUBLIC, self.PUBLIC_READWRITE]:
            return True
        return False

    def has_public_write_perms(self):
        '''
        存储桶是否是公共读写访问权限

        :return: True(公共可读可写); False(不可写)
        '''
        if self.access_permission == self.PUBLIC_READWRITE:
            return True
        return False

    def obj_count_increase(self, save=True):
        '''
        存储桶对象数量加1

        :param save: 是否更新到数据库
        :return: True(success); False(failure)
        '''
        self.obj_count += 1
        if save:
            try:
                self.save()
            except:
                return False

        return True

    def obj_count_decrease(self, save=True):
        '''
        存储桶对象数量减1

        :param save: 是否更新到数据库
        :return: True(success); False(failure)
        '''
        self.obj_count = max(self.obj_count - 1, 0)
        if not save:
            try:
                self.save()
            except:
                return False

        return True

    def __update_stats(self):
        from .utils import get_bfmanager

        table_name = self.get_bucket_table_name()
        bfm = get_bfmanager(table_name=table_name)
        data = bfm.get_bucket_space_and_count()
        count = data.get('count')
        space = data.get('space')
        if space is None:
            space = 0

        now_time = timezone.now()
        self.objs_count = count
        self.size = space
        self.stats_time = now_time
        try:
            self.save(update_fields=['objs_count', 'size', 'stats_time'])
        except Exception as e:
            pass

    def get_stats(self, now=False):
        '''
        获取存储桶统计数据

        :param now:  重新统计
        :return: dict
            {
                'stats': {
                    'space': xxx, 'count': xxx
                },
                'stats_time': xxxx-xx-xx xx:xx:xx
            }
        '''
        # 强制重新统计，或者旧的统计结果时间超过了50分钟， 满足其一条件重新统计
        now_t = timezone.now() - timedelta(minutes=50)
        ts_now = now_t.timestamp()
        stats_t = self.stats_time
        if stats_t:
            ts_stats = stats_t.timestamp()
        else:
            ts_stats = 0
        if now or (ts_now > ts_stats):
            self.__update_stats()

        stats = {'space': self.size, 'count': self.objs_count}
        time_str = self.stats_time.astimezone(timezone.get_current_timezone()).isoformat()
        return {'stats': stats, 'stats_time': time_str}

    def is_ftp_enable(self):
        '''是否开启了ftp'''
        return self.ftp_enable

    def check_ftp_password(self, password):
        '''检查ftp密码是否一致'''
        if password and (self.ftp_password == password):
            return True

        return False

    def check_ftp_ro_password(self, password):
        '''检查ftp只读密码是否一致'''
        if password and (self.ftp_ro_password == password):
            return True

        return False

    def set_ftp_password(self, password):
        '''
        设置ftp可读写密码，更改不会自动提交到数据库

        :param password: 要设置的密码
        :return:
            (True, str)    # 设置成功
            (False, str)   # 设置失败
        '''
        if not (6 <= len(password) <= 20):
            return False, _('密码长度必须为6-20个字符')
        if self.ftp_ro_password == password:
            return False, _('可读写密码不得和只读密码一致')
        self.ftp_password = password
        return True, _('修改成功')

    def set_ftp_ro_password(self, password):
        '''
        设置ftp只读密码，更改不会自动提交到数据库

        :param password: 要设置的密码
        :return:
            (True, str)    # 设置成功
            (False, str)   # 设置失败
        '''
        if not (6 <= len(password) <= 20):
            return False, _('密码长度必须为6-20个字符')
        if self.ftp_password == password:
            return False, _('只读密码不得和可读写密码一致')
        self.ftp_ro_password = password
        return True, _('修改成功')

    def set_remarks(self, remarks: str):
        """
        修改备注信息

        :param remarks: 备注信息
        :return:
            True    # 设置成功
            False   # 设置失败
        """
        self.remarks = remarks
        try:
            self.save(update_fields=['remarks', 'modified_time'])
        except Exception as e:
            return False

        return True

    def lock_readable(self):
        """桶是否允许读操作"""
        if self.lock in [self.LOCK_READWRITE, self.LOCK_READONLY]:
            return True

        return False

    def lock_writeable(self):
        """桶是否允许写操作"""
        if self.lock == self.LOCK_READWRITE:
            return True

        return False


class Archive(BucketBase):
    '''
    存储桶bucket删除归档类
    '''
    id = models.BigAutoField(primary_key=True)
    original_id = models.BigIntegerField(verbose_name='bucket id')
    archive_time = models.DateTimeField(auto_now_add=True, verbose_name='删除归档时间')
    name = models.CharField(max_length=63, db_index=True, verbose_name='bucket名称')
    created_time = models.DateTimeField(verbose_name='创建时间')
    table_name = models.CharField(max_length=50, default='', blank=True, verbose_name='存储桶对应的表名')
    modified_time = models.DateTimeField(verbose_name='修改时间')

    class Meta:
        ordering = ['-id']
        verbose_name = '存储桶归档'
        verbose_name_plural = verbose_name

    def __str__(self):
        return self.name if isinstance(self.name, str) else str(self.name)

    def get_bucket_table_name(self):
        '''
        获得bucket对应的数据库表名
        :return: 表名
        '''
        if not self.table_name:
            name = f'bucket_{self.original_id}'
            self.table_name = name
            self.save(update_fields=['table_name'])

        return self.table_name

    def get_pool_name(self):
        return self.pool_name


class BucketLimitConfig(models.Model):
    '''
    用户可拥有存储桶数量限制配置模型
    '''
    limit = models.IntegerField(verbose_name='可拥有存储桶上限', default=2)
    user = models.OneToOneField(to=User, related_name='bucketlimit', on_delete=models.CASCADE, verbose_name='用户')

    class Meta:
        verbose_name = '桶上限配置'
        verbose_name_plural = verbose_name

    def __str__(self):
        return str(self.limit)

    def __repr__(self):
        return f'limit<={self.limit}'

    @classmethod
    def get_user_bucket_limit(cls, user:User):
        obj, created = cls.objects.get_or_create(user=user)
        return obj.limit


class ApiUsageDescription(models.Model):
    '''
    iHarbor 使用说明
    '''
    DESC_API = 0
    DESC_FTP = 1
    DESC_S3_API = 2
    DESC_FOR_CHOICES = (
        (DESC_API, '原生API说明'),
        (DESC_FTP, 'FTP说明'),
        (DESC_S3_API, 'S3兼容API说明')
    )

    title = models.CharField(verbose_name='标题', default='使用说明', max_length=255)
    desc_for = models.SmallIntegerField(verbose_name='关于什么的说明', choices=DESC_FOR_CHOICES, default=DESC_API)
    content = RichTextField(verbose_name='说明内容', default='')
    created_time = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    modified_time = models.DateTimeField(auto_now=True, verbose_name='修改时间')

    class Meta:
        verbose_name = '使用说明'
        verbose_name_plural = verbose_name

    def __str__(self):
        return f'<UsageDescription>{self.title}'

    def __repr__(self):
        return f'<UsageDescription>{self.title}'


SHARE_ACCESS_NO = 0
SHARE_ACCESS_READONLY = 1
SHARE_ACCESS_READWRITE = 2


class BucketFileBase(models.Model):
    '''
    存储桶bucket文件信息模型基类

    @ na : name，若该doc代表文件，则na为全路径文件名，若该doc代表目录，则na为目录路径;
    @ fos: file_or_dir，用于判断该doc代表的是一个文件还是一个目录，若fod为True，则是文件，若fod为False，则是目录;
    @ did: 所在目录的objectID，若该doc代表文件，则did为该文件所属目录的id，若该doc代表目录，则did为该目录的上一级
                目录(父目录)的id;
    @ si : size,文件大小,字节数，若该doc代表文件，则si为该文件的大小，若该doc代表目录，则si为空；
    @ ult: upload_time，若该doc代表文件，则ult为该文件的上传时间，若该doc代表目录，则ult为该目录的创建时间
    @ upt: update_time，若该doc代表文件，则upt为该文件的最近修改时间，若该doc代表目录，则upt为空;
    @ shp: share_password，若该doc代表文件，且允许共享，则shp为该文件的共享密码，若该doc代表目录，则shp为空;
    @ stl: share_time_limit，若该doc代表文件，且允许共享，则stl用于判断该文件是否有共享时间限制，若stl为True，则文件有
                共享时间限制，若stl为False，则文件无共享时间限制，且sst，set等字段为空；若该doc代表目录，则stl为空;
    @ sst: share_start_time，允许共享且有时间限制，则sst为该文件的共享起始时间，若该doc代表目录，则sst为空;
    @ set: share_end_time，  允许共享且有时间限制，则set为该文件的共享终止时间，若该doc代表目录，则set为空;
    @ sds: soft delete status,软删除,True->删除状态，get_sds_display()可获取可读值
    '''
    SOFT_DELETE_STATUS_CHOICES = (
        (True, '删除'),
        (False, '正常'),
    )

    SHARE_ACCESS_NO = SHARE_ACCESS_NO
    SHARE_ACCESS_READONLY = SHARE_ACCESS_READONLY
    SHARE_ACCESS_READWRITE = SHARE_ACCESS_READWRITE
    SHARE_ACCESS_CHOICES = (
        (SHARE_ACCESS_NO, '禁止访问'),
        (SHARE_ACCESS_READONLY, '只读'),
        (SHARE_ACCESS_READWRITE, '可读可写'),
    )

    id = models.BigAutoField(auto_created=True, primary_key=True)
    na = models.TextField(verbose_name='全路径文件名或目录名')
    na_md5 = models.CharField(max_length=32, null=True, default=None, verbose_name='全路径MD5值')
    name = models.CharField(verbose_name='文件名或目录名', max_length=255)
    fod = models.BooleanField(default=True, verbose_name='文件或目录') # file_or_dir; True==文件，False==目录
    did = models.BigIntegerField(default=0, verbose_name='父节点id')
    si = models.BigIntegerField(default=0, verbose_name='文件大小') # 字节数
    ult = models.DateTimeField(auto_now_add=True, default=timezone.now) # 文件的上传时间，或目录的创建时间
    upt = models.DateTimeField(blank=True, null=True, auto_now=True, verbose_name='修改时间') # 文件的最近修改时间，目录，则upt为空
    dlc = models.IntegerField(default=0, verbose_name='下载次数')  # 该文件的下载次数，目录时dlc为0
    shp = models.CharField(default='', max_length=10, verbose_name='共享密码') # 该文件的共享密码，目录时为空
    stl = models.BooleanField(default=True, verbose_name='是否有共享时间限制') # True: 文件有共享时间限制; False: 则文件无共享时间限制
    sst = models.DateTimeField(blank=True, null=True, verbose_name='共享起始时间') # share_start_time, 该文件的共享起始时间
    set = models.DateTimeField(blank=True, null=True, verbose_name='共享终止时间') # share_end_time,该文件的共享终止时间
    sds = models.BooleanField(default=False, choices=SOFT_DELETE_STATUS_CHOICES) # soft delete status,软删除,True->删除状态
    md5 = models.CharField(default='', max_length=32, verbose_name='md5')  # 该文件的md5码，32位十六进制字符串
    share = models.SmallIntegerField(verbose_name='分享访问权限', choices=SHARE_ACCESS_CHOICES, default=SHARE_ACCESS_NO)

    class Meta:
        abstract = True
        app_label = 'metadata'      # 用于db路由指定此模型对应的数据库
        ordering = ['fod', '-id']
        indexes = [models.Index(fields=('na_md5',), name='na_md5_idx')]
        unique_together = ('did', 'name')
        verbose_name = '对象模型抽象基类'
        verbose_name_plural = verbose_name

    def __str__(self):
        return self.na if isinstance(self.na, str) else str(self.na)

    @property
    def obj_size(self):
        return self.si

    @property
    def hex_md5(self):
        if self.obj_size == 0:
            return EMPTY_HEX_MD5

        return self.md5

    def do_soft_delete(self):
        '''
        软删除

        :return: True(success); False(error)
        '''
        self.sds = True
        self.upt = timezone.now() # 修改时间标记删除时间

        try:
            self.save()
        except:
            return False
        return True

    def set_shared(self, share=SHARE_ACCESS_NO, days=0, password:str=''):
        '''
        设置对象共享或私有权限

        :param share: 读写权限；0（禁止访问），1（只读），2（可读可写）
        :param days: 共享天数，0表示永久共享, <0表示不共享
        :param password: 共享密码
        :return: True(success); False(error)
        '''
        if share not in [self.SHARE_ACCESS_NO, self.SHARE_ACCESS_READONLY, self.SHARE_ACCESS_READWRITE]:
            return False

        if share != self.SHARE_ACCESS_NO:
            self.share = share
            self.set_share_password(password=password, commit=False) # 设置共享密码
            now = timezone.now()
            self.sst = now          # 共享时间
            if days == 0:
                self.stl = False    # 永久共享,没有共享时间限制
            elif days < 0:
                self.share = self.SHARE_ACCESS_NO     # 私有
            else:
                self.stl = True     # 有共享时间限制
                self.set = now + timedelta(days=days) # 共享终止时间
        else:
            self.share = self.SHARE_ACCESS_NO
            self.set_share_password(password='', commit=False)  # 设置共享密码       # 清除共享密码

        try:
            self.save(update_fields=['share', 'shp', 'stl', 'sst', 'set'])
        except:
            return False
        return True

    def is_shared_and_in_shared_time(self):
        '''
        对象是否是分享的, 并且在有效分享时间内，即是否可公共访问
        :return: True(是), False(否)
        '''
        # 是否可读
        if not self.is_read_perms():
            return False

        # 是否有分享时间限制
        if not self.has_shared_limit():
            return True

        # 检查是否已过共享终止时间
        if self.is_shared_end_time_out():
            return False

        return True

    def has_share_password(self):
        '''
        是否设置了共享密码
        :return:
            True    # 是, 有密码
            False   # 否, 无密码
        '''
        if self.shp:
            return True

        return False

    def check_share_password(self, password: str):
        '''
        检测共享密码是否一致

        :return:
            True    # 一致, 或未设置密码
            False   # 否
        '''
        shp = self.shp
        if shp and shp == password:
            return True

        return False

    def get_share_password(self):
        '''
        共享密码
        '''
        return self.shp

    def set_share_password(self, password: str, commit=True):
        '''
        设置新的共享密码

        :param commit: 是否立即更新到数据库；默认True，立即更新
        :return:
            True    # success
            False   # failed
        '''
        if not password:
            password = ''

        self.shp = password
        if not commit:
            return True

        try:
            self.save(update_fields=['shp'])
        except Exception as e:
            return False

        return True

    def can_shared_write(self):
        '''
        是否分享并可写

        :return:
            True(是), False(否)
        '''
        if self.is_shared_and_in_shared_time() and self.is_read_write_perms():
            return True
        return False

    def is_read_write_perms(self):
        '''
        是否可读可写权限

        :return:
            True(是), False(否)
        '''
        if self.share == self.SHARE_ACCESS_READWRITE:
            return True
        return False

    def is_read_perms(self):
        '''
        是否有读权限

        :return:
            True(有), False(没有)
        '''
        if self.share in [self.SHARE_ACCESS_READONLY, self.SHARE_ACCESS_READWRITE]:
            return True
        return False

    def has_shared_limit(self):
        '''
        是否有分享时间限制
        :return: True(有), False(无)
        '''
        return self.stl

    def is_shared_end_time_out(self):
        '''
        是否超过分享终止时间
        :return: True(已过共享终止时间)，False(未超时)
        '''
        ret = True
        if not isinstance(self.set, datetime):
            return ret

        try:
            td = timezone.now() - self.set
            ret = td.total_seconds() > 0
        except:
            pass

        return ret

    def download_cound_increase(self):
        '''
        下载次数加1

        :return: True(success); False(error)
        '''
        self.dlc = F('dlc') + 1 # (self.dlc or 0) + 1  # 下载次数+1
        try:
            self.save(update_fields=['dlc'])
        except:
            return False
        return True

    def is_file(self):
        return self.fod

    def is_dir(self):
        return not self.is_file()

    def do_delete(self):
        '''
        删除
        :return: True(删除成功); False(删除失败)
        '''
        try:
            self.delete()
        except Exception:
            return False

        return True

    def get_obj_key(self, bucket_id):
        '''
        获取此文档在ceph中对应的对象id

        :param bucket_id:
        :return: type:str; 无效的参数返回None
        '''
        if self.id is None:
            raise ValueError('get_obj_key cannot be called before the model object is saved or after it is deleted')

        if isinstance(bucket_id, str) or isinstance(bucket_id, int):
            return f'{str(bucket_id)}_{str(self.id)}'
        return None

    def reset_na_md5(self):
        '''
        na更改时，计算并重设新的na_md5
        :return: None

        :备注：不会自动更新的数据库
        '''
        na = self.na if self.na else ''
        self.na_md5 = get_str_hexMD5(na)

    def save(self, force_insert=False, force_update=False, using=None, update_fields=None):
        if not self.na_md5:
            self.reset_na_md5()
        super().save(force_insert=force_insert, force_update=force_update, using=using, update_fields=update_fields)

    def do_save(self, **kwargs):
        '''
        创建一个文档或更新一个已存在的文档

        :return: True(成功); False(失败)
        '''
        try:
            self.save(**kwargs)
        except Exception as e:
            return False

        return True

    def get_parent_path(self):
        '''获取父路经字符串'''
        path, _ = PathParser(filepath=self.na).get_path_and_filename()
        return path

    def get_access_permission_code(self, bucket):
        """
        判断并获取对象或目录的访问权限码

        :return: int
        """
        # 桶公有权限，对象都为公有权限
        if bucket:
            if bucket.has_public_write_perms():
                return self.SHARE_ACCESS_READWRITE
            elif bucket.is_public_permission():
                return self.SHARE_ACCESS_READONLY

        try:
            if self.is_shared_and_in_shared_time():
                if self.is_dir() and self.is_read_write_perms():
                    return self.SHARE_ACCESS_READWRITE

                return self.SHARE_ACCESS_READONLY
        except Exception as e:
            pass

        return self.SHARE_ACCESS_NO


class BucketToken(models.Model):
    PERMISSION_READWRITE = 'readwrite'
    PERMISSION_READONLY = 'readonly'
    CHOICES_PERMISSION = (
        (PERMISSION_READWRITE, _('可读写')),
        (PERMISSION_READONLY, _('只读'))
    )

    key = models.CharField(verbose_name=_("Key"), max_length=40, primary_key=True)
    bucket = models.ForeignKey(to=Bucket, related_name='token_set', on_delete=models.CASCADE, verbose_name=_("存储桶"))
    permission = models.CharField(verbose_name=_('访问权限'), max_length=20, choices=CHOICES_PERMISSION, default=PERMISSION_READWRITE)
    created = models.DateTimeField(_("创建时间"), auto_now_add=True)

    class Meta:
        db_table = 'bucket_token'
        verbose_name = _("存储桶Token")
        verbose_name_plural = _("存储桶Token")

    def save(self, *args, **kwargs):
        if not self.key:
            self.key = self.generate_key()
        return super().save(*args, **kwargs)

    @staticmethod
    def generate_key():
        return binascii.hexlify(os.urandom(20)).decode()

    def __str__(self):
        return self.key
