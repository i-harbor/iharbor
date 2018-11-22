import uuid
from datetime import datetime

from django.db import models
from django.contrib.auth import get_user_model
from mongoengine import DynamicDocument
from mongoengine import fields


#获取用户模型
User = get_user_model()

# Create your models here.

def get_uuid1_hex_string():
    return uuid.uuid1().hex

class Bucket(models.Model):
    '''
    存储桶bucket类，bucket名称必须唯一（不包括软删除记录）
    '''
    PUBLIC = 1
    PRIVATE = 2
    ACCESS_PERMISSION_CHOICES = (
        (PUBLIC, '公有'),
        (PRIVATE, '私有'),
    )
    SOFT_DELETE_CHOICES = (
        (True, '删除'),
        (False, '正常'),
    )

    name = models.CharField(max_length=63, db_index=True, verbose_name='bucket名称')
    user = models.ForeignKey(to=User, on_delete=models.CASCADE, verbose_name='所属用户')
    created_time = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    collection_name = models.CharField(max_length=50, default=get_uuid1_hex_string, editable=False, verbose_name='存储桶对应的集合表名')
    access_permission = models.SmallIntegerField(choices=ACCESS_PERMISSION_CHOICES, default=PRIVATE, verbose_name='访问权限')
    soft_delete = models.BooleanField(choices=SOFT_DELETE_CHOICES, default=False, verbose_name='软删除') #True->删除状态

    class Meta:
        verbose_name = '存储桶'
        verbose_name_plural = verbose_name

    @classmethod
    def get_bucket_by_name(self, bucket_name):
        '''
        获取存储通对象
        :param bucket_name: 存储通名称
        :return: Bucket对象; None(不存在)
        '''
        query_set = Bucket.objects.filter(models.Q(name=bucket_name) & models.Q(soft_delete=False))
        if query_set.exists():
            return query_set.first()

        return None

    def do_soft_delete(self):
        self.soft_delete = True
        self.save()

    def check_user_own_bucket(self, request):
        # bucket是否属于当前用户
        return request.user.id == self.user.id

    def get_bucket_mongo_collection_name(self):
        '''
        获得bucket对应的mongodb集合名
        :return: 集合名
        '''
        return f'bucket_{self.collection_name}'


# def get_bucket_by_name(self, bucket_name):
#     '''
#     获取存储通对象
#     :param bucket_name: 存储通名称
#     :return: Bucket对象; None(不存在)
#     '''
#     query_set = Bucket.objects.filter(models.Q(name=bucket_name) & models.Q(soft_delete=False))
#     if query_set.exists():
#         return query_set.first()
#
#     return None


class BucketFileInfo(DynamicDocument):
    '''
    存储桶bucket文件信息模型

    @ na : name，若该doc代表文件，则na为文件名，若该doc代表目录，则na为目录路径;
    @ fos: file_or_dir，用于判断该doc代表的是一个文件还是一个目录，若fod为True，则是文件，若fod为False，则是目录;
    @ did: 所在目录的objectID，若该doc代表文件，则did为该文件所属目录的id，若该doc代表目录，则did为该目录的上一级
                目录(父目录)的id;
    @ si : size,文件大小,字节数，若该doc代表文件，则si为该文件的大小，若该doc代表目录，则si为空；
    @ ult: upload_time，若该doc代表文件，则ult为该文件的上传时间，若该doc代表目录，则ult为该目录的创建时间
    @ upt: update_time，若该doc代表文件，则upt为该文件的最近修改时间，若该doc代表目录，则upt为空;
    @ sh : shared，若该doc代表文件，则sh用于判断文件是否允许共享，若sh为True，则文件可共享，若sh为False，则文件不能共享，
                且shp，stl，sst，set等字段为空；若该doc代表目录，则sh为空；
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

    na = fields.StringField(required=True) # name,文件名或目录名
    fod = fields.BooleanField(required=True) # file_or_dir; True==文件，False==目录
    did = fields.ObjectIdField() #父节点objectID
    si = fields.LongField() # 文件大小,字节数
    ult = fields.DateTimeField(default=datetime.utcnow) # 文件的上传时间，或目录的创建时间
    upt = fields.DateTimeField() # 文件的最近修改时间，目录，则upt为空
    dlc = fields.IntField() # 该文件的下载次数，目录时dlc为空
    bac = fields.ListField(fields.StringField()) # backup，该文件的备份地址，目录时为空
    arc = fields.ListField(fields.StringField()) # archive，该文件的归档地址，目录时arc为空
    sh = fields.BooleanField() # shared，若sh为True，则文件可共享，若sh为False，则文件不能共享
    shp = fields.StringField() # 该文件的共享密码，目录时为空
    stl = fields.BooleanField() # True: 文件有共享时间限制; False: 则文件无共享时间限制
    sst = fields.DateTimeField() # share_start_time, 该文件的共享起始时间
    set = fields.DateTimeField() # share_end_time,该文件的共享终止时间
    sds = fields.BooleanField(default=False, choices=SOFT_DELETE_STATUS_CHOICES) # soft delete status,软删除,True->删除状态

    meta = {
        #db_alias用于指定当前模型默认绑定的mongodb连接，但可以用switch_db(Model, 'db2')临时改变对应的数据库连接
        'db_alias': 'default',
        'indexes': ['did', 'ult'],#索引
        'ordering': ['fod', '-ult'], #文档降序，最近日期靠前
        # 'collection':'uploadfileinfo',#集合名字，默认为小写字母的类名
        # 'max_documents': 10000, #集合存储文档最大数量
        # 'max_size': 2000000, #集合的最大字节数
    }

    def do_soft_delete(self):
        '''软删除'''
        self.sds = True
        self.save()

