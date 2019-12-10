import logging
import traceback

from django.db.backends.mysql.schema import DatabaseSchemaEditor
from django.db import connections, router
from django.db.models import Sum, Count
from django.db.models.query import Q
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
from django.apps import apps

from .models import BucketFileBase, get_str_hexMD5


logger = logging.getLogger('django.request')#这里的日志记录器要和setting中的loggers选项对应，不能随意给参


def create_table_for_model_class(model):
    '''
    创建Model类对应的数据库表

    :param model: Model类
    :return:
            True: success
            False: failure
    '''
    try:
        using = router.db_for_write(model)
        with DatabaseSchemaEditor(connection=connections[using]) as schema_editor:
            schema_editor.create_model(model)
    except Exception as e:
        msg = traceback.format_exc()
        logger.error(msg)
        return False

    return True

def delete_table_for_model_class(model):
    '''
    删除Model类对应的数据库表

    :param model: Model类
    :return:
            True: success
            False: failure
    '''
    try:
        using = router.db_for_write(model)
        with DatabaseSchemaEditor(connection=connections[using]) as schema_editor:
            schema_editor.delete_model(model)
    except Exception as e:
        msg = traceback.format_exc()
        logger.error(msg)
        return False

    return True

def is_model_table_exists(model):
    '''
    检查模型类Model的数据库表是否已存在
    :param model:
    :return: True(existing); False(not existing)
    '''
    using = router.db_for_write(model)
    connection = connections[using]
    return model.Meta.db_table in connection.introspection.table_names()

def get_obj_model_class(table_name):
    '''
    动态创建存储桶对应的对象模型类

    RuntimeWarning: Model 'xxxxx_' was already registered. Reloading models is not advised as it can
    lead to inconsistencies most notably with related models.
    如上述警告所述, Django 不建议重复创建Model 的定义.可以直接通过get_obj_model_class创建，无视警告.
    这里先通过 get_registered_model 获取已经注册的 Model, 如果获取不到， 再生成新的模型类.

    :param table_name: 数据库表名，模型类对应的数据库表名
    :return: Model class
    '''
    model_name = 'ObjModel' + table_name
    app_leble = BucketFileBase.Meta.app_label
    try:
        cls = apps.get_registered_model(app_label=app_leble, model_name=model_name)
        return cls
    except LookupError:
        pass

    meta = BucketFileBase.Meta
    meta.abstract = False
    meta.db_table = table_name  # 数据库表名
    return type(model_name, (BucketFileBase,), {'Meta': meta, '__module__': BucketFileBase.__module__})


def get_bfmanager(path='', table_name=''):
    return BucketFileManagement(path=path, collection_name=table_name)


class BucketFileManagement():
    '''
    存储桶相关的操作方法类
    '''
    ROOT_DIR_ID = 0 # 根目录ID

    def __init__(self, path='', collection_name='', *args, **kwargs):
        self._path = self._hand_path(path)
        self._collection_name = collection_name # bucket's database table name
        self.cur_dir_id = None
        self._bucket_file_class = self.creat_obj_model_class()

    def creat_obj_model_class(self):
        '''
        动态创建各存储桶数据库表对应的模型类
        '''
        db_table = self.get_collection_name() # 数据库表名
        return get_obj_model_class(db_table)

    def get_obj_model_class(self):
        if not self._bucket_file_class:
            self._bucket_file_class = self.creat_obj_model_class()

        return self._bucket_file_class

    def get_collection_name(self):
        return self._collection_name

    def _hand_path(self, path):
        '''去除path字符串两边可能的空白和右边/'''
        if isinstance(path, str):
            path.strip(' ')
            return path.rstrip('/')
        return ''

    def get_cur_dir_id(self, dir_path=None):
        '''
        获得当前目录节点id
        @ return: (ok, id)，ok指示是否有错误(路径参数错误)
            正常返回：(True, id)，顶级目录时id=ROOT_DIR_ID
            未找到记录返回(False, None)，即参数有误
        '''
        if self.cur_dir_id:
            return (True, self.cur_dir_id)

        path = dir_path if dir_path else self._path
        # path为空，根目录为存储桶
        if path == '' or path == '/':
            return (True, self.ROOT_DIR_ID)

        path = self._hand_path(path)
        if not path:
            return (False, None) # path参数有误

        na_md5 = get_str_hexMD5(path)
        model_class = self.get_obj_model_class()
        try:
            dir = model_class.objects.get(Q(na_md5=na_md5) | Q(na_md5__isnull=True), Q(fod=False) & Q(na=path))  # 查找目录记录
        except model_class.DoesNotExist as e:
            return (False, None)  # path参数有误,未找到对应目录信息
        except MultipleObjectsReturned as e:
            logger.error(f'数据库表{self.get_collection_name()}中存在多个相同的目录：{path}')
            return (False, None)  # path参数有误,未找到对应目录信息
        if dir:
            self.cur_dir_id = dir.id
        return (True, self.cur_dir_id)


    def get_cur_dir_files(self, cur_dir_id=None):
        '''
        获得当前目录下的文件或文件夹记录

        :param cur_dir_id: 目录id;
        :return: 目录id下的文件或目录记录list; id==None时，返回存储桶下的文件或目录记录list
        '''
        dir_id = None
        if cur_dir_id is not None:
            dir_id = cur_dir_id

        if dir_id is None and self._path:
            ok, dir_id = self.get_cur_dir_id()

            # path路径有误
            if not ok:
                return False, None

        model_class = self.get_obj_model_class()
        try:
            if dir_id:
                files = model_class.objects.filter(did=dir_id).all()
            else:
                #存储桶下文件目录,did=0表示是存储桶下的文件目录
                files = model_class.objects.filter(did=self.ROOT_DIR_ID).all()
        except Exception as e:
            logger.error('In get_cur_dir_files:' + str(e))
            return False, None

        return True, files

    def get_file_exists(self, file_name):
        '''
        通过文件名获取当前目录下的文件信息
        :param file_name: 文件名
        :return: 如果存在返回文件记录对象，否则None
        '''
        file_name = file_name.strip('/')
        ok, did = self.get_cur_dir_id()
        if not ok:
            return None

        bfis = self.get_obj_model_class().objects.filter(Q(did=did)  & Q(name=file_name)& Q(fod=True))
        bfi = bfis.first()

        return bfi

    def get_dir_exists(self, dir_name):
        '''
        通过目录名获取当前目录下的目录信息
        :param dir_name: 目录名称（不含父路径）
        :return:
            第一个返回值：表示是否有错误发生，(可能错误：当前目录参数有误，对应目录不存在)
            第二个返回值：如果存在返回文件记录对象，否则None
        '''
        # 先检测当前目录存在
        ok, did = self.get_cur_dir_id()
        if not ok:
            return False, None

        model_class = self.get_obj_model_class()
        dir = model_class.objects.filter(Q(did=did) & Q(name=dir_name) & Q(fod=False)).first()  # 查找目录记录

        return True, dir

    def get_dir_or_obj_exists(self, name, cur_dir_id=None):
        '''
        通过名称获取当前路径下的子目录或对象
        :param name: 目录名或对象名称
        :param cur_dir_id: 如果给定ID,基于此ID的目录查找；默认基于当前路径查找,
        :return:
            第一个返回值：表示是否有错误发生，(可能错误：当前目录参数有误，对应目录不存在)
            第二个返回值：如果存在返回文件记录对象，否则None
        '''
        if cur_dir_id is None:
            ok, did = self.get_cur_dir_id()
            if not ok:
                return False, None
        else:
            did = cur_dir_id

        model_class = self.get_obj_model_class()
        dir_or_obj = model_class.objects.filter(Q(did=did) & Q(name=name)).first()  # 查找目录或对象记录

        return True, dir_or_obj

    def build_dir_full_name(self, dir_name):
        '''
        拼接全路径

        :param dir_name: 目录名
        :return: 目录绝对路径
        '''
        dir_name.strip('/')
        path = self._hand_path(self._path)
        return (path + '/' + dir_name) if path else dir_name

    def get_file_obj_by_id(self, id):
        '''
        通过id获取文件对象
        :return:
        '''
        model_class = self.get_obj_model_class()
        try:
            bfis = model_class.objects.get(id=id)
        except model_class.DoesNotExist:
            return None

        return bfis.first()

    def get_count(self):
        '''
        获取存储桶数据库表的对象和目录记录总数量
        :return:
        '''
        return self.get_obj_model_class().objects.count()

    def get_obj_count(self):
        '''
        获取存储桶中的对象总数量
        :return:
        '''
        return self.get_obj_model_class().objects.filter(fod=True).count()

    def get_valid_obj_count(self):
        '''
        获取存储桶中的有效（未删除状态）对象数量
        :return:
        '''
        return self.get_obj_model_class().objects.filter(Q(fod=True) & Q(sds=False)).count()

    def cur_dir_is_empty(self):
        '''
        当前目录是否为空目录
        :return:True(空); False(非空); None(有错误或目录不存在)
        '''
        ok, did = self.get_cur_dir_id()
        # 有错误发生
        if not ok:
            return None

        # 未找到目录
        if did is None:
            return None

        if self.get_obj_model_class().objects.filter(did=did).exists():
            return False

        return True

    def dir_is_empty(self, dir_obj):
        '''
        给定目录是否为空目录

        :params dir_obj: 目录对象
        :return:True(空); False(非空)
        '''
        did = dir_obj.id

        if self.get_obj_model_class().objects.filter(did=did).exists():
            return False

        return True

    def get_bucket_space_and_count(self):
        '''
        获取存储桶中的对象占用总空间大小和对象数量
        :return:
            {'space': 123, 'count: 456}
        '''
        data = self.get_obj_model_class().objects.filter(fod=True).aggregate(space=Sum('si'), count=Count('fod'))
        return data




