import logging
import traceback
import random

from django.db.backends.mysql.schema import DatabaseSchemaEditor
from django.db import connections, router
from django.db.models import Sum, Count
# from django.db.models.functions import Lower
from django.db.models.query import Q
from django.core.exceptions import MultipleObjectsReturned
from django.apps import apps
from django.conf import settings

from .models import BucketFileBase, get_str_hexMD5, Bucket, get_next_bucket_max_id
from api import exceptions


logger = logging.getLogger('django.request')    # 这里的日志记录器要和setting中的loggers选项对应，不能随意给参
debug_logger = logging.getLogger('debug')       # 这里的日志记录器要和setting中的loggers选项对应，不能随意给参


def get_ceph_alias_rand():
    """
    从配置的CEPH集群中随机获取一个ceph集群的配置的别名
    :return:
        str

    :raises: ValueError
    """
    cephs = settings.CEPH_RADOS
    aliases = []
    for k in cephs.keys():
        ceph = cephs[k]
        if ('DISABLE_CHOICE' in ceph) and (ceph['DISABLE_CHOICE'] is True):
            continue

        aliases.append(k)

    if not aliases:
        raise ValueError('配置文件CEPH_RADOS中没有可供选择的CEPH集群配置')

    return random.choice(aliases)


def get_ceph_poolnames(using: str):
    """
    从配置中获取CEPH pool name元组
    :return:
        tuple

    :raises: ValueError
    """
    pools = settings.CEPH_RADOS[using].get('POOL_NAME', None)
    if not pools:
        raise ValueError(f'配置文件CEPH_RADOS中别名“{using}”配置中POOL_NAME配置项无效')

    if isinstance(pools, str):
        return (pools,)

    if isinstance(pools, tuple):
       return pools

    if isinstance(pools, list):
        return tuple(pools)

    raise ValueError(f'配置文件CEPH_RADOS中别名“{using}”配置中POOL_NAME配置项需要是一个元组tuple')

def get_ceph_poolname_rand(using: str):
    """
    从配置的CEPH pool name随机获取一个
    :return:
        poolname: str

    :raises: ValueError
    """
    pools = get_ceph_poolnames(using)
    return random.choice(pools)


def create_table_for_model_class(model):
    """
    创建Model类对应的数据库表

    :param model: Model类
    :return:
            True: success
            False: failure
    """
    try:
        using = router.db_for_write(model)
        with DatabaseSchemaEditor(connection=connections[using]) as schema_editor:
            schema_editor.create_model(model)
            if issubclass(model, BucketFileBase):
                try:
                    table_name = schema_editor.quote_name(model._meta.db_table)
                    sql1 = f"ALTER TABLE {table_name} CHANGE COLUMN `na` `na` LONGTEXT NOT " \
                           f"NULL COLLATE 'utf8mb4_bin' AFTER `id`;"
                    sql2 = f"ALTER TABLE {table_name} CHANGE COLUMN `name` `name` VARCHAR(255) " \
                           f"NOT NULL COLLATE 'utf8mb4_bin' AFTER `na_md5`;"
                    schema_editor.execute(sql=sql1)
                    schema_editor.execute(sql=sql2)
                except Exception as exc:
                    if delete_table_for_model_class(model):
                        raise exc       # model table 删除成功，抛出错误
    except Exception as e:
        msg = traceback.format_exc()
        logger.error(msg)
        return False

    return True


def delete_table_for_model_class(model):
    """
    删除Model类对应的数据库表

    :param model: Model类
    :return:
            True: success
            False: failure
    """
    try:
        using = router.db_for_write(model)
        with DatabaseSchemaEditor(connection=connections[using]) as schema_editor:
            schema_editor.delete_model(model)
    except Exception as e:
        logger.error(str(e))
        if e.args[0] in [1051, 1146]:  # unknown table or table not exists
            return True

        return False

    return True


def is_model_table_exists(model):
    """
    检查模型类Model的数据库表是否已存在
    :param model:
    :return: True(existing); False(not existing)
    """
    using = router.db_for_write(model)
    connection = connections[using]
    return model._meta.db_table in connection.introspection.table_names()


def get_obj_model_class(table_name):
    """
    动态创建存储桶对应的对象模型类

    RuntimeWarning: Model 'xxxxx_' was already registered. Reloading models is not advised as it can
    lead to inconsistencies most notably with related models.
    如上述警告所述, Django 不建议重复创建Model 的定义.可以直接通过get_obj_model_class创建，无视警告.
    这里先通过 get_registered_model 获取已经注册的 Model, 如果获取不到， 再生成新的模型类.

    :param table_name: 数据库表名，模型类对应的数据库表名
    :return: Model class
    """
    model_name = 'ObjModel' + table_name
    app_leble = BucketFileBase.Meta.app_label
    try:
        cls = apps.get_registered_model(app_label=app_leble, model_name=model_name)
        return cls
    except LookupError:
        pass

    meta = BucketFileBase.Meta()
    meta.abstract = False
    meta.db_table = table_name  # 数据库表名
    return type(model_name, (BucketFileBase,), {'Meta': meta, '__module__': BucketFileBase.__module__})


def create_bucket(name: str, user, _id: int = None, ceph_using: str = None, pool_name: str = None):
    """
    创建存储桶

    :param name: bucket name
    :param user: user bucket belong to
    :param _id: bucket id
    :param ceph_using: 多ceph集群时，指定使用那个ceph集群
    :param pool_name: ceph pool name that bucket objects data storage
    """
    if not ceph_using and pool_name:
        raise exceptions.Error(message=f'指定"pool_name"时必须同时指定"ceph_using"')

    if not ceph_using:
        ceph_using = get_ceph_alias_rand()

    if pool_name:
        pools = get_ceph_poolnames(ceph_using)
        if pool_name not in pools:
            raise exceptions.Error(message=f'指定"pool_name"（{pool_name}）不在"ceph_using"（{ceph_using}）中')
    else:
        pool_name = get_ceph_poolname_rand(ceph_using)

    bucket_id = _id
    bucket_name = name
    if not _id:
        bucket_id = get_next_bucket_max_id()

    try:
        bucket = Bucket(id=bucket_id, name=bucket_name,
                        ceph_using=ceph_using, pool_name=pool_name,
                        user=user)
        bucket.save(force_insert=True)
    except Exception as e:
        raise exceptions.Error(message=f"create bucket metadata failed, {str(e)}.")

    col_name = bucket.get_bucket_table_name()
    bfm = BucketFileManagement(collection_name=col_name)
    model_class = bfm.get_obj_model_class()
    if not create_table_for_model_class(model=model_class):
        if not create_table_for_model_class(model=model_class):
            bucket.delete()
            delete_table_for_model_class(model=model_class)
            raise exceptions.Error(f"create bucket table failed.")

    return bucket


def get_bfmanager(path='', table_name=''):
    return BucketFileManagement(path=path, collection_name=table_name)


class BucketFileManagement:
    """
    存储桶相关的操作方法类
    """
    ROOT_DIR_ID = 0 # 根目录ID

    def __init__(self, path='', collection_name='', *args, **kwargs):
        self._path = self._hand_path(path)
        self._collection_name = collection_name # bucket's database table name
        self.cur_dir_id = None
        self._bucket_file_class = self.creat_obj_model_class()

    def creat_obj_model_class(self):
        """
        动态创建各存储桶数据库表对应的模型类
        """
        db_table = self.get_collection_name() # 数据库表名
        return get_obj_model_class(db_table)

    def get_obj_model_class(self):
        if not self._bucket_file_class:
            self._bucket_file_class = self.creat_obj_model_class()

        return self._bucket_file_class

    def root_dir(self):
        """
        根目录对象
        :return:
        """
        c = self.get_obj_model_class()
        return c(id=self.ROOT_DIR_ID, na='', name='', fod=False, did=self.ROOT_DIR_ID, si=0)

    def get_collection_name(self):
        return self._collection_name

    @staticmethod
    def _hand_path(path):
        """
        path字符串两边可能的空白和右边/
        """
        if isinstance(path, str):
            path.strip(' ')
            return path.rstrip('/')

        return ''

    def get_cur_dir_id(self, dir_path=None):
        """
        获得当前目录节点id
        :return:
            id： int     # 顶级目录时id=ROOT_DIR_ID

        :raises: Error，InvalidKey，SameKeyAlreadyExists，NoParentPath
        """
        if self.cur_dir_id:
            return self.cur_dir_id

        path = dir_path if dir_path else self._path
        # path为空，根目录为存储桶
        if path == '' or path == '/':
            return self.ROOT_DIR_ID

        path = self._hand_path(path)
        if not path:
            raise exceptions.InvalidKey(message='父路径无效')      # path参数有误

        try:
            obj = self.get_obj(path=path)
        except Exception as e:
            raise exceptions.Error(message=f'查询目录id错误，{str(e)}')

        if obj:
            if obj.is_dir():
                self.cur_dir_id = obj.id
                return self.cur_dir_id
            else:
                raise  exceptions.SameKeyAlreadyExists(message='无效目录，同名对象已存在')

        raise exceptions.NoParentPath(message='目录路径不存在')

    def get_cur_dir_files(self, cur_dir_id=None, only_obj: bool = None):
        """
        获得当前目录下的文件或文件夹记录

        * 指定cur_dir_id时，list cur_dir_id下的文件或目录记录;

        :param cur_dir_id: 目录id;
        :param only_obj: True(只列举对象), 其他忽略
        :return:
            QuerySet()

        :raises: Error
        """
        dir_id = None
        if cur_dir_id is not None:
            dir_id = cur_dir_id

        if dir_id is None and self._path:
            dir_id = self.get_cur_dir_id()

        model_class = self.get_obj_model_class()
        if dir_id:
            filters = {'did': dir_id}
        else:
            filters = {'did': self.ROOT_DIR_ID}

        if only_obj:
            filters['fod'] = True

        try:
            files = model_class.objects.filter(**filters).all()
        except Exception as e:
            logger.error('In get_cur_dir_files:' + str(e))
            raise exceptions.Error.from_error(e)

        return files

    def get_dir_or_obj_exists(self, name, check_path_exists: bool = True):
        """
        通过名称获取当前路径下的子目录或对象
        :param name: 目录名或对象名称
        :param check_path_exists: 是否检查当前路径是否存在
        :return:
            文件目录对象 or None
            raises: Exception   # 发生错误，或当前目录参数有误，对应目录不存在

        :raises: Error, NoParentPath
        """
        if check_path_exists:
            self.get_cur_dir_id()

        path = self.build_dir_full_name(name)
        try:
            dir_or_obj = self.get_obj(path=path)
        except Exception as e:
            raise exceptions.Error(message=f'查询目录id错误，{str(e)}')

        return dir_or_obj

    def build_dir_full_name(self, dir_name):
        """
        拼接全路径

        :param dir_name: 目录名
        :return: 目录绝对路径
        """
        dir_name.strip('/')
        path = self._hand_path(self._path)
        return (path + '/' + dir_name) if path else dir_name

    def get_count(self):
        """
        获取存储桶数据库表的对象和目录记录总数量
        :return:
        """
        return self.get_obj_model_class().objects.count()

    def get_obj_count(self):
        """
        获取存储桶中的对象总数量
        :return:
        """
        return self.get_obj_model_class().objects.filter(fod=True).count()

    def get_valid_obj_count(self):
        """
        获取存储桶中的有效（未删除状态）对象数量
        :return:
        """
        return self.get_obj_model_class().objects.filter(Q(fod=True) & Q(sds=False)).count()

    def cur_dir_is_empty(self):
        """
        当前目录是否为空目录
        :return:
            True(空); False(非空)

        :raises: Error
        """
        try:
            did = self.get_cur_dir_id()
        except exceptions.Error as exc:
            raise exc

        if self.get_obj_model_class().objects.filter(did=did).exists():
            return False

        return True

    def dir_is_empty(self, dir_obj):
        """
        给定目录是否为空目录

        :params dir_obj: 目录对象
        :return:True(空); False(非空)
        """
        did = dir_obj.id

        if self.get_obj_model_class().objects.filter(did=did).exists():
            return False

        return True

    def get_bucket_space_and_count(self):
        """
        获取存储桶中的对象占用总空间大小和对象数量
        :return:
            {'space': 123, 'count: 456}
        """
        data = self.get_obj_model_class().objects.filter(fod=True).aggregate(space=Sum('si'), count=Count('fod'))
        return data

    def get_obj(self, path:str):
        """
        获取目录或对象

        :param path: 目录或对象路径
        :return:
            obj     # success
            None    # 不存在

        :raises: Error
        """
        na_md5 = get_str_hexMD5(path)
        model_class = self.get_obj_model_class()
        try:
            obj = model_class.objects.get(Q(na_md5=na_md5) | Q(na_md5__isnull=True), na=path)
        except model_class.DoesNotExist as e:
            return None
        except MultipleObjectsReturned as e:
            msg = f'数据库表{self.get_collection_name()}中存在多个相同的目录：{path}'
            logger.error(msg)
            raise exceptions.Error(message=msg)
        except Exception as e:
            msg = f'select {self.get_collection_name()},path={path},err={str(e)}'
            logger.error(msg)
            raise exceptions.Error(msg)

        return obj

    def get_search_object_queryset(self, search: str, contain_dir: bool = False):
        """
        检索对象
        """
        # if contain_dir:
        #     lookup = {'lower_name__icontains': search}
        # else:
        #     lookup = {'fod': True, 'lower_name__icontains': search}
        #
        # model_class = self.get_obj_model_class()
        # return model_class.objects.annotate(lower_name=Lower('name')).filter(**lookup)

        if contain_dir:
            lookup = {'name__icontains': search}
        else:
            lookup = {'fod': True, 'name__icontains': search}
        model_class = self.get_obj_model_class()
        return model_class.objects.filter(**lookup)

    def get_objects_dirs_queryset(self):
        """
        获得所有文件对象和目录记录

        :return: QuerySet()
        """
        model_class = self.get_obj_model_class()
        return model_class.objects.all()

    def get_prefix_objects_dirs_queryset(self, prefix: str):
        """
        获得指定路径前缀的对象和目录查询集

        :param prefix: 路径前缀
        :return: QuerySet()
        """
        model_class = self.get_obj_model_class()
        return model_class.objects.filter(na__startswith=prefix).all()
