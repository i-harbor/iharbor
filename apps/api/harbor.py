import logging

from django.utils import timezone
from django.db.models import Case, Value, When, F
from django.db import close_old_connections
from django.db.models import BigIntegerField

from buckets.models import Bucket
from buckets.utils import BucketFileManagement
from utils.storagers import PathParser, try_close_file
from utils.oss import build_harbor_object, get_size
from .paginations import BucketFileLimitOffsetPagination
from utils.log.decorators import log_op_info
from utils.md5 import FileMD5Handler
from . import exceptions


# 这里的日志记录器要和setting中的loggers选项对应，不能随意给参
debug_logger = logging.getLogger('debug')


def ftp_close_old_connections(func):
    def wrapper(*args, **kwargs):
        close_old_connections()
        return func(*args, **kwargs)

    return wrapper


class HarborManager:
    """
    操作harbor对象数据和元数据管理接口封装
    """
    def get_bucket(self, bucket_name:str, user=None):
        """
        获取存储桶

        :param bucket_name: 桶名
        :param user: 用户对象；如果给定用户，只查找属于此用户的存储桶
        :return:
            Bucket() # 成功时
            None     # 桶不存在，或不属于给定用户时
        """
        if user:
            return self.get_user_own_bucket(bucket_name, user)

        return self.get_bucket_by_name(bucket_name)

    @staticmethod
    def get_bucket_by_name(name: str):
        """
        通过桶名获取Bucket实例
        :param name: 桶名
        :return:
            Bucket() # success
            None     # not exist
        """
        bucket = Bucket.get_bucket_by_name(name)
        if not bucket:
            return None

        return bucket

    def get_user_own_bucket(self, name: str, user):
        """
        获取用户的存储桶

        :param name: 存储通名称
        :param user: 用户对象
        :return:
            success: bucket
            failure: None
        """
        bucket = self.get_bucket_by_name(name)
        if bucket and bucket.check_user_own_bucket(user):
            return bucket

        return None

    @log_op_info(logger=debug_logger, mark_text='is_dir')
    def is_dir(self, bucket_name: str, path_name: str):
        """
        是否时一个目录

        :param bucket_name: 桶名
        :param path_name: 目录路径
        :return:
            true: is dir
            false: is file

        :raise HarborError  # 桶或路径不存在，发生错误
        """
        # path为空或根目录时
        if not path_name or path_name == '/':
            return True

        obj = self.get_object(bucket_name, path_name)
        if obj.is_dir():
            return True

        return False

    def is_file(self, bucket_name:str, path_name:str):
        """
        是否时一个文件
        :param bucket_name: 桶名
        :param path_name: 文件路径
        :return:
            true: is file
            false: is dir

        :raise HarborError  # 桶或路径不存在，发生错误
        """
        # path为空或根目录时
        if not path_name or path_name == '/':
            return False

        obj = self.get_object(bucket_name, path_name)
        if obj.is_file():
            return True

        return False

    def get_object(self, bucket_name:str, path_name:str, user=None):
        """
        获取对象或目录实例

        :param bucket_name: 桶名
        :param path_name: 文件路径
        :param user: 用户，默认为None，如果给定用户只获取属于此用户的对象（只查找此用户的存储桶）
        :return:
            obj or dir    # 对象或目录实例

        :raise HarborError
        """
        bucket, obj = self.get_bucket_and_obj_or_dir(bucket_name=bucket_name, path=path_name, user=user)
        if obj is None:
            err = exceptions.NoSuchKey(message='指定对象或目录不存在')
            raise exceptions.HarborError.from_error(err)

        return obj

    @staticmethod
    def get_metadata_obj(table_name:str, path:str):
        """
        直接获取目录或对象元数据对象，不检查父节点是否存在

        :param table_name: 数据库表名
        :param path: 目录或对象的全路径
        :return:
            obj     #
            None    #
        :raises: HarborError
        """
        bfm = BucketFileManagement(collection_name=table_name)
        try:
            obj = bfm.get_obj(path=path)
        except Exception as e:
            raise exceptions.HarborError(message=str(e))
        if obj:
            return obj

        return None

    @staticmethod
    def _get_obj_or_dir_and_bfm(table_name, path, name):
        """
        获取文件对象或目录,和BucketFileManagement对象

        :param table_name: 数据库表名
        :param path: 父目录路经
        :param name: 对象名称或目录名称
        :return:
            obj，bfm: 对象或目录

            obj, bfm   # 目录或对象存在
            None, bfm  # 目录或对象不存在
            raise Exception    # 父目录路径错误，不存在

        :raises: HarborError
        """
        bfm = BucketFileManagement(path=path, collection_name=table_name)
        try:
            obj = bfm.get_dir_or_obj_exists(name=name)
        except Exception as e:
            raise exceptions.HarborError.from_error(e)

        if obj:
            return obj, bfm   # 目录或对象存在

        return None, bfm  # 目录或对象不存在

    def _get_obj_or_dir(self, table_name, path, name):
        """
        获取文件对象或目录

        :param table_name: 数据库表名
        :param path: 父目录路经
        :param name: 对象名称或目录名称
        :return:
            raise Exception    # 父目录路径错误，不存在
            obj or None: 目录或对象不存在

        :raises: HarborError
        """
        obj, _ = self._get_obj_or_dir_and_bfm(table_name, path, name)
        return obj

    def mkdir(self, bucket_name: str, path: str, user=None):
        """
        创建一个目录
        :param bucket_name: 桶名
        :param path: 目录全路径
        :param user: 用户，默认为None，如果给定用户只创建属于此用户的目录（只查找此用户的存储桶）
        :return:
            True, dir: success
            raise HarborError: failed
        :raise HarborError
        """
        if not bucket_name:
            err = exceptions.BadRequest(message='目录路径参数无效，要同时包含有效的存储桶和目录名称')
            raise exceptions.HarborError.from_error(err)

        # bucket是否属于当前用户,检测存储桶名称是否存在
        bucket = self.get_bucket(bucket_name, user=user)
        if not isinstance(bucket, Bucket):
            err = exceptions.NoSuchBucket(message='存储桶不存在')
            raise exceptions.HarborError.from_error(err)

        # 桶锁操作检查
        if not bucket.lock_writeable():
            raise exceptions.BucketLockWrite()

        return self._mkdir(bucket=bucket, path=path)

    def _mkdir(self, bucket, path: str):
        """
        :param bucket: bucket instance
        :param path: 目录路径
        :return:
            True, dir: success
            raise HarborError: failed

        :raise HarborError
        """
        validated_data = self.validate_mkdir_params(bucket, path)
        did = validated_data.get('did', None)
        collection_name = validated_data.get('collection_name')
        if not isinstance(did, int):
            raise exceptions.HarborError(message='父路径id无效')

        obj = self._mkdir_metadata(table_name=collection_name, p_id=did, dir_path_name=path)
        return True, obj

    @staticmethod
    def _mkdir_metadata(table_name: str, p_id: int, dir_path_name: str):
        """
        创建目录元数据

        :param table_name: 目录所在存储桶对应的数据库表名
        :param p_id: 父目录id
        :param dir_path_name: 目录全路径
        :return:
            dir对象

        :raises: S3Error
        """
        _, dir_name = PathParser(dir_path_name).get_path_and_filename()

        bfm = BucketFileManagement(collection_name=table_name)
        object_class = bfm.get_obj_model_class()
        dir1 = object_class(na=dir_path_name,  # 全路经目录名
                            name=dir_name,  # 目录名
                            fod=False,  # 目录
                            did=p_id)  # 父目录id
        try:
            dir1.save(force_insert=True)  # 仅尝试创建文档，不修改已存在文档
        except Exception as e:
            raise exceptions.HarborError(f'创建目录元数据错误, {str(e)}')

        return dir1

    def create_parent_path(self, table_name, path: str):
        """
        创建整个目录路径

        :param table_name: 桶的数据库表明称
        :param path: 要创建的目录路径字符串
        :return:
            [dir,]      # 创建的目录路径上的目录(只包含新建的目录和路径上最后一个目录)的列表

        :raises: S3Error
        """
        bfm = BucketFileManagement(collection_name=table_name)
        paths = PathParser(path).get_path_breadcrumb()
        if len(paths) == 0:
            return bfm.root_dir()  # 根目录对象

        index_paths = list(enumerate(paths))
        index = 0
        last_exist_dir = None  # 路径中已存在的最后的目录
        for index, item in reversed(index_paths):
            dir_name, dir_path_name = item
            try:
                obj = bfm.get_obj(path=dir_path_name)
            except Exception as e:
                raise exceptions.HarborError.from_error(str(e))
            if not obj:
                continue

            if obj.is_file():
                raise exceptions.HarborError.from_error(exceptions.SameKeyAlreadyExists(
                    "The path of the object's key conflicts with the existing object's key"))

            last_exist_dir = obj
            break

        if index == (len(index_paths) - 1) and last_exist_dir:  # 整个路径已存在
            return [last_exist_dir]

        # 从整个路径上已存在的目录处开始向后创建路径
        if last_exist_dir:
            now_last_dir = last_exist_dir  # 记录现在已创建的路径上的最后的目录
            create_paths = index_paths[index + 1:]  # index目录存在不需创建
        else:
            now_last_dir = bfm.root_dir()  # 根目录对象
            create_paths = index_paths[index:]  # index目录不存在需创建

        dirs = []
        for i, p in create_paths:
            p_id = now_last_dir.id
            dir_name, dir_path_name = p
            dir1 = self._mkdir_metadata(table_name=table_name, p_id=p_id, dir_path_name=dir_path_name)
            dirs.append(dir1)
            now_last_dir = dir1

        return dirs

    @staticmethod
    def get_root_dir():
        return BucketFileManagement().root_dir()

    def validate_mkdir_params(self, bucket, dirpath: str):
        """
        post_detail参数验证

        :param bucket: bucket instance
        :param dirpath: 目录路径
        :return:
                success: {data}
                failure: raise HarborError

        :raise HarborError
        """
        data = {}
        dir_path, dir_name = PathParser(filepath=dirpath).get_path_and_filename()

        if not dir_name:
            err = exceptions.BadRequest(message='目录路径参数无效，要同时包含有效的存储桶和目录名称')
            raise exceptions.HarborError.from_error(err)

        if '/' in dir_name:
            err = exceptions.BadRequest(message='目录名称不能包含‘/’')
            raise exceptions.HarborError.from_error(err)

        if len(dir_name) > 255:
            err = exceptions.BadRequest(message='目录名称长度最大为255字符')
            raise exceptions.HarborError.from_error(err)

        _collection_name = bucket.get_bucket_table_name()
        data['collection_name'] = _collection_name

        try:
            _dir, bfm = self._get_obj_or_dir_and_bfm(table_name=_collection_name, path=dir_path, name=dir_name)
        except exceptions.HarborError as e:
            raise e

        if _dir:
            # 目录已存在
            if _dir.is_dir():
                err = exceptions.KeyAlreadyExists(message=f'"{dir_name}"目录已存在')
                raise exceptions.HarborError.from_error(err)

            # 同名对象已存在
            if _dir.is_file():
                err = exceptions.SameKeyAlreadyExists(message=f'"指定目录名称{dir_name}"已存在重名对象，请重新指定一个目录名称')
                raise exceptions.HarborError.from_error(err)

        try:
            data['did'] = bfm.get_cur_dir_id()
        except exceptions.Error as e:
            raise exceptions.HarborError.from_error(e)

        data['bucket_name'] = bucket.name
        data['dir_path'] = dir_path
        data['dir_name'] = dir_name
        return data

    def rmdir(self, bucket_name:str, dirpath:str, user=None):
        """
        删除一个空目录
        :param bucket_name:桶名
        :param dirpath: 目录全路径
        :param user: 用户，默认为None，如果给定用户只删除属于此用户的目录（只查找此用户的存储桶）
        :return:
            True: success
            raise HarborError(): failed

        :raise HarborError()
        """
        if not bucket_name:
            err = exceptions.BadRequest(message='bucket name参数无效')
            raise exceptions.HarborError.from_error(err)

        # bucket是否属于当前用户,检测存储桶名称是否存在
        bucket = self.get_bucket(bucket_name, user=user)
        if not isinstance(bucket, Bucket):
            raise exceptions.HarborError.from_error(
                exceptions.NoSuchBucket(message='存储桶不存在'))

        return self._rmdir(bucket=bucket, dirpath=dirpath)

    def _rmdir(self, bucket, dirpath: str):
        """
        删除目录，不检测bucket用户权限

        :param bucket: bucket实例
        :return:
            True

        :raises: HarborError
        """
        path, dir_name = PathParser(filepath=dirpath).get_path_and_filename()
        if not dir_name:
            raise exceptions.HarborError.from_error(
                exceptions.BadRequest(message='参数无效'))

        # 桶锁操作检查
        if not bucket.lock_writeable():
            raise exceptions.BucketLockWrite()

        table_name = bucket.get_bucket_table_name()

        try:
            _dir, bfm = self._get_obj_or_dir_and_bfm(table_name=table_name, path=path, name=dir_name)
        except exceptions.HarborError as e:
            raise e

        if not _dir or _dir.is_file():
            raise exceptions.HarborError.from_error(
                exceptions.NoSuchKey(message='目录不存在'))

        if not bfm.dir_is_empty(_dir):
            raise exceptions.HarborError.from_error(
                exceptions.NoEmptyDir(message='无法删除非空目录'))

        if not _dir.do_delete():
            raise exceptions.HarborError(message='删除目录失败，数据库错误')

        return True

    def _list_dir_queryset(self, bucket_name: str, path: str, user=None,
                           only_obj: bool = None):
        """
        获取目录下的文件列表信息

        :param bucket_name: 桶名
        :param path: 目录路径
        :param user: 用户，默认为None，如果给定用户只获取属于此用户的目录下的文件列表信息（只查找此用户的存储桶）
        :param only_obj: True(只列举对象), 其他忽略
        :return:
                success:    (QuerySet(), bucket) # django QuerySet实例和bucket实例
                failed:      raise HarborError

        :raise HarborError
        """
        bucket = self.get_bucket(bucket_name, user)
        if not bucket:
            raise exceptions.HarborError.from_error(
                exceptions.NoSuchBucket(message='存储桶不存在'))

        qs = self._list_bucket_dir_queryset(bucket=bucket, path=path, only_obj=only_obj)
        return qs, bucket

    @staticmethod
    def _list_bucket_dir_queryset(bucket, path: str, only_obj: bool = None):
        """
        :param bucket: bucket instance
        :param path: 目录路径
        :param only_obj: True(只列举对象), 其他忽略
        :return:
                success:    QuerySet()
                failed:      raise HarborError

        :raise HarborError
        """
        # 桶锁操作检查
        if not bucket.lock_readable():
            raise exceptions.BucketLockWrite()

        collection_name = bucket.get_bucket_table_name()
        bfm = BucketFileManagement(path=path, collection_name=collection_name)
        try:
            qs = bfm.get_cur_dir_files(only_obj=only_obj)
        except Exception as exc:
            raise exceptions.HarborError.from_error(exc)

        return qs

    @staticmethod
    def get_queryset_list_dir(bucket, dir_id, only_obj: bool = None):
        """
        获取目录下的对象和目录列表信息

        :param bucket: 桶实例
        :param dir_id: 目录实例id
        :param only_obj: True(只列举对象), 其他忽略
        :return:
                success:    QuerySet()   # django QuerySet实例
                failed:      raise HarborError
        """
        collection_name = bucket.get_bucket_table_name()
        bfm = BucketFileManagement(collection_name=collection_name)
        try:
            qs = bfm.get_cur_dir_files(cur_dir_id=dir_id, only_obj=only_obj)
        except Exception as e:
            raise exceptions.HarborError.from_error(e)

        return qs

    def list_dir_generator(self, bucket_name:str, path:str, per_num:int=1000, user=None, paginator=None):
        """
        获取目录下的文件列表信息生成器

        :param bucket_name: 桶名
        :param path: 目录路径
        :param per_num: 每次获取文件信息数量
        :param user: 用户，默认为None，如果给定用户只获取属于此用户的目录下的文件列表信息（只查找此用户的存储桶）
        :param paginator: 分页器，默认为None
        :return:
                generator           # success
                :raise HarborError  # failed
        :usage:
            for objs in generator:
                print(objs)         # objs type list, raise HarborError when error

        :raise HarborError
        """
        qs, _ = self._list_dir_queryset(bucket_name=bucket_name, path=path, user=user)

        def generator(queryset, _per_num, _paginator=None):
            offset = 0
            limit = _per_num
            if _paginator is None:
                _paginator = BucketFileLimitOffsetPagination()

            while True:
                try:
                    ret = _paginator.pagenate_to_list(queryset, offset=offset, limit=limit)
                except Exception as e:
                    raise exceptions.HarborError(message=str(e))

                yield ret
                length = len(ret)
                if length < _per_num:
                    return
                offset = offset + length

        return generator(queryset=qs, _per_num=per_num, _paginator=paginator)

    def list_dir(self, bucket_name: str, path: str, offset: int = 0,
                 limit: int = 1000, user=None, paginator=None, only_obj: bool = None):
        """
        获取目录下的文件列表信息

        :param bucket_name: 桶名
        :param path: 目录路径
        :param offset: 目录下文件列表偏移量
        :param limit: 获取文件信息数量
        :param user: 用户，默认为None，如果给定用户只获取属于此用户的目录下的文件列表信息（只查找此用户的存储桶）
        :param paginator: 分页器，默认为None
        :param only_obj: True(只列举对象), 其他忽略
        :return:
                success:    (list[object, object,], bucket) # list和bucket实例
                failed:      raise HarborError

        :raise HarborError
        """
        files, bucket = self._list_dir_queryset(bucket_name=bucket_name, path=path,
                                                user=user, only_obj=only_obj)

        if paginator is None:
            paginator = BucketFileLimitOffsetPagination()
        if limit <= 0:
            limit = paginator.default_limit
        else:
            limit = min(limit, paginator.max_limit)

        if offset < 0:
            offset = 0

        paginator.offset = offset
        paginator.limit = limit

        try:
            li = paginator.pagenate_to_list(files, offset=offset, limit=limit)
        except Exception as e:
            raise exceptions.HarborError(message=str(e))

        return li, bucket

    def get_bucket_objects_dirs_queryset(self, bucket_name: str, user, prefix: str = '', only_obj: bool = False):
        """
        获得所有对象和目录记录

        :param bucket_name: 桶名
        :param user: 用户对象
        :param prefix: 路径前缀
        :param only_obj: True只查询对象
        :return:
            bucket, QuerySet()

        :raises: S3Error
        """
        # 存储桶验证和获取桶对象
        bucket = self.get_bucket_by_name(bucket_name)
        if not bucket:
            raise exceptions.HarborError.from_error(
                exceptions.NoSuchBucket('存储桶不存在'))

        self.check_public_or_user_bucket(bucket=bucket, user=user, all_public=False)

        table_name = bucket.get_bucket_table_name()
        if not prefix:
            objs = self.get_objects_dirs_queryset(table_name=table_name)
        else:
            objs = self.get_prefix_objects_dirs_queryset(table_name=table_name, prefix=prefix)

        if only_obj:
            objs = objs.filter(fod=True)

        return bucket, objs

    @staticmethod
    def get_objects_dirs_queryset(table_name: str):
        """
        获得所有文件对象和目录记录

        :return: QuerySet()
        """
        return BucketFileManagement(collection_name=table_name).get_objects_dirs_queryset()

    @staticmethod
    def get_prefix_objects_dirs_queryset(table_name: str, prefix: str):
        """
        获取存储桶下指定路径前缀的对象和目录查询集

        :param table_name: 桶对应的数据库表名
        :param prefix: 路径前缀
        :return:
                success:    QuerySet()   # django QuerySet实例
        """
        bfm = BucketFileManagement(collection_name=table_name)
        return bfm.get_prefix_objects_dirs_queryset(prefix=prefix)

    def move_rename(self, bucket_name: str, obj_path: str, rename=None, move=None, user=None):
        """
        移动或重命名对象

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param rename: 重命名新名称，默认为None不重命名
        :param move: 移动到move路径下，默认为None不移动
        :param user: 用户，默认为None，如果给定用户只操作属于此用户的对象（只查找此用户的存储桶）
        :return:
            success: (object, bucket)     # 移动后的对象实例
            failed : raise HarborError

        :raise HarborError
        """
        path, filename = PathParser(filepath=obj_path).get_path_and_filename()
        if not bucket_name or not filename:
            raise exceptions.HarborError.from_error(
                exceptions.BadRequest(message='参数有误'))

        move_to, rename = self._validate_move_rename_params(move_to=move, rename=rename)
        if move_to is None and rename is None:
            raise exceptions.HarborError.from_error(
                exceptions.BadRequest(message='请至少提交一个要执行操作的参数'))

        # 存储桶验证和获取桶对象
        try:
            bucket, obj = self.get_bucket_and_obj(bucket_name=bucket_name, obj_path=obj_path, user=user)
        except exceptions.HarborError as e:
            raise e

        # 桶锁操作检查
        if not bucket.lock_writeable():
            raise exceptions.BucketLockWrite()

        if obj is None:
            raise exceptions.HarborError.from_error(
                exceptions.NoSuchKey(message='文件对象不存在'))

        return self._move_rename_obj(bucket=bucket, obj=obj, move_to=move_to, rename=rename)

    @staticmethod
    def _move_rename_obj(bucket, obj, move_to, rename):
        """
        移动重命名对象

        :param bucket: 对象所在桶
        :param obj: 文件对象
        :param move_to: 移动目标路径
        :param rename: 重命名的新名称
        :return:
            success: (object, bucket)
            failed : raise HarborError

        :raise HarborError
        """
        table_name = bucket.get_bucket_table_name()
        new_obj_name = rename if rename else obj.name # 移动后对象的名称，对象名称不变或重命名

        # 检查是否符合移动或重命名条件，目标路径下是否已存在同名对象或子目录
        try:
            if move_to is None:     # 仅仅重命名对象，不移动
                path, _ = PathParser(filepath=obj.na).get_path_and_filename()
                new_na = path + '/' + new_obj_name if path else new_obj_name
                bfm = BucketFileManagement(path=path, collection_name=table_name)
                target_obj = bfm.get_obj(path=new_na)
            else:   # 需要移动对象
                bfm = BucketFileManagement(path=move_to, collection_name=table_name)
                target_obj = bfm.get_dir_or_obj_exists(name=new_obj_name)
                new_na = bfm.build_dir_full_name(new_obj_name)
        except exceptions.Error as exc:
            raise exceptions.HarborError.from_error(exc)
        except Exception as e:
            raise exceptions.HarborError(message=f'移动对象操作失败, 查询是否已存在同名对象或子目录时发生错误, {str(e)}')

        if target_obj:
            raise exceptions.HarborError.from_error(
                exceptions.SameKeyAlreadyExists(message='无法完成对象的移动操作，指定的目标路径下已存在同名的对象或目录'))

        if move_to is not None:     # 移动对象或重命名
            try:
                did = bfm.get_cur_dir_id()
            except exceptions.Error as exc:
                raise exceptions.HarborError.from_error(exc)

            obj.did = did

        obj.na = new_na
        obj.name = new_obj_name
        obj.reset_na_md5()
        if not obj.do_save():
            raise exceptions.HarborError(message='移动对象操作失败')

        return obj, bucket

    @staticmethod
    def _validate_move_rename_params(move_to, rename):
        """
        校验移动或重命名参数

        :return:
                (move_to, rename)
                move_to # None 或 string
                rename  # None 或 string

        :raise HarborError
        """
        # 移动对象参数
        if move_to is not None:
            move_to = move_to.strip('/')

        # 重命名对象参数
        if rename is not None:
            if '/' in rename:
                raise exceptions.HarborError.from_error(
                    exceptions.BadRequest(message='对象名称不能含“/”'))

            if len(rename) > 255:
                raise exceptions.HarborError.from_error(
                    exceptions.BadRequest(message='对象名称不能大于255个字符长度'))

        return move_to, rename

    def write_chunk(self, bucket_name:str, obj_path:str, offset:int, chunk:bytes, reset:bool=False, user=None):
        """
        向对象写入一个数据块

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param offset: 写入对象偏移量
        :param chunk: 要写入的数据，bytes
        :param reset: 为True时，先重置对象大小为0后再写入数据；
        :param user: 用户，默认为None，如果给定用户只操作属于此用户的对象（只查找此用户的存储桶）
        :return:
                created             # created==True表示对象是新建的；created==False表示对象不是新建的
                raise HarborError   # 写入失败
        """
        if not isinstance(chunk, bytes):
            raise exceptions.HarborError.from_error(
                exceptions.BadRequest(message='数据不是bytes类型'))

        return self.write_to_object(bucket_name=bucket_name, obj_path=obj_path, offset=offset, data=chunk,
                                    reset=reset, user=user)

    def write_file(self, bucket_name:str, obj_path:str, offset:int, file, reset:bool=False, user=None):
        """
        向对象写入一个文件

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param offset: 写入对象偏移量
        :param file: 要写入的数据，已打开的类文件句柄
        :param reset: 为True时，先重置对象大小为0后再写入数据；
        :param user: 用户，默认为None，如果给定用户只操作属于此用户的对象（只查找此用户的存储桶）
        :return:
                created             # created==True表示对象是新建的；created==False表示对象不是新建的
                raise HarborError   # 写入失败
        """
        return self.write_to_object(bucket_name=bucket_name, obj_path=obj_path, offset=offset, data=file,
                                    reset=reset, user=user)

    def write_to_object(self, bucket_name:str, obj_path:str, offset:int, data, reset:bool=False, user=None):
        """
        向对象写入一个数据

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param offset: 写入对象偏移量
        :param data: 要写入的数据，file或bytes
        :param reset: 为True时，先重置对象大小为0后再写入数据；
        :param user: 用户，默认为None，如果给定用户只操作属于此用户的对象（只查找此用户的存储桶）
        :return:
                created             # created==True表示对象是新建的；created==False表示对象不是新建的
                raise HarborError   # 写入失败
        """
        bucket, obj, created = self.create_empty_obj(bucket_name=bucket_name, obj_path=obj_path, user=user)
        obj_key = obj.get_obj_key(bucket.id)
        pool_name = bucket.get_pool_name()
        rados = build_harbor_object(using=bucket.ceph_using, pool_name=pool_name, obj_id=obj_key, obj_size=obj.si)
        if created is False:  # 对象已存在，不是新建的
            if reset:  # 重置对象大小
                self._pre_reset_upload(obj=obj, rados=rados)

        try:
            if isinstance(data, bytes):
                self._save_one_chunk(obj=obj, rados=rados, offset=offset, chunk=data)
            else:
                self._save_one_file(obj=obj, rados=rados, offset=offset, file=data)
        except exceptions.HarborError as e:
            # 如果对象是新创建的，上传失败删除对象元数据
            if created is True:
                obj.do_delete()
            raise e

        return created

    def create_empty_obj(self, bucket_name: str, obj_path: str, user):
        """
        创建一个空对象

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param user: 用户，默认为None，如果给定用户只操作属于此用户的对象（只查找此用户的存储桶）
        :return:
                (bucket, obj, False) # 对象已存在
                (bucket, obj, True)  # 对象不存在，创建一个新对象
                raise HarborError # 有错误，路径不存在，或已存在同名目录

        :raise HarborError
        """
        pp = PathParser(filepath=obj_path)
        path, filename = pp.get_path_and_filename()
        if not bucket_name or not filename:
            raise exceptions.HarborError.from_error(
                exceptions.BadRequest(message='参数有误'))

        if len(filename) > 255:
            raise exceptions.HarborError.from_error(
                exceptions.BadRequest(message='对象名称长度最大为255字符'))

        # 存储桶验证和获取桶对象
        bucket = self.get_bucket(bucket_name, user=user)
        if not bucket:
            raise exceptions.HarborError.from_error(
                exceptions.NoSuchBucket(message='存储桶不存在'))

        # 桶锁操作检查
        if not bucket.lock_writeable():
            raise exceptions.BucketLockWrite()

        collection_name = bucket.get_bucket_table_name()
        try:
            obj, created = self._get_obj_and_check_limit_or_create(collection_name, path, filename)
        except exceptions.HarborError as exc:
            if not exc.is_same_code(exceptions.NoParentPath()):
                raise exc

            self.create_parent_path(table_name=collection_name, path=path)
            obj, created = self._get_obj_and_check_limit_or_create(collection_name, path, filename)

        return bucket, obj, created

    @staticmethod
    def _get_obj_and_check_limit_or_create(table_name, path, filename):
        """
        获取文件对象, 验证存储桶对象和目录数量上限，不存在并且验证通过则创建

        :param table_name: 桶对应的数据库表名
        :param path: 文件对象所在的父路径
        :param filename: 文件对象名称
        :return:
                (obj, False) # 对象已存在
                (obj, True)  # 对象不存在，创建一个新对象
                raise HarborError # 有错误，路径不存在，或已存在同名目录

        :raise HarborError
        """
        bfm = BucketFileManagement(path=path, collection_name=table_name)

        try:
            obj = bfm.get_dir_or_obj_exists(name=filename)
        except Exception as e:
            raise exceptions.HarborError.from_error(e)

        # 文件对象已存在
        if obj and obj.is_file():
            return obj, False

        # 已存在同名的目录
        if obj and obj.is_dir():
            raise exceptions.HarborError.from_error(
                exceptions.SameKeyAlreadyExists(message='指定的对象名称与已有的目录重名，请重新指定一个名称'))

        try:
            did = bfm.get_cur_dir_id()
        except exceptions.Error as exc:
            raise exceptions.HarborError.from_error(exc)

        # 验证集合文档上限
        # if not self.do_bucket_limit_validate(bfm):
        #     return None, None

        # 创建文件对象
        model_cls = bfm.get_obj_model_class()
        full_filename = bfm.build_dir_full_name(filename)
        bfinfo = model_cls(na=full_filename,  # 全路径文件名
                           name=filename,       # 文件名
                           fod=True,  # 文件
                           si=0, upt=timezone.now())  # 文件大小
        # 有父节点
        if did:
            bfinfo.did = did

        try:
            bfinfo.save()
            obj = bfinfo
        except Exception as e:
            raise exceptions.HarborError(message=f'新建对象元数据失败，数据库错误, {str(e)}')

        return obj, True

    @staticmethod
    def _pre_reset_upload(obj, rados):
        """
        覆盖上传前的一些操作

        :param obj: 文件对象元数据
        :param rados: rados接口类对象
        :return:
                正常：True
                错误：raise HarborError
        """
        # 先更新元数据，后删除rados数据（如果删除失败，恢复元数据）
        # 更新文件上传时间
        old_ult = obj.ult
        old_size = obj.si
        old_md5 = obj.md5

        obj.ult = timezone.now()
        obj.si = 0
        obj.md5 = ''
        if not obj.do_save(update_fields=['ult', 'si', 'md5']):
            raise exceptions.HarborError(message='修改对象元数据失败')

        ok, _ = rados.delete()
        if not ok:
            # 恢复元数据
            obj.ult = old_ult
            obj.si = old_size
            obj.md5 = old_md5
            obj.do_save(update_fields=['ult', 'si', 'md5'])
            raise exceptions.HarborError(message='rados文件对象删除失败')

        return True

    def _save_one_chunk(self, obj, rados, offset:int, chunk:bytes, md5: str = ''):
        """
        保存一个上传的分片

        :param obj: 对象元数据
        :param rados: rados接口
        :param offset: 分片偏移量
        :param chunk: 分片数据
        :param md5: 更新对象元数据MD5值，默认为空忽略
        :return:
            成功：True
            失败：raise HarborError
        """
        # 先更新元数据，后写rados数据
        # 更新文件修改时间和对象大小
        new_size = offset + len(chunk) # 分片数据写入后 对象偏移量大小
        if not self._update_obj_metadata(obj, size=new_size, md5=md5):
            raise exceptions.HarborError(message='修改对象元数据失败')

        # 存储文件块
        try:
            ok, msg = rados.write(offset=offset, data_block=chunk)
        except Exception as e:
            ok = False
            msg = str(e)

        if not ok:
            # 手动回滚对象元数据
            self._update_obj_metadata(obj, obj.si, obj.upt, md5=obj.md5)
            raise exceptions.HarborError(message='文件块rados写入失败:' + msg)

        return True

    def _save_one_file(self, obj, rados, offset:int, file):
        """
        向对象写入一个文件

        :param obj: 对象元数据
        :param rados: rados接口
        :param offset: 分片偏移量
        :param file: 文件
        :return:
            成功：True
            失败：raise HarborError
        """
        # 先更新元数据，后写rados数据
        # 更新文件修改时间和对象大小
        try:
            file_size = get_size(file)
        except AttributeError:
            raise exceptions.HarborError.from_error(
                exceptions.BadRequest(message='输入必须是一个文件'))

        new_size = offset + file_size # 分片数据写入后 对象偏移量大小
        if not self._update_obj_metadata(obj, size=new_size):
            raise exceptions.HarborError(message='修改对象元数据失败')

        # 存储文件
        try:
            ok, msg = rados.write_file(offset=offset, file=file)
        except Exception as e:
            ok = False
            msg = str(e)

        try_close_file(file)

        if not ok:
            # 手动回滚对象元数据
            self._update_obj_metadata(obj, obj.si, obj.upt)
            raise exceptions.HarborError(message='文件块rados写入失败:' + msg)

        return True

    @staticmethod
    def _update_obj_metadata(obj, size, upt=None, md5: str = ''):
        """
        更新对象元数据
        :param obj: 对象, obj实例不会被修改
        :param size: 对象大小
        :param upt: 修改时间
        :param md5: 更新对象元数据MD5值，默认为空忽略
        :return:
            success: True
            failed: False
        """
        if not upt:
            upt = timezone.now()

        model = obj._meta.model

        # 更新文件修改时间和对象大小
        old_size = obj.si if obj.si else 0
        new_size = max(size, old_size)  # 更新文件大小（只增不减）

        kwargs = {
            'si': Case(When(si__lt=new_size, then=Value(new_size)), default=F('si'), output_field=BigIntegerField()),
            'upt': upt
        }
        if md5 and len(md5) == 32:
            kwargs['md5'] = md5
        try:
            # r = model.objects.filter(id=obj.id, si=obj.si).update(si=new_size, upt=timezone.now())  # 乐观锁方式
            r = model.objects.filter(id=obj.id).update(**kwargs)
        except Exception as e:
            return False
        if r > 0:  # 更新行数
            return True

        return False

    def delete_object(self, bucket_name:str, obj_path:str, user=None):
        """
        删除一个对象

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param user: 用户，默认为None，如果给定用户只删除属于此用户的对象（只查找此用户的存储桶）
        :return:
            success: True
            failed:  raise HarborError

        :raise HarborError
        """
        path, filename = PathParser(filepath=obj_path).get_path_and_filename()
        if not bucket_name or not filename:
            raise exceptions.HarborError.from_error(
                exceptions.BadRequest(message='参数有误'))

        # 存储桶验证和获取桶对象
        try:
            bucket, fileobj = self.get_bucket_and_obj(bucket_name=bucket_name, obj_path=obj_path, user=user)
        except exceptions.HarborError as e:
            raise e

        # 桶锁操作检查
        if not bucket.lock_writeable():
            raise exceptions.BucketLockWrite()

        if fileobj is None:
            raise exceptions.HarborError.from_error(
                exceptions.NoSuchKey(message='文件对象不存在'))

        obj_key = fileobj.get_obj_key(bucket.id)
        old_id = fileobj.id
        # 先删除元数据，后删除rados对象（删除失败恢复元数据）
        if not fileobj.do_delete():
            raise exceptions.HarborError(message='删除对象原数据时错误')

        pool_name = bucket.get_pool_name()
        ho = build_harbor_object(using=bucket.ceph_using, pool_name=pool_name, obj_id=obj_key, obj_size=fileobj.si)
        ok, _ = ho.delete()
        if not ok:
            # 恢复元数据
            fileobj.id = old_id
            fileobj.do_save(force_insert=True)  # 仅尝试创建文档，不修改已存在文档
            raise exceptions.HarborError(message='删除对象rados数据时错误')

        return True

    def read_chunk(self, bucket_name:str, obj_path:str, offset:int, size:int, user=None):
        """
        从对象读取一个数据块

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param offset: 读起始偏移量
        :param size: 读取数据字节长度, 最大20Mb
        :param user: 用户，默认为None，如果给定用户只删除属于此用户的对象（只查找此用户的存储桶）
        :return:
            success: （chunk:bytes, obj）  #  数据块，对象元数据实例
            failed:   raise HarborError         # 读取失败，抛出HarborError

        :raise HarborError
        """
        if offset < 0 or size < 0 or size > 20 * 1024 ** 2:  # 20Mb
            raise exceptions.HarborError.from_error(
                exceptions.BadRequest(message='参数有误'))

        # 存储桶验证和获取桶对象
        try:
            bucket, obj = self.get_bucket_and_obj(bucket_name=bucket_name, obj_path=obj_path, user=user)
        except exceptions.HarborError as e:
            raise e

        # 桶锁操作检查
        if not bucket.lock_readable():
            raise exceptions.BucketLockWrite()

        if obj is None:
            raise exceptions.HarborError.from_error(
                exceptions.NoSuchKey(message='文件对象不存在'))

        # 自定义读取文件对象
        if size == 0:
            return bytes(), obj.si

        obj_key = obj.get_obj_key(bucket.id)
        pool_name = bucket.get_pool_name()
        rados = build_harbor_object(using=bucket.ceph_using, pool_name=pool_name, obj_id=obj_key, obj_size=obj.si)
        ok, chunk = rados.read(offset=offset, size=size)
        if not ok:
            raise exceptions.HarborError(message='文件块读取失败')

        # 如果从0读文件就增加一次下载次数
        if offset == 0:
            obj.download_cound_increase()

        return chunk, obj

    def get_obj_generator(self, bucket_name:str, obj_path:str, offset:int=0, end:int=None, per_size=10 * 1024 ** 2, user=None, all_public=False):
        """
        获取一个读取对象的生成器函数

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param offset: 读起始偏移量；type: int
        :param end: 读结束偏移量(包含)；type: int；默认None表示对象结尾；
        :param per_size: 每次读取数据块长度；type: int， 默认10Mb
        :param user: 用户，默认为None，如果给定用户只查属于此用户的对象（只查找此用户的存储桶）
        :param all_public: 默认False(忽略); True(查找所有公有权限存储桶);
        :return: (generator, object)
                for data in generator:
                    do something
        :raise HarborError
        """
        if per_size < 0 or per_size > 20 * 1024 ** 2:  # 20Mb
            per_size = 10 * 1024 ** 2   # 10Mb

        if offset < 0 or (end is not None and end < 0):
            raise exceptions.HarborError.from_error(
                exceptions.BadRequest(message='参数有误'))

        # 存储桶验证和获取桶对象
        try:
            bucket, obj = self.get_bucket_and_obj(bucket_name=bucket_name, obj_path=obj_path, user=user, all_public=all_public)
        except exceptions.HarborError as e:
            raise e

        # 桶锁操作检查
        if not bucket.lock_readable():
            raise exceptions.BucketLockWrite()

        if obj is None:
            raise exceptions.HarborError.from_error(
                exceptions.NoSuchKey(message='文件对象不存在'))

        # 增加一次下载次数
        if offset == 0:
            obj.download_cound_increase()

        generator = self._get_obj_generator(bucket=bucket, obj=obj, offset=offset, end=end, per_size=per_size)
        return generator, obj

    @staticmethod
    def _get_obj_generator(bucket, obj, offset:int=0, end:int=None, per_size=10 * 1024 ** 2):
        """
        获取一个读取对象的生成器函数

        :param bucket: 存储桶实例
        :param obj: 对象实例
        :param offset: 读起始偏移量；type: int
        :param end: 读结束偏移量(包含)；type: int；默认None表示对象结尾；
        :param per_size: 每次读取数据块长度；type: int， 默认10Mb
        :return: generator
                for data in generator:
                    do something
        :raise HarborError
        """
        # 读取文件对象生成器
        obj_key = obj.get_obj_key(bucket.id)
        pool_name = bucket.get_pool_name()
        rados = build_harbor_object(using=bucket.ceph_using, pool_name=pool_name, obj_id=obj_key, obj_size=obj.si)
        return rados.read_obj_generator(offset=offset, end=end, block_size=per_size)

    def get_write_generator(self, bucket_name: str, obj_path: str, is_break_point: bool = False, user=None):
        """
        获取一个写入对象的生成器函数

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param is_break_point: True(断点续传)，False(非断点续传)
        :param user: 用户，默认为None，如果给定用户只删除属于此用户的对象（只查找此用户的存储桶）
        :return:
                generator           # success
                :raise HarborError  # failed

        :usage:
            ok = next(generator)
            ok = generator.send((offset, bytes))  # ok = True写入成功， ok=False写入失败

        :raise HarborError
        """
        # 对象路径分析
        pp = PathParser(filepath=obj_path)
        path, filename = pp.get_path_and_filename()
        if not bucket_name or not filename:
            raise exceptions.HarborError.from_error(
                exceptions.BadRequest(message='参数有误'))

        if len(filename) > 255:
            raise exceptions.HarborError.from_error(
                exceptions.BadRequest(message='对象名称长度最大为255字符'))

        # 存储桶验证和获取桶对象
        bucket = self.get_bucket(bucket_name, user=user)
        if not bucket:
            raise exceptions.HarborError.from_error(
                exceptions.NoSuchBucket(message='存储桶不存在'))

        # 桶锁操作检查
        if not bucket.lock_writeable():
            raise exceptions.BucketLockWrite()

        collection_name = bucket.get_bucket_table_name()
        obj, created = self._get_obj_and_check_limit_or_create(collection_name, path, filename)
        obj_key = obj.get_obj_key(bucket.id)
        pool_name = bucket.get_pool_name()

        def generator():
            ok = True
            rados = build_harbor_object(using=bucket.ceph_using, pool_name=pool_name, obj_id=obj_key, obj_size=obj.si)
            if (created is False) and (not is_break_point):  # 对象已存在，不是新建的,非断点续传，重置对象大小
                self._pre_reset_upload(obj=obj, rados=rados)

            md5_handler = FileMD5Handler()
            hex_md5 = ''
            while True:
                offset, data = yield ok
                try:
                    if not is_break_point:      # 非断点续传计算MD5
                        md5_handler.update(offset=offset, data=data)
                        hex_md5 = md5_handler.hex_md5
                    ok = self._save_one_chunk(obj=obj, rados=rados, offset=offset, chunk=data, md5=hex_md5)
                except exceptions.HarborError:
                    ok = False

        return generator()

    @staticmethod
    def check_public_or_user_bucket(bucket, user, all_public):
        """
        公有桶或用户的桶

        :param bucket: 桶对象
        :param user: 用户，默认为None，如果给定用户只查找此用户的存储桶
        :param all_public: 默认False(忽略); True(查找所有公有权限存储桶);
        :return:
                success: bucket
                failed:   raise HarborError

        :raise HarborError
        """
        if all_public:
            if bucket.is_public_permission():   # 公有桶
                return bucket

        if user:
            if bucket.check_user_own_bucket(user):
                return bucket
            else:
                raise exceptions.HarborError.from_error(
                    exceptions.AccessDenied(message='无权限访问存储桶'))
        return bucket

    def get_bucket_and_obj_or_dir(self,bucket_name:str, path:str, user=None, all_public=False):
        """
        获取存储桶和对象或目录实例

        :param bucket_name: 桶名
        :param path: 对象全路径
        :param user: 用户，默认为None，如果给定用户只查找此用户的存储桶
        :param all_public: 默认False(忽略); True(查找所有公有权限存储桶);
        :return:
                success: （bucket, object） # obj == None表示对象或目录不存在
                failed:   raise HarborError # 存储桶不存在，或参数有误，或有错误发生

        :raise HarborError
        """
        pp = PathParser(filepath=path)
        dir_path, filename = pp.get_path_and_filename()
        if not bucket_name or not filename:
            raise exceptions.HarborError.from_error(
                exceptions.BadRequest(message='参数有误'))

        # 存储桶验证和获取桶对象
        bucket = self.get_bucket_by_name(bucket_name)
        if not bucket:
            raise exceptions.HarborError.from_error(
                exceptions.NoSuchBucket(message='存储桶不存在'))
        self.check_public_or_user_bucket(bucket=bucket, user=user, all_public=all_public)

        table_name = bucket.get_bucket_table_name()
        try:
            obj = self._get_obj_or_dir(table_name=table_name, path=dir_path, name=filename)
        except exceptions.HarborError as e:
            raise e
        except Exception as e:
            raise exceptions.HarborError(message=f'查询目录或对象错误，{str(e)}')

        if not obj:
            return bucket, None

        return bucket, obj

    def get_bucket_and_obj(self, bucket_name: str, obj_path: str, user=None, all_public=False):
        """
        获取存储桶和对象实例

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param user: 用户，默认为None，如果给定用户只查找此用户的存储桶
        :param all_public: 默认False(忽略); True(查找所有公有权限存储桶);
        :return:
                success: （bucket, obj）  # obj == None表示对象不存在
                failed:   raise HarborError # 存储桶不存在，或参数有误，或有错误发生

        :raise HarborError
        """
        bucket, obj = self.get_bucket_and_obj_or_dir(
            bucket_name=bucket_name, path=obj_path, user=user, all_public=all_public)
        if obj and obj.is_file():
            return bucket, obj

        return bucket, None

    def share_object(self, bucket_name:str, obj_path:str, share:int, days:int=0, password:str='', user=None):
        """
        设置对象共享或私有权限

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param user: 用户，默认为None，如果给定用户只查找此用户的存储桶
        :param share: 读写权限；0（禁止访问），1（只读），2（可读可写）
        :param days: 共享天数，0表示永久共享, <0表示不共享
        :param password: 共享密码
        :return:
            ok: bool, access_code: int      # ok == True: success; ok == False: failed

        :raise HarborError
        """
        bucket, obj = self.get_bucket_and_obj(bucket_name=bucket_name, obj_path=obj_path, user=user)
        # 桶锁操作检查
        if not bucket.lock_writeable():
            raise exceptions.BucketLockWrite()

        if obj is None:
            raise exceptions.HarborError.from_error(
                exceptions.NoSuchKey(message='对象不存在'))

        if obj.set_shared(share=share, days=days, password=password):
            return True, obj.get_access_permission_code(bucket)

        return False, obj.get_access_permission_code(bucket)

    def share_dir(self, bucket_name:str, path:str, share:int, days:int=0, password:str='', user=None):
        """
        设置目录共享或私有权限

        :param bucket_name: 桶名
        :param path: 目录全路径
        :param user: 用户，默认为None，如果给定用户只查找此用户的存储桶
        :param share: 读写权限；0（禁止访问），1（只读），2（可读可写）
        :param days: 共享天数，0表示永久共享, <0表示不共享
        :param password: 共享密码
        :return:
            ok: bool, access_code: int      # ok == True: success; ok == False: failed

        :raise HarborError
        """
        bucket, obj = self.get_bucket_and_obj_or_dir(bucket_name=bucket_name, path=path, user=user)
        # 桶锁操作检查
        if not bucket.lock_writeable():
            raise exceptions.BucketLockWrite()

        if not obj or obj.is_file():
            raise exceptions.HarborError.from_error(
                exceptions.NoSuchKey(message='目录不存在'))

        if obj.set_shared(share=share, days=days, password=password):
            return True, obj.get_access_permission_code(bucket)

        return False, obj.get_access_permission_code(bucket)

    def search_object_queryset(self, bucket, search: str, user):
        """
        检索对象查询集

        :param bucket: bucket名称或对象
        :param search: 搜索关键字
        :param user: 用户对象
        :return:
            Bucket(), Queryset()

        :raises: HarborError
        """
        if isinstance(bucket, str):
            # 存储桶验证和获取桶对象
            bucket = self.get_bucket_by_name(bucket)
            if not bucket:
                raise exceptions.HarborError.from_error(
                    exceptions.NoSuchBucket(message='存储桶不存在'))

        self.check_public_or_user_bucket(bucket=bucket, user=user, all_public=False)
        table_name = bucket.get_bucket_table_name()
        bfm = BucketFileManagement(path='', collection_name=table_name)
        try:
            queryset = bfm.get_search_object_queryset(search=search, contain_dir=False)
        except Exception as e:
            raise exceptions.HarborError(message=str(e))

        return bucket, queryset


class FtpHarborManager:
    """
    ftp操作harbor对象数据元数据管理接口封装
    """
    def __init__(self):
        self.__hbManager = HarborManager()

    @ftp_close_old_connections
    def ftp_authenticate(self, bucket_name:str, password:str):
        """
        Bucket桶ftp访问认证
        :return:    (ok:bool, permission:bool, msg:str)
            ok:         True，认证成功；False, 认证失败
            permission: True, 可读可写权限；False, 只读权限
            msg:        认证结果字符串
        """
        bucket = self.__hbManager.get_bucket(bucket_name)
        if not bucket:
            return False, False, 'Have no this bucket.'

        if not bucket.is_ftp_enable():
            return False, False, 'Bucket is not enable for ftp.'

        if bucket.check_ftp_password(password):
            return True, True, 'authenticate successfully'
        if bucket.check_ftp_ro_password(password):
            return True, False, 'authenticate successfully'

        return False, False, 'Wrong password'

    @ftp_close_old_connections
    def ftp_write_chunk(self, bucket_name:str, obj_path:str, offset:int, chunk:bytes, reset:bool=False):
        """
        向对象写入一个数据块

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param offset: 写入对象偏移量
        :param chunk: 要写入的数据
        :param reset: 为True时，先重置对象大小为0后再写入数据；
        :return:
                created             # created==True表示对象是新建的；created==False表示对象不是新建的
                raise HarborError   # 写入失败
        """
        return self.__hbManager.write_chunk(bucket_name=bucket_name, obj_path=obj_path, offset=offset, chunk=chunk)

    @ftp_close_old_connections
    def ftp_write_file(self, bucket_name:str, obj_path:str, offset:int, file, reset:bool=False, user=None):
        """
        向对象写入一个文件

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param offset: 写入对象偏移量
        :param file: 要写入的数据，已打开的类文件句柄（文件描述符）
        :param reset: 为True时，先重置对象大小为0后再写入数据；
        :param user: 用户，默认为None，如果给定用户只操作属于此用户的对象（只查找此用户的存储桶）
        :return:
                created             # created==True表示对象是新建的；created==False表示对象不是新建的
                raise HarborError   # 写入失败
        """
        return self.__hbManager.write_file(bucket_name=bucket_name, obj_path=obj_path, offset=offset,
                                           file=file, reset=reset, user=user)

    @ftp_close_old_connections
    def ftp_move_rename(self, bucket_name:str, obj_path:str, rename=None, move=None):
        """
        移动或重命名对象

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param rename: 重命名新名称，默认为None不重命名
        :param move: 移动到move路径下，默认为None不移动
        :return:
            success: object
            failed : raise HarborError

        :raise HarborError
        """
        return self.__hbManager.move_rename(bucket_name, obj_path=obj_path, rename=rename, move=move)

    @ftp_close_old_connections
    def ftp_rename(self, bucket_name:str, obj_path:str, rename):
        """
        重命名对象

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param rename: 重命名新名称
        :return:
            success: object
            failed : raise HarborError

        :raise HarborError
        """
        return self.__hbManager.move_rename(bucket_name, obj_path=obj_path, rename=rename)

    @ftp_close_old_connections
    def ftp_read_chunk(self, bucket_name:str, obj_path:str, offset:int, size:int):
        """
        从对象读取一个数据块

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param offset: 读起始偏移量
        :param size: 读取数据字节长度, 最大20Mb
        :return:
            success: （chunk:bytes, object）  #  数据块，对象元数据实例
            failed:   raise HarborError         # 读取失败，抛出HarborError

        :raise HarborError
        """
        return self.__hbManager.read_chunk(bucket_name=bucket_name, obj_path=obj_path, offset=offset, size=size)

    @ftp_close_old_connections
    def ftp_get_obj_generator(self, bucket_name:str, obj_path:str, offset:int=0, end:int=None, per_size=10 * 1024 ** 2):
        """
        获取一个读取对象的生成器函数

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param offset: 读起始偏移量；type: int
        :param end: 读结束偏移量(包含)；type: int；默认None表示对象结尾；
        :param per_size: 每次读取数据块长度；type: int， 默认10Mb
        :return: (generator, object)
                for data in generator:
                    do something
        :raise HarborError
        """
        return self.__hbManager.get_obj_generator(
            bucket_name=bucket_name, obj_path=obj_path, offset=offset, end=end, per_size=per_size)

    @ftp_close_old_connections
    def ftp_delete_object(self, bucket_name:str, obj_path:str):
        """
        删除一个对象

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :return:
            success: True
            failed:  raise HarborError

        :raise HarborError
        """
        return self.__hbManager.delete_object(bucket_name=bucket_name, obj_path=obj_path)

    @ftp_close_old_connections
    def ftp_list_dir(self, bucket_name:str, path:str, offset:int=0, limit:int=1000):
        """
        获取目录下的文件列表信息

        :param bucket_name: 桶名
        :param path: 目录路径
        :param offset: 目录下文件列表偏移量
        :param limit: 获取文件信息数量
        :return:
                success:    (list[object, object,], bucket) # list和bucket实例
                failed:      raise HarborError

        :raise HarborError
        """
        return self.__hbManager.list_dir(bucket_name, path, offset=offset, limit=limit)

    @ftp_close_old_connections
    def ftp_list_dir_generator(self, bucket_name:str, path:str, per_num:int=1000):
        """
        获取目录下的文件列表信息生成器

        :param bucket_name: 桶名
        :param path: 目录路径
        :param per_num: 每次获取文件信息数量
        :return:
                generator           # success
                :raise HarborError  # failed
        :usage:
            for objs in generator:
                print(objs)         # objs type list, raise HarborError when error

        :raise HarborError
        """
        return self.__hbManager.list_dir_generator(bucket_name=bucket_name, path=path, per_num=per_num)

    @ftp_close_old_connections
    def ftp_mkdir(self, bucket_name:str, path:str):
        """
        创建一个目录
        :param bucket_name: 桶名
        :param path: 目录全路径
        :return:
            True, dir: success
            raise HarborError: failed
        :raise HarborError
        """
        return self.__hbManager.mkdir(bucket_name=bucket_name, path=path)

    @ftp_close_old_connections
    def ftp_rmdir(self, bucket_name:str, path:str):
        """
        删除一个空目录
        :param bucket_name:桶名
        :param path: 目录全路径
        :return:
            True: success
            raise HarborError(): failed

        :raise HarborError()
        """
        return self.__hbManager.rmdir(bucket_name, path)

    @ftp_close_old_connections
    def ftp_is_dir(self, bucket_name:str, path_name:str):
        """
        是否时一个目录

        :param bucket_name: 桶名
        :param path_name: 目录路径
        :return:
            true: is dir
            false: is file

        :raise HarborError  # 桶或路径不存在，发生错误
        """
        return self.__hbManager.is_dir(bucket_name=bucket_name, path_name=path_name)

    @ftp_close_old_connections
    def ftp_is_file(self, bucket_name:str, path_name:str):
        """
        是否时一个文件
        :param bucket_name: 桶名
        :param path_name: 文件路径
        :return:
            true: is file
            false: is dir

        :raise HarborError  # 桶或路径不存在，发生错误
        """
        return self.__hbManager.is_file(bucket_name=bucket_name, path_name=path_name)

    @ftp_close_old_connections
    def ftp_get_obj(self, bucket_name: str, path_name: str):
        """
        获取对象或目录实例

        :param bucket_name: 桶名
        :param path_name: 文件路径
        :return:
            success: 对象实例

        :raise HarborError
        """
        return self.__hbManager.get_object(bucket_name=bucket_name, path_name=path_name)

    @ftp_close_old_connections
    def ftp_get_obj_size(self, bucket_name: str, path_name: str):
        """
        获取对象大小

        :param bucket_name: 桶名
        :param path_name: 文件路径
        :return:
            int: 对象大小

        :raise HarborError  # 对象不存在, 不是文件是目录
        """
        obj = self.__hbManager.get_object(bucket_name=bucket_name, path_name=path_name)
        if obj.is_file():
            return obj.obj_size

        raise exceptions.HarborError.from_error(
            exceptions.BadRequest(message='目标是一个目录'))

    @ftp_close_old_connections
    def ftp_get_write_generator(self, bucket_name: str, obj_path: str, is_break_point: bool):
        """
        获取一个写入对象的生成器函数

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param is_break_point: True(断点续传)，False(非断点续传)
        :return:
                generator           # success
                :raise HarborError  # failed

        :usage:
            ok = next(generator)
            ok = generator.send((offset, bytes))  # ok = True写入成功， ok=False写入失败

        :raise HarborError
        """
        return self.__hbManager.get_write_generator(bucket_name=bucket_name, obj_path=obj_path,
                                                    is_break_point=is_break_point)
