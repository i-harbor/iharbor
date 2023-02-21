from collections import OrderedDict

from django.utils import timezone
from django.db.models import Case, Value, When, F
from django.db.models import BigIntegerField
from django.utils.translation import gettext
from django.conf import settings

from buckets.models import Bucket, get_str_hexMD5
from buckets.utils import BucketFileManagement
from utils.md5 import S3ObjectMultipartETagHandler
from utils.oss.pyrados import HarborObject
from utils.oss.shortcuts import build_harbor_object
from utils.storagers import PathParser
from api import exceptions as iharbor_errors
from . import exceptions
from .models import MultipartUpload


S3_MULTIPART_UPLOAD_MAX_SIZE = getattr(settings, 'S3_MULTIPART_UPLOAD_MAX_SIZE', 5 * 1024 ** 3)     # default 5GB
S3_MULTIPART_UPLOAD_MIN_SIZE = getattr(settings, 'S3_MULTIPART_UPLOAD_MIN_SIZE', 5 * 1024 ** 2)     # default 5MB


class HarborManager:
    """
    操作harbor对象数据和元数据管理接口封装
    """

    def get_bucket(self, bucket_name: str, user=None):
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

    def get_public_or_user_bucket(self, name: str, user, all_public: bool = False):
        """
        获取公有权限桶或用户的桶
        :param name: 桶名
        :param user: 用户对象
        :param all_public: 默认False(忽略); True(查找所有公有权限存储桶);
        :return:
            Bucket()
        :raises: S3Error
        """
        bucket = self.get_bucket_by_name(name)
        if not bucket:
            raise exceptions.S3NoSuchBucket('存储桶不存在')

        self.check_public_or_user_bucket(bucket=bucket, user=user, all_public=all_public)
        return bucket

    def is_dir(self, bucket_name: str, path_name: str):
        """
        是否时一个目录

        :param bucket_name: 桶名
        :param path_name: 目录路径
        :return:
            true: is dir
            false: is file

        :raises: S3Error  # 桶或路径不存在，发生错误
        """
        # path为空或根目录时
        if not path_name or path_name == '/':
            return True

        obj = self.get_object(bucket_name, path_name)
        if obj.is_dir():
            return True

        return False

    def is_file(self, bucket_name: str, path_name: str):
        """
        是否时一个文件
        :param bucket_name: 桶名
        :param path_name: 文件路径
        :return:
            true: is file
            false: is dir

        :raise S3Error  # 桶或路径不存在，发生错误
        """
        # path为空或根目录时
        if not path_name or path_name == '/':
            return False

        obj = self.get_object(bucket_name, path_name)
        if obj.is_file():
            return True

        return False

    def get_object(self, bucket_name: str, path_name: str, user=None):
        """
        获取对象或目录实例

        :param bucket_name: 桶名
        :param path_name: 文件路径
        :param user: 用户，默认为None，如果给定用户只获取属于此用户的对象（只查找此用户的存储桶）
        :return:
            obj or dir    # 对象或目录实例

        :raise S3Error
        """
        bucket, obj = self.get_bucket_and_obj_or_dir(bucket_name=bucket_name, path=path_name, user=user)
        if obj is None:
            raise exceptions.S3NoSuchKey('指定对象或目录不存在')

        return obj

    @staticmethod
    def get_metadata_obj(table_name: str, path: str):
        """
        直接获取目录或对象元数据对象，不检查父节点是否存在

        :param table_name: 数据库表名
        :param path: 目录或对象的全路径
        :return:
            obj     #
            None    #
        :raises: S3Error
        """
        bfm = BucketFileManagement(collection_name=table_name)
        try:
            obj = bfm.get_obj(path=path)
        except Exception as e:
            raise exceptions.S3InternalError(str(e))
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

        :raises: S3Error
        """
        bfm = BucketFileManagement(path=path, collection_name=table_name)
        try:
            obj = bfm.get_dir_or_obj_exists(name=name)
        except iharbor_errors.NoParentPath as e:
            raise exceptions.S3NoSuchKey(str(e))
        except Exception as e:
            raise exceptions.S3InternalError(str(e))

        if obj:
            return obj, bfm  # 目录或对象存在

        return None, bfm  # 目录或对象不存在

    def _get_obj_or_dir(self, table_name, path, name):
        """
        获取文件对象或目录

        :param table_name: 数据库表名
        :param path: 父目录路经
        :param name: 对象名称或目录名称
        :return:
            obj or None: 目录或对象不存在

        :raises: S3Error
        """
        obj, _ = self._get_obj_or_dir_and_bfm(table_name, path, name)
        return obj

    @staticmethod
    def mkdir_metadata(table_name: str, p_id: int, dir_path_name: str):
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
        if len(dir_name) > 255:
            raise exceptions.S3InvalidSuchKey(_('目录名称长度最大为255字符'))

        bfm = BucketFileManagement(collection_name=table_name)
        object_class = bfm.get_obj_model_class()
        dir1 = object_class(na=dir_path_name,  # 全路经目录名
                            name=dir_name,  # 目录名
                            fod=False,  # 目录
                            did=p_id)  # 父目录id
        try:
            dir1.save(force_insert=True)  # 仅尝试创建文档，不修改已存在文档
        except Exception as e:
            raise exceptions.S3InternalError('创建目录元数据错误')

        return dir1

    def mkdir(self, bucket_name: str, path: str, user=None):
        """
        创建一个目录
        :param bucket_name: 桶名
        :param path: 目录全路径
        :param user: 用户，默认为None，如果给定用户只创建属于此用户的目录（只查找此用户的存储桶）
        :return:
            True, dir: success
            raise S3Error: failed

        :raise S3Error
        """
        validated_data = self.validate_mkdir_params(bucket_name, path, user)
        did = validated_data.get('did', None)
        collection_name = validated_data.get('collection_name')

        dir1 = self.mkdir_metadata(table_name=collection_name, p_id=did, dir_path_name=path)
        return True, dir1

    def validate_mkdir_params(self, bucket_name: str, dirpath: str, user=None):
        """
        post_detail参数验证

        :return:
                success: {data}
                failure: raise S3Error

        :raise S3Error
        """
        data = {}
        dir_path, dir_name = PathParser(filepath=dirpath).get_path_and_filename()

        if not bucket_name or not dir_name:
            raise exceptions.S3InvalidRequest('目录路径参数无效，要同时包含有效的存储桶和目录名称')

        if '/' in dir_name:
            raise exceptions.S3InvalidRequest('目录名称不能包含‘/’')

        if len(dir_name) > 255:
            raise exceptions.S3InvalidRequest('目录名称长度最大为255字符')

        # bucket是否属于当前用户,检测存储桶名称是否存在
        bucket = self.get_bucket(bucket_name, user=user)
        if not isinstance(bucket, Bucket):
            raise exceptions.S3NoSuchBucket()

        _collection_name = bucket.get_bucket_table_name()
        data['collection_name'] = _collection_name

        dir1, bfm = self._get_obj_or_dir_and_bfm(table_name=_collection_name, path=dir_path, name=dir_name)

        if dir1:
            # 目录已存在
            if dir1.is_dir():
                raise exceptions.DirectoryAlreadyExists(f'"{dir_name}"目录已存在')

            # 同名对象已存在
            if dir1.is_file():
                raise exceptions.ObjectKeyAlreadyExists(f'"指定目录名称{dir_name}"已存在重名对象，请重新指定一个目录名称')

        data['did'] = bfm.cur_dir_id if bfm.cur_dir_id else bfm.get_cur_dir_id()[-1]
        data['bucket_name'] = bucket_name
        data['dir_path'] = dir_path
        data['dir_name'] = dir_name
        return data

    def create_path(self, table_name, path: str):
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
                raise exceptions.S3InternalError(str(e))
            if not obj:
                continue

            if obj.is_file():
                raise exceptions.S3InvalidSuchKey(
                    message="The path of the object's key conflicts with the existing object's key")

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
            dir1 = self.mkdir_metadata(table_name=table_name, p_id=p_id, dir_path_name=dir_path_name)
            dirs.append(dir1)
            now_last_dir = dir1

        return dirs

    def rmdir(self, bucket_name: str, dirpath: str, user=None):
        """
        删除一个空目录
        :param bucket_name:桶名
        :param dirpath: 目录全路径
        :param user: 用户，默认为None，如果给定用户只删除属于此用户的目录（只查找此用户的存储桶）
        :return:
            True: success
            raise S3Error(): failed

        :raise S3Error()
        """
        path, dir_name = PathParser(filepath=dirpath).get_path_and_filename()

        if not bucket_name or not dir_name:
            raise exceptions.S3InvalidRequest('参数无效')

        # bucket是否属于当前用户,检测存储桶名称是否存在
        bucket = self.get_bucket(bucket_name, user=user)
        if not isinstance(bucket, Bucket):
            raise exceptions.S3NoSuchBucket('存储桶不存在')

        # 桶锁操作检查
        if not bucket.lock_writeable():
            raise exceptions.S3BucketLockWrite()

        table_name = bucket.get_bucket_table_name()
        dir1, bfm = self._get_obj_or_dir_and_bfm(table_name=table_name, path=path, name=dir_name)

        if not dir1 or dir1.is_file():
            raise exceptions.S3NoSuchKey('目录不存在')

        if not bfm.dir_is_empty(dir1):
            raise exceptions.S3InvalidRequest('无法删除非空目录')

        if not dir1.do_delete():
            raise exceptions.S3InternalError('删除目录元数据失败')

        return True

    def _list_dir_queryset(self, bucket_name: str, path: str, user=None):
        """
        获取目录下的文件列表信息

        :param bucket_name: 桶名
        :param path: 目录路径
        :param user: 用户，默认为None，如果给定用户只获取属于此用户的目录下的文件列表信息（只查找此用户的存储桶）
        :return:
                success:    (QuerySet(), bucket) # django QuerySet实例和bucket实例
                failed:      raise S3Error

        :raise S3Error
        """
        bucket = self.get_bucket(bucket_name, user)
        if not bucket:
            raise exceptions.S3NoSuchBucket('存储桶不存在')

        collection_name = bucket.get_bucket_table_name()
        bfm = BucketFileManagement(path=path, collection_name=collection_name)
        ok, qs = bfm.get_cur_dir_files()
        if not ok:
            raise exceptions.S3NotFound('未找到相关记录')
        return qs, bucket

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
            failed : raise S3Error

        :raise S3Error
        """
        path, filename = PathParser(filepath=obj_path).get_path_and_filename()
        if not bucket_name or not filename:
            raise exceptions.S3InvalidRequest('参数有误')

        move_to, rename = self._validate_move_rename_params(move_to=move, rename=rename)
        if move_to is None and rename is None:
            raise exceptions.S3InvalidRequest('请至少提交一个要执行操作的参数')

        # 存储桶验证和获取桶对象
        bucket, obj = self.get_bucket_and_obj(bucket_name=bucket_name, obj_path=obj_path, user=user)
        if obj is None:
            raise exceptions.S3NoSuchKey('文件对象不存在')

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
            failed : raise S3Error

        :raise S3Error
        """
        table_name = bucket.get_bucket_table_name()
        new_obj_name = rename if rename else obj.name  # 移动后对象的名称，对象名称不变或重命名

        # 检查是否符合移动或重命名条件，目标路径下是否已存在同名对象或子目录
        try:
            if move_to is None:  # 仅仅重命名对象，不移动
                path, _ = PathParser(filepath=obj.na).get_path_and_filename()
                new_na = path + '/' + new_obj_name if path else new_obj_name
                bfm = BucketFileManagement(path=path, collection_name=table_name)
                target_obj = bfm.get_obj(path=new_na)
            else:  # 需要移动对象
                bfm = BucketFileManagement(path=move_to, collection_name=table_name)
                try:
                    target_obj = bfm.get_dir_or_obj_exists(name=new_obj_name)
                except iharbor_errors.NoParentPath as e:
                    raise exceptions.S3InvalidRequest('无法完成对象的移动操作，指定的目标路径不存在')

                new_na = bfm.build_dir_full_name(new_obj_name)
        except Exception as e:
            raise exceptions.S3InternalError('移动对象操作失败, 查询是否已存在同名对象或子目录时发生错误')

        if target_obj:
            raise exceptions.S3InvalidRequest('无法完成对象的移动操作，指定的目标路径下已存在同名的对象或目录')

        # 仅仅重命名对象，不移动
        if move_to is not None:  # 移动对象或重命名
            _, did = bfm.get_cur_dir_id()
            obj.did = did

        obj.na = new_na
        obj.name = new_obj_name
        obj.reset_na_md5()
        if not obj.do_save():
            raise exceptions.S3InternalError('移动对象操作失败')

        return obj, bucket

    @staticmethod
    def _validate_move_rename_params(move_to, rename):
        """
        校验移动或重命名参数

        :return:
                (move_to, rename)
                move_to # None 或 string
                rename  # None 或 string

        :raise S3Error
        """
        # 移动对象参数
        if move_to is not None:
            move_to = move_to.strip('/')

        # 重命名对象参数
        if rename is not None:
            if '/' in rename:
                raise exceptions.S3InvalidRequest('对象名称不能含“/”')

            if len(rename) > 255:
                raise exceptions.S3InvalidRequest('对象名称不能大于255个字符长度')

        return move_to, rename

    def create_empty_obj(self, bucket_name: str, obj_path: str, user):
        """
        创建一个空对象

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param user: 用户，默认为None，如果给定用户只操作属于此用户的对象（只查找此用户的存储桶）
        :return:
                (bucket, obj, False) # 对象已存在
                (bucket, obj, True)  # 对象不存在，创建一个新对象
                raise S3Error        # 有错误，路径不存在，或已存在同名目录

        :raise S3Error
        """
        # 存储桶验证和获取桶对象
        bucket = self.get_bucket(bucket_name, user=user)
        if not bucket:
            raise exceptions.S3NoSuchBucket('存储桶不存在')

        # 桶锁操作检查
        if not bucket.lock_writeable():
            raise exceptions.S3BucketLockWrite()

        collection_name = bucket.get_bucket_table_name()
        obj, created = self.get_or_create_obj(collection_name, obj_path)
        return bucket, obj, created

    def get_or_create_obj(self, table_name: str, obj_path_name: str):
        """
        获取或创建空对象

        :param table_name: bucket的对象元数据table
        :param obj_path_name: 对象全路径
        :return:
            (obj, False) # 对象已存在
            (obj, True)  # 对象不存在，新创建的对象

        :raise S3Error
        """
        pp = PathParser(filepath=obj_path_name)
        path, filename = pp.get_path_and_filename()
        if len(filename) > 255:
            raise exceptions.S3InvalidRequest('对象名称长度最大为255字符')

        obj, created = self._get_or_create_path_obj(table_name, path, filename)
        return obj, created

    def ensure_path_and_no_same_name_dir(self, table_name: str, obj_path_name: str):
        """
        确保对象父路径存在（不存在创建）并且没有同名的目录存在，确保满足创建整个对象路径的条件

        :param table_name: bucket的对象元数据table
        :param obj_path_name: 对象全路径
        :return:
            True
            False

        :raise S3Error
        """
        pp = PathParser(filepath=obj_path_name)
        path, filename = pp.get_path_and_filename()
        if len(filename) > 255:
            raise exceptions.S3InvalidRequest('对象名称长度最大为255字符')

        self.create_path_get_obj(table_name=table_name, path=path, filename=filename)
        return True

    def create_path_get_obj(self, table_name, path, filename):
        """
        路径不存在则创建路径，获取对象元数据

        :param table_name: 桶对应的数据库表名
        :param path: 文件对象所在的父路径
        :param filename: 文件对象名称
        :return:
                obj, did            # 已存在, did是path节点id
                None, did           # 不存在, did是path节点id
                raise S3Error       # 已存在同名目录

        :raise S3Error
        """
        did = None
        # 有路径，创建整个路径
        if path:
            dirs = self.create_path(table_name=table_name, path=path)
            base_dir = dirs[-1]
            did = base_dir.id

        bfm = BucketFileManagement(path=path, collection_name=table_name)
        try:
            obj = bfm.get_dir_or_obj_exists(name=filename, check_path_exists=False)
        except Exception as e:
            raise exceptions.S3InternalError(f'查询对象错误，{str(e)}')

        if obj is None:
            if not did:
                did = bfm.get_cur_dir_id()

            return None, did

        # 文件对象已存在
        if obj.is_file():
            return obj, obj.did

        # 已存在同名的目录
        if obj.is_dir():
            raise exceptions.S3InvalidRequest('指定的对象名称与已有的目录重名，请重新指定一个名称')

    def _get_or_create_path_obj(self, table_name, path, filename):
        """
        获取文件对象, 不存在则创建路径和对象元数据

        :param table_name: 桶对应的数据库表名
        :param path: 文件对象所在的父路径
        :param filename: 文件对象名称
        :return:
                (obj, False) # 对象已存在
                (obj, True)  # 对象不存在，创建一个新对象

        :raise S3Error
        """
        obj, did = self.create_path_get_obj(table_name=table_name, path=path, filename=filename)
        if obj:
            return obj, False

        # 创建文件对象
        bfm = BucketFileManagement(path=path, collection_name=table_name)
        obj_model_class = bfm.get_obj_model_class()
        full_filename = bfm.build_dir_full_name(filename)
        now_time = timezone.now()
        obj = obj_model_class(na=full_filename,  # 全路径文件名
                              name=filename,  # 文件名
                              fod=True,  # 文件
                              did=did,  # 父节点id
                              si=0,  # 文件大小
                              ult=now_time, upt=now_time)
        try:
            obj.save()
        except Exception as e:
            raise exceptions.S3InternalError('新建对象元数据失败')

        return obj, True

    @staticmethod
    def _pre_reset_upload(bucket, obj, rados):
        """
        覆盖上传前的一些操作

        :param bucket: 桶实例
        :param obj: 文件对象元数据
        :param rados: rados接口类对象
        :return:
                正常：True
                错误：raise S3Error
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
            raise exceptions.S3InternalError('修改对象元数据失败')

        # multipart delete need
        try:
            MultipartUploadManager.delete_multipart_upload_by_bucket_obj(bucket=bucket, obj=obj)
            ok, _ = rados.delete()
            if not ok:
                raise exceptions.S3InternalError('rados文件对象删除失败')
        except exceptions.S3Error as exc:
            # 恢复元数据
            obj.ult = old_ult
            obj.si = old_size
            obj.md5 = old_md5
            obj.do_save(update_fields=['ult', 'si', 'md5'])
            raise exc

        return True

    def _save_one_chunk(self, obj, rados, offset: int, chunk: bytes):
        """
        保存一个上传的分片

        :param obj: 对象元数据
        :param rados: rados接口
        :param offset: 分片偏移量
        :param chunk: 分片数据
        :return:
            成功：True
            失败：raise S3Error
        """
        # 先更新元数据，后写rados数据
        # 更新文件修改时间和对象大小
        new_size = offset + len(chunk)  # 分片数据写入后 对象偏移量大小
        if not self._update_obj_metadata(obj, size=new_size):
            raise exceptions.S3InternalError('修改对象元数据失败')

        # 存储文件块
        try:
            ok, msg = rados.write(offset=offset, data_block=chunk)
        except Exception as e:
            ok = False
            msg = str(e)

        if not ok:
            # 手动回滚对象元数据
            self._update_obj_metadata(obj, obj.si, obj.upt)
            raise exceptions.S3InternalError('文件块rados写入失败:' + msg)

        return True

    @staticmethod
    def _update_obj_metadata(obj, size, upt=None):
        """
        更新对象元数据
        :param obj: 对象, obj实例不会被修改
        :param size: 对象大小
        :param upt: 修改时间
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
        try:
            # r = model.objects.filter(id=obj.id, si=obj.si).update(si=new_size, upt=timezone.now())  # 乐观锁方式
            r = model.objects.filter(id=obj.id).update(
                si=Case(When(si__lt=new_size, then=Value(new_size)),
                        default=F('si'), output_field=BigIntegerField()),
                upt=upt)
        except Exception as e:
            return False
        if r > 0:  # 更新行数
            return True

        return False

    @staticmethod
    def update_obj_metadata_time(obj, create_time=None, modified_time=None):
        """
        更新对象元数据时间

        :return:
            object model instance
        :raises: S3Error
        """
        if create_time is not None:
            obj.ult = create_time

        if modified_time is not None:
            obj.upt = modified_time

        try:
            obj.save(update_fields=['ult', 'upt'])
        except Exception as e:
            raise exceptions.S3InternalError(f'更新对象元数据上传和修改时间失败, {str(e)}')

        return obj

    def delete_object(self, bucket_name: str, obj_path: str, user=None):
        """
        删除一个对象

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param user: 用户，默认为None，如果给定用户只删除属于此用户的对象（只查找此用户的存储桶）
        :return:
            success: True
            failed:  raise S3Error

        :raise S3Error
        """
        path, filename = PathParser(filepath=obj_path).get_path_and_filename()
        if not bucket_name or not filename:
            raise exceptions.S3InvalidRequest('参数有误')

        # 存储桶验证和获取桶对象
        bucket, fileobj = self.get_bucket_and_obj(bucket_name=bucket_name, obj_path=obj_path, user=user)

        # 桶锁操作检查
        if not bucket.lock_writeable():
            raise exceptions.S3BucketLockWrite()

        if fileobj is None:
            raise exceptions.S3NoSuchKey('文件对象不存在')

        return self.do_delete_obj_or_dir(bucket, fileobj)

    def read_chunk(self, bucket_name: str, obj_path: str, offset: int, size: int, user=None):
        """
        从对象读取一个数据块

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param offset: 读起始偏移量
        :param size: 读取数据字节长度, 最大20Mb
        :param user: 用户，默认为None，如果给定用户只删除属于此用户的对象（只查找此用户的存储桶）
        :return:
            success: （chunk:bytes, obj）  #  数据块，对象元数据实例
            failed:   raise S3Error         # 读取失败

        :raise S3Error
        """
        if offset < 0 or size < 0 or size > 20 * 1024 ** 2:  # 20Mb
            raise exceptions.S3InvalidRequest('参数有误')

        # 存储桶验证和获取桶对象
        bucket, obj = self.get_bucket_and_obj(bucket_name=bucket_name, obj_path=obj_path, user=user)

        if obj is None:
            raise exceptions.S3NoSuchKey('文件对象不存在')

        # 自定义读取文件对象
        if size == 0:
            return bytes(), obj.si

        rados = self.get_obj_rados(bucket=bucket, obj=obj)
        ok, chunk = rados.read(offset=offset, size=size)
        if not ok:
            raise exceptions.S3InternalError('文件块读取失败')

        # 如果从0读文件就增加一次下载次数
        if offset == 0:
            obj.download_cound_increase()

        return chunk, obj

    def get_obj_generator(
            self, bucket_name: str, obj_path: str, offset: int = 0, end: int = None,
            per_size=10 * 1024 ** 2, user=None, all_public=False
    ):
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
        :raise S3Error
        """
        if per_size < 0 or per_size > 20 * 1024 ** 2:  # 20Mb
            per_size = 10 * 1024 ** 2  # 10Mb

        if offset < 0 or (end is not None and end < 0):
            raise exceptions.S3InvalidRequest('参数有误')

        # 存储桶验证和获取桶对象
        bucket, obj = self.get_bucket_and_obj(
            bucket_name=bucket_name, obj_path=obj_path, user=user, all_public=all_public)

        if obj is None:
            raise exceptions.S3NoSuchKey('文件对象不存在')

        # 增加一次下载次数
        obj.download_cound_increase()
        generator = self._get_obj_generator(bucket=bucket, obj=obj, offset=offset, end=end, per_size=per_size)
        return generator, obj

    @staticmethod
    def _get_obj_generator(bucket, obj, offset: int = 0, end: int = None, per_size=10 * 1024 ** 2):
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
        :raise S3Error
        """
        rados = HarborManager.get_obj_rados(bucket=bucket, obj=obj)
        return rados.read_obj_generator(offset=offset, end=end, block_size=per_size)

    def get_write_generator(self, bucket_name: str, obj_path: str, user=None):
        """
        获取一个写入对象的生成器函数

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param user: 用户，默认为None，如果给定用户只删除属于此用户的对象（只查找此用户的存储桶）
        :return:
                generator           # success
                :raise S3Error  # failed

        :usage:
            ok = next(generator)
            ok = generator.send((offset, bytes))  # ok = True写入成功， ok=False写入失败

        :raise S3Error
        """
        # 对象路径分析
        pp = PathParser(filepath=obj_path)
        path, filename = pp.get_path_and_filename()
        if not bucket_name or not filename:
            raise exceptions.S3InvalidRequest('参数有误')

        if len(filename) > 255:
            raise exceptions.S3InvalidRequest('对象名称长度最大为255字符')

        # 存储桶验证和获取桶对象
        bucket = self.get_bucket(bucket_name, user=user)
        if not bucket:
            raise exceptions.S3NoSuchBucket('存储桶不存在')

        collection_name = bucket.get_bucket_table_name()
        obj, created = self._get_or_create_path_obj(collection_name, path, filename)
        obj_key = obj.get_obj_key(bucket.id)
        pool_name = obj.get_pool_name()

        return self.__write_generator(bucket=bucket, pool_name=pool_name, obj_rados_key=obj_key,
                                      obj=obj, created=created)

    def __write_generator(self, bucket, pool_name, obj_rados_key, obj, created):
        ok = True
        pool_id = obj.get_pool_id()
        rados = build_harbor_object(using=str(pool_id), pool_name=pool_name, obj_id=obj_rados_key, obj_size=obj.si)
        if created is False:  # 对象已存在，不是新建的,重置对象大小
            self._pre_reset_upload(bucket=bucket, obj=obj, rados=rados)

        while True:
            offset, data = yield ok
            try:
                ok = self._save_one_chunk(obj=obj, rados=rados, offset=offset, chunk=data)
            except exceptions.S3Error:
                ok = False

    @staticmethod
    def check_public_or_user_bucket(bucket, user, all_public):
        """
        公有桶或用户的桶

        :param bucket: 桶对象
        :param user: 用户，默认为None，如果给定用户只查找此用户的存储桶
        :param all_public: 默认False(忽略); True(查找所有公有权限存储桶);
        :return:
                success: bucket
                failed:   raise S3Error

        :raise S3Error
        """
        if all_public:
            if bucket.is_public_permission():  # 公有桶
                return bucket

        if user:
            if bucket.check_user_own_bucket(user):
                return bucket
            else:
                raise exceptions.S3AccessDenied('无权限访问存储桶')
        return bucket

    def get_bucket_and_obj_or_dir(self, bucket_name: str, path: str, user=None, all_public=False):
        """
        获取存储桶和对象或目录实例

        :param bucket_name: 桶名
        :param path: 对象全路径
        :param user: 用户，默认为None，如果给定用户只查找此用户的存储桶
        :param all_public: 默认False(忽略); True(查找所有公有权限存储桶);
        :return:
                success: （bucket, object） # obj == None表示对象或目录不存在
                failed:   raise S3Error # 存储桶不存在，或参数有误，或有错误发生

        :raise S3Error
        """
        pp = PathParser(filepath=path)
        dir_path, filename = pp.get_path_and_filename()
        if not bucket_name:
            raise exceptions.S3InvalidRequest('bucket_name参数有误')

        # 存储桶验证和获取桶对象
        bucket = self.get_public_or_user_bucket(name=bucket_name, user=user, all_public=all_public)

        if not dir_path and not filename:
            root_dir = BucketFileManagement().root_dir()
            return bucket, root_dir

        table_name = bucket.get_bucket_table_name()
        try:
            obj = self._get_obj_or_dir(table_name=table_name, path=dir_path, name=filename)
        except exceptions.S3Error as e:
            raise e
        except Exception as e:
            raise exceptions.S3InternalError(f'查询目录或对象错误，{str(e)}')

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
                failed:   raise S3Error # 存储桶不存在，或参数有误，或有错误发生

        :raise S3Error
        """
        bucket, obj = self.get_bucket_and_obj_or_dir(
            bucket_name=bucket_name, path=obj_path, user=user, all_public=all_public)
        if obj and obj.is_file():
            return bucket, obj

        return bucket, None

    def share_object(self, bucket_name: str, obj_path: str, share: int, days: int = 0, password: str = '', user=None):
        """
        设置对象共享或私有权限

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param user: 用户，默认为None，如果给定用户只查找此用户的存储桶
        :param share: 读写权限；0（禁止访问），1（只读），2（可读可写）
        :param days: 共享天数，0表示永久共享, <0表示不共享
        :param password: 共享密码
        :return:
            success: True
            failed: False

        :raise S3Error
        """
        bucket, obj = self.get_bucket_and_obj(bucket_name=bucket_name, obj_path=obj_path, user=user)
        if obj is None:
            raise exceptions.S3NoSuchKey('对象不存在')

        if obj.set_shared(share=share, days=days, password=password):
            return True

        return False

    def share_dir(self, bucket_name: str, path: str, share: int, days: int = 0, password: str = '', user=None):
        """
        设置目录共享或私有权限

        :param bucket_name: 桶名
        :param path: 目录全路径
        :param user: 用户，默认为None，如果给定用户只查找此用户的存储桶
        :param share: 读写权限；0（禁止访问），1（只读），2（可读可写）
        :param days: 共享天数，0表示永久共享, <0表示不共享
        :param password: 共享密码
        :return:
            success: True
            failed: False

        :raise S3Error
        """
        bucket, obj = self.get_bucket_and_obj_or_dir(bucket_name=bucket_name, path=path, user=user)
        if not obj or obj.is_file():
            raise exceptions.S3NoSuchKey('目录不存在')

        if obj.set_shared(share=share, days=days, password=password):
            return True

        return False

    def get_bucket_objects_dirs_queryset(self, bucket_name: str, user, prefix: str = ''):
        """
        获得所有对象和目录记录

        :param bucket_name: 桶名
        :param user: 用户对象
        :param prefix: 路径前缀
        :return:
            bucket, QuerySet()

        :raises: S3Error
        """
        # 存储桶验证和获取桶对象
        bucket = self.get_bucket_by_name(bucket_name)
        if not bucket:
            raise exceptions.S3NoSuchBucket('存储桶不存在')
        self.check_public_or_user_bucket(bucket=bucket, user=user, all_public=False)

        table_name = bucket.get_bucket_table_name()
        if not prefix:
            objs = self.get_objects_dirs_queryset(table_name=table_name)
        else:
            objs = self.get_prefix_objects_dirs_queryset(table_name=table_name, prefix=prefix)

        return bucket, objs

    @staticmethod
    def get_objects_dirs_queryset(table_name: str):
        """
        获得所有文件对象和目录记录

        :return: QuerySet()
        """
        return BucketFileManagement(collection_name=table_name).get_objects_dirs_queryset()

    @staticmethod
    def get_dir_queryset(table_name: str, path: str):
        """
        获取路径下的所有目录记录

        :param table_name:
        :param path:
        :return:
            QuerySet()
        :raises: S3Error
        """
        bfm = BucketFileManagement(path=path, collection_name=table_name)
        ok, qs = bfm.get_cur_dir_files()
        if not ok:
            raise exceptions.S3NotFound('未找到相关记录')
        return qs.filter(fod=False).all()

    @staticmethod
    def list_dir_queryset(bucket, dir_obj):
        """
        获取目录下的对象和目录列表信息

        :param bucket: 桶实例
        :param dir_obj: 目录实例
        :return:
                success:    QuerySet()   # django QuerySet实例
                failed:      raise S3Error
        """
        collection_name = bucket.get_bucket_table_name()
        bfm = BucketFileManagement(collection_name=collection_name)
        try:
            qs = bfm.get_cur_dir_files(cur_dir_id=dir_obj.id)
        except Exception as e:
            raise exceptions.S3InternalError(message=str(e))

        return qs

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

    def delete_objects(self, bucket_name: str, obj_keys: list, user=None):
        """
        删除多个对象

        不存在的对象将包含在已删除的结果中
        :param bucket_name: 桶名
        :param obj_keys: 对象全路径列表；[{"Key": "xxx"}, ]
        :param user: 用户，默认为None，如果给定用户只删除属于此用户的对象（只查找此用户的存储桶）
        :return:
            (deleted_objs: list, err_deleted_objs: list)

            deleted_objs like [{"Key": "xxx"},...]
            err_deleted_objs like [{"Code": "xxx", "Message": "xxx", "Key": "xxx"}, ]

        :raises: S3Error
        """
        bucket = self.get_public_or_user_bucket(name=bucket_name, user=user)
        # 桶锁操作检查
        if not bucket.lock_writeable():
            raise exceptions.S3BucketLockWrite()

        table_name = bucket.get_bucket_table_name()

        deleted_objects = []
        not_delete_objects = []
        for item in obj_keys:
            obj_key = key = item.get('Key', '')
            if key.endswith('/'):  # 目录
                key_is_dir = True
                obj_key = key.rstrip('/')
            else:
                key_is_dir = False

            try:
                try:
                    obj = self.get_metadata_obj(table_name=table_name, path=obj_key)  # 不检查父路径
                except Exception as e:
                    if not isinstance(e, exceptions.S3Error):
                        raise exceptions.S3InternalError(f'查询对象元数据错误，{str(e)}')
                    raise e

                if not obj:
                    raise exceptions.S3NoSuchKey()

                if (key_is_dir and obj.is_dir()) or (not key_is_dir and obj.is_file()):
                    try:
                        self.do_delete_obj_or_dir(bucket=bucket, obj=obj)
                    except exceptions.S3Error as e:
                        raise e

                    deleted_objects.append({"Key": key})
                else:
                    raise exceptions.S3NoSuchKey()
            except exceptions.S3NoSuchKey as e:
                deleted_objects.append({"Key": key})
            except exceptions.S3Error as e:
                err = e.err_data()
                err['Key'] = key
                not_delete_objects.append(err)

        return deleted_objects, not_delete_objects

    @staticmethod
    def do_delete_obj_or_dir(bucket, obj):
        """
        删除一个对象或目录
        :param bucket:
        :param obj:
        :return:
            True

        :raises: S3Error
        """
        old_id = obj.id

        if obj.is_dir():
            if not BucketFileManagement(collection_name=bucket.get_bucket_table_name()).dir_is_empty(obj):
                raise exceptions.S3InvalidRequest('无法删除非空目录')

        # multipart object delete
        if not obj.is_dir():
            s3_obj_multipart_data = MultipartUploadManager.get_multipart_upload_by_bucket_obj(bucket=bucket, obj=obj)
            if s3_obj_multipart_data:
                try:
                    s3_obj_multipart_data.delete()
                except exceptions.S3Error as e:
                    raise exceptions.S3InternalError('删除对象多部分上传元数据时错误')

        # 先删除元数据，后删除rados对象（删除失败恢复元数据）
        if not obj.do_delete():
            raise exceptions.S3InternalError('删除对象原数据时错误')

        if obj.is_dir():
            return True

        obj.id = old_id  # obj为None的情况
        rados = HarborManager.get_obj_rados(bucket=bucket, obj=obj)
        ok, _ = rados.delete()
        if not ok:
            # 恢复元数据
            obj.id = old_id
            obj.do_save(force_insert=True)  # 仅尝试创建文档，不修改已存在文档
            raise exceptions.S3InternalError('删除对象rados数据时错误')

        return True

    @staticmethod
    def create_multipart_data(bucket_id: int, bucket_name: str, obj, obj_perms_code: int):
        """
        创建s3多部分上传元数据
        """
        try:
            upload = MultipartUpload.create_multipart(
                bucket_id=bucket_id, bucket_name=bucket_name,
                obj_id=obj.id, obj_key=obj.na, obj_upload_time=obj.ult, obj_perms_code=obj_perms_code
            )
        except Exception as e:
            raise exceptions.S3InternalError(f'创建多部分上传元数据错误。{str(e)}')

        return upload

    # s3 重置对象
    def s3_reset_upload(self, bucket, obj):
        rados = self.get_obj_rados(bucket=bucket, obj=obj)
        # 重置对象
        try:
            self._pre_reset_upload(bucket=bucket, obj=obj, rados=rados)  # 重置对象大小
        except exceptions.S3Error as e:
            raise exceptions.S3InternalError('重置对象元数据失败')

    @staticmethod
    def get_obj_rados(bucket: Bucket, obj) -> HarborObject:
        obj_raods_key = obj.get_obj_key(bucket.id)
        pool_id = obj.get_pool_id()
        pool_name = obj.get_pool_name()
        obj_rados = build_harbor_object(
            using=str(pool_id), pool_name=pool_name, obj_id=obj_raods_key, obj_size=obj.si)

        return obj_rados


class MultipartUploadManager:
    @staticmethod
    def get_multipart_upload_by_id(upload_id: str) -> MultipartUpload:
        """
        查询多部分上传记录

        :param upload_id: uuid
        :return:
            MultipartUpload() or None

        :raises: S3Error
        """
        try:
            obj = MultipartUpload.objects.filter(id=upload_id).first()
        except Exception as e:
            raise exceptions.S3InternalError()

        return obj

    @staticmethod
    def get_multipart_upload_by_bucket_obj(bucket, obj) -> MultipartUpload:
        """
        通过 桶 对象 多条件查询
        :param bucket: 桶实例
        :param obj: 对象实例
        :return:
            MultipartUpload()   # 多部分上传对象的实例
            or None
        """
        try:
            upload = MultipartUpload.objects.filter(
                bucket_name=bucket.name, key_md5=obj.na_md5, bucket_id=bucket.id,
                obj_id=obj.id, obj_key=obj.na
            ).first()
        except Exception as e:
            raise exceptions.S3InternalError(message=f'查询对象多部分上传元数据错误，{str(e)}')

        return upload

    @staticmethod
    def delete_multipart_upload_by_bucket_obj(bucket, obj) -> MultipartUpload:
        """
        通过 桶 对象 删除可能存在的多部分上传记录
        :param bucket: 桶实例
        :param obj: 对象实例
        :return:
            int     # 删除的数量
        """
        try:
            count, d = MultipartUpload.objects.filter(
                key_md5=obj.na_md5, bucket_name=bucket.name, bucket_id=bucket.id,
                obj_id=obj.id, obj_key=obj.na
            ).delete()
        except Exception as e:
            raise exceptions.S3InternalError(message=f'删除对象多部分上传元数据错误，{str(e)}')

        return count

    @staticmethod
    def get_multipart_upload(bucket, obj_key: str):
        """
        通过 桶 对象key 查询多部分上传元数据

        :param bucket: 桶实例
        :param obj_key: 对象key
        :return:
            MultipartUpload() or None
        """
        try:
            key_md5 = get_str_hexMD5(obj_key)
            upload = MultipartUpload.objects.filter(
                key_md5=key_md5, bucket_name=bucket.name, bucket_id=bucket.id, obj_key=obj_key).first()
        except Exception as e:
            raise exceptions.S3InternalError()

        return upload

    @staticmethod
    def list_multipart_uploads_queryset(bucket_id: int, bucket_name: str, prefix: str = None, delimiter: str = None):
        """
        查询多部分上传记录

        :param bucket_id: 用于过滤掉删除归档的同名的桶
        :param bucket_name: 桶名
        :param prefix: prefix of s3 object key
        :param delimiter: 暂时不支持
        :return:
            Queryset()

        :raises: S3Error
        """
        lookups = {}
        if prefix:
            lookups['obj_key__startswith'] = prefix

        try:
            return MultipartUpload.objects.filter(
                bucket_name=bucket_name, bucket_id=bucket_id, **lookups,
                status=MultipartUpload.UploadStatus.UPLOADING.value
            ).order_by('-create_time').all()
        except Exception as e:
            raise exceptions.S3InternalError(extend_msg=str(e))

    @staticmethod
    def get_multipart_upload_queryset(bucket_name: str, obj_path: str):
        """
        查询多部分上传记录

        :param bucket_name: 桶名
        :param obj_path: s3 object key
        :return:
            Queryset()

        :raises: S3Error
        """
        key_md5 = get_str_hexMD5(obj_path)
        try:
            return MultipartUpload.objects.filter(key_md5=key_md5, bucket_name=bucket_name, obj_key=obj_path,
                                                  status='uploading').first()
        except Exception as e:
            raise exceptions.S3InternalError(extend_msg=str(e))

    def get_multipart_upload_delete_invalid(self, bucket, obj_path: str):
        """
        获取上传记录，顺便删除无效的上传记录

        :param bucket:
        :param obj_path:
        :return:
            MultipartUpload() or None

        :raises: S3Error
        """
        qs = self.get_multipart_upload_queryset(bucket_name=bucket.name, obj_path=obj_path)
        valid_uploads = []
        try:
            for upload in qs:
                if not upload.belong_to_bucket(bucket):
                    try:
                        upload.delete()
                    except Exception as e:
                        pass
                else:
                    valid_uploads.append(upload)
        except Exception as e:
            raise exceptions.S3InternalError(extend_msg='select multipart upload error.')

        if len(valid_uploads) == 0:
            return None

        return valid_uploads[0]

    @staticmethod
    def get_upload_parts_and_validate(upload, complete_parts: dict, complete_numbers: list):
        """
        多部分上传part元数据获取和验证

        :param upload: 上传任务实例
        :param complete_parts:  客户端请求组合提交的part信息，dict
        :param complete_numbers: 客户端请求组合提交的所有part的编号list，升序
        :return:
                object_etag: str                # 对象的ETag
        :raises: S3Error
        """
        obj_etag_handler = S3ObjectMultipartETagHandler()
        last_part_number = complete_numbers[-1]
        parts = upload.get_parts()

        # 请求合并的part和已上传的part数量必须一致
        obj_parts_count = len(parts)
        if obj_parts_count != len(complete_parts):
            raise exceptions.S3InvalidPart(message=gettext('请求合并的part和已上传的part数量不一致。'))

        first_part_size = parts[0]['Size']  # 第一part大小
        pre_part_number = 0
        for part in parts:
            num = part['PartNumber']
            if num not in complete_numbers:
                raise exceptions.S3InvalidPart(extend_msg=f'PartNumber={num}')

            # part编号必须从1开始，并且连续
            pre_part_number += 1
            if num != pre_part_number:
                raise exceptions.S3InvalidPart(message=gettext('Part编号必须从1开始，并且连续。'), extend_msg=f'PartNumber={num}')

            c_part = complete_parts[num]
            # part最小限制，最后一个part除外
            if part['Size'] < S3_MULTIPART_UPLOAD_MIN_SIZE and num != last_part_number:
                raise exceptions.S3EntityTooSmall()

            # 块大小检测
            if part['Size'] != first_part_size and num != last_part_number:
                raise exceptions.S3InvalidPart(message=gettext('The block information size is inconsistent.'))

            if 'ETag' not in c_part:
                raise exceptions.S3InvalidPart(extend_msg=f'PartNumber={num}')

            if c_part["ETag"].strip('"') != part['ETag']:
                raise exceptions.S3InvalidPart(extend_msg=f'PartNumber={num}')

            obj_etag_handler.update(part['ETag'])

        obj_etag = f'"{obj_etag_handler.hex_md5}-{obj_parts_count}"'
        return obj_etag

    @staticmethod
    def handle_validate_complete_parts(parts: list):
        """
        检查对象part列表是否是升序排列, 是否有效（1-10000）
        :return: parts_dict, numbers
                parts_dict: dict, {PartNumber: parts[index]} 把parts列表转为以PartNumber为键值的有序字典
                numbers: list, [PartNumber, PartNumber]

        :raises: S3Error
        """
        pre_num = 0
        numbers = []
        parts_dict = OrderedDict()
        for part in parts:
            part_num = part.get('PartNumber', None)
            etag = part.get('ETag', None)
            if part_num is None or etag is None:
                raise exceptions.S3MalformedXML()

            if not (1 <= part_num <= 10000):
                raise exceptions.S3InvalidPart()

            if part_num <= pre_num:
                raise exceptions.S3InvalidPartOrder()

            parts_dict[part_num] = part
            numbers.append(part_num)
            pre_num = part_num

        return parts_dict, numbers
