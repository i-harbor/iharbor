from django.utils import timezone
from django.db.models import Case, Value, When, F
from django.db import connections
from rest_framework import status

from buckets.models import Bucket
from buckets.utils import BucketFileManagement
from utils.storagers import PathParser
from utils.oss import HarborObject, get_size
from.paginations import BucketFileLimitOffsetPagination

def ftp_close_old_connections(func):
    def wrapper(*args, **kwargs):
        for conn in connections.all():
            conn.close_if_unusable_or_obsolete()
        return func(*args, **kwargs)

    return wrapper

class HarborError(BaseException):
    def __init__(self, code:int, msg:str, **kwargs):
        self.code = code    # 错误码
        self.msg = msg      # 错误描述
        self.data = kwargs  # 一些希望传递的数据

    def __str__(self):
        return self.detail()

    def detail(self):
        return f'{self.code},{self.msg}'


class HarborManager():
    '''
    操作harbor对象数据和元数据管理接口封装
    '''
    def get_bucket(self, bucket_name:str, user=None):
        '''
        获取存储桶

        :param bucket_name: 桶名
        :param user: 用户对象；如果给定用户，只查找属于此用户的存储桶
        :return:
            Bucket() # 成功时
            None     # 桶不存在，或不属于给定用户时
        '''
        if user:
            return self.get_user_own_bucket(bucket_name, user)

        return self.get_bucket_by_name(bucket_name)

    def get_bucket_by_name(self, name:str):
        '''
        通过桶名获取Bucket实例
        :param name: 桶名
        :return:
            Bucket() # success
            None     # not exist
        '''
        bucket = Bucket.get_bucket_by_name(name)
        if not bucket:
            return None

        return bucket

    def get_user_own_bucket(self, name:str, user):
        '''
        获取用户的存储桶

        :param name: 存储通名称
        :param user: 用户对象
        :return:
            success: bucket
            failure: None
        '''
        bucket = self.get_bucket_by_name(name)
        if bucket and bucket.check_user_own_bucket(user):
            return bucket

        return None

    def is_dir(self, bucket_name:str, path_name:str):
        '''
        是否时一个目录

        :param bucket_name: 桶名
        :param path_name: 目录路径
        :return:
            true: is dir
            false: is file

        :raise HarborError  # 桶或路径不存在，发生错误
        '''
        # path为空或根目录时
        if not path_name or path_name == '/':
            return True

        obj = self.get_object(bucket_name, path_name)
        if obj.is_dir():
            return True

        return False

    def is_file(self, bucket_name:str, path_name:str):
        '''
        是否时一个文件
        :param bucket_name: 桶名
        :param path_name: 文件路径
        :return:
            true: is file
            false: is dir

        :raise HarborError  # 桶或路径不存在，发生错误
        '''
        # path为空或根目录时
        if not path_name or path_name == '/':
            return False

        obj = self.get_object(bucket_name, path_name)
        if obj.is_file():
            return True

        return False

    def get_object(self, bucket_name:str, path_name:str, user=None):
        '''
        获取对象或目录实例

        :param bucket_name: 桶名
        :param path_name: 文件路径
        :param user: 用户，默认为None，如果给定用户只获取属于此用户的对象（只查找此用户的存储桶）
        :return:
            success: 对象实例

        :raise HarborError
        '''
        path, name = PathParser(filepath=path_name).get_path_and_filename()
        if not bucket_name or not name:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg="path参数有误")

        # 存储桶验证和获取桶对象
        bucket = self.get_bucket(bucket_name, user=user)
        if not bucket:
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='存储桶不存在')

        table_name = bucket.get_bucket_table_name()
        ok, obj = self._get_obj_or_dir(table_name, path, name)
        if not ok:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='目录路径参数无效，父节点目录不存在')

        if obj is None:
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='指定对象或目录不存在')

        return obj

    def get_metadata_obj(self, table_name:str, path:str):
        '''
        直接获取目录或对象元数据对象，不检查父节点是否存在

        :param table_name: 数据库表名
        :param path: 目录或对象的全路径
        :return:
            obj     #
            None    #
        :raises: HarborError
        '''
        bfm = BucketFileManagement(collection_name=table_name)
        try:
            obj = bfm.get_obj(path=path)
        except Exception as e:
            raise HarborError(code=status.HTTP_500_INTERNAL_SERVER_ERROR, msg=str(e))
        if obj:
            return obj
        return None

    def _get_obj_or_dir_and_bfm(self, table_name, path, name):
        '''
        获取文件对象或目录,和BucketFileManagement对象

        :param table_name: 数据库表名
        :param path: 父目录路经
        :param name: 对象名称或目录名称
        :return:
            ok, obj，bfm: 对象或目录

            False, None, bfm    # 父目录路径错误，不存在
            True, obj, bfm   # 目录或对象存在
            True, None, bfm  # 目录或对象不存在
        '''
        bfm = BucketFileManagement(path=path, collection_name=table_name)
        ok, obj = bfm.get_dir_or_obj_exists(name=name)
        if not ok:
            return False, None, bfm    # 父目录路径错误不存在

        if obj:
            return True, obj, bfm   # 目录或对象存在

        return True, None, bfm  # 目录或对象不存在

    def _get_obj_or_dir(self, table_name, path, name):
        '''
        获取文件对象或目录

        :param table_name: 数据库表名
        :param path: 父目录路经
        :param name: 对象名称或目录名称
        :return:
            ok, obj: 对象或目录
            ok == False: 父目录路径错误，不存在
            None: 目录或对象不存在
        '''
        ok, obj, _ = self._get_obj_or_dir_and_bfm(table_name, path, name)

        return ok, obj

    def mkdir(self, bucket_name:str, path:str, user=None):
        '''
        创建一个目录
        :param bucket_name: 桶名
        :param path: 目录全路径
        :param user: 用户，默认为None，如果给定用户只创建属于此用户的目录（只查找此用户的存储桶）
        :return:
            True, dir: success
            raise HarborError: failed
        :raise HarborError
        '''
        validated_data = self.validate_mkdir_params(bucket_name, path, user)

        dir_path = validated_data.get('dir_path', '')
        dir_name = validated_data.get('dir_name', '')
        did = validated_data.get('did', None)
        collection_name = validated_data.get('collection_name')

        bfm = BucketFileManagement(dir_path, collection_name=collection_name)
        dir_path_name = bfm.build_dir_full_name(dir_name)
        BucketFileClass = bfm.get_obj_model_class()
        bfinfo = BucketFileClass(na=dir_path_name,  # 全路经目录名
                                 name=dir_name,  # 目录名
                                 fod=False,  # 目录
                                 )
        # 有父节点
        if did:
            bfinfo.did = did
        try:
            bfinfo.save(force_insert=True)  # 仅尝试创建文档，不修改已存在文档
        except:
            raise HarborError(code=status.HTTP_500_INTERNAL_SERVER_ERROR, msg='创建失败，数据库错误')

        return True, bfinfo

    def validate_mkdir_params(self, bucket_name:str, dirpath:str, user=None):
        '''
        post_detail参数验证

        :param request:
        :param kwargs:
        :return:
                success: {data}
                failure: raise HarborError

        :raise HarborError
        '''
        data = {}
        dir_path, dir_name = PathParser(filepath=dirpath).get_path_and_filename()

        if not bucket_name or not dir_name:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='目录路径参数无效，要同时包含有效的存储桶和目录名称')

        if '/' in dir_name:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='目录名称不能包含‘/’')

        if len(dir_name) > 255:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='目录名称长度最大为255字符')

        # bucket是否属于当前用户,检测存储桶名称是否存在
        bucket = self.get_bucket(bucket_name, user=user)
        if not isinstance(bucket, Bucket):
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='存储桶不存在')

        _collection_name = bucket.get_bucket_table_name()
        data['collection_name'] = _collection_name

        ok, dir, bfm = self._get_obj_or_dir_and_bfm(table_name=_collection_name, path=dir_path, name=dir_name)
        if not ok:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='目录路径参数无效，父节点目录不存在')

        if dir:
            # 目录已存在
            if dir.is_dir():
                raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg=f'"{dir_name}"目录已存在', existing=True)

            # 同名对象已存在
            if dir.is_file():
                raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg=f'"指定目录名称{dir_name}"已存在重名对象，请重新指定一个目录名称')

        data['did'] = bfm.cur_dir_id if bfm.cur_dir_id else bfm.get_cur_dir_id()[-1]
        data['bucket_name'] = bucket_name
        data['dir_path'] = dir_path
        data['dir_name'] = dir_name
        return data

    def rmdir(self, bucket_name:str, dirpath:str, user=None):
        '''
        删除一个空目录
        :param bucket_name:桶名
        :param dirpath: 目录全路径
        :param user: 用户，默认为None，如果给定用户只删除属于此用户的目录（只查找此用户的存储桶）
        :return:
            True: success
            raise HarborError(): failed

        :raise HarborError()
        '''

        path, dir_name = PathParser(filepath=dirpath).get_path_and_filename()

        if not bucket_name or not dir_name:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='参数无效')

        # bucket是否属于当前用户,检测存储桶名称是否存在
        bucket = self.get_bucket(bucket_name, user=user)
        if not isinstance(bucket, Bucket):
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='存储桶不存在')

        table_name = bucket.get_bucket_table_name()

        ok, dir, bfm = self._get_obj_or_dir_and_bfm(table_name=table_name, path=path, name=dir_name)
        if not ok:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='目录路径参数无效，父节点目录不存在')

        if not dir or dir.is_file():
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='目录不存在')

        if not bfm.dir_is_empty(dir):
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='无法删除非空目录')

        if not dir.do_delete():
            raise HarborError(code=status.HTTP_500_INTERNAL_SERVER_ERROR, msg='删除目录失败，数据库错误')

        return True

    def list_dir(self, bucket_name:str, path:str, offset:int=0, limit:int=1000, user=None, paginator=None):
        '''
        获取目录下的文件列表信息

        :param bucket_name: 桶名
        :param path: 目录路径
        :param offset: 目录下文件列表偏移量
        :param limit: 获取文件信息数量
        :param user: 用户，默认为None，如果给定用户只获取属于此用户的目录下的文件列表信息（只查找此用户的存储桶）
        :param paginator: 分页器，默认为None
        :return:
                success:    (list[object, object,], bucket) # list和bucket实例
                failed:      raise HarborError

        :raise HarborError
        '''
        bucket = self.get_bucket(bucket_name, user)
        if not bucket:
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='存储桶不存在')

        collection_name = bucket.get_bucket_table_name()
        bfm = BucketFileManagement(path=path, collection_name=collection_name)
        ok, files = bfm.get_cur_dir_files()
        if not ok:
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='未找到相关记录')

        if paginator is None:
            paginator = BucketFileLimitOffsetPagination()
        if limit <= 0 :
            limit = paginator.default_limit
        else:
            limit = min(limit, paginator.max_limit)

        if offset < 0:
            offset = 0

        paginator.offset = offset
        paginator.limit = limit

        try:
            l = paginator.pagenate_to_list(files, offset=offset, limit=limit)
        except Exception as e:
            raise HarborError(code=status.HTTP_500_INTERNAL_SERVER_ERROR, msg=str(e))

        return (l, bucket)

    def move_rename(self, bucket_name:str, obj_path:str, rename=None, move=None, user=None):
        '''
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
        '''
        path, filename = PathParser(filepath=obj_path).get_path_and_filename()
        if not bucket_name or not filename:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='参数有误')

        move_to, rename = self._validate_move_rename_params(move_to=move, rename=rename)
        if move_to is None and rename is None:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='请至少提交一个要执行操作的参数')

        # 存储桶验证和获取桶对象
        try:
            bucket, obj = self.get_bucket_and_obj(bucket_name=bucket_name, obj_path=obj_path, user=user)
        except HarborError as e:
            raise e

        if obj is None:
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='文件对象不存在')

        return self._move_rename_obj(bucket=bucket, obj=obj, move_to=move_to, rename=rename)

    def _move_rename_obj(self, bucket, obj, move_to, rename):
        '''
        移动重命名对象

        :param bucket: 对象所在桶
        :param obj: 文件对象
        :param move_to: 移动目标路径
        :param rename: 重命名的新名称
        :return:
            success: (object, bucket)
            failed : raise HarborError

        :raise HarborError
        '''
        table_name = bucket.get_bucket_table_name()
        new_obj_name = rename if rename else obj.name # 移动后对象的名称，对象名称不变或重命名

        # 检查是否符合移动或重命名条件，目标路径下是否已存在同名对象或子目录
        if move_to is None: # 仅仅重命名对象，不移动
            bfm = BucketFileManagement( collection_name=table_name)
            ok, target_obj = bfm.get_dir_or_obj_exists(name=new_obj_name, cur_dir_id=obj.did)
        else: # 需要移动对象
            bfm = BucketFileManagement(path=move_to, collection_name=table_name)
            ok, target_obj = bfm.get_dir_or_obj_exists(name=new_obj_name)

        if not ok:
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='无法完成对象的移动操作，指定的目标路径未找到')

        if target_obj:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='无法完成对象的移动操作，指定的目标路径下已存在同名的对象或目录')

        # 仅仅重命名对象，不移动
        if move_to is None:
            path, _ = PathParser(filepath=obj.na).get_path_and_filename()
            obj.na = path + '/' + new_obj_name if path else  new_obj_name
            obj.name = new_obj_name
        else: # 移动对象或重命名
            _, did = bfm.get_cur_dir_id()
            obj.did = did
            obj.na = bfm.build_dir_full_name(new_obj_name)
            obj.name = new_obj_name

        obj.reset_na_md5()
        if not obj.do_save():
            raise HarborError(code=status.HTTP_500_INTERNAL_SERVER_ERROR, msg= '移动对象操作失败')

        return obj, bucket

    def _validate_move_rename_params(self, move_to, rename):
        '''
        校验移动或重命名参数
        :param request:
        :return:
                (move_to, rename)
                move_to # None 或 string
                rename  # None 或 string

        :raise HarborError
        '''
        # 移动对象参数
        if move_to is not None:
            move_to = move_to.strip('/')

        # 重命名对象参数
        if rename is not None:
            if '/' in rename:
                raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='对象名称不能含“/”')

            if len(rename) > 255:
                raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='对象名称不能大于255个字符长度')

        return move_to, rename

    def write_chunk(self, bucket_name:str, obj_path:str, offset:int, chunk:bytes, reset:bool=False, user=None):
        '''
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
        '''
        if not isinstance(chunk, bytes):
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='数据不是bytes类型')

        return self.write_to_object(bucket_name=bucket_name, obj_path=obj_path, offset=offset, data=chunk,
                                    reset=reset, user=user)

    def write_file(self, bucket_name:str, obj_path:str, offset:int, file, reset:bool=False, user=None):
        '''
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
        '''
        return self.write_to_object(bucket_name=bucket_name, obj_path=obj_path, offset=offset, data=file,
                                    reset=reset, user=user)

    def write_to_object(self, bucket_name:str, obj_path:str, offset:int, data, reset:bool=False, user=None):
        '''
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
        '''
        # 对象路径分析
        pp = PathParser(filepath=obj_path)
        path, filename = pp.get_path_and_filename()
        if not bucket_name or not filename:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='参数有误')

        if len(filename) > 255:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='对象名称长度最大为255字符')

        # 存储桶验证和获取桶对象
        bucket = self.get_bucket(bucket_name, user=user)
        if not bucket:
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='存储桶不存在')

        collection_name = bucket.get_bucket_table_name()
        obj, created = self._get_obj_and_check_limit_or_create(collection_name, path, filename)
        obj_key = obj.get_obj_key(bucket.id)
        rados = HarborObject(obj_key, obj_size=obj.si)
        if created is False:  # 对象已存在，不是新建的
            if reset:  # 重置对象大小
                self._pre_reset_upload(obj=obj, rados=rados)

        try:
            if isinstance(data, bytes):
                self._save_one_chunk(obj=obj, rados=rados, offset=offset, chunk=data)
            else:
                self._save_one_file(obj=obj, rados=rados, offset=offset, file=data)
        except HarborError as e:
            # 如果对象是新创建的，上传失败删除对象元数据
            if created is True:
                obj.do_delete()
            raise e

        return created

    def _get_obj_and_check_limit_or_create(self, table_name, path, filename):
        '''
        获取文件对象, 验证存储桶对象和目录数量上限，不存在并且验证通过则创建

        :param table_name: 桶对应的数据库表名
        :param path: 文件对象所在的父路径
        :param filename: 文件对象名称
        :return:
                (obj, False) # 对象已存在
                (obj, True)  # 对象不存在，创建一个新对象
                raise HarborError # 有错误，路径不存在，或已存在同名目录

        :raise HarborError
        '''
        bfm = BucketFileManagement(path=path, collection_name=table_name)

        ok, obj = bfm.get_dir_or_obj_exists(name=filename)
        # 父路经不存在或有错误
        if not ok:
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='对象路经不存在')

        # 文件对象已存在
        if obj and obj.is_file():
            return obj, False

        # 已存在同名的目录
        if obj and obj.is_dir():
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='指定的对象名称与已有的目录重名，请重新指定一个名称')

        ok, did = bfm.get_cur_dir_id()
        if not ok:
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='对象路经不存在')

        # 验证集合文档上限
        # if not self.do_bucket_limit_validate(bfm):
        #     return None, None

        # 创建文件对象
        BucketFileClass = bfm.get_obj_model_class()
        full_filename = bfm.build_dir_full_name(filename)
        bfinfo = BucketFileClass(na=full_filename,  # 全路径文件名
                                 name=filename, #  文件名
                                fod=True,  # 文件
                                si=0)  # 文件大小
        # 有父节点
        if did:
            bfinfo.did = did

        try:
            bfinfo.save()
            obj = bfinfo
        except:
            raise HarborError(code=status.HTTP_500_INTERNAL_SERVER_ERROR, msg='新建对象元数据失败，数据库错误')

        return obj, True

    def _pre_reset_upload(self, obj, rados):
        '''
        覆盖上传前的一些操作

        :param obj: 文件对象元数据
        :param rados: rados接口类对象
        :return:
                正常：True
                错误：raise HarborError
        '''
        # 先更新元数据，后删除rados数据（如果删除失败，恢复元数据）
        # 更新文件上传时间
        old_ult = obj.ult
        old_size = obj.si

        obj.ult = timezone.now()
        obj.si = 0
        if not obj.do_save(update_fields=['ult', 'si']):
            raise HarborError(code=status.HTTP_500_INTERNAL_SERVER_ERROR, msg='修改对象元数据失败')

        ok, _ = rados.delete()
        if not ok:
            # 恢复元数据
            obj.ult = old_ult
            obj.si = old_size
            obj.do_save(update_fields=['ult', 'si'])
            raise HarborError(code=status.HTTP_500_INTERNAL_SERVER_ERROR, msg='rados文件对象删除失败')

        return True

    def _save_one_chunk(self, obj, rados, offset:int, chunk:bytes):
        '''
        保存一个上传的分片

        :param obj: 对象元数据
        :param rados: rados接口
        :param offset: 分片偏移量
        :param chunk: 分片数据
        :return:
            成功：True
            失败：raise HarborError
        '''
        # 先更新元数据，后写rados数据
        # 更新文件修改时间和对象大小
        new_size = offset + len(chunk) # 分片数据写入后 对象偏移量大小
        if not self._update_obj_metadata(obj, size=new_size):
            raise HarborError(code=status.HTTP_500_INTERNAL_SERVER_ERROR, msg='修改对象元数据失败')

        # 存储文件块
        try:
            ok, msg = rados.write(offset=offset, data_block=chunk)
        except Exception as e:
            ok = False
            msg = str(e)

        if not ok:
            # 手动回滚对象元数据
            self._update_obj_metadata(obj, obj.si, obj.upt)
            raise HarborError(code=status.HTTP_500_INTERNAL_SERVER_ERROR, msg='文件块rados写入失败:' + msg)

        return True

    def _save_one_file(self, obj, rados, offset:int, file):
        '''
        向对象写入一个文件

        :param obj: 对象元数据
        :param rados: rados接口
        :param offset: 分片偏移量
        :param file: 文件
        :return:
            成功：True
            失败：raise HarborError
        '''
        # 先更新元数据，后写rados数据
        # 更新文件修改时间和对象大小
        try:
            file_size = get_size(file)
        except AttributeError:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='输入必须是一个文件')

        new_size = offset + file_size # 分片数据写入后 对象偏移量大小
        if not self._update_obj_metadata(obj, size=new_size):
            raise HarborError(code=status.HTTP_500_INTERNAL_SERVER_ERROR, msg='修改对象元数据失败')

        # 存储文件
        try:
            ok, msg = rados.write_file(offset=offset, file=file)
        except Exception as e:
            ok = False
            msg = str(e)

        if not ok:
            # 手动回滚对象元数据
            self._update_obj_metadata(obj, obj.si, obj.upt)
            raise HarborError(code=status.HTTP_500_INTERNAL_SERVER_ERROR, msg='文件块rados写入失败:' + msg)

        return True

    def _update_obj_metadata(self, obj, size, upt=None):
        '''
        更新对象元数据
        :param obj: 对象, obj实例不会被修改
        :param size: 对象大小
        :param upt: 修改时间
        :return:
            success: True
            failed: False
        '''
        if not upt:
            upt = timezone.now()

        model = obj._meta.model

        # 更新文件修改时间和对象大小
        old_size = obj.si if obj.si else 0
        new_size = max(size, old_size)  # 更新文件大小（只增不减）
        try:
            # r = model.objects.filter(id=obj.id, si=obj.si).update(si=new_size, upt=timezone.now())  # 乐观锁方式
            r = model.objects.filter(id=obj.id).update(si=Case(When(si__lt=new_size, then=Value(new_size)),
                                                               default=F('si')), upt=upt)
        except Exception as e:
            return False
        if r > 0:  # 更新行数
            return True

        return False

    def delete_object(self, bucket_name:str, obj_path:str, user=None):
        '''
        删除一个对象

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param user: 用户，默认为None，如果给定用户只删除属于此用户的对象（只查找此用户的存储桶）
        :return:
            success: True
            failed:  raise HarborError

        :raise HarborError
        '''
        path, filename = PathParser(filepath=obj_path).get_path_and_filename()
        if not bucket_name or not filename:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='参数有误')

        # 存储桶验证和获取桶对象
        try:
            bucket, fileobj = self.get_bucket_and_obj(bucket_name=bucket_name, obj_path=obj_path, user=user)
        except HarborError as e:
            raise e

        if fileobj is None:
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='文件对象不存在')

        obj_key = fileobj.get_obj_key(bucket.id)
        old_id = fileobj.id
        # 先删除元数据，后删除rados对象（删除失败恢复元数据）
        if not fileobj.do_delete():
            raise HarborError(code=status.HTTP_500_INTERNAL_SERVER_ERROR, msg='删除对象原数据时错误')

        ho = HarborObject(obj_id=obj_key, obj_size=fileobj.si)
        ok, _ = ho.delete()
        if not ok:
            # 恢复元数据
            fileobj.id = old_id
            fileobj.do_save(force_insert=True)  # 仅尝试创建文档，不修改已存在文档
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='删除对象rados数据时错误')

        return True

    def read_chunk(self, bucket_name:str, obj_path:str, offset:int, size:int, user=None):
        '''
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
        '''
        if offset < 0 or size < 0 or size > 20 * 1024 ** 2:  # 20Mb
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='参数有误')

        # 存储桶验证和获取桶对象
        try:
            bucket, obj = self.get_bucket_and_obj(bucket_name=bucket_name, obj_path=obj_path, user=user)
        except HarborError as e:
            raise e

        if obj is None:
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='文件对象不存在')

        # 自定义读取文件对象
        if size == 0:
            return (bytes(), obj.si)

        obj_key = obj.get_obj_key(bucket.id)
        rados = HarborObject(obj_key, obj_size=obj.si)
        ok, chunk = rados.read(offset=offset, size=size)
        if not ok:
            raise HarborError(code=status.HTTP_500_INTERNAL_SERVER_ERROR, msg='文件块读取失败')

        # 如果从0读文件就增加一次下载次数
        if offset == 0:
            obj.download_cound_increase()

        return (chunk, obj)

    def get_obj_generator(self, bucket_name:str, obj_path:str, offset:int=0, end:int=None, per_size=10 * 1024 ** 2, user=None):
        '''
        获取一个读取对象的生成器函数

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param offset: 读起始偏移量；type: int
        :param end: 读结束偏移量(包含)；type: int；默认None表示对象结尾；
        :param per_size: 每次读取数据块长度；type: int， 默认10Mb
        :param user: 用户，默认为None，如果给定用户只删除属于此用户的对象（只查找此用户的存储桶）
        :return: (generator, object)
                for data in generator:
                    do something
        :raise HarborError
        '''
        if per_size < 0 or per_size > 20 * 1024 ** 2:  # 20Mb
            per_size = 10 * 1024 ** 2   # 10Mb

        if offset < 0 or (end is not None and end < 0):
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='参数有误')

        # 存储桶验证和获取桶对象
        try:
            bucket, obj = self.get_bucket_and_obj(bucket_name=bucket_name, obj_path=obj_path, user=user)
        except HarborError as e:
            raise e

        if obj is None:
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='文件对象不存在')

        # 增加一次下载次数
        obj.download_cound_increase()
        generator = self._get_obj_generator(bucket=bucket, obj=obj, offset=offset, end=end, per_size=per_size)
        return  generator, obj

    def _get_obj_generator(self, bucket, obj, offset:int=0, end:int=None, per_size=10 * 1024 ** 2):
        '''
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
        '''
        # 读取文件对象生成器
        obj_key = obj.get_obj_key(bucket.id)
        rados = HarborObject(obj_key, obj_size=obj.si)
        return rados.read_obj_generator(offset=offset, end=end, block_size=per_size)

    def get_write_generator(self, bucket_name:str, obj_path:str, user=None):
        '''
        获取一个写入对象的生成器函数

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param user: 用户，默认为None，如果给定用户只删除属于此用户的对象（只查找此用户的存储桶）
        :return:
                generator           # success
                :raise HarborError  # failed

        :usage:
            ok = next(generator)
            ok = generator.send((offset, bytes))  # ok = True写入成功， ok=False写入失败

        :raise HarborError
        '''
        # 对象路径分析
        pp = PathParser(filepath=obj_path)
        path, filename = pp.get_path_and_filename()
        if not bucket_name or not filename:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='参数有误')

        if len(filename) > 255:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='对象名称长度最大为255字符')

        # 存储桶验证和获取桶对象
        bucket = self.get_bucket(bucket_name, user=user)
        if not bucket:
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='存储桶不存在')

        collection_name = bucket.get_bucket_table_name()
        obj, created = self._get_obj_and_check_limit_or_create(collection_name, path, filename)
        obj_key = obj.get_obj_key(bucket.id)

        def generator():
            ok = True
            rados = HarborObject(obj_key, obj_size=obj.si)
            if created is False:  # 对象已存在，不是新建的,重置对象大小
                self._pre_reset_upload(obj=obj, rados=rados)

            while True:
                offset, data = yield ok
                try:
                    ok = self._save_one_chunk(obj=obj, rados=rados, offset=offset, chunk=data)
                except HarborError:
                    ok = False

        return generator()

    def get_bucket_and_obj_or_dir(self,bucket_name:str, path:str, user=None):
        '''
        获取存储桶和对象或目录实例

        :param bucket_name: 桶名
        :param path: 对象全路径
        :param user: 用户，默认为None，如果给定用户只查找此用户的存储桶
        :return:
                success: （bucket, object） # obj == None表示对象或目录不存在
                failed:   raise HarborError # 存储桶不存在，或参数有误，或有错误发生

        :raise HarborError
        '''
        pp = PathParser(filepath=path)
        dir_path, filename = pp.get_path_and_filename()
        if not bucket_name or not filename:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='参数有误')

        # 存储桶验证和获取桶对象
        bucket = self.get_bucket(bucket_name=bucket_name, user=user)
        if not bucket:
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='存储桶不存在')

        table_name = bucket.get_bucket_table_name()
        ok, obj = self._get_obj_or_dir(table_name=table_name, path=dir_path, name=filename)
        if not ok:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='目录路径参数无效，父节点目录不存在')

        if not obj:
            return bucket, None

        return bucket, obj

    def get_bucket_and_obj(self, bucket_name: str, obj_path: str, user=None):
        '''
        获取存储桶和对象实例

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param user: 用户，默认为None，如果给定用户只查找此用户的存储桶
        :return:
                success: （bucket, obj）  # obj == None表示对象不存在
                failed:   raise HarborError # 存储桶不存在，或参数有误，或有错误发生

        :raise HarborError
        '''
        bucket, obj = self.get_bucket_and_obj_or_dir(bucket_name=bucket_name, path=obj_path, user=user)
        if obj and obj.is_file():
            return bucket, obj

        return bucket, None

    def share_object(self, bucket_name:str, obj_path:str, share:bool=False, rw=1, days:int=0, user=None):
        '''
        设置对象共享或私有权限

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param user: 用户，默认为None，如果给定用户只查找此用户的存储桶
        :param share: 共享(True)或私有(False)
        :param rw: 读写权限；0（禁止访问），1（只读），2（可读可写）
        :param days: 共享天数，0表示永久共享, <0表示不共享
        :return:
            success: True
            failed: False

        :raise HarborError
        '''
        bucket, obj = self.get_bucket_and_obj(bucket_name=bucket_name, obj_path=obj_path, user=user)
        if obj is None:
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='对象不存在')

        if obj.set_shared(sh=share, rw=rw, days=days):
            return True

        return False

    def share_dir(self, bucket_name:str, path:str, share:int, days:int=0, user=None):
        '''
        设置目录共享或私有权限

        :param bucket_name: 桶名
        :param path: 目录全路径
        :param user: 用户，默认为None，如果给定用户只查找此用户的存储桶
        :param share: 读写权限；0（禁止访问），1（只读），2（可读可写）
        :param days: 共享天数，0表示永久共享, <0表示不共享
        :return:
            success: True
            failed: False

        :raise HarborError
        '''
        bucket, obj = self.get_bucket_and_obj_or_dir(bucket_name=bucket_name, path=path, user=user)
        if not obj or obj.is_file():
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='目录不存在')

        sh = False if share == 0 else True
        if obj.set_shared(sh=sh, rw=share, days=days):
            return True

        return False


class FtpHarborManager():
    '''
    ftp操作harbor对象数据元数据管理接口封装
    '''
    def __init__(self):
        self.__hbManager = HarborManager()

    def ftp_authenticate(self, bucket_name:str, password:str):
        '''
        Bucket桶ftp访问认证
        :return:    (ok:bool, permission:bool, msg:str)
            ok:         True，认证成功；False, 认证失败
            permission: True, 可读可写权限；False, 只读权限
            msg:        认证结果字符串
        '''
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

    def ftp_write_chunk(self, bucket_name:str, obj_path:str, offset:int, chunk:bytes, reset:bool=False):
        '''
        向对象写入一个数据块

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param offset: 写入对象偏移量
        :param chunk: 要写入的数据
        :param reset: 为True时，先重置对象大小为0后再写入数据；
        :return:
                created             # created==True表示对象是新建的；created==False表示对象不是新建的
                raise HarborError   # 写入失败
        '''
        return self.__hbManager.write_chunk(bucket_name=bucket_name, obj_path=obj_path, offset=offset, chunk=chunk)

    def ftp_write_file(self, bucket_name:str, obj_path:str, offset:int, file, reset:bool=False, user=None):
        '''
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
        '''
        return self.__hbManager.write_file(bucket_name=bucket_name, obj_path=obj_path, offset=offset,
                                           file=file, reset=reset, user=user)

    def ftp_move_rename(self, bucket_name:str, obj_path:str, rename=None, move=None):
        '''
        移动或重命名对象

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param rename: 重命名新名称，默认为None不重命名
        :param move: 移动到move路径下，默认为None不移动
        :return:
            success: object
            failed : raise HarborError

        :raise HarborError
        '''
        return self.__hbManager.move_rename(bucket_name, obj_path=obj_path, rename=rename, move=move)

    def ftp_rename(self, bucket_name:str, obj_path:str, rename):
        '''
        重命名对象

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param rename: 重命名新名称
        :return:
            success: object
            failed : raise HarborError

        :raise HarborError
        '''
        return self.__hbManager.move_rename(bucket_name, obj_path=obj_path, rename=rename)

    def ftp_read_chunk(self, bucket_name:str, obj_path:str, offset:int, size:int):
        '''
        从对象读取一个数据块

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param offset: 读起始偏移量
        :param size: 读取数据字节长度, 最大20Mb
        :return:
            success: （chunk:bytes, object）  #  数据块，对象元数据实例
            failed:   raise HarborError         # 读取失败，抛出HarborError

        :raise HarborError
        '''
        return self.__hbManager.read_chunk(bucket_name=bucket_name, obj_path=obj_path, offset=offset, size=size)

    def ftp_get_obj_generator(self, bucket_name:str, obj_path:str, offset:int=0, end:int=None, per_size=10 * 1024 ** 2):
        '''
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
        '''
        return self.__hbManager.get_obj_generator(bucket_name=bucket_name, obj_path=obj_path, offset=offset, end=end, per_size=per_size)

    def ftp_delete_object(self, bucket_name:str, obj_path:str):
        '''
        删除一个对象

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :return:
            success: True
            failed:  raise HarborError

        :raise HarborError
        '''
        return self.__hbManager.delete_object(bucket_name=bucket_name, obj_path=obj_path)

    def ftp_list_dir(self, bucket_name:str, path:str, offset:int=0, limit:int=1000):
        '''
        获取目录下的文件列表信息

        :param bucket_name: 桶名
        :param path: 目录路径
        :param offset: 目录下文件列表偏移量
        :param limit: 获取文件信息数量
        :return:
                success:    (list[object, object,], bucket) # list和bucket实例
                failed:      raise HarborError

        :raise HarborError
        '''
        return self.__hbManager.list_dir(bucket_name, path, offset=offset, limit=limit)

    def ftp_mkdir(self, bucket_name:str, path:str):
        '''
        创建一个目录
        :param bucket_name: 桶名
        :param path: 目录全路径
        :return:
            True, dir: success
            raise HarborError: failed
        :raise HarborError
        '''
        return self.__hbManager.mkdir(bucket_name, path)

    def ftp_rmdir(self, bucket_name:str, path:str):
        '''
        删除一个空目录
        :param bucket_name:桶名
        :param path: 目录全路径
        :return:
            True: success
            raise HarborError(): failed

        :raise HarborError()
        '''
        return self.__hbManager.rmdir(bucket_name, path)

    def ftp_is_dir(self, bucket_name:str, path_name:str):
        '''
        是否时一个目录

        :param bucket_name: 桶名
        :param path_name: 目录路径
        :return:
            true: is dir
            false: is file

        :raise HarborError  # 桶或路径不存在，发生错误
        '''
        return self.__hbManager.is_dir(bucket_name=bucket_name, path_name=path_name)

    def ftp_is_file(self, bucket_name:str, path_name:str):
        '''
        是否时一个文件
        :param bucket_name: 桶名
        :param path_name: 文件路径
        :return:
            true: is file
            false: is dir

        :raise HarborError  # 桶或路径不存在，发生错误
        '''
        return self.__hbManager.is_file(bucket_name=bucket_name, path_name=path_name)

    def ftp_get_obj(self, buckest_name:str, path_name:str):
        '''
        获取对象或目录实例

        :param bucket_name: 桶名
        :param path_name: 文件路径
        :param user: 用户，默认为None，如果给定用户只获取属于此用户的对象（只查找此用户的存储桶）
        :return:
            success: 对象实例

        :raise HarborError
        '''
        return self.__hbManager.get_object(bucket_name=buckest_name, path_name=path_name)

    def ftp_get_write_generator(self, bucket_name: str, obj_path: str):
        '''
        获取一个写入对象的生成器函数

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param user: 用户，默认为None，如果给定用户只删除属于此用户的对象（只查找此用户的存储桶）
        :return:
                generator           # success
                :raise HarborError  # failed

        :usage:
            ok = next(generator)
            ok = generator.send((offset, bytes))  # ok = True写入成功， ok=False写入失败

        :raise HarborError
        '''
        return  self.__hbManager.get_write_generator(bucket_name=bucket_name, obj_path=obj_path)

