from django.utils import timezone
from django.db.models import Case, Value, When, F
from rest_framework import status

from buckets.models import Bucket
from buckets.utils import BucketFileManagement
from utils.storagers import PathParser
from utils.oss import HarborObject
from.paginations import BucketFileLimitOffsetPagination


class HarborError():
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
        obj = self._get_obj_or_dir(table_name, path, name)
        if obj is None:
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='指定对象或目录不存在')

        return obj

    def _get_obj_or_dir_and_bfm(self, table_name, path, name):
        '''
        获取文件对象或目录,和BucketFileManagement对象

        :param table_name: 数据库表名
        :param path: 父目录路经
        :param name: 对象名称或目录名称
        :return:
            obj，bfm: 对象或目录
            None, bfm: 目录或对象不存在，父目录路径错误，不存在
        '''
        bfm = BucketFileManagement(path=path, collection_name=table_name)
        ok, obj = bfm.get_dir_or_obj_exists(name=name)
        if not ok:
            return None, bfm

        if obj:
            return obj, bfm

        return None, bfm

    def _get_obj_or_dir(self, table_name, path, name):
        '''
        获取文件对象或目录

        :param table_name: 数据库表名
        :param path: 父目录路经
        :param name: 对象名称或目录名称
        :return:
            obj: 对象或目录
            None: 目录或对象不存在，父目录路径错误，不存在
        '''
        obj, _ = self._get_obj_or_dir_and_bfm(table_name, path, name)

        return obj

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

        dir, bfm = self._get_obj_or_dir_and_bfm(table_name=_collection_name, path=dir_path, name=dir_name)
        if not dir:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='目录路径参数无效，父节点目录不存在')

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

        dir, bfm = self._get_obj_or_dir_and_bfm(table_name=table_name, path=path, name=dir_name)
        if not dir or dir.is_file():
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='目录不存在')

        if not bfm.dir_is_empty(dir):
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='无法删除非空目录')

        if not dir.do_delete():
            raise HarborError(code=status.HTTP_500_INTERNAL_SERVER_ERROR, msg='删除目录失败，数据库错误')

        return True

    def list_dir(self, bucket_name:str, path:str, offset:int=0, limit:int=1000, user=None):
        '''
        获取目录下的文件列表信息

        :param bucket_name: 桶名
        :param path: 目录路径
        :param offset: 目录下文件列表偏移量
        :param limit: 获取文件信息数量
        :param user: 用户，默认为None，如果给定用户只获取属于此用户的目录下的文件列表信息（只查找此用户的存储桶）
        :return:
                list[object, object,]: success
                raise HarborError:  failed

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

        paginator = BucketFileLimitOffsetPagination()
        if limit <= 0 :
            limit = paginator.default_limit
        else:
            limit = min(limit, paginator.max_limit)

        if offset < 0:
            offset = 0

        try:
            l = paginator.pagenate_to_list(files, offset=offset, limit=limit)
        except Exception as e:
            raise HarborError(code=status.HTTP_500_INTERNAL_SERVER_ERROR, msg=str(e))

        return l

    def move_rename(self, bucket_name:str, obj_path:str, rename=None, move=None, user=None):
        '''
        移动或重命名对象

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param rename: 重命名新名称，默认为None不重命名
        :param move: 移动到move路径下，默认为None不移动
        :param user: 用户，默认为None，如果给定用户只操作属于此用户的对象（只查找此用户的存储桶）
        :return:
            success: object
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
        bucket = self.get_bucket(bucket_name=bucket_name, user=user)
        if not bucket:
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='存储桶不存在')

        table_name = bucket.get_bucket_table_name()
        obj = self._get_obj_or_dir(table_name, path, filename)
        if (not obj) or (not obj.is_file()):
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='对象不存在')

        return self._move_rename_obj(bucket=bucket, obj=obj, move_to=move_to, rename=rename)

    def _move_rename_obj(self, bucket, obj, move_to, rename):
        '''
        移动重命名对象

        :param bucket: 对象所在桶
        :param obj: 文件对象
        :param move_to: 移动目标路径
        :param rename: 重命名的新名称
        :return:
            success: object
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

        if not obj.do_save():
            raise HarborError(code=status.HTTP_500_INTERNAL_SERVER_ERROR, msg= '移动对象操作失败')

        return obj

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
        :param chunk: 要写入的数据
        :param reset: 为True时，先重置对象大小为0后再写入数据；
        :param user: 用户，默认为None，如果给定用户只操作属于此用户的对象（只查找此用户的存储桶）
        :return:
                created             # created==True表示对象是新建的；created==False表示对象不是新建的
                raise HarborError   # 写入失败
        '''
        if not isinstance(chunk, bytes):
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='数据不是bytes类型')

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
                response = self._pre_reset_upload(obj=obj, rados=rados)
                if response is not True:
                    return response

        try:
            self._save_one_chunk(obj=obj, rados=rados, chunk_offset=offset, chunk=chunk)
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

    def _save_one_chunk(self, obj, rados, chunk_offset, chunk:bytes):
        '''
        保存一个上传的分片

        :param obj: 对象元数据
        :param rados: rados接口
        :param chunk_offset: 分片偏移量
        :param chunk: 分片数据
        :return:
            成功：True
            失败：raise HarborError
        '''
        # 先更新元数据，后写rados数据
        # 更新文件修改时间和对象大小
        new_size = chunk_offset + len(chunk) # 分片数据写入后 对象偏移量大小
        if not self._update_obj_metadata(obj, size=new_size):
            raise HarborError(code=status.HTTP_500_INTERNAL_SERVER_ERROR, msg='修改对象元数据失败')

        # 存储文件块
        try:
            ok, msg = rados.write(offset=chunk_offset, data_block=chunk)
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
        bucket = self.get_bucket(bucket_name=bucket_name, user=user)
        if not bucket:
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='存储桶不存在')

        table_name = bucket.get_bucket_table_name()
        fileobj = self._get_obj_or_dir(table_name=table_name, path=path, name=filename)
        if not fileobj or fileobj.isdir():
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
            success: （chunk:bytes, size:int）  #  数据块，对象总大小
            failed:   raise HarborError         # 读取失败，抛出HarborError

        :raise HarborError
        '''
        pp = PathParser(filepath=obj_path)
        path, filename = pp.get_path_and_filename()
        if not bucket_name or not filename:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='参数有误')

        if offset < 0 or size < 0 or size > 20 * 1024 ** 2:  # 20Mb
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='参数有误')

        # 存储桶验证和获取桶对象
        bucket = self.get_bucket(bucket_name=bucket_name, user=user)
        if not bucket:
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='存储桶不存在')

        table_name = bucket.get_bucket_table_name()
        obj = self._get_obj_or_dir(table_name=table_name, path=path, name=filename)
        if not obj or obj.isdir():
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='文件对象不存在')

        # 自定义读取文件对象
        if size == 0:
            return (bytes(), obj.si)

        obj_key = obj.get_obj_key(bucket.id)
        rados = HarborObject(obj_key, obj_size=obj.si)
        ok, chunk = rados.read(offset=offset, size=size)
        if not ok:
            raise HarborError(code=status.HTTP_500_INTERNAL_SERVER_ERROR, msg='文件块读取失败')

        return (chunk, obj.si)

    def get_obj_generator(self, bucket_name:str, obj_path:str, offset:int=0, end:int=None, per_size=10 * 1024 ** 2, user=None):
        '''
        获取一个读取对象的生成器函数

        :param bucket_name: 桶名
        :param obj_path: 对象全路径
        :param offset: 读起始偏移量；type: int
        :param end: 读结束偏移量(包含)；type: int；默认None表示对象结尾；
        :param per_size: 每次读取数据块长度；type: int， 默认10Mb
        :param user: 用户，默认为None，如果给定用户只删除属于此用户的对象（只查找此用户的存储桶）
        :return: generator
                for data in generator:

        :raise HarborError
        '''
        pp = PathParser(filepath=obj_path)
        path, filename = pp.get_path_and_filename()
        if not bucket_name or not filename:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='参数有误')

        if per_size < 0 or per_size > 20 * 1024 ** 2:  # 20Mb
            per_size = 10 * 1024 ** 2   # 10Mb

        if offset < 0 or end < 0:
            raise HarborError(code=status.HTTP_400_BAD_REQUEST, msg='参数有误')

        # 存储桶验证和获取桶对象
        bucket = self.get_bucket(bucket_name=bucket_name, user=user)
        if not bucket:
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='存储桶不存在')

        table_name = bucket.get_bucket_table_name()
        obj = self._get_obj_or_dir(table_name=table_name, path=path, name=filename)
        if not obj or obj.isdir():
            raise HarborError(code=status.HTTP_404_NOT_FOUND, msg='文件对象不存在')

        # 读取文件对象生成器
        obj_key = obj.get_obj_key(bucket.id)
        rados = HarborObject(obj_key, obj_size=obj.si)
        return rados.read_obj_generator(offset=offset, end=end, block_size=per_size)


class FtpHarborManager():
    '''
    ftp操作harbor对象数据元数据管理接口封装
    '''
    def __init__(self):
        self.__hbManager = HarborManager()

    def ftp_authenticate(self, bucket_name:str, password:str):
        '''
        Bucket桶ftp访问认证
        :return:
            False, msg:str
            True, Bucket()
        '''
        bucket = self.__hbManager.get_bucket(bucket_name)
        if not bucket:
            return False, '存储桶不存在'

        if not bucket.is_ftp_enable():
            return False, '存储桶未开启ftp访问权限'

        if bucket.check_ftp_password(password):
            return True, bucket

        return False, '密码有误'

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
            success: （chunk:bytes, size:int）  #  数据块，对象总大小
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
        :return: generator
                for data in generator:

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
                list[object, object,]: success
                raise HarborError:  failed

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
