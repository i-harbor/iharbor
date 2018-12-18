from bson import ObjectId

from mongoengine.queryset.visitor import Q as mQ
from mongoengine.queryset import DoesNotExist, MultipleObjectsReturned

from .models import BucketFileInfoBase


class BucketFileManagement():
    '''
    存储桶相关的操作方法类
    '''
    def __init__(self, path='', collection_name='', *args, **kwargs):
        self._path = path if path else ''
        self._collection_name = collection_name
        self.cur_dir_id = None
        self._bucket_file_class = self.creat_bucket_file_class()

    def creat_bucket_file_class(self):
        '''动态创建类，以避免mongoengine switch_collection的线程安全问题'''
        return type('BucketFile', (BucketFileInfoBase,), {
            'meta': {
                'db_alias': 'default',
                'indexes': ['did', 'ult', 'na'],#索引
                'ordering': ['fod', '-ult'], #文档降序，最近日期靠前
                'collection': self.get_collection_name(),
            }
        })

    def get_bucket_file_class(self):
        if not self._bucket_file_class:
            self._bucket_file_class = self.creat_bucket_file_class()

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
            正常返回(True, path目录的id)；未找到记录返回(False, None)，即参数有误
        '''
        if self.cur_dir_id:
            return (True, self.cur_dir_id)

        path = dir_path if dir_path else self._path
        # path为空，根目录为存储桶
        if path == '':
            return (True, None)

        path = self._hand_path(path)
        if not path:
            return (False, None) # path参数有误

        try:
            dir = self.get_bucket_file_class().objects.get(mQ(na=path) & mQ(fod=False))  # 查找目录记录
        except DoesNotExist as e:
            return (False, None)  # path参数有误,未找到对应目录信息
        except MultipleObjectsReturned as e:
            return (False, None)  # path参数有误,未找到对应目录信息
        if dir:
            self.cur_dir_id = dir.id
        return (True, self.cur_dir_id)  # None->未找到对应目录


    def get_cur_dir_files(self, cur_dir_id=None):
        '''
        获得当前目录下的文件或文件夹记录

        :param cur_dir_id: 目录id;
        :return: 目录id下的文件或目录记录list; id==None时，返回存储桶下的文件或目录记录list
        '''
        if cur_dir_id:
            dir_id = cur_dir_id
            files = self.get_bucket_file_class().objects(did=dir_id).all()
            return True, files

        if self._path:
            ok, dir_id = self.get_cur_dir_id()

            # path路径有误
            if not ok:
                return False, None

            if dir_id:
                files = self.get_bucket_file_class().objects(mQ(did=dir_id) & mQ(na__exists=True) & (mQ(sds__exists=False) | mQ(sds=False))).all()
                return True, files

        #存储桶下文件目录
        files = self.get_bucket_file_class().objects(mQ(did__exists=False) & mQ(na__exists=True) & (mQ(sds__exists=False) | mQ(sds=False))).all()  # did不存在表示是存储桶下的文件目录
        return True, files

    def get_file_exists(self, file_name):
        '''
        通过文件名获取当前目录下的文件信息
        :param file_name:
        :return:
            第一个返回值表示是否有错去发生
            第二个返回值，如果存在返回文件记录对象，否则None
        '''
        ok, did = self.get_cur_dir_id()
        if not ok:
            return False, None

        file_name.strip('/')
        if did:
            bfis = self.get_bucket_file_class().objects((mQ(na=file_name) & mQ(did=did) & mQ(fod=True)) &
                                          (mQ(sds__exists=False) | mQ(sds=False)))# 目录下是否存在给定文件名的文件
        else:
            bfis = self.get_bucket_file_class().objects((mQ(na=file_name) & mQ(did__exists=False) & mQ(fod=True)) &
                                          (mQ(sds__exists=False) | mQ(sds=False)))  # 存储桶下是否存在给定文件名的文件

        bfi = bfis.first()

        return True, bfi if bfi else None

    def get_dir_exists(self, dir_name):
        '''
        通过目录名获取当前目录下的目录信息
        :param dir_name: 目录名称（不含父路径），
        :return:
            第一个返回值：表示是否有错去发生，(可能错误：当前目录参数有误，对应目录不存在)
            第二个返回值：如果存在返回文件记录对象，否则None
        '''
        # 先检测当前目录存在
        ok, did = self.get_cur_dir_id()
        if not ok:
            return False, None

        dir_path_name = self.build_dir_full_name(dir_name)

        try:
            dir = self.get_bucket_file_class().objects.get((mQ(na=dir_path_name) & mQ(fod=False)) & ((mQ(sds__exists=False) | mQ(sds=False))))  # 查找目录记录
        except DoesNotExist as e:
            return (True, None)  # 未找到对应目录信息
        except MultipleObjectsReturned as e:
            raise e

        return True, dir

    def build_dir_full_name(self, dir_name):
        dir_name.strip('/')
        path = self._hand_path(self._path)
        return (path + '/' + dir_name) if path else dir_name

    def get_file_obj_by_id(self, id):
        '''
        通过id获取文件对象
        :return:
        '''
        try:
            bfis = self.get_bucket_file_class().objects((mQ(id=id)) & (mQ(sds__exists=False) | mQ(sds=False)))  # 目录下是否存在给定文件名的文件
        except:
            return None

        return bfis.first()






