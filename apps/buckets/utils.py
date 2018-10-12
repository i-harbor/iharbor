import os
import uuid

from django.conf import settings
from django.http import FileResponse
from mongoengine.context_managers import switch_collection
from mongoengine.queryset.visitor import Q as mQ
from mongoengine.queryset import DoesNotExist, MultipleObjectsReturned

from .models import BucketFileInfo


class FileSystemHandlerBackend():
    '''
    基于文件系统的文件处理器后端
    '''

    ACTION_STORAGE = 1 #存储
    ACTION_DELETE = 2 #删除
    ACTION_DOWNLOAD = 3 #下载


    def __init__(self, request, action, bucket_name, cur_path='', id=None, *args, **kwargs):
        '''
        @ id:要操作的文件id,上传文件时参数id不需要传值
        @ action:操作类型
        '''
        #文件对应id
        self.id = id
        #文件存储的目录
        self.base_dir = os.path.join(settings.MEDIA_ROOT, 'upload')
        self.request = request
        self._action = action #处理方式
        self._collection_name = get_collection_name(self.request.user.username, bucket_name)
        self.cur_path = cur_path


    def file_storage(self):
        '''
        存储文件
        :return: 成功：True，失败：False
        '''
        #获取上传的文件对象
        file_obj = self.request.FILES.get('file', None)
        if not file_obj:
            return False
        #路径不存在时创建路径
        base_dir = self.get_base_dir()
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)

        # 保存对应文件记录到指定集合
        # with switch_collection(UploadFileInfo, self._collection_name) as FileInfo:
        info = BucketFileInfo(
            na=file_obj.name, #文件名
            fod=True, #true:文件；false:目录
            si=file_obj.size,
            # did=None#父节点id,属于存储桶根目录时
        )
        p_id = self.get_cur_dir_id() # 父节点id
        if p_id:
            info.did = p_id
        info.switch_collection(self._collection_name)
        info.save()
        self.id = str(info.id)

        #保存文件
        full_path_filename = self.get_full_path_filename()
        with open(full_path_filename, 'wb') as f:
            for chunk in file_obj.chunks():
                f.write(chunk)

        return True


    def file_detele(self):
        '''删除文件'''
        #是否存在uuid对应文件
        ok, finfo = self.get_file_info()
        if not ok:
            return False

        full_path_filename = self.get_full_path_filename()
        #删除文件和文件记录
        try:
            os.remove(full_path_filename)
        except FileNotFoundError:
            pass

        #切换到对应集合
        # with switch_collection(UploadFileInfo, self.get_collection_name()):
        finfo.switch_collection(self._collection_name)
        finfo.delete()

        return True


    def file_download(self):
        #是否存在uuid对应文件
        ok, finfo = self.get_file_info()
        if not ok:
            return False

        #文件是否存在
        full_path_filename = self.get_full_path_filename()
        if not self.is_file_exists(full_path_filename):
            return False

        # response = StreamingHttpResponse(file_read_iterator(full_path_filename)) 
        response = FileResponse(self.file_read_iterator(full_path_filename))
        response['Content-Type'] = 'application/octet-stream'  # 注意格式
        response['Content-Disposition'] = f'attachment;filename="{finfo.na}"'  # 注意filename 这个是下载后的名字
        return response

            
    def file_read_iterator(self, file_name, chunk_size=1024*2):
        '''
        读取文件生成器
        '''
        with open(file_name, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if chunk:
                    yield chunk
                else:
                    break

    def do_action(self, action=None):
        '''上传/下载/删除操作执行者'''
        act = action if action else self._action
        if act == self.ACTION_STORAGE:
            return self.file_storage()
        elif act == self.ACTION_DOWNLOAD:
            return self.file_download()
        elif act == self.ACTION_DELETE:
            return self.file_detele()


    def _get_new_uuid(self):
        '''创建一个新的uuid字符串'''
        uid = uuid.uuid1()
        return str(uid)

    def get_base_dir(self):
        '''获得文件存储的目录'''
        return self.base_dir

    def get_full_path_filename(self):
        '''文件绝对路径'''
        return os.path.join(self.base_dir, self.id)

    def get_file_info(self):
        '''是否存在uuid对应文件记录'''
        # 切换到指定集合查询对应文件记录
        with switch_collection(BucketFileInfo, self.get_collection_name()):
            finfos = BucketFileInfo.objects(id=self.id)
            if finfos:
                finfo = finfos.first()
                return True, finfo
        return False, None

    def is_file_exists(self, full_path_filename=None):
        '''检查文件是否存在'''
        filename = full_path_filename if full_path_filename else self.get_full_path_filename()
        return os.path.exists(filename)

    def get_collection_name(self):
        '''获得当前用户存储桶Bucket对应集合名称'''
        return self._collection_name

    def get_cur_dir_id(self, path=None):
        '''
        获得当前目录节点id
        @ return: 正常返回path目录的id；未找到记录返回None，即参数有误
        '''
        path = path if path else self.cur_path
        path = path.strip()
        if path.endswith('/'):
            path = path.rstrip('/')
        if not path:
            return None  # path参数有误

        with switch_collection(BucketFileInfo, self.get_collection_name()):
            try:
                dir = BucketFileInfo.objects.get(mQ(na=path) & mQ(fod=False))  # 查找目录记录
            except DoesNotExist as e:
                pass
            except MultipleObjectsReturned as e:
                raise e

        return dir.id if dir else None  # None->未找到对应目录


def get_collection_name(username, bucket_name):
    '''
    获得当前用户存储桶Bucket对应集合名称
    每个存储桶对应的集合表名==用户名_存储桶名称
    '''
    return f'{username}_{bucket_name}'


class BucketFileManagement():
    '''
    存储桶相关的操作方法类
    '''
    def __init__(self, path='', *args, **kwargs):
        self._path = path if path else ''
        self.cur_dir_id = None

    def _hand_path(self, path):
        '''去除path字符串两边可能的空白和/'''
        if isinstance(path, str):
            return path.strip(' /')
        return ''

    def get_dir_link_paths(self, dir_path=None):
        '''
        目录路径导航连接路径path
        :return: {dir_name: dir_full_path}
        '''
        dir_link_paths = {}
        path = dir_path if dir_path  else self._path
        if path == '':
            return dir_link_paths
        path = self._hand_path(path)
        dirs = path.split('/')
        for i, key in enumerate(dirs):
            dir_link_paths[key] = '/'.join(dirs[0:i+1])
        return dir_link_paths


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
            dir = BucketFileInfo.objects.get(mQ(na=path) & mQ(fod=False))  # 查找目录记录
        except DoesNotExist as e:
            return (False, None)  # path参数有误,未找到对应目录信息
        except MultipleObjectsReturned as e:
            raise e
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
            return True, BucketFileInfo.objects(did=dir_id).all()

        if self._path:
            ok, dir_id = self.get_cur_dir_id()

            # path路径有误
            if not ok:
                return False, None

            if dir_id:
                return True, BucketFileInfo.objects(did=dir_id).all()

        #存储桶下文件目录
        return True, BucketFileInfo.objects(did__exists=False).all()  # did不存在表示是存储桶下的文件目录

    def get_file_exists(self, file_name):
        '''
        通过文件名获取当前目录下的文件信息
        :param file_name:
        :return: 如果存在返回文件记录，否则None
        '''
        ok, did = self.get_cur_dir_id()
        if not ok:
            return False, None

        if did:
            bfis = BucketFileInfo.objects(mQ(na=file_name) & mQ(did=did) & mQ(fod=True))# 目录下是否存在给定文件名的文件
        else:
            bfis = BucketFileInfo.objects(mQ(na=file_name) & mQ(did__exists=False) & mQ(fod=True))  # 存储桶下是否存在给定文件名的文件

        bfi = bfis.first()

        return True, bfi if bfi else None



