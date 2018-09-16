import os
import uuid

from django.conf import settings
from django.http import Http404, StreamingHttpResponse, FileResponse
from mongoengine.context_managers import switch_db

from .models import UploadFileInfo



class FileSystemHandlerBackend():
    '''
    基于文件系统的文件处理器后端
    '''

    ACTION_STORAGE = 1 #存储
    ACTION_DELETE = 2 #删除
    ACTION_DOWNLOAD = 3 #下载


    def __init__(self, request, uuid, action, *args, **kwargs):
        '''
        uuid
        '''
        #文件对应uuid
        self.uuid = uuid if uuid else self._get_new_uuid()
        #文件存储的目录
        self.base_dir = os.path.join(settings.MEDIA_ROOT, 'upload')
        self.request = request
        self.action = action #处理方式


    def file_storage(self):
        '''存储文件'''
        #获取上传的文件对象
        file_obj = self.request.FILES.get('file', None)
        if not file_obj:
            return False
        #路径不存在时创建路径
        base_dir = self.get_base_dir()
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)

        #保存文件
        full_path_filename = self.get_full_path_filename()
        with open(full_path_filename, 'wb') as f:
            for chunk in file_obj.chunks():
                f.write(chunk)

        #保存对应文件记录到指定数据库
        with switch_db(UploadFileInfo, 'db2'):
            UploadFileInfo(uuid=self.uuid, filename=file_obj.name, size=file_obj.size).save()

        return True



    def file_detele(self):
        '''删除文件'''
        #是否存在uuid对应文件
        ok, finfo = self.is_file_info_exists()
        if not ok:
            raise Http404('文件不存在')

        full_path_filename = self.get_full_path_filename()
        #删除文件和文件记录
        try:
            os.remove(full_path_filename)
        except FileNotFoundError:
            pass
        with switch_db(UploadFileInfo, 'db2'):
            finfo.delete()

        return True


    def file_download(self):
        #是否存在uuid对应文件
        ok, finfo = self.is_file_info_exists()
        if not ok:
            raise Http404('文件不存在')

        full_path_filename = self.get_full_path_filename()

        # response = StreamingHttpResponse(file_read_iterator(full_path_filename)) 
        response = FileResponse(self.file_read_iterator(full_path_filename))
        response['Content-Type'] = 'application/octet-stream'  # 注意格式
        response['Content-Disposition'] = f'attachment;filename="{finfo.filename}"'  # 注意filename 这个是下载后的名字
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
        act = action if action else self.action
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
        return os.path.join(self.base_dir, self.uuid) 

    def is_file_info_exists(self):
        '''是否存在uuid对应文件记录'''
        with switch_db(UploadFileInfo, 'db2'):
            finfo = UploadFileInfo.objects(uuid=self.uuid)
            if not finfo:
                return False, None
            finfo = finfo.first()
        return True, finfo




