import os

from django.conf import settings


class FileStorage():
    '''
    基于文件系统的文件存储
    '''
    def __init__(self, file_id, storage_to=None):
        self._file_id = file_id
        self._storage_to = storage_to if storage_to else os.path.join(settings.MEDIA_ROOT, 'upload')
        self._file_absolute_path = os.path.join(self._storage_to, file_id)

    def write(self,chunk,     #要写入的文件块
            chunk_size, # 文件块大小，字节数
            offset = 0  # 数据写入偏移量
            ):
        if chunk.size != chunk_size:
            return False

        try:
            # 路径不存在时创建路径
            if not os.path.exists(self._storage_to):
                os.makedirs(self._storage_to)

            with open(self._file_absolute_path, 'ab+') as f:
                f.seek(offset, 0) # 文件的开头作为移动字节的参考位置
                for chunk in chunk.chunks():
                    f.write(chunk)
        except:
            return False
        return True

    def read(self, read_size, offset=0):
        # 检查文件是否存在
        if not os.path.exists(self._file_absolute_path):
            return False
        try:
            with open(self._file_absolute_path, 'rb') as f:
                f.seek(offset, 0) # 文件的开头作为移动字节的参考位置
                data = f.read(read_size)
        except:
            return False
        return data

    def size(self):
        try:
            fsize = os.path.getsize(self._file_id)
        except FileNotFoundError:
            return -1
        return fsize

    def delete(self):
        # 删除文件
        try:
            os.remove(self._file_absolute_path)
        except FileNotFoundError:
            pass

