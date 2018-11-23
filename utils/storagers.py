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

    def get_file_generator(self, chunk_size=2*1024*1024):
            '''
            获取读取文件生成器
            :param chunk_size: 每次迭代返回的数据块大小，type: int
            :return:
                success: a generator function to read file
                error: None
            '''
            if chunk_size <= 0:
                chunk_size = 2 * 1024 * 1024 #2MB

            # 文件是否存在
            if not os.path.exists(self._file_absolute_path):
                return None

            def file_generator(size=chunk_size):
                with open(self._file_absolute_path, 'rb') as f:
                    while True:
                        chunk = f.read(size)
                        if chunk:
                            yield chunk
                        else:
                            break
            return file_generator


class PathParser():
    '''
    路径字符串解析
    '''
    def __init__(self, filepath, *args, **kwargs):
        self._path = filepath # 绝对路径， type: str

    def get_path_and_filename(self):
        '''
        分割一个绝对路径，获取文件名和父路径,优先获取文件名
        :return: Tuple(path, filename)
        '''
        fullpath = self._path.strip('/')
        if not fullpath:
            return ('', '')
        l = fullpath.rsplit('/', maxsplit=1)
        filename = l[-1]
        path = l[0] if len(l) == 2 else ''
        return (path, filename)

    def get_bucket_path_and_filename(self):
        '''
       分割一个绝对路径，获取文件名、存储通名和父路径，优先获取文件名、存储通名
       :return: Tuple(bucket_name, path, filename)
       '''
        bucket_path, filename = self.get_path_and_filename()
        if not bucket_path:
            return ('', '', filename)
        l = bucket_path.split('/', maxsplit=1)
        bucket_name = l[0]
        path = l[-1] if len(l) == 2 else ''
        return (bucket_name, path, filename)

    def get_bucket_and_dirpath(self):
        '''
       分割一个绝对路径，获取存储通名、文件夹路径，优先获取存储桶路径
       :return: Tuple(bucket_name, dirpath)
       '''
        fullpath = self._path.strip('/')
        if not fullpath:
            return ('', '')

        l = fullpath.split('/', maxsplit=1)
        bucket_name = l[0]
        dirpath = l[-1] if len(l) == 2 else ''
        return (bucket_name, dirpath)

    def get_bucket_path_and_dirname(self):
        '''
       分割一个绝对路径，获取存储通名、文件夹名、和父路径，优先获取存储通名、文件夹名
       :return: Tuple(bucket_name, path, dirname)
       '''
        bucket_name, dirpath = self.get_bucket_and_dirpath()

        if not dirpath:
            return (bucket_name, '', '')

        l = dirpath.rsplit('/', maxsplit=1)
        dirname = l[-1]
        path = l[0] if len(l) == 2 else ''

        return (bucket_name, path, dirname)

    def get_path_breadcrumb(self, path=None):
        '''
        路径面包屑
        :return: list([dir_name，dir_full_path])
        '''
        breadcrumb = []
        _path = path if path is not None else self._path
        if _path == '':
            return breadcrumb

        _path = _path.strip('/')
        dirs = _path.split('/')
        for i, key in enumerate(dirs):
            breadcrumb.append([key, '/'.join(dirs[0:i+1])])
        return breadcrumb
