import os

from django.conf import settings
from django.core.files.uploadhandler import (FileUploadHandler,
                                             MemoryFileUploadHandler,
                                             TemporaryFileUploadHandler)
from django.core.files.uploadedfile import UploadedFile
from django.core.exceptions import RequestDataTooBig
from django.utils.translation import gettext

from utils.oss.pyrados import FileWrapper
from utils.oss.shortcuts import build_harbor_object
from utils.md5 import FileMD5Handler, Sha256Handler


def try_close_file(f):
    try:
        if hasattr(f, 'close'):
            f.close()
    except Exception as e:
        return False

    return True


class FileStorage:
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


class PathParser:
    '''
    路径字符串解析
    '''
    def __init__(self, filepath, *args, **kwargs):
        self._path = filepath if isinstance(filepath, str) else '' # 绝对路径， type: str

    def get_path_and_filename(self):
        '''
        分割一个绝对路径，获取文件名和父路径,优先获取文件名
        :return: Tuple(path, filename)
        '''
        fullpath = self._path #.strip('/')
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


class CephUploadFile(UploadedFile):
    """
    上传存储到ceph的一个文件
    """
    DEFAULT_CHUNK_SIZE = 5 * 2**20     # default 5MB

    def __init__(
            self, file, field_name, name, content_type, size, charset, file_md5='', content_type_extra=None,
            md5_handler=None
    ):
        super().__init__(file, name, content_type, size, charset, content_type_extra)
        self.field_name = field_name
        self.file_md5 = file_md5
        self.md5_handler = md5_handler
        self.sha256_handler = None

    def open(self, mode=None):
        self.file.seek(0)
        return self

    def delete(self):
        self.file.delete()


class FileUploadToCephHandler(FileUploadHandler):
    """
    直接存储到ceph的自定义文件上传处理器
    """
    chunk_size = 5 * 2 ** 20    # 5MB
    max_size_upload_limit = None

    def __init__(self, request, using, pool_name='', obj_key=''):
        super().__init__(request=request)
        self.using = using
        self.pool_name = pool_name
        self.obj_key = obj_key
        self.file = None
        self.file_md5_handler = None

    def get_max_size_upload_limit(self):
        if self.max_size_upload_limit:
            return self.max_size_upload_limit

        return getattr(settings, 'CUSTOM_UPLOAD_MAX_FILE_SIZE', 5 * 2 ** 30)  # default 5GB

    def handle_raw_input(self, input_data, META, content_length, boundary, encoding=None):
        """
        Handle the raw input from the client.
        """
        # max_size = getattr(settings, 'CUSTOM_UPLOAD_MAX_FILE_SIZE', 10 * 2 ** 30)    # default 10GB
        max_size = self.get_max_size_upload_limit()
        if max_size is None:
            return

        if content_length > max_size:
            raise RequestDataTooBig(gettext('上传文件超过大小限制'))

        if content_length <= 0:
            raise Exception(gettext('无效的标头Content-Length'))

    def new_file(self, *args, **kwargs):
        """
        Create the file object to append to as data is coming in.
        """
        super().new_file(*args, **kwargs)
        ho = build_harbor_object(using=self.using, pool_name=self.pool_name, obj_id=self.obj_key)
        self.file = FileWrapper(ho)
        self.file_md5_handler = FileMD5Handler()

    def receive_data_chunk(self, raw_data, start):
        self.file.write(raw_data, offset=start)
        if self.file_md5_handler:
            self.file_md5_handler.update(offset=start, data=raw_data)

    def file_complete(self, file_size):
        self.file.seek(0)
        self.file.size = file_size
        return CephUploadFile(
            file=self.file,
            field_name=self.field_name,
            name=self.file_name,
            content_type=self.content_type,
            size=file_size,
            charset=self.charset,
            file_md5=self.file_md5(),
            content_type_extra=self.content_type_extra,
            md5_handler=self.file_md5_handler
        )

    def file_md5(self):
        fmh = self.file_md5_handler
        if fmh:
            return fmh.hex_md5

        return ''


class Md5MemoryFileUploadHandler(MemoryFileUploadHandler):

    def new_file(self, *args, **kwargs):

        if self.activated:
            self.file_md5_handler = FileMD5Handler()

        super().new_file(*args, **kwargs)

    def receive_data_chunk(self, raw_data, start):
        """Add the data to the BytesIO file."""
        if self.activated and self.file_md5_handler:
            self.file_md5_handler.update(offset=start, data=raw_data)

        return super().receive_data_chunk(raw_data=raw_data, start=start)

    def file_complete(self, file_size):
        f = super().file_complete(file_size=file_size)
        f.file_md5_handler = self.file_md5_handler
        f.file_md5 = self.file_md5_handler.hex_md5
        return f


class Md5TemporaryFileUploadHandler(TemporaryFileUploadHandler):
    def new_file(self, *args, **kwargs):
        """
        Create the file object to append to as data is coming in.
        """
        super().new_file(*args, **kwargs)
        self.file_md5_handler = FileMD5Handler()

    def receive_data_chunk(self, raw_data, start):
        super().receive_data_chunk(raw_data=raw_data, start=start)
        self.file_md5_handler.update(offset=start, data=raw_data)

    def file_complete(self, file_size):
        f = super().file_complete(file_size=file_size)
        f.file_md5_handler = self.file_md5_handler
        f.file_md5 = self.file_md5_handler.hex_md5
        return f


class AllFileUploadInMemoryHandler(Md5MemoryFileUploadHandler):
    """
    File upload handler to stream uploads into memory (used for small files).
    """

    def handle_raw_input(self, input_data, META, content_length, boundary, encoding=None):
        """
        Use the content_length to signal whether or not this handler should be
        used.
        """
        # Check the content-length header to see if we should
        # If the post is too large, we cannot use the Memory handler.
        self.activated = True


class PartUploadToCephHandler(FileUploadToCephHandler):
    """
    multipart upload直接存储到ceph上传处理器
    """
    chunk_size = 5 * 2 ** 20    # 5MB
    max_size_upload_limit = 5 * 1024 ** 3       # 5GB

    def __init__(self, request, using: str, pool_name='', obj_key='', offset=0):
        self.offset = offset
        super().__init__(request=request, using=using, pool_name=pool_name, obj_key=obj_key)
        amz_content_sha256 = self.request.headers.get('X-Amz-Content-SHA256', None)
        if amz_content_sha256 and amz_content_sha256 != 'UNSIGNED-PAYLOAD':
            self.file_sha256_handler = Sha256Handler()
        else:
            self.file_sha256_handler = None

    def receive_data_chunk(self, raw_data, start):
        """
        :raises: RadosError
        """
        self.file.write(raw_data, offset=self.offset)
        self.offset += len(raw_data)
        if self.file_md5_handler:
            self.file_md5_handler.update(offset=start, data=raw_data)

        if self.file_sha256_handler is not None:
            self.file_sha256_handler.update(offset=start, data=raw_data)

    def file_complete(self, file_size):
        f = super().file_complete(file_size)
        f.sha256_handler = self.file_sha256_handler
        return f
