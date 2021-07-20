from pyftpdlib.filesystems import AbstractedFS, FilesystemError
from io import BytesIO
import django
import sys
import os

# 将项目路径添加到系统搜寻路径当中，查找方式为从当前脚本开始，找到要调用的django项目的路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# 设置项目的配置文件 不做修改的话就是 settings 文件
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webserver.settings")
django.setup()  # 加载项目配置

from api.harbor import FtpHarborManager
from api.exceptions import HarborError


class HarborFileSystem(AbstractedFS):
    def __init__(self, *args, **kwargs):
        super(HarborFileSystem, self).__init__(*args, **kwargs)
        self.bucket_name = self.root
        self.root = '/'
        self.client = FtpHarborManager()

    def realpath(self, path):
        return path

    def isdir(self, path):
        try:
            return self.client.ftp_is_dir(self.bucket_name, path.lstrip('/'))
        except HarborError as error:
            return False

    def isfile(self, path):
        try:
            return self.client.ftp_is_file(self.bucket_name, path.lstrip('/'))
        except HarborError as error:
            return False

    def islink(self, fs_path):
        return False

    def chdir(self, path):
        if self.isdir(path):
            self._cwd = self.fs2ftp(path)
        else:
            raise FilesystemError('Not a dir.')

    def stat(self, path):
        pass

    def lstat(self, path):
        pass

    def listdir(self, path):
        dir_list = []
        try:
            files = []
            files_generator = self.client.ftp_list_dir_generator(self.bucket_name, path[1:], per_num=2000)
            for _ in range(10):
                try:
                    data = next(files_generator)
                except StopIteration as error:
                    break 
                files.append(data)
            for fi in files:
                for file in fi: 
                    if file.fod is True:
                        if file.upt:
                            dir_list.append((file.name, file.upt, file.si))
                        else:
                            dir_list.append((file.name, file.ult, file.si))
                    else:
                        dir_list.append((file.name + '/', file.ult, 0))
        except HarborError as error:
            raise FilesystemError(error.msg)
        return dir_list

    def format_list(self, basedir, listing, ignore_err=True):
        assert isinstance(basedir, str), basedir

        # path = self.fs2ftp(basedir)
        for tmp in listing:
            if isinstance(tmp, tuple):
                filename, mtimestr, size = tmp
                if filename.endswith('/'):
                    perm = "drwxrwxrwx"
                    mtimestr = mtimestr.strftime('%b %d %X')[:-3]
                    filename = filename[:-1]
                else:
                    mtimestr = mtimestr.strftime('%b %d %X')[:-3]
                    perm = "-r-xr-xr-x"
                line = "%s %3s %-8s %-8s %8s %12s %s\r\n" % (
                    perm, 1, 'root', 'root', size, mtimestr, filename)

                if self.cmd_channel is not None:
                    yield line.encode("utf8", self.cmd_channel.unicode_errors)
                else:
                    yield line.encode("utf8")
            else:
                perm = "-r-xr-xr-x"
                line = "%s %3s %-8s %-8s %8s %12s %s\r\n" % (
                    perm, 1, 'root', 'root', '', '', tmp)

                if self.cmd_channel is not None:
                    yield line.encode("utf8", self.cmd_channel.unicode_errors)
                else:
                    yield line.encode("utf8")

    def format_mlsx(self, basedir, listing, perms, facts, ignore_err=True):
        assert isinstance(basedir, str), basedir

        ftp_path = self.fs2ftp(basedir)
        if len(listing) == 1:
            if isinstance(listing[0], tuple):
                if listing[0][0].endswith('/'):
                    _type = "dir"
                    perm = 'r'
                    filename = listing[0][0][:-1]
                    mtimestr = listing[0][1]
                    mtimestr = str(mtimestr).split('.')[0].replace('-', '').replace(':', '').replace(' ', '')
                    size = listing[0][2]
                    line = "type=%s;size=%d;perm=%s;modify=%s;unique=%s; %s\r\n" % (
                        _type, size, perm, mtimestr, '', filename)
                    if self.cmd_channel is not None:
                        yield line.encode("utf8", self.cmd_channel.unicode_errors)
                    else:
                        yield line.encode("utf8")
                else:
                    filename = listing[0][0]
                    mtimestr = listing[0][1]
                    mtimestr = str(mtimestr).split('.')[0].replace('-', '').replace(':', '').replace(' ', '')
                    size = listing[0][2]
                    perm = 'el'
                    _type = "file"
                    line = "type=%s;size=%d;perm=%s;modify=%s;unique=%s; %s\r\n" % (
                        _type, size, perm, mtimestr, '', filename)
                    if self.cmd_channel is not None:
                        yield line.encode("utf8", self.cmd_channel.unicode_errors)
                    else:
                        yield line.encode("utf8")
            else:
                ftp_path = os.path.join(ftp_path, listing[0])
                try:
                    data = self.client.ftp_get_obj(self.bucket_name, ftp_path[1:])
                    filename = data.name
                    mtimestr = data.upt
                    mtimestr = str(mtimestr).split('.')[0].replace('-', '').replace(':', '').replace(' ', '')
                    size = data.si
                    perm = 'el'
                    _type = "file"
                    line = "type=%s;size=%d;perm=%s;modify=%s;unique=%s; %s\r\n" % (
                        _type, size, perm, mtimestr, '', filename)
                    if self.cmd_channel is not None:
                        yield line.encode("utf8", self.cmd_channel.unicode_errors)
                    else:
                        yield line.encode("utf8")
                except HarborError as error:
                    raise FilesystemError(error.msg)
        else:
            for filename, mtimestr, size in listing:
                if filename.endswith('/'):
                    _type = "dir"
                    perm = 'r'
                    filename = filename[:-1]
                else:
                    perm = 'el'
                    _type = "file"
                mtimestr = str(mtimestr).split('.')[0].replace('-', '').replace(':', '').replace(' ', '')
                line = "type=%s;size=%d;perm=%s;modify=%s;unique=%s; %s\r\n" % (
                    _type, size, perm, mtimestr, '', filename)

                if self.cmd_channel is not None:
                    yield line.encode("utf8", self.cmd_channel.unicode_errors)
                else:
                    yield line.encode("utf8")

    def open(self, filename, mode):
        """Open a file returning its handler."""
        assert isinstance(filename, str), filename
        # print('function: open', filename, mode)
        ftp_path = self.fs2ftp(filename)
        mode = mode.lower()
        return FileHandler(self.bucket_name, ftp_path, self.client, mode)

    def mkdir(self, path):
        ftp_path = self.fs2ftp(path)
        try:
            self.client.ftp_mkdir(self.bucket_name, ftp_path[1:])
        except (HarborError, Exception) as error:
            raise FilesystemError(str(error))

    def rename(self, src, dst):
        new_name = os.path.basename(dst)
        new_dir = os.path.dirname(dst)
        try:
            self.client.ftp_move_rename(self.bucket_name, src[1:], new_name, new_dir)
        except HarborError as error:
            raise FilesystemError(f'rename dir is not supported, {str(error)}')
        except Exception as error:
            raise FilesystemError(str(error))

    def lexists(self, path):
        ftp_path = self.fs2ftp(path)

        if self.isdir(ftp_path) or self.isfile(ftp_path):
            return True
        else:
            return False

    def rmdir(self, path):
        ftp_path = self.fs2ftp(path)
        try:
            self.client.ftp_rmdir(self.bucket_name, ftp_path[1:])
        except (HarborError, Exception) as error:
            raise FilesystemError(str(error))

    def remove(self, path):
        ftp_path = self.fs2ftp(path)
        try:
            self.client.ftp_delete_object(self.bucket_name, ftp_path[1:])
        except (HarborError, Exception) as error:
            raise FilesystemError(str(error.msg))

    def getsize(self, path):
        ftp_path = self.fs2ftp(path)
        try:
            return self.client.ftp_get_obj_size(self.bucket_name, ftp_path[1:])
        except (HarborError, Exception) as error:
            raise FilesystemError(str(error.msg))


class FileHandler(object):
    def __init__(self, bucket_name, ftp_path, client, mode):
        self.bucket_name = bucket_name
        self.name = os.path.basename(ftp_path)
        self.ftp_path = ftp_path
        self.client = client
        self.closed = False
        self.file = BytesIO()
        self.offset = 0             # file pointer position
        self.is_breakpoint = False  # 标记是否断点续传
        self.write_generator = None
        self.read_generator = None
        self.mode = mode

    def ensure_init_write_generator(self, is_break_point=None):
        """
        确保已初始化 写生成器
        """
        if self.write_generator:
            return

        is_break_point = self.is_breakpoint if is_break_point is None else is_break_point
        try:
            self.write_generator = self.client.ftp_get_write_generator(
                self.bucket_name, self.ftp_path[1:], is_break_point)
            next(self.write_generator)
        except HarborError as error:
            raise FilesystemError(error.msg)

    def ensure_init_read_generator(self):
        if self.read_generator:
            return

        try:
            self.read_generator, ob = self.client.ftp_get_obj_generator(
                self.bucket_name, self.ftp_path[1:], offset=self.offset, per_size=4 * 1024 ** 2)
        except HarborError as error:
            raise FilesystemError(error.msg)

    def write(self, data):
        self.ensure_init_write_generator()
        self.file.write(data)
        if self.file.tell() >= 1024 ** 2 * 32:
            self._sync_cache()

        return len(data)

    def read(self, size=None):
        self.ensure_init_read_generator()
        try:
            data = next(self.read_generator)
        except Exception as error:
            return b''

        return data

    def close(self):
        # 写模式时，确认init写生成器创建对象，防止ftp上传空文件时没有创建对象的问题
        if 'w' in self.mode:
            self.ensure_init_write_generator()

        self._sync_cache()
        self.file.close()
        self.closed = True

    def seek(self, offset):
        self._sync_cache()      # seek前，同步可能缓存的数据
        if self.offset == offset:
            return

        self.offset = offset
        if self.offset == 0:
            self.is_breakpoint = False
        else:
            self.is_breakpoint = True

        # 当前文件指针变offset了，读、写生成器都需要根据offset从新init
        self.write_generator = None
        self.read_generator = None

    def _sync_cache(self):
        """
        缓存的文件数据同步到存储桶
        """
        lenght = self.file.tell()
        if lenght == 0:
            return

        self.ensure_init_write_generator()

        try:
            self.write_generator.send((self.offset, self.file.getvalue()))
        except HarborError as error:
            raise FilesystemError(error.msg)

        self.offset += lenght
        self.file.truncate(0)       # 清缓存
        self.file.seek(0)           # 重置缓存指针


class DownLoader(object):
    def __init__(self, bucket_name, ftp_path, client):
        self.bucket_name = bucket_name
        self.name = os.path.basename(ftp_path)
        # self.name = '/'
        self.ftp_path = ftp_path
        self.client = client
        self.closed = False
        self.id = 0
        try:
            self.obj_generator, ob = self.client.ftp_get_obj_generator(
                self.bucket_name, self.ftp_path[1:], per_size=4 * 1024 ** 2)
        except HarborError as error:
            raise FilesystemError(error.msg)

    def read(self, size=None):
        try:
            data = next(self.obj_generator)
        except Exception as error:
            return b''

        return data

    def close(self):
        self.closed = True


if __name__ == '__main__':
    print('no info')
    f = open('harbor_auto.py')
    f.read()
