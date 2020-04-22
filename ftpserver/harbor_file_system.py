from pyftpdlib.filesystems import AbstractedFS, FilesystemError
from io import BytesIO
import django
import sys
import os
import datetime
import time

# 将项目路径添加到系统搜寻路径当中，查找方式为从当前脚本开始，找到要调用的django项目的路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# 设置项目的配置文件 不做修改的话就是 settings 文件
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webserver.settings")
django.setup()  # 加载项目配置

from api.harbor import FtpHarborManager, HarborError


class HarborFileSystem(AbstractedFS):
    def __init__(self, *args, **kwargs):
        super(HarborFileSystem, self).__init__(*args, **kwargs)
        self.bucket_name = self._root
        self._root = '/'
        self.client = FtpHarborManager()

    def realpath(self, path):
        # print('function: realpath', 'path: ' + path)
        return path

    def isdir(self, path):
        # print('function: isdir', 'path: ' + path)
        # print('isdir return', self.client.ftp_is_dir(self.bucket_name, path[1:]))
        try:
            return self.client.ftp_is_dir(self.bucket_name, path.lstrip('/'))
        except HarborError as error:
            print(error)
            return False

    def isfile(self, path):
        # print('function: isfile', 'path: ' + path)
        # print('isfile return', self.client.ftp_is_file(self.bucket_name, path[1:]))
        try:
            return self.client.ftp_is_file(self.bucket_name, path.lstrip('/'))
        except HarborError as error:
            print(error)
            return False

    def islink(self, fs_path):
        # print('function: islink', 'fs_path: ' + fs_path)
        # print('islink return', False)
        return False

    def chdir(self, path):
        # print('function: chdir', 'path: ' + path)
        if self.isdir(path):
            self._cwd = self.fs2ftp(path)
            # print('赋值_cwd:', self._cwd)
        else:
            raise FilesystemError('Not a dir.')

    def stat(self, path):
        pass

    def lstat(self, path):
        pass

    def listdir(self, path):
        # print('functioin: listdir', 'path:' + path)
        dir_list = []
        try:
            # files = self.client.ftp_list_dir(self.bucket_name, path[1:])
            files = []
            files_generator = self.client.ftp_list_dir_generator(self.bucket_name, path[1:], per_num=2000)
            for _ in range(10):
                try:
                    data = next(files_generator)
                except StopIteration as error:
                    break 
                files.append(data)
            # print(files)
            for fi in files:
                for file in fi: 
                    if file.fod == True:
                        if file.upt:
                            dir_list.append((file.name, file.upt, file.si))
                        else:
                            dir_list.append((file.name, file.ult, file.si))
                    else:
                        # print(file.shp, file.si, file.sst, file.stl, file.ult, file.upt)
                        dir_list.append((file.name + '/', file.ult, 0))
        except HarborError as error:
            raise FilesystemError(error.msg)
        # print('listdir return', dir_list)
        return dir_list


    def format_list(self, basedir, listing, ignore_err=True):
        # print('function: format_list', basedir, listing)
        assert isinstance(basedir, str), basedir
        # months_map = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
        #               7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}

        path = self.fs2ftp(basedir)
        for tmp in listing:
            if isinstance(tmp, tuple):
                filename, mtimestr, size = tmp
                ftp_path = os.path.join(path, filename)
                if filename.endswith('/'):
                    perm = "drwxrwxrwx"
                    # print(mtimestr, '-----------')
                    # mtimestr = 'Jan 01 00:00'
                    mtimestr = mtimestr.strftime('%b %d %X')[:-3]
                    filename = filename[:-1]
                else:
                    mtimestr = mtimestr.strftime('%b %d %X')[:-3]
                    perm = "-r-xr-xr-x"
                line = "%s %3s %-8s %-8s %8s %12s %s\r\n" % (
                    perm, 1, 'root', 'root', size, mtimestr, filename)

                if self.cmd_channel is not None:
                    # print(line.encode("utf8", self.cmd_channel.unicode_errors))
                    yield line.encode("utf8", self.cmd_channel.unicode_errors)
                else:
                    # print(line.encode("utf8", self.cmd_channel.unicode_errors))
                    yield line.encode("utf8")
            else:
                # mtimestr = mtimestr.strftime('%b %d %X')[:-3]
                perm = "-r-xr-xr-x"
                line = "%s %3s %-8s %-8s %8s %12s %s\r\n" % (
                    perm, 1, 'root', 'root', '', '', tmp)

                if self.cmd_channel is not None:
                    # print(line.encode("utf8", self.cmd_channel.unicode_errors))
                    yield line.encode("utf8", self.cmd_channel.unicode_errors)
                else:
                    # print(line.encode("utf8", self.cmd_channel.unicode_errors))
                    yield line.encode("utf8")


    def format_mlsx(self, basedir, listing, perms, facts, ignore_err=True):
        assert isinstance(basedir, str), basedir
        # print('function: format_mlsx', basedir, listing, perms, facts)

        ftp_path = self.fs2ftp(basedir)
        if len(listing) == 1:
            if isinstance(listing[0], tuple):
                if listing[0][0].endswith('/'):
                    type = "dir"
                    perm = 'r'
                    filename = listing[0][0][:-1]
                    # print(listing[0][1], '--------------1')
                    # mtimestr = '20000101000000'
                    mtimestr = listing[0][1]
                    mtimestr = str(mtimestr).split('.')[0].replace('-', '').replace(':', '').replace(' ', '')
                    size = listing[0][2]
                    line = "type=%s;size=%d;perm=%s;modify=%s;unique=%s; %s\r\n" % (
                        type, size, perm, mtimestr, '', filename)
                    if self.cmd_channel is not None:
                        # print(line.encode("utf8", self.cmd_channel.unicode_errors))
                        yield line.encode("utf8", self.cmd_channel.unicode_errors)
                    else:
                        # print(line.encode("utf8"))
                        yield line.encode("utf8")
                else:
                    filename = listing[0][0]
                    mtimestr = listing[0][1]
                    mtimestr = str(mtimestr).split('.')[0].replace('-', '').replace(':', '').replace(' ', '')
                    size = listing[0][2]
                    perm = 'el'
                    type = "file"
                    line = "type=%s;size=%d;perm=%s;modify=%s;unique=%s; %s\r\n" % (
                        type, size, perm, mtimestr, '', filename)
                    if self.cmd_channel is not None:
                        # print(line.encode("utf8", self.cmd_channel.unicode_errors))
                        yield line.encode("utf8", self.cmd_channel.unicode_errors)
                    else:
                        # print(line.encode("utf8"))
                        yield line.encode("utf8")
            else:
                ftp_path = os.path.join(ftp_path, listing[0])
                try:
                    data = self.client.ftp_get_obj(self.bucket_name, ftp_path[1:])
                    # print('---------------', data)
                    filename = data.name
                    mtimestr = data.upt
                    mtimestr = mtimestr = str(mtimestr).split('.')[0].replace('-', '').replace(':', '').replace(' ', '')
                    size = data.si
                    perm = 'el'
                    type = "file"
                    line = "type=%s;size=%d;perm=%s;modify=%s;unique=%s; %s\r\n" % (
                        type, size, perm, mtimestr, '', filename)
                    if self.cmd_channel is not None:
                        # print(line.encode("utf8", self.cmd_channel.unicode_errors))
                        yield line.encode("utf8", self.cmd_channel.unicode_errors)
                    else:
                        # print(line.encode("utf8"))
                        yield line.encode("utf8")
                except HarborError as error:
                    raise FilesystemError(error.msg)
        else:
            for filename, mtimestr, size in listing:
                if filename.endswith('/'):
                    type = "dir"
                    perm = 'r'
                    filename = filename[:-1]
                    # print(mtimestr, '--------------2')
                    # mtimestr = '20000101000000'
                else:
                    perm = 'el'
                    type = "file"
                mtimestr = mtimestr = str(mtimestr).split('.')[0].replace('-', '').replace(':', '').replace(' ', '')
                line = "type=%s;size=%d;perm=%s;modify=%s;unique=%s; %s\r\n" % (
                    type, size, perm, mtimestr, '', filename)

                if self.cmd_channel is not None:
                    # print(line.encode("utf8", self.cmd_channel.unicode_errors))
                    yield line.encode("utf8", self.cmd_channel.unicode_errors)
                else:
                    # print(line.encode("utf8"))
                    yield line.encode("utf8")

    def open(self, filename, mode):
        """Open a file returning its handler."""
        assert isinstance(filename, str), filename
        # print('function: open', filename, mode)
        ftp_path = self.fs2ftp(filename)
        if mode.startswith('r') or mode.startswith('R'):
            return DownLoader(self.bucket_name, ftp_path, self.client)
        else:
            return Uploader(self.bucket_name, ftp_path, self.client)


    def mkdir(self, path):
        # print('function:mkdir', 'path: '+ path)

        ftp_path = self.fs2ftp(path)
        try:
            self.client.ftp_mkdir(self.bucket_name, ftp_path[1:])
        except (HarborError, Exception) as error:
            raise FilesystemError(str(error))


    def rename(self, src, dst):
        # print('function: rename', 'src: ' + src, 'dst ' + dst)
        new_name = os.path.basename(dst)
        try:
            self.client.ftp_rename(self.bucket_name, src[1:], new_name)
        except HarborError as error:
            raise FilesystemError('rename dir is not supported')
        except Exception as error:
            raise FilesystemError(str(error))


    def lexists(self, path):
        # print('function: lexists', 'path: ' + path)
        ftp_path = self.fs2ftp(path)

        if self.isdir(ftp_path) or self.isfile(ftp_path):
            return True
        else:
            return False


    def rmdir(self, path):
        # print('function: rmdir', 'path: ' + path)

        ftp_path = self.fs2ftp(path)
        try:
            self.client.ftp_rmdir(self.bucket_name, ftp_path[1:])
        except (HarborError, Exception) as error:
            raise FilesystemError(str(error))

    def remove(self, path):
        # print('function: remove', 'path: ' + path)
        ftp_path = self.fs2ftp(path)
        try:
            self.client.ftp_delete_object(self.bucket_name, ftp_path[1:])
        except (HarborError, Exception) as error:
            raise FilesystemError(str(error.msg))


class Uploader(object):
    def __init__(self, bucket_name, ftp_path, client):
        self.bucket_name = bucket_name
        self.name = os.path.basename(ftp_path)
        self.ftp_path = ftp_path
        self.client = client
        self.closed = False
        self.file = BytesIO()
        # self.file_list = []
        self.id = 0
        # self.count = 0
        try:
            self.write_generator = self.client.ftp_get_write_generator(self.bucket_name, self.ftp_path[1:])
            next(self.write_generator)
        except HarborError as error:
            raise FilesystemError(error.msg)

    def write(self, data):
        self.file.write(data)
        if self.file.tell() >= 1024 ** 2 * 64:
            try:
                self.write_generator.send((self.id, self.file.getvalue()))
                # pass
            except HarborError as error:
                raise FilesystemError(error.msg)
            self.id += self.file.tell()
            self.file = BytesIO()
        return len(data)

        # self.file = b''.join((self.file, data))
        # if len(self.file) >= 1024 * 1024 * 4:
        #     try:
        #         self.write_generator.send((self.id, self.file))
        #         pass
        #     except HarborError as error:
        #         raise FilesystemError(error.msg)
        #     self.id += len(self.file)
        #     self.file = bytes()
        # return len(data)

        # self.file_list.append(data)  # 利用列表，效果不佳
        # if len(self.file_list) >= 30:
        #     self.file_list = b''.join(self.file_list)
        #     try:
        #         self.write_generator.send((self.id, self.file_list))
        #     except HarborError as error:
        #         raise FilesystemError(error.msg)
        #     self.file_list = []
        #     self.id = self.count
        # self.count += len(data)
        # return len(data)

    def close(self):
        if self.file.tell():
            try:
                self.write_generator.send((self.id, self.file.getvalue()))
                # self.client.ftp_write_chunk(self.bucket_name, self.ftp_path[1:], self.id, self.file)
                # print(self.id + len(self.file.getvalue()), '---------')
            except HarborError as error:
                raise FilesystemError(error.msg)
        self.closed = True


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
            self.obj_generator, ob = self.client.ftp_get_obj_generator(self.bucket_name, self.ftp_path[1:], per_size= 4 * 1024 ** 2)
            # print(self.obj_generator, ob)
        except HarborError as error:
            raise FilesystemError(error.msg)

    def read(self, size=None):
        # print(size, '--------------')
        # try:
        #     data, obj = self.client.ftp_read_chunk(self.bucket_name, self.ftp_path[1:], self.id, size)
        #     self.id += size
        #
        # except HarborError as error:
        #     raise FilesystemError(error.msg)
        try:
            data = next(self.obj_generator)
            # print(data)
        # except StopIteration as error:
        except Exception as error:
            return b''
        return data

    def close(self):
        self.closed = True
        # pass


if __name__ == '__main__':
    print('no info')
    f = open('harbor_auto.py')
    f.read()