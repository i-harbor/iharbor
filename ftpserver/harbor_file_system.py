from pyftpdlib.filesystems import AbstractedFS, FilesystemError
import django
import sys
import os

# 将项目路径添加到系统搜寻路径当中，查找方式为从当前脚本开始，找到要调用的django项目的路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# 设置项目的配置文件 不做修改的话就是 settings 文件
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webserver.settings")
django.setup()  # 加载项目配置

from api.harbor import FtpHarborManager


class HarborFileSystem(AbstractedFS):
    def __init__(self, *args, **kwargs):
        super(HarborFileSystem, self).__init__(*args, **kwargs)
        self.bucket_name = self._root
        self._root = '/'
        self.client = FtpHarborManager()

    def realpath(self, path):
        return path

    def isdir(self, path):
        print(path)
        print(self.bucket_name)
        return self.client.ftp_is_dir(self.bucket_name, path)

    def isfile(self, path):
        print(path)
        return self.client.ftp_is_file(self.bucket_name, path)

    def islink(self, fs_path):
        return False

    def chdir(self, path):
        assert isinstance(path, str), path
        print(sys._getframe().f_code.co_name)
        print('输入了', end='')
        print(path)
        print('输出了')
        if self.isdir(path):
            self._cwd = self.fs2ftp(path)
            print('赋值_cwd:' + self._cwd)
        else:
            raise FilesystemError('Not a dir.')

    def listdir(self, path):
        files = self.client.ftp_list_dir(self.bucket_name, path)
        print(files)
        # assert isinstance(path, str), path
        # ftp_path = self.fs2ftp(path)
        # dir_list = []
        # page = self.client.list_dir(bucket_name=self.bucket_name, dir_name=ftp_path, per_page=100)
        #
        # for obj in page.get_list():
        #     if obj['fod'] == True:
        #         dir_list.append((obj['name'], obj['upt'], obj['si']))
        #     else:
        #         dir_list.append((obj['name'] + '/', '', 0))
        # while page.has_next():
        #     page = page.next_page()
        #     for obj in page.get_list():
        #         if obj['fod'] == True:
        #             dir_list.append((obj['name'], obj['upt'], obj['si']))
        #         else:
        #             dir_list.append((obj['name'] + '/', '', 0))
        #
        # print(sys._getframe().f_code.co_name)
        # print('输入了', end='')
        # print(path)
        # print('输出了', end='')
        # print(dir_list)
        # return dir_list

    def format_list(self, basedir, listing, ignore_err=True):
        assert isinstance(basedir, str), basedir
        months_map = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                      7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
        print(sys._getframe().f_code.co_name)
        print('输入了', end='')
        print(basedir, end='')
        print(listing)
        print('输出了')
        path = self.fs2ftp(basedir)
        for filename, mtimestr, size in listing:
            ftp_path = os.path.join(path, filename)
            if filename.endswith('/'):
                perm = "drwxrwxrwx"
                mtimestr = 'Jan 01 00:00'
                filename = filename[:-1]
            else:
                month = mtimestr.split(' ')[0].split('-')[1]
                day = mtimestr.split(' ')[0].split('-')[2]
                time = mtimestr.split(' ')[1][:-3]
                mtimestr = '%s %s %5s' % (months_map[int(month)], day, time)
                perm = "-r-xr-xr-x"
            line = "%s %3s %-8s %-8s %8s %12s %s\r\n" % (
                perm, 1, 'root', 'root', size, mtimestr, filename)

            if self.cmd_channel is not None:
                print(line.encode("utf8", self.cmd_channel.unicode_errors))
                yield line.encode("utf8", self.cmd_channel.unicode_errors)
            else:
                print(line.encode("utf8", self.cmd_channel.unicode_errors))
                yield line.encode("utf8")

    def format_mlsx(self, basedir, listing, perms, facts, ignore_err=True):
        assert isinstance(basedir, str), basedir
        print(sys._getframe().f_code.co_name)
        print('输入了', end='')
        print(basedir, end='')
        print(listing)
        print(perms)
        print(facts)
        print('输出了')
        ftp_path = self.fs2ftp(basedir)
        if len(listing) == 1:
            if isinstance(listing[0], tuple):
                if listing[0][0].endswith('/'):
                    type = "dir"
                    perm = 'r'
                    filename = listing[0][0][:-1]
                    mtimestr = '20000101000000'
                    size = listing[0][2]
                    line = "type=%s;size=%d;perm=%s;modify=%s;unique=%s; %s\r\n" % (
                        type, size, perm, mtimestr, '', filename)
                    if self.cmd_channel is not None:
                        print(line.encode("utf8", self.cmd_channel.unicode_errors))
                        yield line.encode("utf8", self.cmd_channel.unicode_errors)
                    else:
                        print(line.encode("utf8"))
                        yield line.encode("utf8")
                else:
                    filename = listing[0][0]
                    mtimestr = listing[0][1].replace(' ', '').replace(':', '').replace('-', '')
                    size = listing[0][2]
                    perm = 'el'
                    type = "file"
                    line = "type=%s;size=%d;perm=%s;modify=%s;unique=%s; %s\r\n" % (
                        type, size, perm, mtimestr, '', filename)
                    if self.cmd_channel is not None:
                        print(line.encode("utf8", self.cmd_channel.unicode_errors))
                        yield line.encode("utf8", self.cmd_channel.unicode_errors)
                    else:
                        print(line.encode("utf8"))
                        yield line.encode("utf8")
            else:
                ftp_path = os.path.join(ftp_path, listing[0])
                data, code, msg = self.client.get_obj_info(bucket_name=self.bucket_name, obj_name=ftp_path)
                if code == 200:
                    filename = data['obj']['name']
                    mtimestr = data['obj']['upt'].replace(' ', '').replace(':', '').replace('-', '')
                    size = data['obj']['si']
                    perm = 'el'
                    type = "file"
                    line = "type=%s;size=%d;perm=%s;modify=%s;unique=%s; %s\r\n" % (
                        type, size, perm, mtimestr, '', filename)
                    if self.cmd_channel is not None:
                        print(line.encode("utf8", self.cmd_channel.unicode_errors))
                        yield line.encode("utf8", self.cmd_channel.unicode_errors)
                    else:
                        print(line.encode("utf8"))
                        yield line.encode("utf8")
                else:
                    raise FilesystemError(msg)
        else:
            for filename, mtimestr, size in listing:
                if filename.endswith('/'):
                    type = "dir"
                    perm = 'r'
                    filename = filename[:-1]
                    mtimestr = '20000101000000'
                else:
                    perm = 'el'
                    type = "file"
                    mtimestr = mtimestr.replace(' ', '').replace(':', '').replace('-', '')

                line = "type=%s;size=%d;perm=%s;modify=%s;unique=%s; %s\r\n" % (
                    type, size, perm, mtimestr, '', filename)

                if self.cmd_channel is not None:
                    print(line.encode("utf8", self.cmd_channel.unicode_errors))
                    yield line.encode("utf8", self.cmd_channel.unicode_errors)
                else:
                    print(line.encode("utf8"))
                    yield line.encode("utf8")

    def open(self, filename, mode):
        """Open a file returning its handler."""
        assert isinstance(filename, str), filename
        print(sys._getframe().f_code.co_name)
        print('输入了', end='')
        print(filename, end='')
        print(mode)
        print('输出了', end='')

        ftp_path = self.fs2ftp(filename)
        if mode.startswith('r') or mode.startswith('R'):
            name = './download_temp/' + os.path.basename(filename)
            self.client.download_object(bucket_name=self.bucket_name, obj_name=ftp_path, filename=name)
            file = open(name, mode)
            os.remove(name)
            return file
        else:
            return Uploader(self.bucket_name, ftp_path, self.client)


    def mkdir(self, path):
        assert isinstance(path, str), path

        ftp_path = self.fs2ftp(path)
        ok, msg = self.client.create_dir(bucket_name=self.bucket_name, dir_name= ftp_path)

        if msg != '创建文件夹成功':
            raise FilesystemError(msg)

    def rename(self, src, dst):
        """Rename the specified src file to the dst filename."""
        assert isinstance(src, str), src
        assert isinstance(dst, str), dst
        ftp_path = self.fs2ftp(src)
        new_name = os.path.basename(dst)
        ok, data = self.client.rename_object(bucket_name=self.bucket_name, obj_name=ftp_path, rename=new_name)
        if not ok:
            if data['code'] == 404:
                raise FilesystemError('暂不支持修改文件夹名')
            else:
                raise FilesystemError(data['msg'])

    def lexists(self, path):
        ftp_path = self.fs2ftp(path)

        if ftp_path.startswith("/"):
            if self.isdir(path) or self.isfile(path):
                return True
            else:
                return False
        return False

    def rmdir(self, path):
        """Remove the specified directory."""
        assert isinstance(path, str), path
        ftp_path = self.fs2ftp(path)
        ok, msg = self.client.delete_dir(bucket_name=self.bucket_name, dir_name=ftp_path)
        if not ok:
            raise FilesystemError(msg)

    def remove(self, path):
        """Remove the specified file."""
        assert isinstance(path, str), path
        ftp_path = self.fs2ftp(path)
        ok, msg = self.client.delete_object(bucket_name=self.bucket_name, obj_name=ftp_path)
        if not ok:
            raise FilesystemError(msg)


class Uploader(object):
    def __init__(self, bucket_name, ftp_path, client):
        self.bucket_name = bucket_name
        self.name = os.path.basename(ftp_path)
        self.ftp_path = ftp_path
        self.client = client
        self.closed = False
        #self.buffer = FifoBuffer()

    def write(self, data):
        path = './download_temp/' + os.path.basename(self.ftp_path)
        f = open(path, 'wb')
        f.write(data)
        f.close()
        ok, offset, msg = self.client.put_object(bucket_name=self.bucket_name, obj_name=self.ftp_path, filename=path)
        os.remove(path)
        return len(data)

    def close(self):
        self.closed = True


if __name__ == '__main__':
    print('no info')