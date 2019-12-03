from pyftpdlib.authorizers import DummyAuthorizer, AuthenticationFailed
from pyftpdlib.filesystems import FilesystemError
import os
import sys
import django

# 将项目路径添加到系统搜寻路径当中，查找方式为从当前脚本开始，找到要调用的django项目的路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# 设置项目的配置文件 不做修改的话就是 settings 文件
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webserver.settings")
django.setup()  # 加载项目配置

from django.contrib.auth import authenticate
from users.models import AuthKey, UserProfile
from buckets.models import Bucket
from api.harbor import FtpHarborManager, ftp_close_old_connections

class HarborAuthorizer(DummyAuthorizer):
    '''
    继承DummyAuthorizer
    主要修改pyftpdlib的认证模块的一些函数
    修改pyftpdlib的认证函数
    修改pyftpdlib的获得根目录的函数
    修改pyftpdlib的成功登陆提示函数
    修改pyftpdlib的判断是否有权限函数
    修改pyftpdlib的获得权限函数
    '''
    @ftp_close_old_connections
    def validate_authentication(self, user_name, password, handler):
        '''
        @ftp_close_old_connections装饰器是防止连接django后台数据库的连接超时，保证服务的一直开启
        使用api里harbor提供的api，进行认证
        认证完对本次登录的权限问题进行处理
        '''
        # for login_user_name, login_password, login_permission in HarborFtpCfg().login_users:
        #     if user_name == login_user_name and password == login_password:
        #         pass
        #
        # raise AuthenticationFailed
        # if not (user_name == 'root' and password == 'root'):
        #     raise AuthenticationFailed
        # if not authenticate(username=user_name, password=password):
        #     raise AuthenticationFailed
        # if not Bucket.objects.filter(name=user_name):
        #     raise AuthenticationFailed('Have no this bucket.')
        # if not Bucket.objects.get(name=user_name).ftp_enable:
        #     raise AuthenticationFailed('Bucket is not enable for ftp.')
        # if not Bucket.objects.get(name=user_name).ftp_password == password:
        #     raise AuthenticationFailed
        flag, perm, msg = FtpHarborManager().ftp_authenticate(user_name, password)
        if not flag:
            raise AuthenticationFailed(msg)
        perms = 'elradfmwM' if perm else 'elr'
        self.user_table[user_name] = {'perm': perms}

    def get_home_dir(self, username):
        """Return the user's home directory.
        Since this is called during authentication (PASS),
        AuthenticationFailed can be freely raised by subclasses in case
        the provided username no longer exists.
        """
        # return self.user_table[username]['home']
        # user = UserProfile.objects.get(username=username)
        # auth = AuthKey.objects.filter(user=user)
        # if not auth:
        #     auth = [AuthKey.objects.create(user=user)]
        # bucket = Bucket.objects.filter(user=user)
        # if not bucket:
        #     raise AuthenticationFailed('There is not a bucket')
        # # access_key = '40a95de4472411e99b7bc8000a00c8d3'
        # # secret_key = 'a2d3b4be966f4ed3381b535306b5e59b7983e40e'
        # # bucket = 'liyun'
        # access_key = auth[0].id
        # secret_key = auth[0].secret_key
        # bucket = bucket[0].name
        # print(access_key, secret_key, bucket)
        return username

    def get_msg_login(self, username):
        """Return the user's login message."""
        return "Login successful."

    def has_perm(self, username, perm, path=None):
        """Whether the user has permission over path (an absolute
        pathname of a file or a directory).

        Expected perm argument is one of the following letters:
        "elradfmwMT".
        """
        return perm in self.user_table[username]['perm']

    def get_perms(self, username):
        """Return current user permissions."""
        return self.user_table[username]['perm']


