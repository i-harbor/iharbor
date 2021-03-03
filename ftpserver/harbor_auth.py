from pyftpdlib.authorizers import DummyAuthorizer, AuthenticationFailed
import os
import sys
import django

# 将项目路径添加到系统搜寻路径当中，查找方式为从当前脚本开始，找到要调用的django项目的路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# 设置项目的配置文件 不做修改的话就是 settings 文件
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webserver.settings")
django.setup()  # 加载项目配置


from api.harbor import FtpHarborManager


class HarborAuthorizer(DummyAuthorizer):
    """
    继承DummyAuthorizer
    主要修改pyftpdlib的认证模块的一些函数
    修改pyftpdlib的认证函数
    修改pyftpdlib的获得根目录的函数
    修改pyftpdlib的成功登陆提示函数
    修改pyftpdlib的判断是否有权限函数
    修改pyftpdlib的获得权限函数
    """
    def validate_authentication(self, user_name, password, handler):
        """
        使用api里harbor提供的api，进行认证
        认证完对本次登录的权限问题进行处理
        """
        flag, perm, msg = FtpHarborManager().ftp_authenticate(user_name, password)
        if not flag:
            raise AuthenticationFailed(msg)
        perms = 'elradfmwMT' if perm else 'elr'
        self.user_table[user_name] = {'perm': perms}

    def get_home_dir(self, username):
        """Return the user's home directory.
        Since this is called during authentication (PASS),
        AuthenticationFailed can be freely raised by subclasses in case
        the provided username no longer exists.
        """
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


