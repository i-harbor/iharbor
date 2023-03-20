import paramiko
from paramiko import SSHClient
from paramiko.ssh_exception import (
    SSHException,
    BadHostKeyException,
    NoValidConnectionsError, AuthenticationException,
)


class NodeClient:
    """
    节点客户端连接
    """

    def __init__(self):
        self.client = SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    def connect(self, hostname, username=None, password=None):

        try:
            self.client.load_system_host_keys()
            self.client.connect(hostname=hostname, username=username, password=password)
        except BadHostKeyException as e:
            raise e
        except AuthenticationException:
            raise AuthenticationException('身份认证失败')
        except SSHException:
            raise SSHException('无法连接服务器')
        except Exception as e:
            raise e

        return self.client

    def exec_command(self, command):
        # stdin, stdout, stderr
        # stdout = None
        # stderr = None
        # result = None
        try:
            result = self.client.exec_command(command=command, timeout=20)
        except SSHException as e:
            raise e

        if result is None:
            raise SSHException()
        return result

    def close(self):
        try:
            self.client.close()
        except Exception as e:
            raise e


if __name__ == "__main__":
    client = NodeClient()

    try:
        client.connect("223.193.36.64", username='root', password='root')
        stdin, stdout, stderr = client.exec_command("ls -al")

        print(f" stdout = {stdout.read(size=1024).decode('utf8')} \n")
        # print(f" stderr = {stderr.read().decode('utf8')} \n")
        # print(f" stdout = {stdout.readline()}")
        # print(f" stdout = {stdout.readline()} \n")
        client.close()
    except KeyboardInterrupt:
        client.close()
    except Exception as e:
        print(f"err {str(e)}")
