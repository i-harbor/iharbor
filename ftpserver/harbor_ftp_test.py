import ftplib
import random
from io import BytesIO
import datetime
import os
import socket
import threading

class HarborFTP():
    def __init__(self, host, username, password):
        self.client = ftplib.FTP(host, timeout=7200)
        self.client.login(username, password)

    def upload(self, num=1000000):
        for i in range(num):
            f = BytesIO(str(random.uniform(1, 10)).encode('utf8'))
            self.client.storbinary(f'STOR {i}.txt', f, 1024)
            # print(f'第{i}个文件上传成功')
            
    def change_dir(self, dir='test'):
        self.client.cwd(dir)

    def mkdir(self, dir='test'):
        self.client.mkd(dir)

def work(bucket_name):
    harborftp = HarborFTP('159.226.91.141', bucket_name, '123456')
    try:
        harborftp.mkdir()
    except Exception:
        pass
    harborftp.change_dir()
    harborftp.upload(100000)

def multi_upload():
    works = []
    for bucket_name in get_bucket_names():
        t = threading.Thread(target=work, args=(bucket_name,))
        works.append(t)
        t.start()
    for w in works:
        w.join()

def create_bucket(bucket_name):
    cmd = 'curl -X POST "http://159.226.91.141/api/v1/buckets/" -H "Content-Type: application/json" -H "Authorization: token e1d7b5301280bc9760d4435e64f7cae643239955" -d "{  \\"name\\": \\"' + bucket_name + '\\"}" >/dev/null 2>&1'
    os.system(cmd)

def get_bucket_names(num=5):
    hostname = socket.gethostname()
    ip_end = socket.gethostbyname(hostname).split('.')[-1]
    return [ip_end + '-' + str(i) for i in range(num)]


if __name__ == "__main__":
    start_time = datetime.datetime.now()
    # for bucket_name in get_bucket_names():
    #     create_bucket(bucket_name)
    multi_upload()
    use_seconds = (datetime.datetime.now() - start_time).total_seconds()
    print('用时：', use_seconds)
    
    
