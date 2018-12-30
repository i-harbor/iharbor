## 1 环境搭建
### 1.1 安装python和Git
请自行安装python3.6和Git。
使用Git拉取代码： 
```
git clone https://github.com/evobstore/webserver.git
```
### 1.2 安装python虚拟环境和包管理工具pipenv
使用pip命令安装pipenv。  
```pip3 install pipenv```
### 1.3  使用pipenv搭建python虚拟环境
在代码工程根目录下，即文件Pipfile同目录下运行命令：  
```pipenv install```
### 1.4 数据库安装
请自行安装MongoDB数据库。
根据自己的情况修改webserver/settings.py文件中有关MongoDB数据库的配置代码
```python
#mongodb数据库连接
from mongoengine import connect

connect(
    alias='default',
    db='metadata',
    host='10.0.86.213',
    port=27017,
    # username='root',
    # password='pwd123',
    # authentication_source='admin'
)
```
### 1.5 ceph配置
从[rados_io](https://github.com/evobstore/rados_io)下载rados.so库文件，放于项目‘utils/oss/’路径下；以下配置根据实际情况自行修改。
```
CEPH_RADOS = {
    'CLUSTER_NAME': 'ceph',
    'USER_NAME': 'client.objstore',
    'CONF_FILE_PATH': '/etc/ceph/ceph.conf',
    'POOL_NAME': 'objstore',
    'RADOS_DLL_PATH': 'rados.so'
}
```

### 1.6 security_settings.py
在settings.py文件最后导入了security_settings.py（项目中缺少），security_settings.py中定义了一些安全敏感信息，请自行添加此文件，并根据自己情况参考settings.py中例子完成配置。

## 2 运行webserver
### 2.1 激活python虚拟环境  
```pipenv shell```
### 2.2 数据库迁移
django用户管理、验证、session等使用sqlite3数据库，需要数据库迁移创建对应的数据库表,在项目根目录下运行如下命令完成数据库迁移。  
```
python manage.py migrate
```
### 2.3 运行web服务
在代码工程根目录下，即文件Pipfile同目录下运行命令：  
```python manage.py runserver 0.0.0.0:8000```   
如果一切正常，打开浏览器输入url(主机IP:8000, 如：127.0.0.1：8000)即可查看站点;

