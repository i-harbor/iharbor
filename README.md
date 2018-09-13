## 1 环境搭建
### 1.1 安装python.Git
请自行安装python3.6和Git。
使用Git拉去代码：
```
git clone https://github.com/evobstore/webserver.git
```
### 1.2 安装python虚拟环境和包管理工具pipenv
使用pip命令安装pipenv
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
    db='testdb1',
    host='10.0.86.213',
    port=27017,
    # username='root',
    # password='pwd123',
    # authentication_source='admin'
)
connect(alias='db2', db='testdb2', host='10.0.86.213', port=27017)
connect(alias='db3', db='testdb3', host='10.0.86.213', port=27017)
```

## 2 运行webserver
### 2.1 激活python虚拟环境
```pipenv shell```
### 2.2 运行web服务
在代码工程根目录下，即文件Pipfile同目录下运行命令：
```python manage.py runserver 0.0.0.0:8000```
如果一切正常，打开浏览器输入url(主机IP:8000, 如：127.0.0.1：8000)即可查看站点;
从网页上可以上传文件，上传的文件记录会展示在下方的表格，上传的文件保存在webserver工程根目录
下的media/upload目录下，文件名已改为对应的UUID。

