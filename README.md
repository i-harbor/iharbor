## 1 环境搭建(CentOS7)
### 1.1 安装python和Git
请自行安装python3.6和Git。
使用Git拉取代码： 
```
git clone https://github.com/evobstore/webserver.git
```
* 如果不用python虚拟环境请跳过下面1.2和1.3小节python虚拟环境的搭建内容，直接安装iharbor服务需要的python依赖库：   
```pip3 install -r requirements.txt```   
### 1.2 安装python虚拟环境和包管理工具pipenv
使用pip命令安装pipenv。  
```pip3 install pipenv```
### 1.3  使用pipenv搭建python虚拟环境
在代码工程根目录下，即文件Pipfile同目录下运行命令：  
```pipenv install```

### 1.4 安全敏感信息配置文件security_settings.py
创建配置文件security_settings.py，复制项目下webserver/security_settings_demo.py文件为webserver/security_settings.py。
security_settings.py中定义了一些安全敏感信息，请根据自己情况自行修改完成配置（有关配置下面有关小节有介绍），此文件信息在settings.py文件最后被导入。

### 1.5 数据库安装
请自行安装mysql数据库。 
根据自己的情况修改security_settings.py中有关数据库的配置示例代码
```
# Mysql
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',   # 数据库引擎
        'NAME': 'xxx',       # 数据的库名，事先要创建之
        'HOST': 'localhost',    # 主机
        'PORT': '3306',         # 数据库使用的端口
        'OPTIONS': {'init_command': "SET sql_mode='STRICT_TRANS_TABLES'"}
    },
    'metadata': {
        'ENGINE': 'django.db.backends.mysql',  # 数据库引擎
        'NAME': 'metadata',  # 数据的库名，事先要创建之
        'USER': 'xxx',  # 数据库用户名
        'PASSWORD': 'xxx',  # 密码
        'HOST': '127.0.0.1',  # 主机
        'PORT': '3306',  # 数据库使用的端口
        'OPTIONS': {'init_command': "SET sql_mode='STRICT_TRANS_TABLES'"}
    },
}
```
### 1.6 ceph配置和依赖库安装
与ceph的通信使用官方librados的python包python36-rados。  
* 推荐直接安装ceph客户端，或者只安装python36-rados的rpm包（参考下面命令）,安装成功后，python包会自动安装到系统python3第三方扩展包路径下（/usr/lib64/python3.6/site-packages/）。   
```
wget http://download.ceph.com/rpm-nautilus/el7/x86_64/librados2-14.2.1-0.el7.x86_64.rpm
wget http://download.ceph.com/rpm-nautilus/el7/x86_64/python36-rados-14.2.1-0.el7.x86_64.rpm
yum localinstall -y librados2-14.2.1-0.el7.x86_64.rpm python36-rados-14.2.1-0.el7.x86_64.rpm
```
* 如果使用的是pipenv创建的Python虚拟环境(否则忽略此步骤)，需要把`/usr/lib64/python3.6/site-packages/`路径下的python包文
件rados-2.0.0-py3.6.egg-info和rados.cpython-36m-x86_64-linux-gnu.so复制到你的虚拟python环境*/site-packages/下。

* ceph的配置, 支持多个ceph集群：   
```
CEPH_RADOS = {
    'default': {
        'CLUSTER_NAME': 'ceph',
        'USER_NAME': 'client.admin',
        'CONF_FILE_PATH': '/etc/ceph/ceph.conf',
        'KEYRING_FILE_PATH': '/etc/ceph/ceph.client.admin.keyring',
        'POOL_NAME': ('obs-test',),
        'DISABLE_CHOICE': False,                # True: 创建bucket时不选择；
    },
    # 'ceph2': {
    #     'CLUSTER_NAME': 'ceph',
    #     'USER_NAME': 'client.admin',
    #     'CONF_FILE_PATH': '/etc/ceph/ceph.conf',
    #     'KEYRING_FILE_PATH': '/etc/ceph/ceph.client.admin.keyring',
    #     'POOL_NAME': ('obs-test',),
    #     'DISABLE_CHOICE': False,               # True: 创建bucket时不选择；
    # }
}
```

### 1.7 FTP配置
ftp默认开启TLS加密，需要域名证书文件`/etc/nginx/conf.d/ftp-keycert.pem`。  
如果不开启TLS加密，需要修改`ftpserver/harbor_handler.py`文件开头部分代码`work_mode_in_tls = False`。

## 2 运行webserver
### 2.1 激活python虚拟环境  
* 非python虚拟环境忽略此步骤。  
```pipenv shell```

### 2.2 数据库迁移
django用户管理、验证、session等使用mysql(sqlite3)数据库，需要数据库迁移创建对应的数据库表,在项目根目录下运行如下命令完成数据库迁移。  
```
python manage.py migrate
```
### 2.3 运行服务
#### 2.3.1 开发测试模式运行服务
* 启动WEB服务   
在代码工程根目录下运行命令：  
```python3 manage.py runserver 0.0.0.0:8000```   
如果一切正常，打开浏览器输入url(主机IP:8000, 如：127.0.0.1：8000)即可查看站点;
* 启动FTP服务   
```python3 ftpserver/harbor_ftp.py```

#### 2.3.2 生产环境模式运行服务
* 收集静态文件  
```python3 manage.py colectstatic```

* systemctl管理配置，开机自启服务
在项目根目录下执行脚本：   
```./config_systemctl.sh```   
脚本会复制iharbor.service、iharbor_ftp.service两个文件到 /usr/lib/systemd/system/ 目录下，并开启服务开机自启动。
然后可以使用systemctl命令管理ihabor的web和ftp服务了：
```
systemctl start/reload/stop iharbor.service
systemctl start/reload/stop iharbor_ftp.service
```


