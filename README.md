## 1 环境搭建(CentOS9)
### 1.1 安装软件 
1. **python**： python3.9 (Centos9默认已安装python3.9版本)  
2. **Git**
3. **mysql** (django支持的数据库)
4. **ceph** 集群 ：版本17.2.3 （自行配置）

### 1.2 使用Git拉取代码： 
```
git clone https://gitee.com/gosc-cnic/iharbor.git
```
* 如果不用python虚拟环境请跳过下面1.2和1.3小节python虚拟环境的搭建内容，直接安装iharbor服务需要的python依赖库：   
```pip install -r requirements.txt```   
### 1.3 安装Python依赖管理工具pipenv
使用pip命令安装pipenv。  
```pip install pipenv```
### 1.4  使用pipenv搭建python虚拟环境
在代码工程根目录下，即文件Pipfile同目录下运行命令：  
```pipenv install```

### 1.5 安全敏感信息配置文件security_settings.py
1. 创建配置文件**security_settings.py**或 复制项目下 **webserver/security_settings_demo.py** 文件为 **webserver/security_settings.py**。  
2. **security_settings.py** 中定义了一些安全敏感信息，请根据自己情况自行修改完成配置（有关配置下面有关小节有介绍），此文件信息在 **settings.py** 文件最后被导入。

### 1.6 数据库安装
1. 请自行安装 **mysql** 数据库，如果使用其他django支持的数据库，请根据官方文档自行配置。  
2. 安装数据库依赖
   ``` dnf install mariadb-connector-c-devel ```
3. 根据自己的情况修改 **security_settings.py** 中有关数据库的配置 **mysql** 示例代码
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
### 1.7 ceph配置和依赖库安装
与ceph的通信使用官方librados的python包python3-rados。
1. 安装 **python3-rados**  
``` dnf install python3-rados.x86_64 ```
2. python包会自动安装到系统python3第三方扩展包路径下（/usr/lib64/python3.9/site-packages/）。  
    注意：在使用虚拟环境时需要将以下文件复制到虚拟环境下对应的目录中。
```
 rados.cpython-39-x86_64-linux-gnu.so 
 rados-2.0.0-py3.9.egg-info  
```
3. ceph的配置, 支持多个ceph集群：
    security_settings.py文件中无需配置ceph参数，在项目在第一次启动后需要登录到后端配置ceph，否则无法使用存储服务。对应的ceph配置文件存储在工程文件 **data** 目录中。配置文件内容大致如下：
   后端ceph配置中必须有一个 **别名** 为`default`。
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

### 1.8 FTP配置
1. 不开启TLF加密情况下：  
需要修改`ftpserver/harbor_handler.py`文件开头部分代码`work_mode_in_tls = False`。
   
2. ftp默认开启TLS加密，需要域名证书文件`/etc/nginx/conf.d/ftp-keycert.pem`。  

## 2 运行iharbor
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
```python3 manage.py collectstatic```

* systemctl管理配置，开机自启服务
在项目根目录下执行脚本：   
```./config_systemctl.sh```   
脚本会复制iharbor.service、iharbor_ftp.service两个文件到 /usr/lib/systemd/system/ 目录下，并开启服务开机自启动。
然后可以使用systemctl命令管理ihabor的web和ftp服务了：
```
systemctl start/reload/stop iharbor.service
systemctl start/reload/stop iharbor_ftp.service
```


