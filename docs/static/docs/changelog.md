## v0.8.1
2022-11-24
* S3多部分上传接口重构，其他与多部分有关的S3接口优化和bug修复;
* s3是否开启可配置，hosts路由根据设置的s3域名动态配置;   
* S3接口检查存储桶的读写锁；  
* 科技云通行证jwt中单位为null时，创建用户失败，无法完成认证的bug修复。
* 固定依赖包cryptography==37.0.4版本，解决ftp启动错误的问题;
* drf路由map没有head方法，无法访问S3 HeadObject问题修复;

## v0.8.0
* 添加s3 app，添加s3兼容接口;   
* 修改对象元数据表'na'和'name'字段排序规则的优化，改为model定义字段时通过参数直接指定，db_collation='utf8mb4_bin';  
* 增加 AdminListBucket 接口和测试用例;   
* 增加 command 'exportbucket'； 
* 移除csrf验证中间件，DRF SessionAuthentication不做csrf验证； 

## v0.7.5
* BucketFTP api优化错误时返回的错误代码   
* 用户桶数量限制默认值可通过配置项BUCKET_LIMIT_DEFAULT设置,默认为0，对应的桶数量限制为0时测试用例无法创建
  桶的问题修改；StatsBucket api优化错误时返回的错误代码
* 移除ceph组件信息查询接口、ceph错误信息查询接口、服务可用性接口   

## v0.7.4
* ceph配置信息从数据库加载方式后，单元测试相关修改。
* 查询存储桶（StatsBucket）接口返回内容增加桶的所属用户的id和username。
* 桶允许创建个数默认改为0  

## v0.7.3
* 上传时临时文件缓存改成全部内存缓存。  
* 桶同步备份脚本增加参数以指定按对象大小先同步小的。  
* 桶同步脚本数据库连接中断后无法触发重连的问题修复。  
* 增加ceph app管理ceph配置信息和配置文件，ceph配置信息不再放在项目配置文件中，
  每次服务启动时从数据库加载ceph配置信息。   
* ftp追加上传功能支持。 


## v0.7.2
* 取消科技云登录界面，修改成本地登录界面。
* AdminCreateBucket、AdminBucketLock、AdminDeleteBucket api and testcase
* 增加关于版本信息和日志信息内容
* 列举分享目录api修改，返回字符串错误码。
* 列举目录和对象元数据查询接口增加字段async1、async2字段内容。
* 存储桶备份点创建、删除、列举、修改api和单元测试。  

## v0.7.1
* 本地注册用户未激活时，通科技云通行证无法登录的问题，本地登录未激活用户时提示未激活
* 上传文件临时缓存路径设置
* 增加home视图重定向到桶列表视图；上导航栏增加'管理控制台'按钮，可设置链接到独立前端服务，默认为桶列表视图
* 增加独立前端打包文件
* 测试代码拆分，以tests包方式管理
* python依赖包升级，升级到python3.9
* aai jwt testcase
* 对象元数据字段na和name字符集utf8_bin改为utf8mb4_bin

## v0.7.0
* 科技云通行证jwt认证支持
* 更新依赖库，django3.2.12
* 创建桶时ftp密码未加密修复
* 重写桶同步脚本，直接sql操作数据库，不再依赖django orm
* 一些命令优化

## v0.6.0
* 归档恢复桶功能
* 桶同步功能
* PutObjectV2空对象上传支持
* 上传时临时文件占满磁盘存储空间问题优化
* django无法得知nginx代理请求是否是https的问题
* 创建bucket时指定id不再自增
* 后台bucket列表防删除优化
* 多ceph集群支持
* add statsbucket command
* bucket ftp密码加密
* TiDB兼容问题修改
* ListBucketObjects query param 'exclude-dir'

## v0.5.4
* v2 object upload api
* 对象上传自动创建父路径
* ftp同时兼容utf8/gbk乱码,改为只支持utf8
* 其他优化和bug修复

## v0.5.3
* 对象上传自动创建父路径，其他代码优化
* 优化 v2 对象上传接口
* 增加一些api测试内容
* 删除 evcloud、 vpn app

## v0.5.2
* 升级django版本到3.2
* 修改部分api在线文档内容
* 优化 ListBucketObjects 和 GetObjectMeta api 
* 对象部分下载大小不再限制大小，ListDir api增加参数only-obj
* 添加开机自启服务
* 优化 ftps
* 可配置本地登录功能

## v0.5.1
* 搜索桶内对象功能
* ftp支持上传空文件和下载续传
* CreateBucket api桶已存在返回409
* python环境依赖更新
* 准备废弃vpn和evcloud app
* 一些代码优化整理修改

## v0.5.0
* share下载api浏览器缓存有关支持，增加ETag、Last_Modified、Cache-Control标头
* sweetalert2升级，增加分享url查询api,前端分享设置修改
* 默认中国科技云通行证登录
* bucket lock功能
* bucket token功能实现，copydir命令
* ftp写接口断点续传修改，新增ftp_get_obj_size函数
* 桶详情视图，listobjects命令
* 创建桶时，对象元数据表na和name字段校验规则为utf8_bin,区分字母大小写
* ftp支持文件的move操作

## v0.4.4
* 存储桶和桶归档model增加字段type，及clearbucket命令只清理原生桶；
* 兼容s3 api使用说明网页视图实现；
* ceph rados读写代码修改和单元测试代码；
* 其他一些细节和代码优化；

## v0.4.3
* s3兼容api文档
* obj元数据查询条件修改
* ftp upload calculate md5
* obj元数据api返回数据增加rados信息

## v0.4.2
* add put object api
* 网页端上传改为使用put object api
* get object api未认证可以下载公有桶对象

## v0.4.1
* 升级到jquery3,bootstrap4
* ftp服务解决ls文件报错bug

## v0.4.0
* 国际化多语言   

## v0.3.11
* 桶统计API超级用户可以访问所有桶
* ftp服务list dir最多可返回两万条数据
* 目录对象列表网页实现跳转到指定页码

## v0.3.10
* 时间统一ISO格式   

## v0.3.9
* 桶API同时支持桶ID和桶name   
* 创建空对象元数据API   
* 自动同步更新对象大小API   

## v0.3.8
* 桶备注功能

## v0.3.7 
* 弃用旧jwt和API文档库相关代码移除   
* 桶列表网页桶资源统计查看实现   

## v0.3.6
* 多ceph pool实现，桶记录自己的pool name

## v0.3.5
* 使用drf-yasg生成API文档
* 新的jwt api， 旧api弃用
* evcloud v3版
* 带密码的分享

## v0.3.4
* clearbucket命令修改，增加selectobject命令，桶归档桶名去除唯一索引   
* 对象rados存储信息API obj-rados   
* 日志库concurrent_log_handler和日志相关修改   

## v0.3.3
* 去除桶软删除，增加存储通归档表   
* 增加vpn口令密码   

## v0.3.2 
* ftp兼容filezilla客户端  
* 增加ftp list_dir生成器接口  
* 域名修改科技云通行证登录配置修改，logo修改   

## v0.3.1
* 对象和目录model删除sh字段，重命名srd字段为share   
* 对象分享API参数修改   

## v0.3.0
* 增加分享功能，分享桶和目录，分享链接浏览下载分享内容    
* 对象model增加na_md5和srd字段    
* 分享功能相关的API实现   

## v0.2.11
* 修复科技云通行证登录回调500 bug  
* FTP修复小文件下载存在的问题  

## v0.2.10
* 修复obs下载找不到桶的bug   

## v0.2.9   
* 存储桶ftp访问增加只读密码   

## v0.2.8  
* 群发通知邮件命令
* 修复list dir分页bug

## v0.2.7  
* ftp性能优化  

## v0.2.6
* 为ftp封装一些操作Harbor对象的接口，rest API也使用这些接口   
*  ceph配置修改    

## v0.2.5
* 用户模型增加role字段   
* 获取ceph IO状态接口完善   
* 修复用户未认证时记录用户活跃日期时的错误   
* 对象元数据通过metadata api获取，删除通过obj api获取对象元数据的功能   
* 上传对象分片时，更新对象元数据修改，对象大小字段只在更新的值比数据库中此字段的值大时才更新  
* 存储桶删除和设置权限API的参数ids改为通过url传递    

## v0.2.4
* 添加获取元数据API，一些路由格式修改   
* 文件上传时乐观锁更新对象元数据，防止并发数据不一致   
* 前端页面实现对象重命名   
* 前端js修改，对象列表页面包屑路径改为在前端生成渲染，移除无用的python第三方包  
* 列举目录下的对象或子目录时不再按创建时间倒叙排序  
* clearbucket命令启用多线程    
* ceph集群的访问接口基于官方python包封装实现   
* 增加获取ceph集群统计信息API，获取ceph集群组件信息API,获取ceph集群io性能信息API    
* 增加用户资源统计API，查询用户总量API, 系统可用性监控API，系统访问信息统计API，系统是否可用查询API    
* 对象分片上传由PUT改为POST方法    
* list dir分页优化    
* 对象下载支持Range和Content-Range标头参数    
* 交互式api文档Schema相关修改，用action或method区分manual fields参数    

## v0.2.3
* 增加用户信息修改API和用户API权限修改
* 增加通过用户名获取用户安全凭证的API   
* 通过django-cors-headers实现跨域支持   
* 限制同一路径下存在重名对象或目录  
* 增加移动对象和重命名对象API   

## v0.2.2
* 增加存储桶所占资源统计API   
* 日志文件存放路径改为/var/log/evharbor   
* 支持第三方科技云通行证登录认证  

## v0.2.1  
* 对象模型添加对象名称联合唯一索引，对象和目录名称长度最大255字符限制  
* 对象元数据序列化器修改和对应js修改   
* 添加访问密钥（access_key, secret_key）认证方式，添加访问密钥相关API，安全凭证前端页面添加访问密钥内容  
* 添加django-debug-toolbar，方便多网页时一些调试分析   
* 对象查询管理类的一些修改   
* 添加存储桶名唯一性约束，软删除修改        

## v0.2.0
* 对象元数据存储从mongodb改为mysql数据库   
* 一些依赖包版本更新,如Django 1.11.18   
* 目录创建API修改，对应js文件修改，支持含特殊字符文件夹创建
* 修改clearbucket命令（因对象元数据存储数据库更改）  
* 增加一些统计耗时时长的日志代码   
* rados接口修改   
* 后台管理修改以支持修改用户的密码   
* uwsgi配置文件修改   
* 对象下载API自定义下载时返回不正确的二进制数据流问题修正 
* 压缩迁移文件，对象元数据切换mysql后一些语义化代码修改，耗时统计log修改        
* 虚拟机备注信息修改实现   

## v0.1.7
* 添加各功能部分的说明文档页面，ckeditor富文本编辑支持    
* APIAuth模型字段修改   

## v0.1.6
* 完善虚拟机限制数量问题   
* 对象名作为shard key   
* 添加openvpn autt认证脚本  

## v0.1.5
* rados接口类增加写入方法可传入一个文件  
* 元数据对象名字段唯一unique  
* 对象上传方式为覆盖上传  
* 创建桶时创建shard collection  
* Bucket增加大小和对象数量字段，其他一些代码优化  
* 对象元数据和对象数据原子性操作修改  
* 对象名改为存全路径，桶的集合名为'bucket_' + 桶id, rados对象名key改为桶id+对象原数据id  
* 非空目录不允许删除，对象和目录删除改为物理删除   
* 添加自定义django命令'clearbucket'   
* firefox文件下载中文文件名乱码问题修改，对象下载API参数名修改和自定义读取文件块不得大于20MB限制  

## v0.1.4
* 修复firefox浏览器下载文件时无文件名问题  
* 页面实现文件对象分享公开设置及相关代码修改，js代码api字符串的拼接整理  
* 增加存储桶访问权限设置API和对应页面设置桶权限的功能实现  
* 解决mongoengine上下文管理器switch_collection线程安全问题  
* 修改文件夹对象分页方式，分享下载api添加存储桶访问权限的判断   
* 增加文档app  
* 实现用户桶数量限制  
* 增加用户模型字段，注册时获取更多用户信息  
* 桶内对象数量限制

## v0.1.3
* 找回密码修改
* 用户注册bug修复

## v0.1.2
* utc时间转本地时间
* 添加安全凭证页面
* mongoengine swith_collection使用相关修改
* 用户注册修改

## v0.1.1
* 增加找回密码功能。

# v0.1.0
* 第一个发布版本，基础功能和API实现。
