#django数据库相关

## 数据库迁移
`
python manage.py migrate  # 创建各种表
`
## 数据库导出数据
`
python manage.py dumpdata --natural-foreign --natural-primary -e contenttypes -e auth.Permission --indent 4  > data.json
`
## 数据库数据导入
`
python manage.py loaddata data.json  # 指定操作的数据库的别名 --database=default
`
# mysql字符集命令

## 查看MySQL数据库服务器和数据库MySQL字符集
`
mysql>SHOW VARIABLES LIKE '%character%';
`
##查看MySQL数据表（table）的MySQL字符集。
`
mysql> show table status from database_name like '%table_name%';
`
## 查看MySQL数据列（column）的MySQL字符集。
`
mysql> show full columns from 表名;  
`
## 修改数据库字符集
`
alter database 数据库名 character set utf8; #ps:修改完数据库字符集，需要重启mysql数据库。
`
## 修改表字符集
`
ALTER TABLE  表名 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;
`

# mysql配置文件
mysql的默认字符集character_set_database和character_set_server还是latin1。 
最简单的完美修改方法，修改mysql的my.cnf文件中的字符集键值（注意配置的字段细节）：
 
```
在[client]字段里加入default-character-set=utf8，如下： 
    [client]
    port = 3306
    socket = /var/lib/mysql/mysql.sock
    default-character-set=utf8
 
>在[mysqld]字段里加入character-set-server=utf8，如下： 
    [mysqld]
    port = 3306
    socket = /var/lib/mysql/mysql.sock
    character-set-server=utf8
 
>在[mysql]字段里加入default-character-set=utf8，如下： 
    [mysql]
    no-auto-rehash
    default-character-set=utf8
```

# mysql常用命令

```
CREATE DATABASE `mydb` CHARACTER SET utf8 COLLATE utf8_general_ci; # 创建utf8字符集数据库
create database name;               创建数据库
show databases;                     列出数据库
use databasename;                   选择数据库
drop database name;                 直接删除数据库，不提醒
show tables;                        显示表
describe tablename;                 表的详细描述
mysqladmin drop databasename;       删除数据库前，有提示。
select version(),current_date;      显示当前mysql版本和当前日期
```
