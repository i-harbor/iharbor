### 从v0.9.0升级到v1.0.0
#### 数据库变动：
1.  cephcluster model移除字段“ceph_using”，添加“priority_stored_value”。
2. 对象元数据model增加字段“pool_id”、“sync_start1”、“sync_end1”、“sync_start2”、“sync_end2”。
3. 增加对象同步错误记录表；

#### 升级过程简述
1. 先找一个web节点，停止Web服务，然后更新代码。
2. 先保留 cephcluster model的字段“ceph_using”，避免影响线上的服务，升级完后再手动删除此字段。
   先注释掉ceph下的迁移文件0002_auto_20230217_1026.py中的移除字段“alias”的代码。
3. 执行数据库迁移文件，python3 manage.py migrate。
4. v1.0.0版本每个对象通过pool_id字段记录自己存储在那个ceph的那个pool中，bucket的对象元数据表
   增加字段“pool_id”，并填充对象元数据pool_id字段的值（已上传的对象实际存储所在的ceph和pool，
   对应的 cephcluster 记录的id）。  
   使用自定义命令 bucketfilepoolid 来完成，此过程中服务上传功能会受影响，因为旧版本代码没
   有pool_id，pool_id不填充数据无法创建新的对象元数据记录。   
   根据服务中 cephcluster 表，修改命令文件 bucketfilepoolid.py 中 ceph_config，配
   置 cephcluster 的别名alias和id的对应关系，然后执行命令 `python3 manage bucketfilepoolid`，
   为所有bucket的对象元数据表增加字段“pool_id”，并填充实际 cephcluster 记录的id。
5. 手动为所有桶 对象元数据表添加字段“sync_start1”、“sync_end1”、“sync_start2”、“sync_end2”。
6. 更新ftp部署节点代码，web节点代码。
7. 删除cephcluster model表的字段“ceph_using”。
