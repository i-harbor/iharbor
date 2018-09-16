from datetime import datetime

from mongoengine import Document
from mongoengine import fields

# Create your models here.

class UploadFileInfo(Document):
	'''
	上传文件信息模型
	'''
	uuid = fields.UUIDField(binary=False)#以字符串形式存储
	filename = fields.StringField(max_length=500, required=True)
	add_time = fields.DateTimeField(default=datetime.utcnow)
	size = fields.LongField()

	meta = {
		#db_alias用于指定当前模型默认绑定的mongodb连接，但可以用switch_db(UploadFileInfo, 'db2')临时改变对应的数据库连接
		'db_alias': 'default',
		'indexes': ['uuid'],#索引
		'ordering': ['-add_time'], #文档降序，最近日期靠前
		# 'collection':'uploadfileinfo',#集合名字，默认为小写字母的类名
		# 'max_documents': 10000, #集合存储文档最大数量
		# 'max_size': 2000000, #集合的最大字节数
	}





