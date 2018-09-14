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
	size = fields.IntField()

	meta = {
		#db_alias用于指定当前模型默认绑定的mongodb连接，但可以用switch_db(UploadFileInfo, 'db2')临时改变对应的数据库连接
		'db_alias': 'default',
	}





