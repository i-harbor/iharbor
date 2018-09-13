from datetime import datetime

from mongoengine import Document
from mongoengine import fields
from mongoengine.context_managers import switch_db

# Create your models here.

class UploadFileInfo(Document):
	'''
	上传文件信息模型
	'''
	uuid = fields.UUIDField(binary=False)#以字符串形式存储
	filename = fields.StringField(max_length=500, required=True)
	add_time = fields.DateTimeField(default=datetime.utcnow)
	size = fields.IntField()

	meta = {'db_alias': 'default'}


# switch到testdb2连接
# with switch_db(UploadFileInfo, 'testdb2'):
# 	finfo = UploadFileInfo()
# 	finfo.save()



