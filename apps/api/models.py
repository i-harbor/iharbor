from django.db import models
from django.contrib.auth import get_user_model
from buckets.models import Bucket, BucketFileInfo, FileChunkInfo

# Create your models here.

#获取用户模型
User = get_user_model()
