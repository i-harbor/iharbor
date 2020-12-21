from django.contrib.auth import get_user_model

from buckets.models import Bucket, BucketToken, BucketLimitConfig    # 不能移除，其他py会导入

User = get_user_model()         # 不能移除，其他py会导入
