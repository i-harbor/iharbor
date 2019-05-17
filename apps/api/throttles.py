from rest_framework.throttling import UserRateThrottle

class TestRateThrottle(UserRateThrottle):
    scope = 'test'
    THROTTLE_RATES = {
        'test': '12/minute',  # 请求访问限制每分钟次数
    }


