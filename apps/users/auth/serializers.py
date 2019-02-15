from rest_framework import serializers
from rest_framework.authtoken.serializers import AuthTokenSerializer

from utils.time import to_localtime_string_naive_by_utc


class AuthKeySerializer(AuthTokenSerializer):
    pass


class AuthKeyDumpSerializer(serializers.Serializer):
    access_key = serializers.CharField()
    secret_key = serializers.CharField()
    user = serializers.SerializerMethodField()
    create_time = serializers.SerializerMethodField()
    state = serializers.BooleanField()
    permission  = serializers.SerializerMethodField()

    def get_user(self, obj):
        return obj.user.username

    def get_create_time(self, obj):
        return to_localtime_string_naive_by_utc(obj.create_time)

    # def get_state(self, obj):
    #     return obj.get_state_display()

    def get_permission(self, obj):
        return obj.get_permission_display()

