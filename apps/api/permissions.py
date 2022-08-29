from rest_framework.permissions import BasePermission

from buckets.models import BucketToken


class IsSuperUser(BasePermission):
    """
    Does the user have administrator privileges.
    """
    def has_permission(self, request, view):
        return bool(request.user and request.user.is_superuser)


class IsSuperAndStaffUser(BasePermission):
    """
    Does the user have both administrator and employee privileges
    """
    def has_permission(self, request, view):
        return bool(request.user and request.user.is_superuser and request.user.is_staff)


class IsAppSuperUser(BasePermission):
    """
    Does the user have both administrator and employee privileges
    """
    def has_permission(self, request, view):
        if not request.user.id:
            return False
        return request.user.is_app_superuser()


class IsSuperOrAppSuperUser(BasePermission):
    """
    Does the user have administrator privileges.
    """
    def has_permission(self, request, view):
        u = request.user
        if not u.id:
            return False

        return bool(u.is_superuser or u.is_app_superuser())


class IsSuperAndAppSuperUser(BasePermission):
    """
    Does the user have administrator privileges.
    """
    def has_permission(self, request, view):
        u = request.user
        if not u.id:
            return False

        return bool(u.is_superuser and u.is_app_superuser())


class IsOwnObject(BasePermission):
    """
    对象是否属于自己
    """
    message = '您没有操作此数据的权限。'

    def has_object_permission(self, request, view, obj):
        if request.user == obj.user:
            return True

        return False


class IsOwnBucket(IsOwnObject):
    """
    是否是自己的bucket
    """
    message = '您没有操作此存储桶的权限。'


class IsAuthenticatedOrBucketToken(BasePermission):
    """
    用户已认证，或已bucket token认证
    """
    def has_permission(self, request, view):
        if bool(request.user and request.user.is_authenticated):
            return True

        if hasattr(request, 'auth'):
            if isinstance(request.auth, BucketToken):
                return True

        return False


class IsBucketTokenAuthenticated(BasePermission):
    """
    用户已认证，或已bucket token认证
    """
    def has_permission(self, request, view):
        if hasattr(request, 'auth'):
            if isinstance(request.auth, BucketToken):
                return True

        return False
