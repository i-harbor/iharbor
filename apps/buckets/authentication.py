from django.utils.translation import gettext as _
from django.contrib.auth.models import AnonymousUser
from rest_framework.authentication import BaseAuthentication, get_authorization_header
from rest_framework.exceptions import AuthenticationFailed

from .models import BucketToken


class BucketTokenAuthentication(BaseAuthentication):
    """
    存储桶访问token认证

    Clients should authenticate by passing the token key in the "Authorization"
    HTTP header, prepended with the string "BucketToken ".  For example:

        Authorization: BucketToken 401f7ac837da42b97f613d789819ff93537bee6a
    """

    keyword = 'BucketToken'

    def authenticate(self, request):
        auth = get_authorization_header(request).split()

        if not auth or auth[0].lower() != self.keyword.lower().encode():
            return None

        if len(auth) == 1:
            msg = _('Invalid token header. No credentials provided.')
            raise AuthenticationFailed(msg)
        elif len(auth) > 2:
            msg = _('Invalid token header. Token string should not contain spaces.')
            raise AuthenticationFailed(msg)

        try:
            token = auth[1].decode()
        except UnicodeError:
            msg = _('Invalid token header. Token string should not contain invalid characters.')
            raise AuthenticationFailed(msg)

        return self.authenticate_credentials(token)

    @staticmethod
    def authenticate_credentials(key):
        try:
            token = BucketToken.objects.select_related('bucket').get(key=key)
        except BucketToken.DoesNotExist:
            raise AuthenticationFailed(_('Invalid token.'))

        return AnonymousUser(), token

    def authenticate_header(self, request):
        return self.keyword
