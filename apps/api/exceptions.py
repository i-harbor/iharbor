class Error(Exception):
    default_message = 'We encountered an internal error. Please try again.'
    default_code = 'InternalError'
    default_status_code = 500

    def __init__(self, message: str = '', code: str = '', status_code=None, extend_msg=''):
        """
        :param message: 错误描述
        :param code: 错误代码
        :param status_code: HTTP状态码
        :param extend_msg: 扩展错误描述的信息，追加到message后面
        """
        self.message = message if message else self.default_message
        self.code = code if code else self.default_code
        self.status_code = self.default_status_code if status_code is None else status_code
        if extend_msg:
            self.message += '&&' + extend_msg

        # self.data = kwargs  # 一些希望传递的数据

    def __repr__(self):
        return f'{type(self)}(message={self.message}, code={self.code}, status_code={self.status_code})'

    def __str__(self):
        return self.message

    def detail_str(self):
        return self.__repr__()

    def err_data(self):
        return {
            'code': self.code,
            'message': self.message
        }

    def err_data_old(self):
        return {
            'code': self.code,
            'code_text': self.message
        }

    @classmethod
    def from_error(cls, err):
        if isinstance(err, Error):
            return cls(message=err.message, code=err.code, status_code=err.status_code)

        return cls(message=str(err))

    def is_same_code(self, err):
        if not isinstance(err, Error):
            return False

        return err.code == self.code


class AuthenticationFailed(Error):
    default_status_code = 401
    default_message = 'Incorrect authentication credentials.'
    default_code = 'AuthenticationFailed'


class NotAuthenticated(Error):
    default_status_code = 401
    default_message = 'Authentication credentials were not provided.'
    default_code = 'NotAuthenticated'


class BadRequest(Error):
    default_message = 'Bad Request'
    default_code = 'BadRequest'
    default_status_code = 400


class InvalidArgument(BadRequest):
    default_message = 'Invalid Argument'
    default_code = 'InvalidArgument'


class TooManyBucketTokens(Error):
    default_message = "You have attempted to create more tokens than allowed."
    default_code = 'TooManyBucketTokens'
    default_status_code = 400


class InvalidDigest(BadRequest):
    default_message = 'The Content-MD5 you specified is not valid.'
    default_code = 'InvalidDigest'
    default_status_code = 400


class BadDigest(BadRequest):
    default_message = 'The Content-MD5 you specified did not match what we received.'
    default_code = 'BadDigest'
    default_status_code = 400


class InvalidKey(BadRequest):
    default_message = 'The specified key is invalid.'
    default_code = 'InvalidKey'


class AccessDenied(Error):
    default_message = 'Access Denied.'
    default_code = 'AccessDenied'
    default_status_code = 403


class NotShared(Error):
    default_message = 'This resource has not been publicly shared.'
    default_code = 'NotShared'
    default_status_code = 403


class SharedExpired(Error):
    default_message = '分享已过期.'
    default_code = 'SharedExpired'
    default_status_code = 403


class NotFound(Error):
    default_message = 'Not found'
    default_code = 'Notfound'
    default_status_code = 404


class NoSuchKey(NotFound):
    default_message = 'The specified key does not exist.'
    default_code = 'NoSuchKey'


class NoParentPath(NotFound):
    default_message = 'The parent path does not exist.'
    default_code = 'NoParentPath'


class NoSuchToken(NotFound):
    default_message = 'The specified token does not exist.'
    default_code = 'NoSuchToken'


class NoSuchBucket(NotFound):
    default_message = 'The specified bucket does not exist.'
    default_code = 'NoSuchBucket'


class MethodNotAllowed(Error):
    default_message = 'Method not allowed.'
    default_code = 'MethodNotAllowed'
    default_status_code = 405


class BucketAlreadyOwnedByYou(Error):
    default_message = 'The bucket you tried to create already exists, and you own it.'
    default_code = 'BucketAlreadyOwnedByYou'
    default_status_code = 409


class BucketAlreadyExists(Error):
    default_message = 'The requested bucket name is already exists. Please select a different name and try again.'
    default_code = 'BucketAlreadyExists'
    default_status_code = 409


class KeyAlreadyExists(Error):
    default_message = '目标已存在。'
    default_code = 'KeyAlreadyExists'
    default_status_code = 409


class SameKeyAlreadyExists(Error):
    default_message = '同名目录或对象已存在。'
    default_code = 'SameKeyAlreadyExists'
    default_status_code = 409


class NoEmptyDir(Error):
    default_message = '无法删除非空目录。'
    default_code = 'NoEmptyDir'
    default_status_code = 409


class Throttled(Error):
    default_message = 'Request was throttled.'
    default_code = 'Throttled'
    default_status_code = 429


class HarborError(Error):
    @property
    def msg(self):
        return self.message

    @msg.setter
    def msg(self, value):
        self.message = value


class BucketLockWrite(HarborError):
    default_message = '存储桶已锁定写操作.'
    default_code = 'BucketLockWrite'
    default_status_code = 403
