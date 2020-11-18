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


class BadRequest(Error):
    default_message = 'Bad Request'
    default_code = 'BadRequest'
    default_status_code = 400


class AccessDenied(Error):
    default_message = 'Access Denied.'
    default_code = 'AccessDenied'
    default_status_code = 403


class NotShared(Error):
    default_message = 'This resource has not been publicly shared.'
    default_code = 'NotShared'
    default_status_code = 403


class NotFound(Error):
    default_message = 'Not found'
    default_code = 'Notfound'
    default_status_code = 404


class NoSuchKey(NotFound):
    default_message = 'The specified key does not exist.'
    default_code = 'NoSuchKey'


class NoSuchBucket(NotFound):
    default_message = 'The specified bucket does not exist.'
    default_code = 'NoSuchBucket'
