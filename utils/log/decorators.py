import logging
from datetime import datetime

from django.conf import settings


def log_used_time(logger=None, mark_text=''):
    '''
    打印函数执行时间日志装饰器

    :param logger: 日志输出接口
    :param mark_text: 要统计用时的当前目标描述文字
    :return:
    '''
    def _decorator(func):
        def swwaper(*args, **kwargs):
            # 不是debug模式，或者logger无效，不打印日志
            if not settings.DEBUG or not isinstance(logger, logging.Logger):
                return func(*args, **kwargs)

            start_time = datetime.now()
            ret = func(*args, **kwargs)
            end_time = datetime.now()
            logger.debug(f'All used time: {end_time - start_time} s {mark_text}.')

            return ret

        return swwaper
    return _decorator


def log_op_info(logger=None, mark_text=''):
    '''
    执行操作信息日志装饰器

    :param logger: 日志输出接口
    :param mark_text: 描述文字
    :return:
    '''
    def _decorator(func):
        def swwaper(*args, **kwargs):
            try:
                ret = func(*args, **kwargs)
            except Exception as e:
                logger.warning(f'{mark_text},kwargs={kwargs};err={str(e)}')
                raise e

            logger.debug(f'{mark_text},kwargs={kwargs};成功return')
            return ret

        return swwaper
    return _decorator


