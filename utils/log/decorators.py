import logging
from datetime import datetime


def log_used_time(logger=None, mark_text=''):
    '''
    打印函数执行时间日志装饰器

    :param logger: 日志输出接口
    :param mark_text: 要统计用时的当前目标描述文字
    :return:
    '''
    def _decorator(func):
        def swwaper(*args, **kwargs):
            start_time = datetime.now()

            ret = func(*args, **kwargs)
            if not isinstance(logger, logging.Logger):
                return ret

            end_time = datetime.now()
            logger.debug(f'All used time: {end_time - start_time} s {mark_text}.')

            return ret

        return swwaper
    return _decorator




