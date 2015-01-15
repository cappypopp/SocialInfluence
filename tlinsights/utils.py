__author__ = 'cappy'

import logging
from functools import wraps

def logged(level=logging.DEBUG, name=None, message=None):
    def decorate(func):

        logname = name if name else func.__module__
        log = logging.getLogger(logname)
        logmsg = message if message else func.__name__

        @wraps(func)
        def wrapper(*args, **kwargs):
            log.log(level, logmsg)
            return func(*args, **kwargs)
        return wrapper
    return decorate
