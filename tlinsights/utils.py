__author__ = 'cappy'

import logging
import datetime
from functools import wraps

EXCEL_DATETIME_FORMAT_STRING = u"%m/%d/%Y %I:%M:%S %p"

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

def excel_date_from_datetime(dt):
    excel_start_date = datetime.datetime(1899, 12, 30)
    delta = dt - excel_start_date
    return float(delta.days) + (float(delta.seconds) / 86400)

def excel_date_from_string(datestring):
    date1 = datetime.datetime.strptime(datestring, EXCEL_DATETIME_FORMAT_STRING)
    return excel_date_from_datetime(date1)
