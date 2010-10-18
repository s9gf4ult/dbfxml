# -*- coding: utf-8 -*-

__all__ = [
    'sqlite_connection',
    'processor'
    ]

import sqlite3
import datetime
import decimal

logger = None

def defaultlog(a):
    print(a)
          
def initcore(logfun = defaultlog):
    global logger
    logger = logfun
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')
    # register wrappers for dbf compatible types
    def datetimeconvert(date):
        return datetime.datetime(*map(int, re.split("[-T:\.]", date)))
    def dateconvert(date):
        return datetime.date(*map(int, re.split("[-T:\.]", date)))
    sqlite3.register_adapter(datetime.datetime, datetime.datetime.isoformat)
    sqlite3.register_adapter(datetime.date, datetime.date.isoformat)
    sqlite3.register_converter('datetime', datetimeconvert)
    sqlite3.register_converter('date', dateconvert)
    sqlite3.register_adapter(decimal.Decimal, decimal.Decimal.__str__)
    sqlite3.register_converter('decimal', lambda a: decimal.Decimal(a))
    sqlite3.register_converter('bool', lambda a: a == 1)
    sqlite3.register_adapter(bool, lambda a: a and 1 or 0)
    sqlite3.register_adapter(str, lambda a:a.decode())
