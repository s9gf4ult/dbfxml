# -*- coding: utf-8 -*-

import sqlite3
from common_helpers import *

def isTableAre(sq, table_name):
    declare_type(sqlite3.Connection, sq)
    
    return sq.execute("select * from sqlite_master where type = 'table' and name = '{table}'".format(table = table_name)).fetchall().__len__() == 1

def makeTableIfNotExists(sq, table_name, params_hash, constraints = []):
    """(sq:sqlite3.Connection, table_name:str, params_hash:dict, constraints:list = []) -> None
    create table with params, add constraints (as array's elements delimeted by comma) to query"""
    declare_type(sqlite3.Connection, sq)
    query = "create table if not exists {table} (id integer primary key not null, {fields}".format(table = table_name,
        fields = reduce(lambda a, b:"{0}, {1}".format(a, b),  map(lambda a: "{0} {1}".format(a, params_hash[a]), params_hash)))
    if constraints != []:
        query += ", " + reduce(lambda a, b:"{0}, {1}".format(a, b), constraints)
    query += ")"
    sq.execute(query)


def insertInto(sq, table_name, values):
    """insert values into table_name where values is dictionary {key: value}"""
    declare_type(sqlite3.Connection, sq)
    keys = []
    for key in values:
        if values[key]:
            keys.append(key)
        
    query = "insert into {name}({keys}) values ({values})".format(
        name = table_name,
        keys = reduce(lambda a, b:"{0}, {1}".format(a, b), keys),
        values = reduce(lambda a, b:"{0}, {1}".format(a,b), map(lambda a:"?" , keys))) # на самом деле тут мы возвращаем много много вопросиков разделенных запятыми
    try:
        sq.execute(query, map(lambda a: values[a], keys)) # а вот сдесь собсно значения
    except:
        raise Exception("""It seems that one of values gived in hash has unsupported type
        it probably has no adapter in sqlite.
        Here is all keys that gived into
        {0}""".format(reduce(lambda a, b:"{0}\n{1}".format(a,b), map(lambda a:"key:{0}, value:{1}, class{2}".format(a, values[a], values[a].__class__), keys))))

def getIdForTable(sq, table_name):
    """returns maximum value of id field for table_name"""
    declare_type(sqlite3.Connection, sq)
    st = sq.execute("select max(id) from {0}".format(table_name)).fetchone()
    if st == (None,):
        return 1
    else:
        return st[0] + 1


class selectHashIterator:
    def __init__(self, cur):
        declare_type(sqlite3.Cursor, cur)
        self.cur = cur
        self.fields = map(lambda a: a[0], cur.description)

    def __iter__(self):
        return self

    def next(self):
        ret = {}
        vals = self.cur.next()
        for index in range(0, self.fields.__len__()):
            ret[self.fields[index]] = vals[index]
        return ret
    
    
        
