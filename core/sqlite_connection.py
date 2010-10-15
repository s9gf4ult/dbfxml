# -*- coding: utf-8 -*-

import sqlite3
from common_helpers import *

class sqliteConnection(sqlite3.Connection):
    def __init__(self, initstr):
        sqlite3.Connection(self, initstr)
        self.execute('pragma foreign_keys = on')
        self.checkMetadata()

    def checkMetadata(self):
        self.createTableIfNotExists('meta$tables', {'name':'varchar not null',
                                                    'type':'varchar not null',
                                                    'comment':'text'},
                                    constraints = 'unique (name)')
        self.createTableIfNotExists('meta$fields', {'table_name':'varchar not null',
                                                    'field_name':'varchar not null',
                                                    'type':'varchar not null',
                                                    'comment':'text'}
                                    constraints = 'unique(table_name, field_name)')
        if  self.execute("select t.* from meta$tables t where not exists (select t2.* from sqlite_master t2 where t2.type = 'table' and t2.name = t1.name)").fetchall() != []:
            raise Exception('there is some tables in "meta$tables" which does not exists')
        for table_name in self.execute("select t.name from meta$tables t"):
            fields = self.execute(u'select t.field_name from meta$fields t where t.table_name = ?', (table_name,)).fetchall()
            try:
                self.execute(u"select {0} from {1}".format(join_list(fields, ', '), table_name))
            except:
                raise Exception(u"can not select from '{0}' fields \n{1}".format(table_name, join_list(fields, "\n")))
        return self
    
    def createTableIfNotExists(self, name, fields, meta_fields = {'id':'primary key not null'}, constraints = None):
        self.execute(u"create table if not exists {0} ({1})".format(name,
                                                                    self._format_table_creator(fields, meta_fields, constraints)))
        self.registerTable(name, table_type, fields, meta_fields)
        return self

    def createTable(self, name, fields, meta_fields = {'id':'primary key not null'}, constraints = None):
        self. execute(u"create table {0} ({1})".format(name,
                                                       self._format_table_creator(fields, meta_fields, constraints)))
        return self

    def _format_table_creator(self, fields, meta_fields, constraints):
        ret = meta_fields != {} and join_list(map(lambda a:u"{0} {1}".format(a, meta_fields[a]), meta_fields), ", ") or ""
        ret += fields != {} and join_list(map(lambda a:u"{0} {1}".format(a, fields[a]), fields), ", ") or ""
        ret += constraints or ""
        return ret
        
    
        
                                    
                                                    
        
        
        
    

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
    
    
        
