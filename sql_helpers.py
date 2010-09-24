# -*- coding: utf-8 -*-

import sqlite3
import datetime

def isTableAre(sq, table_name):
    if sq.__class__ != sqlite3.Connection:
        raise Exception("sq must be sqlite3.Connection class")
    return sq.execute("select * from sqlite_master where type = 'table' and name = '{table}'".format(table = table_name)).fetchall().__len__() == 1

def makeTableIfNotExists(sq, table_name, params_hash, constraints = []):
    """(sq:sqlite3.Connection, table_name:str, params_hash:dict, constraints:list = []) -> None
    create table with params, add constraints (as array's elements delimeted by comma) to query"""
    
    if sq.__class__ != sqlite3.Connection:
        raise Exception("sq must be sqlite3.Connection class")
    query = "create table if not exists {table} (id integer primary key not null, {fields}".format(table = table_name,
        fields = reduce(lambda a, b:"{0}, {1}".format(a, b),  map(lambda a: "{0} {1}".format(a, params_hash[a]), params_hash)))
    if constraints != []:
        query += ", " + reduce(lambda a, b:"{0}, {1}".format(a, b), constraints)
    query += ")"
    sq.execute(query)


def insertInto(sq, table_name, values):
    if sq.__class__ != sqlite3.Connection:
        raise Exception("sq must be sqlite3.Connection class")
    keys = []
    for key in values:
        if values[key] != None:
            keys.append(key)
        
    query = "insert into {name}({keys}) values ({values})".format(
        name = table_name,
        keys = reduce(lambda a, b:"{0}, {1}".format(a, b), keys),
        values = reduce(lambda a, b:"{0}, {1}".format(a,b), map(lambda a:"?" , keys))) # на самом деле тут мы возвращаем много много вопросиков разделенных запятыми
    sq.execute(query, map(lambda a: values[a], keys)) # а вот сдесь собсно значения

def getIdForTable(sq, table_name):
    st = sq.execute("select max(id) from {0}".format(table_name)).fetchone()
    if st == (None,):
        return 1
    else:
        return st[0] + 1


class hashRetrunter:
    def __init__(self, cur):
        if cur.__class__ != sqlite3.Cursor:
            raise Exception("cur must be sqlite3.Cursor instance")
        self.cur = cur
        self.fields = cur.description

    def __iter__(self):
        return self
    
        