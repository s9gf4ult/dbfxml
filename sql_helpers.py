import sqlite3

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
        
    def formatval(a):
        if a.__class__ == str or a.__class__ == unicode:
            return "'{0}'".format(a)
        elif a.__class__ == datetime.date:
            return "'{0:04}-{1:02}-{2:02}'".format(a.year, a.month, a.day)
        else:
            return a
        
    query = "insert into {name}({keys}) values ({values})".format(name = table_name,
        keys = reduce(lambda a, b:"{0}, {1}".format(a, b), keys),
        values = reduce(lambda a, b:"{0}, {1}".format(a,b), map(lambda a:formatval(values[a]), keys)))
    sq.execute(query)

def getIdForTable(sq, table_name):
    st = sq.execute("select max(id) from {0}".format(table_name)).fetchone()
    if st == (None,):
        return 1
    else:
        return st[0] + 1
