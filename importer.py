# -*- coding: utf-8 -*-
import os

try:
    import ydbf
except:
    raise Exception("can not load module dbf")

try:
    import sqlite3
except:
    raise Exception("can not load module sqlite3")

try:
    import log
except:
    raise Exception("can not load module log")

def isTableAre(sq, table_name):
    if sq.__class__ != sqlite3.Connection:
        raise Exception("sq must be sqlite3.Connection class")
    return sq.execute("select * from sqlite_master where type = 'table' and name = '{table}'".format(table = table_name)).fetchall().__len__() == 1

def makeTableIfNotExists(sq, table_name, params_hash, constraint_hash = {}):
    if sq.__class__ != sqlite3.Connection:
        raise Exception("sq must be sqlite3.Connection class")
    if not isTableAre(sq, table_name):
        query = "create table {table} (id integer primary key not null, {fields}".format(table = table_name,
                fields = reduce(lambda a, b:"{0}, {1}".format(a, b),  map(lambda a: "{0} {1}".format(a, params_hash[a]), params_hash)))
        if constraint_hash != {}:
            query += ", {0}".format(reduce(lambda a, b:"{0}, {1}".format(a,b),
                                           map(lambda cn: "{0} ({1})".format(cn, reduce(lambda f1, f2: "{0}, {1}".format(f1, f2), constraint_hash[cn])), constraint_hash)))
        query += ")"
        sq.execute(query)
    

class importer:
    """importer from dbf to sqlite3"""
    def __init__(self, sqlite_file):
        self.sq_connection = sqlite3.connect(sqlite_file)
        makeTableIfNotExists(self.sq_connection, "processed_files", {"full_path": "varchar not null", "processed": "integer not null", "table_name" : "varchar not null"}, { "unique": ["full_path"]})

    def __del__(self):
        self.sq_connection.close()
        
    def addForProcessing(self, file_name, table_name):
        """add file_name to database for exporting to table_name"""
        if not os.path.exists(file_name):
            raise Exception("file {0} does not exists".format(file_name))
        mid = self.sq_connection.execute("select max(id) from processed_files").fetchone();
        if mid == (None, ):
            id = 1
        else:
            (id, ) = mid;
            id += 1
        self.sq_connection.execute("insert into processed_files(id, full_path, processed, table_name)  values ({0}, '{1}', {2}, '{3}')".format(id, os.path.realpath(file_name), 0, table_name))
        self.sq_connection.commit()

    def processTable(self, table_name):
        """processes table table_name if any file is assigned to it"""
        if self.sq_connection.execute("select * from processed_files where table_name = '{0}'".format(table_name)).fetchall() == [] :
            raise Exception("there is no files attached to table {0}".format(table_name))
        if not isTableAre(self.sq_connection, table_name):
            #таблицы еще нет в базе - создаем, узнаем типы и имена полей из дбф файлов, проверяем соответствия полей типам 
            files_list = map(lambda a: a[0], self.sq_connection.execute("select full_path from processed_files where table_name = {0} and processed = 0".format(table_name)).fetchall())
            for filename in files_list:
                dbfcon = ydbf.open(filename, encoding = 'cp866')
                if not vars().has_key("fields"): # если еще не поеределили переменную
                    fields = {} # тут храним имена и типы полей, которые будем создавать
                    for field in dbfcon.fields:
                        fields[field[0]] = field[1]
                else: # переменная уже определена, если встретятся поля с другим типом - выбросим исключение
                    for field in dbfcon.fields:
                        if fields.has_key(field[0]):
                            if fields[field[0]] != field[1]:
                                raise Exception("file {file} has field {field} with type {type1}, another fields in another files has type {type2}".format(file = filename, field = field[0], type1 = field[1], type2 = fields[field[0]]))
                        else:
                            fields[field[0]] = field[1]
            # теперь надо создать таблицу в базе
            def mapdatatype(a):         # отображение типов из dbf в типы sqlite3
                if a == 'C':
                    return "varchar"
                elif a == 'D':
                    return "text"
                elif a == 'N':
                    return "integer"
                elif a == 'L':
                    return "text"
                else:
                    raise Exception("can not create field with type {0}".format(a))
                
            self.sq_connection.execute("create table {table} (id integer primary key not null, {fields})".format(table = table_name,
                fields = reduce(lambda a, b: "{0}, {1}".format(a, b), map(lambda a:"{0} {1}".format(a, mapdatatype(fields[a])), fields))))
                                
                    
                  
                  
                  
                  
                  
                  
        
        
        
        
        
        
        
        
        
        
        
            
        
        
        
        
	
