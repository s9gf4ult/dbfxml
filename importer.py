# -*- coding: utf-8 -*-
import os

try:
    import ydbf
except:
    print "can not load module dbf"
    exit

try:
    import sqlite3
except:
    print "can not load module sqlite3"
    exit

class importer:
    """importer from dbf to sqlite3"""
    def __init__(self, sqlite_file):
        if not os.path.exists(sqlite_file):
            self.sq_connection = sqlite3.connect(sqlite_file)
            self.sq_connection.execute("create table processed_files (id int primary key not null, full_path varchar not null, processed not null,table_name varchar not null, unique (full_path))")
        else:
            print "file %s already exists" % sqlite_file
            return

    def __del__(self):
        self.sq_connection.close()
        
    def addForProcessing(self, file_name, table_name):
        if not os.path.exists(file_name):
            raise("file {0} does not exist".format(file_name))
        mid = self.sq_connection.execute("select max(id) from processed_files").fetchone();
        if mid == (None, ):
            id = 1
        else:
            (id, ) = mid;
            id += 1
        self.sq_connection.execute("insert into processed_files(id, full_path, processed, table_name)  values ({0}, '{1}', {2}, '{3}')".format(id, os.path.realpath(file_name), 0, table_name))
        self.sq_connection.commit()

    def processTable(self, table_name):
        if self.sq_connection.execute("select * from processed_file where table_name = '{0}'".format(table_name)).fetchall() == [] :
            raise("there is no files attached to table {0}".format(table_name))
        if self.sq_connection.execute("select * from sqlite_master where type = table and name = '{name}'".format(name = table_name)).fetchall() == []:
            #таблицы еще нет в базе - создаем, узнаем типы и имена полей из дбф файлов, проверяем соответствия полей типам 
            files_list = map(lambda a: a[0], self.sq_connection.execute("select full_path from processed_files where table_name = {0} and processed = 0".format(table_name)).fetchall())
            for filename in files_list:
                dbfcon = ydbf.open(filename, encoding = 'cp866')
                if not vars().has_key("fields"): # если еще не поеределили переменную
                    fields = dbfcon.fields       # тут храним имена и типы полей, которые будем создавать
                else:
                    for table_field in dbfcon.fields:
                        same_fields = filter(lambda a: a[0] == table_field[0], fields)
                        if table_field[1] != [0][1]:
                            raise "type does not match in table {0} field {1} and file {3} same field".format(table_name, table_field[0], filename)
                    
                  
                  
                  
                  
                  
                  
        
        
        
        
        
        
        
        
        
        
        
            
        
        
        
        
	
