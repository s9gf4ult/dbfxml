# -*- coding: utf-8 -*-
import os
import re
import datetime
import decimal
import sys
reload(sys)                             # круто да ?
sys.setdefaultencoding('utf-8')         # а вот зачем это было нужно

import ydbf
import sqlite3
import log
import sql_helpers

    
class importer:
    """importer from dbf to sqlite3"""
    def __init__(self, sqlite_file, encoding = 'cp866'):
        self.encoding = encoding
        self.sq_connection = sqlite3.connect(sqlite_file, detect_types = sqlite3.PARSE_DECLTYPES)
        sql_helpers.makeTableIfNotExists(self.sq_connection, "processed_files", {"full_path": "varchar not null", "processed": "integer not null", "table_name" : "varchar not null"}, ["unique(full_path)"])
        self.sq_connection.execute("pragma foreign_keys = on")
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
        

    def __del__(self):
        self.sq_connection.close()
        
    def addForProcessing(self, file_name, table_name):
        """add file_name to database for exporting to table_name"""
        if not os.path.exists(file_name):
            raise Exception("file {0} does not exists".format(file_name))
        id = sql_helpers.getIdForTable(self.sq_connection, "processed_files")
        try:
            self.sq_connection.execute("insert into processed_files(id, full_path, processed, table_name)  values (?,?,?,?)", (id, os.path.realpath(file_name), 0, table_name))
        except:
            if self.sq_connection.execute("select * from processed_files where full_path = ?", (os.path.realpath(file_name),)).fetchall() != []:
                log.log("file {0} did not added for processing (already aded)".format(file_name))
            else:
                raise
        else:
            self.sq_connection.commit()
            log.log("file {0} added".format(file_name))
            

    def processTable(self, table_name):
        """processes table table_name if any file is assigned to it"""
        if self.sq_connection.execute("select * from processed_files where table_name = '{0}'".format(table_name)).fetchall() == [] :
            raise Exception("there is no files attached to table {0}".format(table_name))
        files_list = map(lambda a: a[0], self.sq_connection.execute("select full_path from processed_files where table_name = '{0}' and processed = 0".format(table_name)).fetchall())
        if files_list == []:
            log.log("no one file assigned to table {0}".format(table_name))
        if not sql_helpers.isTableAre(self.sq_connection, table_name):
            #таблицы еще нет в базе - создаем, узнаем типы и имена полей из дбф файлов, проверяем соответствия полей типам 
            for filename in files_list:
                log.log("reading structure of {0}".format(filename))
                dbfcon = ydbf.open(filename, encoding = self.encoding)
                if not vars().has_key("fields"): # если еще не поеределили переменную
                    fields = {} # тут храним имена и типы полей, которые будем создавать
                    for field in dbfcon.fields:
                        fields[field[0]] = [field[1], field[3]]
                else: # переменная уже определена, если встретятся поля с другим типом - выбросим исключение
                    for field in dbfcon.fields:
                        if fields.has_key(field[0]):
                            if fields[field[0]] != [field[1], field[3]]:
                                raise Exception("file {file} has field {field} with type {type1}, another fields in another files has type {type2}".format(file = filename, field = field[0], type1 = field[1], type2 = fields[field[0]]))
                        else:
                            fields[field[0]] = [field[1], field[3]]
                dbfcon.close()
            # теперь надо создать таблицу в базе
            def mapdatatype(a):         # отображение типов из dbf в типы sqlite3
                if a[0] == 'C':
                    return "text"
                elif a[0] == 'D':
                    return "date"
                elif a[0] == 'N' and a[1]:
                    return "decimal"
                elif a[0] == 'N' and not a[1]:
                    return "integer"
                elif a == 'L':
                    return "bool"
                else:
                    raise Exception("can not create field with type {0}".format(a))

            for field in fields:
                fields[field] = mapdatatype(fields[field])
            sql_helpers.makeTableIfNotExists(self.sq_connection, table_name, fields)
            sql_helpers.makeTableIfNotExists(self.sq_connection, "file_assigns", {"file_id" : "integer", "record_id": "integer"}, ["foreign key (file_id) references processed_files(id) on delete cascade", "unique(file_id, record_id)"])
        #таблица существует - заносим в нее данные из файлов
        table_id = sql_helpers.getIdForTable(self.sq_connection, table_name)
        assign_id = sql_helpers.getIdForTable(self.sq_connection, "file_assigns")
        
        try:
            for file_tuple in self.sq_connection.execute("select id, full_path from processed_files where table_name = '{0}' and processed = 0".format(table_name)).fetchall():
                log.log("inserting records from {0}".format(file_tuple[1]))
                for rec in ydbf.open(file_tuple[1], encoding = self.encoding):
                    rec["id"] = table_id
                    sql_helpers.insertInto(self.sq_connection, table_name, rec)
                    sql_helpers.insertInto(self.sq_connection, "file_assigns", {"id" : assign_id, "file_id" : file_tuple[0], "record_id" : table_id})
                    table_id += 1
                    assign_id += 1
                self.sq_connection.execute("update processed_files set processed = 1 where id = {0}".format(file_tuple[0]))
        except:
            self.sq_connection.rollback()
            raise
        else:
            self.sq_connection.commit()
            
        
    def getSqliteConnection(self):
        return self.sq_connection

    def getUnprocessedTables(self):
        return map(lambda a: a[0], self.sq_connection.execute("select distinct table_name from processed_files where processed = 0").fetchall())
    
    def processAllTables(self):
        for table in self.getUnprocessedTables():
            self.processTable(table)

    def addForProcessingByPattern(self, table_name, directory, pattern = '.*' , flags = 0):
        files = findFilesByRegexp(directory, pattern, flags)
        if files != []:
            for file in files:
                self.addForProcessing(file, table_name)
        
        
