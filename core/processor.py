# -*- coding: utf-8 -*-
import os
import re
import binascii
import copy

import ydbf

import core
import core.sqlite_connection
    
class mainProcessor:
    """importer from dbf to sqlite3"""
    def __init__(self, sqlite_file, encoding = 'cp866'):

        self.encoding = encoding
        self.sq_connection = core.sqlite_connection.sqliteConnection(sqlite_file)
        self.sq_connection.createTableIfNotExists("meta$processed_files", {"full_path":"varchar not null", "processed":"integer not null"}, meta_fields = {"meta$id":"int primary key not null","meta$table_name":"varchar", "meta$table_id":"int", "meta$crc32":"int not null"}, constraints = "foreign key (meta$table_id) references meta$tables(meta$id) on delete cascade, unique(full_path,meta$table_id), unique(meta$crc32)", table_type = 'meta') # исходные файлы для работы
        self.sq_connection.createTableIfNotExists("meta$filters", {"name":"varchar not null", "source":"int not null", "dest":"int not null"}, table_type = 'meta', constraints = "unique(source, dest), foreign key (source) references meta$tables(meta$id) on delete cascade, foreign key (dest) references meta$tables(meta$id) on delete cascade") # фильтры для преобразования одной таблицы в другую
        self.sq_connection.createTableIfNotExists("meta$containers", {"name":"varchar not null", "params":"varchar not null"}, table_type = 'meta') # контейнеры это шаблоны для xml выгрузки
        self.sq_connection.createTableIfNotExists("meta$xml_getters", {"name":"varchar not null", "source":"int not null"}, table_type = 'meta', constraints = "foreign key (source) references meta$tables(meta$id) on delete cascade") # геттеры это обекты которые из таблицы получают xml дом
        self.sq_connection.createTableIfNotExists("meta$getter_container", {"getter":"int not null", "container":"int not null"}, table_type = 'meta', constraints = "foreign key (getter) references meta$xml_getters(meta$id) on delete cascade, foreign key (container) references meta$containers(meta$id) on delete cascade") # сдесь хранятся связи для передачи сформированного xml дома в контейнер, путь по которому будет сложен xml дом должен знать сам контейнер
        

    def __del__(self):
        self.sq_connection.close()
        
    def addForProcessing(self, file_name, table_name):
        """add file_name to database for exporting to table_name"""
        if not os.path.exists(file_name):
            raise Exception("file {0} does not exists".format(file_name))
        with open(file_name, 'ro') as file:
            file_crc32 = binascii.crc32(file.read())
        file_found = False
        for exfile in self.sq_connection.executeAdv("select * from meta$processed_files where full_path = ?", (os.path.realpath(file_name),)):
            if exfile['meta$crc32'] != file_crc32 or exfile['meta$table_name'] != table_name:
                raise Exception(u"""there is already exis one record for file {ff}
                existing records {rec1}
                records for insert name:{name}, table:{table}, crc32:{crc}""".format(ff = file_name,
                                                                                     rec1 = exfile,
                                                                                     name = file_name,
                                                                                     table = table_name,
                                                                                     crc = file_crc32))
            file_found = True

        if not file_found:
            self.sq_connection.insertInto("meta$processed_files", {"full_path":os.path.realpath(file_name),
                                                                   "processed":0,
                                                                   "meta$table_name":table_name,
                                                                   "meta$crc32":file_crc32})
            self.sq_connection.commit()
            core.logger(u"file {0} inserted for processing in table {1}".format(file_name, table_name))
            
        return self

            
            
    def loadTable(self, table_name):
        self._createTableToLoad(table_name)
        self._attachFreeFiles(table_name)
        self._loadFilesToTable(table_name)
        return self

    def _loadFilesToTable(self, table_name):
        for filedict in self.sq_connection.executeAdv("select f.full_path, f.meta$id from meta$processed_files f inner join meta$tables t on f.meta$table_id = t.meta$id where t.name = ? and f.processed = 0", (table_name,)):
            self._loadFileToTable(filedict, table_name)
        return self

    def _loadFileToTable(self, filedict, table_name):
        try:
            rid = self.sq_connection.getMaxField(table_name) + 1
            with ydbf.open(filedict["full_path"], encoding = self.encoding) as dbfcon:
                for record in dbfcon:
                    record["meta$id"] = rid
                    rid += 1
                    record["meta$file_id"] = filedict["meta$id"]
                    self.sq_connection.insertInto(table_name, record)
        except:
            self.sq_connection.rollback()
            raise
        else:
            self.sq_connection.commit()
            core.logger("file {0} loaded in table {1}".format(filedict["full_path"], table_name))
        return self
                
    def _attachFreeFiles(self, table_name):
        table_id = self.sq_connection.execute("select meta$id from meta$tables where name = ?", (table_name,)).fetchall()[0][0]
        self.sq_connection.execute(u"update meta$processed_files set meta$table_id = {tid}, meta$table_name = null where processed = 0 and meta$table_name = ?".format(tid = table_id), (table_name,))
        self.sq_connection.commit()
        return self

    def _createTableToLoad(self, table_name):
        if self.sq_connection.execute(u"select * from meta$tables where name = ?", (table_name,)).fetchone() == None:
            self.sq_connection.createTableIfNotExists(table_name,
                                                      self._scanFilesAndGetFieldsForTable(table_name),
                                                      table_type = 'data',
                                                      meta_fields = {"meta$id":"int primary key not null",
                                                                     "meta$file_id":"int not null"},
                                                      constraints = "foreign key (meta$file_id) references meta$processed_files(meta$id) on delete cascade")
            core.logger(u"table {0} is the table for loading source data".format(table_name))
        return self

    def _scanFilesAndGetFieldsForTable(self, table_name):
        """returns collected dict of fields {'name':'type'} for all files attached to 'table_name'"""
        collected = {}
        for file_rec in self.sq_connection.executeAdv(u"select full_path from meta$processed_files where processed = 0 and meta$table_id is null and meta$table_name = ?", (table_name,)):
            collected = self._mergeDicts(collected, self._dbfFields(file_rec['full_path']))
        return collected
            
    def _mergeDicts(self, first, second):
        ret = copy.copy(first)
        for key in second:
            if first.has_key(key) and first[key] != second[key]:
                raise Exception(u"two fields has different types a[{key}] = {val1}, b[{key}] = {val2}".format(key = key,
                                                                                                              val1 = first[key],
                                                                                                              val2 = second[key]))
            ret[key] = second[key]
        return ret
    
    def _dbfFields(self, filename):
        ret = {}
        with ydbf.open(filename, encoding = self.encoding) as dbcon:
            for field in dbcon.fields:
                ret[field[0]] = self._mapDbfTypes(field[1:])
        core.logger("file {0} scanned".format(filename))
        return ret

    def _mapDbfTypes(self, a):
        if a[0] == 'C':
            return "text"
        elif a[0] == 'D':
            return "date"
        elif a[0] == 'N' and a[2] != 0:
            return "decimal"
        elif a[0] == 'N' and a[2] == 0:
            return "integer"
        elif a == 'L':
            return "bool"
        else:
            raise Exception("can not create field with type {0}".format(a))
        
        
                                                      
            
            
            

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
        
        