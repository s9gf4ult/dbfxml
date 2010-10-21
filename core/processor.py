# -*- coding: utf-8 -*-
import os
import re
import binascii
import copy
import ydbf
from common_helpers import *

import core
import core.sqlite_connection
    
class mainProcessor:
    """importer from dbf to sqlite3"""
    def __init__(self, sqlite_file, encoding = 'cp866'):

        self.encoding = encoding
        self.sq_connection = core.sqlite_connection.sqliteConnection(sqlite_file)
        self.sq_connection.createTableIfNotExists("meta$sources", {"source":"varchar not null",
                                                                   "arg":"varchar",
                                                                   "table_name":"varchar not null",
                                                                   "type":"varchar not null"},
                                                  constraints = "unique(source, arg, table_name)",
                                                  table_type = "meta")
                                                                   
        self.sq_connection.createTableIfNotExists("meta$processed_files", {"full_path":"varchar not null", "processed":"integer not null"}, meta_fields = {"meta$id":"int primary key not null","meta$table_name":"varchar", "meta$table_id":"int", "meta$crc32":"int not null"}, constraints = "foreign key (meta$table_id) references meta$tables(meta$id) on delete cascade, unique(full_path,meta$table_id), unique(meta$crc32)", table_type = 'meta') # исходные файлы для работы
        self.sq_connection.createTableIfNotExists("meta$containers", {"name":"varchar not null",
                                                                      "output" : "varchar not null"},
                                                  constraints = "unique(output)",
                                                  table_type = 'meta') # контейнеры это шаблоны для xml выгрузки

        self.sq_connection.createTableIfNotExists("meta$container_records", {"table_id" : "int not null",
                                                                             "record_id" : "int not null",
                                                                             "container_id" : "int not null"}
                                                  table_type = 'meta',
                                                  constraints = "foreign key (table_id) references meta$tables(meta$id) on delete cascade, foreign key (container_id) references meta$containers(meta$id) on delete cascade")
                                                                             

    def processAllContainers(self):
        for cont in self.sq_connection.executeAdv("select * from meta$containers"):
            self.processContainer(cont["meta$id"])
        return self

    # def processContainer(cont):
    #     contname = self.sq_connection.execute("select name from meta$containers where meta$id = ?", (cont,)).fetchall()[0][0]
    #     continstance = eval("containers.{0}()".format(contname))
    #     continstance
        
        
    def addContainer(output, name, table_records):
        try:
            cid = self.sq_connection.getMaxField("meta$containers") + 1
            self.sq_connection.insertInto("meta$containers", {"meta$id" : cid,
                                                              "name" : name,
                                                              "output" : output})
            for key in table_records:
                tid = self.sq_connection.execute("select meta$id from meta$tables where name = ?", (key,)).fetchall()[0][0]
                for rec in table_records[key]:
                    self.sq_connection.insertInto("meta$container_records", {"table_id" : tid,
                                                                             "record_id" : rec,
                                                                             "container_id" : cid})
        except:
            self.sq_connection.rollback()
            raise
        else:
            self.sq_connection.commit()
        return self

    def cleanFilesTable(self):
        try:
            for filedict in self.sq_connection.executeAdv("select meta$id, full_path, meta$crc32 from meta$processed_files"):
                if  os.path.exists(filedict["full_path"]):
                    with open(filedict["full_path"], 'ro') as filehnd:
                        if filedict["meta$crc32"] == binascii.crc32(filehnd.read()):
                            continue
                self.sq_connection.execute("delete from meta$processed_files where meta$id = ?", (filedict["meta$id"],))
            for tablename in self.sq_connection.executeAdv("select t.name from meta$tables t where not exists(select f.* from meta$processed_files f where f.meta$table_name = t.name) and not exists(select f.* from meta$processed_files f where f.meta$table_id = t.meta$id)"):
                self.sq_connection.dropTable(tablename["name"])
                
        except:
            self.sq_connection.rollback()
            raise
        else:
            self.sq_connection.commit()
        return self
        
    def __del__(self):
        self.sq_connection.close()
        
    def addForProcessing(self, file_name, table_name):
        """add file_name to database for exporting to table_name"""
        if not os.path.exists(file_name):
            raise Exception("file {0} does not exists".format(file_name))
        with open(file_name, 'ro') as file:
            file_crc32 = binascii.crc32(file.read())
        file_found = False
        # файлы с одинаковой контрольной суммой не загружаем
        for exfile in self.sq_connection.executeAdv("select * from meta$processed_files where meta$crc32 = ?", (file_crc32,)):
            return self
        # если есть файл с тем же именем но другими атрибутами выбрасываем ошибку
        for exfile in self.sq_connection.executeAdv("select * from meta$processed_files where full_path = ?", (os.path.realpath(file_name),)):
            if exfile["processed"] == 0 and (exfile['meta$crc32'] != file_crc32 or exfile['meta$table_name'] != table_name):
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

    def addSourceDirectory(self, tablename, dirname, filewild):
        rec = {"table_name" : tablename,
               "source" : dirname,
               "arg" : filewild,
               "type" : "directory"}
        self.sq_connection.insertInto("meta$sources", rec)
        return self

    def addSourceFile(self, tablename, filename):
        rec = {"table_name" : tablename,
               "source" : filename,
               "type" : "file"}
        self.sq_connection.insertInto("meta$sources", rec)
        return self

    def rescanSources(self):
        for source in self.sq_connection.executeAdv("select * from meta$sources"):
            self.rescanSource(source)
        return self

    def rescanSource(self, source):
        if source["type"] == 'file':
            self.addForProcessing(source["source"], source["table_name"])
        elif source["type"] == 'directory':
            self.addForProcessingByPattern(source["table_name"], source["source"], source["arg"])
        else:
            raise Exception("source type {0} is not supported yet".format(source["type"]))
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
            self.sq_connection.execute("update meta$processed_files set processed = 1 where meta$id = ?", (filedict["meta$id"],))
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
        
        
    def getUnloadedTables(self):
        return set( map(lambda a: a[0], self.sq_connection.execute("select distinct meta$table_name from meta$processed_files where processed = 0 and meta$table_name is not null and meta$table_id is null").fetchall()) + map(lambda a:a[0], self.sq_connection.execute("select t.name from meta$tables t inner join meta$processed_files f on f.meta$table_id = t.meta$id where f.processed = 0 and f.meta$table_name is null").fetchall()))
    
    def loadAllTables(self):
        for table in self.getUnloadedTables():
            self.loadTable(table)

    def addForProcessingByPattern(self, table_name, directory, pattern = '.*' , flags = 0):
        files = findFilesByRegexp(directory, pattern, flags)
        if files != []:
            for file in files:
                self.addForProcessing(file, table_name)
        
        
