# -*- coding: utf-8 -*-

import sqlite3
from common_helpers import *

class sqliteConnection(sqlite3.Connection):
    def __init__(self, initstr):
        sqlite3.Connection.__init__(self, initstr)
        self.execute('pragma foreign_keys = on')
        self.checkMetadata()

    def checkMetadata(self):
        """create or check (if created) tables 'meta$tables' and 'meta$fields' for using in future"""
        
        self.execute("create table if not exists meta$tables (meta$id int primary key not null, name varchar not null, type varchar not null, comment varchar, unique(name))")
        self.execute("create table if not exists meta$fields (meta$id int primary key not null, meta$table_id int not null, name varchar not null, type varchar not null, comment varchar, unique(meta$table_id, name), foreign key (meta$table_id) references meta$tables(meta$id) on delete cascade)")
        # заносим в метатаблицы информацию о метатаблицах
        if self.execute("select * from meta$tables where name = ?", ("meta$table",)).fetchone() == None:
            self.execute("insert into meta$tables(meta$id, name, type, comment) values  (?,?,?,?)", (self.getMaxField("meta$tables", "meta$id") + 1, "meta$tables", "meta", "meta table with information about tables"))
        if self.execute("select * from meta$tables where name = ?", ("meta$fields",)).fetchone() == None:
            self.execute("insert into meta$tables(meta$id, name, type, comment) values (?,?,?,?)", (self.getMaxField("meta$tables", "meta$id") + 1, "meta$fields", "meta", "meta table with information about fields of tables"))
        # заносим в метататаблицы информацию о полях метатблиц
        table_id =  self.execute("select meta$id from meta$tables where name = ?", ("meta$tables",)).fetchone()[0]
        field_id = self.getMaxField("meta$fields") + 1
        for ff in [("meta$id", "meta", "id of the table"),
                   ("name", "data", "table name"),
                   ("type", "data", "can be 'data' or 'meta'"),
                   ("comment", "data", "no comment")]:
            if self.execute("select * from meta$fields where name = ? and meta$table_id = ?", (ff[0],table_id)).fetchone() == None:
                self.execute("insert into meta$fields (meta$id, meta$table_id, name, type, comment) values (?,?,?,?,?)", (field_id, table_id, ff[0], ff[1], ff[2]))
                field_id += 1

        table_id = self.execute("select meta$id from meta$tables where name = ?", ("meta$fields",)).fetchone()[0]
        field_id = self.getMaxField("meta$fields") + 1
        for ff in [("meta$id", "meta", "id of the field"),
                   ("meta$table_id", "meta", "reference to meta$tables(meta$id)"),
                   ("name", "data", "name of the field"),
                   ("type", "data", "can be 'meta' or 'data'"),
                   ("comment", "data", "comment is comment")]:
            if self.execute("select * from meta$fields where name = ? and meta$table_id = ?", (ff[0], table_id)).fetchone() == None:
                self.execute("insert into meta$fields (meta$id, meta$table_id, name, type, comment) values (?,?,?,?,?)", (field_id, table_id, ff[0], ff[1], ff[2]))
                field_id += 1
                
        #проверяем есть ли в таблице meta$tables записи о не существеющих таблицах
        if  self.execute("select t1.* from meta$tables t1 where not exists (select t2.* from sqlite_master t2 where t2.type = 'table' and t2.name = t1.name)").fetchall() != []:
            raise Exception('there is some tables in "meta$tables" which does not exists')
        #проверяем все ли поля в таблице meta$fields действительно есть в таблицах базы
        for table_ in self.execute("select t.meta$id, t.name from meta$tables t"):
            fields = map(lambda a:a[0], self.execute(u'select t.name from meta$fields t where t.meta$table_id = ?', (table_[0],)).fetchall())
            try:
                self.execute(u"select {0} from {1}".format(join_list(fields, ', '), table_[1]))
            except:
                raise Exception(u"can not select from '{0}' fields \n{1}".format(table_[1], join_list(fields, "\n")))
        return self
    
    def createTableIfNotExists(self, name, fields, meta_fields = {'meta$id':'primary key not null'}, constraints = None, table_type = 'data'):
        """(name, fields, meta_fields = {'meta$id':'primary key not null'}, constraints = None, table_type = 'data')
        create table with name 'name' and register it in 'meta$fields' and 'meta$tables' if it does not exists"""
        
        self.execute(u"create table if not exists {0} ({1})".format(name,
                                                                    self._format_table_creator(fields, meta_fields, constraints)))
        self.registerTable(name, table_type, fields, meta_fields)
        return self

    def createTable(self, name, fields, meta_fields = {'meta$id':'primary key not null'}, constraints = None, table_type = 'data'):
        """same as createTableIfNotExists but will raise exception if table 'name' already exists"""
        
        self. execute(u"create table {0} ({1})".format(name,
                                                       self._format_table_creator(fields, meta_fields, constraints)))
        self.registerTable(name, table_type, fields, meta_fields)
        return self

    def _format_table_creator(self, fields, meta_fields, constraints):
        ret = meta_fields != {} and join_list(map(lambda a:u"{0} {1}".format(a, meta_fields[a]), meta_fields), ", ") or ""
        ret += fields != {} and ", " + join_list(map(lambda a:u"{0} {1}".format(a, fields[a]), fields), ", ") or ""
        ret += constraints and ", " + constraints or ""
        return ret
        
    def registerTable(self, table_name, table_type, fields, meta_fields):
        """(table_name, table_type, fields, meta_fields) register table in meta tables but do not actually create it"""
        
        if self.execute(u"select * from meta$tables where name = ?", (table_name,)).fetchone() != None:
            self.execute(u"delete from meta$tables where name = ?", (table_name,)) # поля удалятся автоматически каскадным удалением по констрейну
        table_id = self.getMaxField('meta$tables', 'meta$id') + 1
        field_id = self.getMaxField('meta$fields', 'meta$id') + 1
        self.execute('insert into meta$tables(meta$id, name, type) values (?,?,?)', (table_id, table_name, table_type))
        for field in fields:
            self.execute('insert into meta$fields(meta$id, meta$table_id, name, type) values (?,?,?,?)', (field_id, table_id, field, 'data'))
            field_id += 1
        for field in meta_fields:
            self.execute('insert into meta$fields(meta$id, meta$table_id, name, type) values (?,?,?,?)', (field_id, table_id, field, 'meta'))
            field_id += 1
        return self

    def getMaxField(self, table_name, field_name = "meta$id"):
        """(table_name, field_name = 'meta$id') usually used to get max value of 'meta$id' to insert new record"""
        rtfd = self.execute(u'select max({0}) from {1}'.format(field_name, table_name)).fetchone()
        if rtfd == (None,):
            return 0
        return rtfd[0]
        
    def insertInto(self, table_name, values):
        """(table_name, values)insert values into table_name where values is dictionary {key: value}"""
        keys = []
        for key in values:
            if values[key]:
                keys.append(key)        # соберем ключи для не пустых значений
        if 'meta$id' not in keys:
            keys.append('meta$id')
            values['meta$id'] = self.getMaxField(table_name) + 1 # в том случае если нам не дали meta$id

        query = u"insert into {name}({keys}) values ({values})".format(
            name = table_name,
            keys = join_list(keys),
            values = join_list(map(lambda a:"?" , keys))) # на самом деле тут мы возвращаем много много вопросиков разделенных запятыми
        try:
            self.execute(query, map(lambda a: values[a], keys)) # а вот сдесь собсно значения
        except:
            raise Exception("""It seems that one of values gived in hash has unsupported type
            it probably has no adapter in sqlite.
            Here is all keys that gived into {0}:\n
            {1}""".format(table_name, reduce(lambda a, b:"{0}\n{1}".format(a,b), map(lambda a:"key:{0}, value:{1}, class{2}".format(a, values[a], values[a].__class__), keys))))
        return self

    def executeAdv(self, query, values=()):
        return selectHashIterator(self.execute(query, values))

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
    
    
        
