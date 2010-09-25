# -*- coding: utf-8 -*-

from common_helpers import *
import sql_helpers
import importer
import datetime

class transformer:
    """common interface for conversion tables"""

    necessary_fields = None
    
    def convertHash(self, hashtable):
        pass
    
    def __init__(self, imp, src, dst):
        declare_type(importer.importer, imp)
        self.sq_connection = imp.getSqliteConnection()
        self.src = src
        self.dst = dst

    def createDstTable(self):
        fields = self.convertHash(sql_helpers.selectHashIterator(self.sq_connection.execute("select * from {0}".format(self.src))).next())
        for key in fields:
            fields[key] = "varchar"
        fields["ref_id"] = "integer"
            
        sql_helpers.makeTableIfNotExists(self.sq_connection, self.dst, fields, ["foreign key (ref_id) references {0}(id) on delete cascade".format(self.src), "unique(ref_id)"])
    
    def convertTable(self):
        self._checkForNecessaryFields()
        if sql_helpers.isTableAre(self.sq_connection, self.dst):
            dst_id = sql_helpers.getIdForTable(self.sq_connection, self.dst)
        else:
            dst_id = 1
            self.createDstTable()
            
        try:
            for rec in sql_helpers.selectHashIterator(self.sq_connection.execute("select * from {0}".format(self.src))):
                try:
                    sql_helpers.insertInto(self.sq_connection, self.dst, self._processAndAddIDReferences(dst_id, rec))
                except:
                    self.sq_connection.execute("delete from {0} where id = ?".format(self.dst), dst_id)
                    sql_helpers.insertInto(self.sq_connection, self.dst, self._processAndAddIDReferences(dst_id, rec))
                    
                dst_id += 1
        except:
            self.sq_connection.rollback()
            raise
        else:
            self.sq_connection.commit()

    def _processAndAddIDReferences(self, dst_id, rec):
        drec = self.convertHash(rec)
        drec["id"] = dst_id
        drec["ref_id"] = rec["id"]
        return drec

    def _checkForNecessaryFields(self):
        if self.necessary_fields and self.necessary_fields.__len__() > 0:
            fields = map(lambda a: a[0].lower() , self.sq_connection.execute("select * from {0}".format(self.src)).description)
            for tf in self.necessary_fields:
                if not tf.lower() in fields:
                    raise Exception("table {0}, has no field {1}".format(self.src, tf))

