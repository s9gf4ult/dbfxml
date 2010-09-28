# -*- coding: utf-8 -*-

from lxml import etree
from common_helpers import *
import sql_helpers
import transformer

def makeXmlmk(trans):
    ret = xmlmk()
    ret._local_init(trans)
    ret.fetchTable()
    return ret
        

class xmlmk(etree.ElementBase):
    def _local_init(self, trans):
        declare_type(transformer.transformer, trans)
        self.sq_connection = trans.sq_connection
        self.table = trans.dst

    def fetchTable(self):
        if not sql_helpers.isTableAre(self.sq_connection, self.table):
            raise Exception("{0}.{1}: table {2} does not exists in the database".format(self.__class__, __name__, self.table))
        self.tag = self.table
        for rec in sql_helpers.selectHashIterator(self.sq_connection.execute('select * from {0}'.format(self.table))):
            record = etree.Element('record')
            for field in rec:
                if field.lower() != 'id' and field.lower() != 'ref_id' and isinstance(rec[field], basestring):
                    etree.SubElement(record, field).text = rec[field]
            self.append(record)
