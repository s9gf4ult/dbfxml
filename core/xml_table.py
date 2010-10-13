# -*- coding: utf-8 -*-

from lxml import etree
from common_helpers import *
import sql_helpers
import transformer

def mapTransformerToContainer(trans):
    classes = {transformer.perstransform:personsxml,
               transformer.rcptransformer:prescxml
               }
    return classes[trans]
    

def makeXmlmk(trans, classtype = None):
    if classtype == None:
        classtype = mapTransformerToContainer(trans.__class__)
    ret = classtype()
    ret._local_init(trans)
    ret.fetchTable()
    return ret

class xmlIterator:
    def __init__(self, trans, classtype = None, max_count = 100000):
        if classtype == None:
            classtype = mapTransformerToContainer(trans.__class__)
        declare_type(transformer.transformer, trans)
        self.transformer = trans
        self.max_count = max_count
        self.xmlmk_class = classtype

    def __iter__(self):
        self.cursor = self.transformer.sq_connection.execute("select * from {0}".format(self.transformer.dst))
        return self

    def next(self):
        ret = self.xmlmk_class()
        ret._local_init(self.transformer)
        iterator = sql_helpers.selectHashIterator(self.cursor).__iter__()
        got = 0
        while True:
               record = iterator.next()
               ret.insertRecord(record)
               got += 1
               if got >= self.max_count:
                   break
        return ret
                   
        
class xmlmk(etree.ElementBase):
    def _local_init(self, trans):
        declare_type(transformer.transformer, trans)
        self.sq_connection = trans.sq_connection
        self.table = trans.dst
        
    def fetchTable(self):
        if not sql_helpers.isTableAre(self.sq_connection, self.table):
            raise Exception("{0}.{1}: table {2} does not exists in the database".format(self.__class__.__name__, __name__, self.table))
        self.tag = self.table
        for rec in sql_helpers.selectHashIterator(self.sq_connection.execute('select * from {0}'.format(self.table))):
            self.insertRecord(rec)
            
    def insertRecord(self, rec, name = 'record'):
        record = etree.Element(name)
        for field in rec:
            if field.lower() != 'id' and field.lower() != 'ref_id' and isinstance(rec[field], basestring):
                etree.SubElement(record, field).text = rec[field]
                self.append(record)
        return record
            
class personsxml(xmlmk):
    def insertRecord(self, rec, name = 'PERSONDLO'):
        rec = xmlmk.insertRecord(self, rec, name = name)
        rec.attrib['op'] = 'I'

class prescxml(xmlmk):
    def insertRecord(self, rec, name = 'PHARMACYRECIPE'):
        xmlmk.insertRecord(self, rec, name = name)
    
