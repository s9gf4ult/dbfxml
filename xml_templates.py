# -*- coding: utf-8 -*-

from lxml import etree
import os
import xml_table
from common_helpers import *
import transformer
import time
import datetime
import uuid

class xmltemplate:
    table_root = None
    
    def __init__(self, filename):
        if not os.path.exists(filename):
            raise Exception("file {0} does not exists".format(filename))
        self.xml = etree.parse(filename).getroot()
        self.sources = []

    def store(self, filename, encoding = 'utf-8'):
        with open(filename, 'w') as fout:
            fout.write(etree.tostring(self.xml, pretty_print = True, encoding = encoding, xml_declaration = True))
        
        
class rcpcontainer(xmltemplate):
    def __init__(self, filename):
        xmltemplate.__init__(self, filename)
        tt = time.localtime(time.time())
        self.xml.find('CREATE_TIME').text = datetime.datetime(tt.tm_year, tt.tm_mon, tt.tm_mday, tt.tm_hour, tt.tm_min, tt.tm_sec).isoformat()
        self.xml.find('SENDINFO').find('SEND_GUID').text = "{{{0}}}".format(uuid.uuid1().__str__().upper())
        for el in xrange(0, self.xml.find('DATAMAIN').__len__()):
            del self.xml.find('DATAMAIN')[-1] # по удаляем все из дерева чигабуга
        
    def insertXmlTable(self, xmltable):
        declare_type(xml_table.xmlmk, xmltable)
        self.xml.find('DATAMAIN').append(xmltable)
        
        
