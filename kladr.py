# -*- coding: utf-8 -*-

from core import *
from lxml import etree

class kuploader:
    def __init__(self, filename):
        self.connection = sqlite_connection.sqliteConnection(filename)
        self.connection.execute("pragma foreign_keys=on")
        self.connection.execute("pragma auto_vacuum=0")
        self.element = etree.Element(u'Записи')

    def attach_roots(self):
        for root in self.connection.executeAdv("select k.*, t.name as tname from kladr_objects k inner join kladr_types t on k.type_id = t.meta$id where k.meta$parent_id is null"):
            el = etree.Element(u'Запись')
            el.attrib[u'Код'] = root['meta$id'].__str__()
            el.attrib[u'Наименование'] = root['name']
            el.attrib[u'Атрибут1'] = root['tname']
            el.attrib[u'Атрибут2'] = root['ocato']
            el.attrib[u'Атрибут3'] = root['post_index']
            self.attach_childrens(el, root['meta$id'])
            self.element.append(el)

    def attach_childrens(self, element, parentid):
        for child in self.connection.executeAdv("select k.*, t.name as tname from kladr_objects k inner join kladr_types t on k.type_id = t.meta$id where k.meta$parent_id = ?", (parentid,)):
            el = etree.Element(u'Запись')
            el.attrib[u'Код'] = child['meta$id'].__str__()
            el.attrib[u'Наименование'] = child['name']
            el.attrib[u'Атрибут1'] = child['tname']
            el.attrib[u'Атрибут2'] = child['ocato']
            el.attrib[u'Атрибут3'] = child['post_index']
            self.attach_childrens(el, child['meta$id'])
            element.append(el)
        
            
            
        
