# -*- coding: utf-8 -*-

from core import *
from lxml import etree

class kladr_root:
    def __init__(self, filename):
        self.records = kuploader(filename)
        self.records.attach_roots()
        self.element = etree.Element(u'УниверсальныйСправочник')
        self.make_template()
        self.element.append(self.records.element)

    def make_template(self):
        self.element.attrib[u'Группа'] = u'БюджетнаяОтчетность'
        self.element.attrib[u'Код'] = u'Кладр'
        self.element.attrib[u'Наименование'] = u'Классификатор Адресов России'
        self.element.attrib[u'Иерархический'] = u'Да'
        self.element.attrib[u'РежимВыбораЗаписей'] = u'НеГрупповые'
        etree.SubElement(self.element, u'Описание').text = ''
        per = etree.Element(u'ПериодДействия')
        per.attrib[u'Начало'] = ''
        per.attrib[u'Конец'] = '31.12.9999 0:00:00'
        self.element.append(per)
        self.element.append(etree.Element(u'ПривязкаУчреждений'))
        atts = self.make_attrs()
        self.element.append(atts)

    def make_attrs(self):
        atts = etree.Element(u'Атрибуты')
        at = []
        for data in [(u'Атрибут1', '1', u'ТипОбъекта', u'Тип Объекта'),
                     (u'Атрибут2', '2', u'Окато', u'Номер окато'),
                     (u'Атрибут3', '3', u'ПочтовыйИндекс', u'Почтовый индекс')]:
            at.append(etree.Element(data[0]))
            at[-1].attrib[u'Номер'] = data[1]
            at[-1].attrib[u'Код'] = data[2]
            at[-1].attrib[u'Наименование'] = data[3]
            at[-1].attrib[u'ТипЗначения'] = u'Строка'

        for atr in at:
            atts.append(atr)

        return atts

    def store(self, filename):
        with open(filename, 'w') as file:
            file.write(etree.tostring(self.element, pretty_print = True, encoding = 'UTF-8', xml_declaration = True))
        

class kuploader:
    def __init__(self, filename):
        self.connection = sqlite_connection.sqliteConnection(filename)
        self.connection.execute("pragma foreign_keys=on")
        self.connection.execute("pragma auto_vacuum=0")
        self.element = etree.Element(u'Записи')

    def attach_roots(self):
        for root in self.connection.executeAdv("select k.*, t.name as tname from kladr_objects k inner join kladr_types t on k.type_id = t.meta$id where k.meta$parent_id is null and k.region_code = 54 order by tname, k.name"):
            el = etree.Element(u'Запись')
            el.attrib[u'Код'] = self.coderestore(root)
            el.attrib[u'Наименование'] = root['name']
            el.attrib[u'Атрибут1'] = root['tname']
            el.attrib[u'Атрибут2'] = root['ocato']
            el.attrib[u'Атрибут3'] = root['post_index']
            self.attach_childrens(el, root['meta$id'])
            self.element.append(el)

    def attach_childrens(self, element, parentid):
        for child in self.connection.executeAdv("select k.*, t.name as tname from kladr_objects k inner join kladr_types t on k.type_id = t.meta$id where k.meta$parent_id = ? and k.region_code = 54 and street_code is null order by tname, k.name", (parentid,)):
            el = etree.Element(u'Запись')
            el.attrib[u'Код'] = self.coderestore(child)
            el.attrib[u'Наименование'] = child['name']
            el.attrib[u'Атрибут1'] = child['tname']
            el.attrib[u'Атрибут2'] = child['ocato']
            el.attrib[u'Атрибут3'] = child['post_index']
            self.attach_childrens(el, child['meta$id'])
            element.append(el)
        
    def coderestore(self, element):
        if element['street_code']:
            return "{0:02}{1:03}{2:03}{3:03}{4:04}{5:02}".format(element['region_code'],
                                                                 element['district_code'],
                                                                 element['city_code'],
                                                                 element['locality_code'],
                                                                 element['street_code'],
                                                                 element['actuality_code'])
        else:
            return "{0:02}{1:03}{2:03}{3:03}{4:02}".format(element['region_code'],
                                                           element['district_code'],
                                                           element['city_code'],
                                                           element['locality_code'],
                                                           element['actuality_code'])
        
                                                                 
            
        
