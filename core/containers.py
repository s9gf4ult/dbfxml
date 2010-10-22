# -*- coding: utf-8 -*-
from lxml import etree
import datetime
import time
from uuid import uuid1


class mainContainer:
    def __init__(self, out, encoding = 'utf-8' ):
        self.out = out
        self.encoding = encoding
        self.element = etree.Element('container')

    def prepare(self, paramsDict):
        self.generateTemplate()
        self.attach_parameters(paramsDict)
        return self

    def store(self):
        with open(self.out, 'w') as fout:
            fout.write(etree.tostring(self.element, pretty_print = True, encoding = self.encoding, xml_declaration = True))
        return self

    def generateTemplate(self):pass
    def attach_parameters(self, params):pass


class dloContainer(mainContainer):
    guid = '{3FB7DEAC-DDF5-11DF-8307-00145E64D9DF}'
    def generateTemplate(self):
        self.element.tag = 'MAIN'
        for fixed in [('FORMAT_GUID', '{E619D0D5-7430-4840-9E35-C15BC1EF0E3D}'),
                      ('PROTOCOL', 'PHARMACY_REESTR'),
                      ('VER', '3.0'),]:
            etree.SubElement(self.element, fixed[0]).text = fixed[1]
        tt = time.localtime(time.time())
        etree.SubElement(self.element, 'CREATE_TIME').text = datetime.datetime(tt.tm_year, tt.tm_mon, tt.tm_mday, tt.tm_hour, tt.tm_min, tt.tm_sec).isoformat()
        
        snd = etree.Element('SENDINFO')
        etree.SubElement(snd, 'HOST_GUID').text = self.guid
        etree.SubElement(snd, 'SEND_GUID').text = "{{{0}}}".format(uuid1().__str__().upper())
        self.element.append(snd)

        etree.SubElement(self.element, 'DATAMAIN').append(etree.Element('DOCUMENTS'))
        return self
        
    def attach_parameters(self, params):
        dcs = self.element.find('DATAMAIN').find('DOCUMENTS')
        dcs.append(self.getPersondlo(params["persondlo"]))
        dcs.append(self.getPrescriptions(params["prescriptions"]))
        self.generateChsm()
        return self
    
    def getPersondlo(self, iterator):
        pers = xmlPersondlo('PERSONDLO_DOC', iterator)
        pers.iterate()
        return pers.element

    def getPrescriptions(self, iterator):
        presc = xmlPrescriptions('PHARMACYRECIPE_DOC', iterator)
        presc.iterate()
        return pers.element
    

class xmlGetter:
    def __init__(self, name, iterator):
        self.element = etree.Element(name)
        self.iterator = iterator

    def iterate(self):
        for elt in self.iterator:
            xmlelt = self.elementclass(elt)
            self.element.append(xmlelt.element)

    elementclass = None
            
class xmlPersondlo(xmlGetter):
    def __init__(self):
        xmlGetter.__init__(self)
        self.elementclass = xmlElementPersondlo

class xmlPrescriptions(xmlGetter):
    def __init__(self):
        xmlGetter.__init__(self)
        self.elementclass = xmlElementPrescriptions

class xmlElement:
    def __init__(self, eltdict):
        self.generateElement()
        self.generateAttribs()
        self.attachParams(eltdict)

    def generateElement(self):pass
    def generateAttribs(self):pass
    def attachParams(self, eltdict):pass

class xmlElementPrescriptions(xmlElement):
    def generateElement(self):
        self.element = etree.Element('PHARMACYRECIPE')
    def generateAttribs(self):
        self.element.attrib['op'] = 'I'


class xmlElementPersondlo(xmlElement):
    def generateElement(self):
        self.element = etree.Element('PERSONDLO')
    def generateAttribs(self):
        self.element = attrib['op'] = 'I'
        













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
    
# -*- coding: utf-8 -*-


class xmltemplate:
    table_root = None
    
    def __init__(self, filename):
        if not os.path.exists(filename):
            raise Exception("file {0} does not exists".format(filename))
        self.xml = etree.parse(filename).getroot()
        self.sources = []

    def insertXmlTable(self, xmltable):
        declare_type(xml_table.xmlmk, xmltable)
    

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
        xmltemplate.insertXmlTable(self, xmltable)
        self.xml.find('DATAMAIN').append(xmltable)
        
        
# -*- coding: utf-8 -*-


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
                    self.sq_connection.execute("delete from {0} where ref_id = ?".format(self.dst), (rec["id"],))
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

def datetimeIsoFormat(ddt):
    if ddt.__class__ == datetime.date:
        return datetime.datetime(ddt.year, ddt.month, ddt.day).isoformat()
    elif ddt.__class__ == datetime.datetime:
        return ddt.isoformat()
    elif ddt.__class__ == str or ddt.__class__ == unicode:
        return datetime.datetime(*map(int, re.split("[-T:\.\/\\]+", ddt))).isoformat()
    elif ddt == None:
        return None
    else:
        raise Exception("Date in some strange format {0}".format(ddt.__class__))


def setDefault(deflt, val = None):
    if val.__class__ == str or val.__class__ == unicode:
        return (not re.match('^\ *$', val)) and val or deflt
    return val and val.__str__() or deflt

def splitWords(string):
    return string and filter(lambda a: a != '', re.split('\ +', string.__str__()))

class rcptransformer(transformer):
    def convertHash(self, hinst):
        ret = {}
        hin = {}
        for key in hinst:
            hin[key.upper()] = hinst[key]
        for key in ['C_OGRN', 'MCOD', 'SS', 'DS', 'C_FINL', 'KO_ALL', 'P_KEK', 'C_KAT', 'C_KATL', 'C_PFS', 'NOMK_LS', 'D_TYPE', 'DOZ_ME', 'SL_ALL', 'TYPE_SCHET', 'FO_OGRN']:
            ret[key] = hin.has_key(key) and  hin[key] and hin[key].__str__()
                
        for key in ['C_KAT', 'C_PFS', 'DOZ_ME', 'SL_ALL', 'TYPE_SCHET']:
            ret[key] = setDefault('0', ret[key])
        ret['DATE_VR'] = datetimeIsoFormat(hin['DATE_VR'])
        pcod = hin['PCOD'].__str__()
        ret['V_C_OGRN'] = pcod[0:13]
        ret['PCOD'] = pcod[13:pcod.__len__()]
        snlr = splitWords(hin['SN_LR'])
        ret['S_LR'] = snlr and snlr.__len__() >= 1 and snlr[0]
        ret['N_LR'] = snlr and snlr.__len__() >= 1 and snlr[-1]
        #PR_LR action range !
        prlr = int(hin['PR_LR'])
        ret['PR_LR'] = (0 <= prlr <= 50) and '2' or '1' # доподлинно неизвестно так ли это 
        ret['DATE_OTP'] = datetimeIsoFormat(hin['DATE_OTP'])
        ret['Delayed_Service'] = '0'
        acod = hin['A_COD'].__str__()
        ret['P_OGRN'] = acod[0:13]
        ret['A_COD'] = acod[13: acod.__len__()]
        ret['DATE_BP'] = '1900-01-01'
        ret['RecipeGUID'] = u'{{{0}}}'.format(uuid.uuid1().__str__().upper())
        ret['NumExport'] = '0'
        ret['DATE_OBR'] = datetimeIsoFormat(hin['DATE_OBR'])
        return ret

class perstransform(transformer):
    def convertHash(self, hinst):
        ret = {}
        hin = {}
        for key in hinst:
            hin[key.upper()] = hinst[key]
        for key in ['SS', 'FAM', 'IM', 'OT', 'W', 'C_KAT', 'C_DOC', 'OKATO_OMS', 'QM_OGRN', 'OKATO_REG', 'D_TYPE']:
            ret[key] = hin.has_key(key) and  hin[key] and hin[key].__str__()

        for key in ['C_DOC', 'OKATO_OMS']:
            ret[key] = setDefault('0', ret[key])

        snpol = splitWords(hin['SN_POL'])
        ret['S_POL'] = snpol and snpol.__len__() >= 1 and snpol[0]
        ret['N_POL'] = snpol and snpol.__len__() >= 1 and snpol[-1]
        ret['DR'] = hin['DR'] and hin['DR'].__str__().replace('/', '-')
        sndoc = splitWords(hin['SN_DOC'])
        ret['S_DOC'] = setDefault('NS', sndoc and sndoc.__len__() >= 1 and sndoc[0])
        ret['N_DOC'] = setDefault('00000000', sndoc and sndoc.__len__() >= 1 and sndoc[1])
        return ret
