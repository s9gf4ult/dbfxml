# -*- coding: utf-8 -*-
from lxml import etree
import datetime
import decimal
import time
from uuid import uuid1
from common_helpers import *
import re
import core
import hashlib


class mainContainer:
    def __init__(self, out, encoding = 'utf-8' ):
        self.out = out
        self.encoding = encoding
        self.element = etree.Element('container')
        core.verboseLogger("Container created")

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
    
    def generateChsm(self):
        chsm = hashlib.sha1(join_list(re.findall('[^\ \t\n]+', etree.tostring(self.element, pretty_print = False, xml_declaration = False, encoding = self.encoding)), '')[1:-1])
        self.element.attrib['chsm'] = chsm.hexdigest().upper()
    
    def getPersondlo(self, iterator):
        pers = xmlPersondlo('PERSONDLO_DOC', iterator)
        pers.iterate()
        return pers.element

    def getPrescriptions(self, iterator):
        presc = xmlPrescriptions('PHARMACYRECIPE_DOC', iterator)
        presc.iterate()
        return presc.element
    

class xmlGetter:
    def __init__(self, name, iterator):
        self.element = etree.Element(name)
        self.iterator = iterator
        core.verboseLogger("xmlGetter created")

    def iterate(self):
        for elt in self.iterator:
            xmlelt = self.elementclass(elt)
            self.element.append(xmlelt.element)

    elementclass = None
            
class xmlPersondlo(xmlGetter):
    def __init__(self, name, iterator):
        xmlGetter.__init__(self, name, iterator)
        self.elementclass = xmlElementPersondlo

class xmlPrescriptions(xmlGetter):
    def __init__(self, name, iterator):
        xmlGetter.__init__(self, name, iterator)
        self.elementclass = xmlElementPrescriptions

class xmlElement:
    def __init__(self, eltdict):
        self.data = eltdict
        self.generateElement()
        self.generateAttribs()
        self.attachParams()
        core.verboseLogger("xmlElement created")

    def generateElement(self):pass
    def generateAttribs(self):pass
    def attachParams(self):pass

def logIt(name, self, dst, src):
    if self.data.has_key(src):
        core.verboseLogger("{0}:: value {1} was transfered to {2} and attached to {3} tag".format(name,
                                                                                                  self.data[src],
                                                                                                  self.element.find(dst).text,
                                                                                                  self.element.find(dst).tag))
    else:
        core.verboseLogger("{0}:: there is not key {1} to attach to {2} tag".format(name,
                                                                                    src,
                                                                                    dst))
                                                                                                  

@multimethod(object, basestring, basestring)
def getFull(self, dst, src):
    etree.SubElement(self.element, dst).text = self.data.has_key(src) and self.data[src] or ''
    logIt("getFull", self, dst, src)

@multimethod(object, list)
def getFull(self, dst):
    for dd in dst:
        getFull(self, dd, dd)

@multimethod(object, dict)
def getFull(self, dst):
    for key in dst:
        getFull(self, key, dst[key])

def executeIfExist(func, self, key1, key2 = None):
    if isinstance(key1, basestring):
        if not key2:
            key2 = key1
        if self.data.has_key(key2):
            func(self, key1, key2)
    elif isinstance(key1, list):
        for k in key1:
            executeIfExist(func, self, k, k)
    elif isinstance(key1, dict):
        for k in key1:
            executeIfExist(func, self, k, key1[1])

@multimethod(object, basestring, basestring)
def getSerial(self, dst, src):
    if not self.data.has_key(src):
        etree.SubElement(self.element, dst).text = ''
    elif isinstance(self.data[src], basestring):
        etree.SubElement(self.element, dst).text = join_list(filter(lambda a: a!='', re.split('[\ \n\t]+', self.data[src])[:-1]), ' ')
    else:
        raise Exception("getSerial not support {0}".format(self.data[src].__class__))
    logIt("getSerial", self, dst, src)
                        
    
@multimethod(object, list)
def getSerial(self, dst):
    for dd in dst:
        getSerial(self, dd, dd)

@multimethod(object, dict)
def getSerial(self, dst):
    for key in dst:
        getSerial(self, key, dst[key])

@multimethod(object, basestring, basestring)
def getNumber(self, dst, src):
    if not self.data.has_key(src):
        etree.SubElement(self.element, dst).text = ''
    elif isinstance(self.data[src], basestring):
        etree.SubElement(self.element, dst).text = re.split('[\ \n\t]+', self.data[src])[-1]
    else:
        raise Exception("getSerial not support {0}".format(self.data[src].__class__))
    logIt("getNumber", self, dst, src)

@multimethod(object, list)
def getNumber(self, dst):
    for dd in dst:
        getNumber(self, dd, dd)

@multimethod(object, dict)
def getNumber(self, dst):
    for key in dst:
        getNumber(self, key, dst[key])

@multimethod(object, basestring, basestring)
def getDecimal(self, dst, src):
    if (not self.data.has_key(src)):
        etree.SubElement(self.element, dst).text = ''
    elif isinstance(self.data[src], basestring):
        if re.match('^[\ \t\n]*$', self.data[src]):
            del self.data[src]
            getDecimal(self, dst, src)
            return None
        etree.SubElement(self.element, dst).text = join_list(re.split('[^0-9]+', self.data[src]), '')
    elif isinstance(self.data[src], (int, float, decimal.Decimal)):
        etree.SubElement(self.element, dst).text = self.data[src].__str__()
    else:
        raise Exception("getDecimal :: unsupported class {0}".format(self.data[src].__class__))
    logIt("getDecimal", self, dst, src)

@multimethod(object, list)
def getDecimal(self, dst):
    for dd in dst:
        getDecimal(self, dd, dd)

@multimethod(object, dict)
def getDecimal(self, dst):
    for key in dst:
        getDecimal(self, key, dst[key])

@multimethod(object, basestring, basestring)
def getDate(self, dst, src):
    if not self.data.has_key(src):
        etree.SubElement(self.element, dst).text = ''
    elif isinstance(self.data[src], basestring):
        etree.SubElement(self.element, dst).text = datetime.date(*map(int, filter(lambda a:a!='', re.split('[^0-9]+', self.data[src])))).isoformat()
    elif isinstance(self.data[src], datetime.date):
        etree.SubElement(self.element, dst).text = self.data[src].isoformat()
    elif isinstance(self.data[src], datetime.datetime):
        etree.SubElement(self.element, dst).text = datetime.date(self.data[src].year, self.data[src].month, self.data[src].day).isoformat()
    else:
        raise Exception("getDate :: unsupported class {0}".format(self.data[src].__class__))
    logIt("getDate", self, dst ,src)

@multimethod(object, list)
def getDate(self, dst):
    for dd in dst:
        getDate(self, dd, dd)

@multimethod(object, dict)
def getDate(self, dst):
    for key in dst:
        getDate(self, key, dst[key])

@multimethod(object, basestring, basestring)
def getDateTime(self, dst, src):
    if not self.data.has_key(src):
        etree.SubElement(self.element, dst).text = ''
    elif isinstance(self.data[src], basestring):
        etree.SubElement(self.element, dst).text = datetime.datetime(*map(int, filter(lambda a:a!='', re.split('[^0-9]+', self.data[src])))).isoformat()
    elif isinstance(self.data[src], datetime.datetime):
        etree.SubElement(self.element, dst).text = self.data[src].isoformat()
    elif isinstance(self.data[src], datetime.date):
        d = self.data[src]
        etree.SubElement(self.element, dst).text = datetime.datetime(d.year, d.month, d.day).isoformat()
    else:
        raise Exception("getDateTime not supported clsss {0}".format(self.data[src].__class__))
    logIt("getDateTime", self, dst ,src)

@multimethod(object, list)
def getDateTime(self, dst):
    for dd in dst:
        getDateTime(self, dd, dd)

@multimethod(object, dict)
def getDateTime(self, dst):
    for key in dst:
        getDateTime(self, key, dst[key])

@multimethod(object, basestring, basestring)
def getInteger(self, dst, src):
    if (not self.data.has_key(src)):
        etree.SubElement(self.element, dst).text = ''
    elif isinstance(self.data[src], basestring):
        if re.match('^[\ \t\n]*$', self.data[src]):
            del self.data[src]
            getInteger(self, dst, src)
            return None
        etree.SubElement(self.element, dst).text = int(join_list(re.findall('[0-9]+', self.data[src]), '')).__str__()
    elif isinstance(self.data[src], int):
        etree.SubElement(self.element, dst).text = self.data[src].__str__()
    elif isinstance(self.data[src], (decimal.Decimal, float)):
        etree.SubElement(self.element, dst).text = self.data[src].__trunc__().__str__()
    else:
        raise Exception("getInteger not suppports class {0}".format(self.data[src].__class__))
    logIt("getInteger", self, dst, src)

@multimethod(object,list)
def getInteger(self, dst):
    for dd in dst:
        getInteger(self, dd ,dd)

@multimethod(object, dict)
def getInteger(self, dst):
    for key in dst:
        getInteger(key, dst[key])

@multimethod(object, basestring, basestring)
def getPRLR(self, dst, src):
    if not self.data.has_key(src):
        raise Exception("get PRLR :: key {0} must be included".format(src))
    elif isinstance(self.data[src], int):
        etree.SubElement(self.element, dst).text = self.data[src] >= 0 and self.data[src] <= 50 and '1' or '2'
    elif isinstance(self.data[src], basestring):
        self.data[src] = int(self.data[src])
        getPRLR(self, dst, src)
    else:
        raise Exception("getPRLR :: not supported class {0}".format(self.data[src].__class__))
    logIt('getPRLR', self, dst, src)

class xmlElementPrescriptions(xmlElement):
    def generateElement(self):
        self.element = etree.Element('PHARMACYRECIPE')
    def generateAttribs(self):
        self.element.attrib['op'] = 'I'

    def attachParams(self):
        getDateTime(self, ['DATE_VR'])
        getFull(self, ['C_OGRN'])
        executeIfExist(getFull, self, ['MCOD'])
        getSerial(self, 'V_C_OGRN', 'PCOD')
        getNumber(self, ['PCOD'])
        getFull(self, ['SS'])
        executeIfExist(getFull, self, ['DS'])
        getSerial(self, 'S_LR', 'SN_LR')
        getNumber(self, 'N_LR', 'SN_LR')
        executeIfExist(getDecimal, self, ['C_MNN'])
        getDecimal(self, ['C_FINL'])
        executeIfExist(getFull, self, ['DOZ_LS'])
        executeIfExist(getDecimal, self, ['KV_ALL'])
        executeIfExist(getInteger, self, ['SR_DAY'])
        getPRLR(self, 'PR_LR', 'PR_LR')
        executeIfExist(getDateTime, self, ['DATE_OTP'])
        executeIfExist(getDecimal, self, ['KO_ALL', 'PRICE', 'P_KEK'] )
        etree.SubElement(self.element, 'Delayed_Service').text = '0'
        getSerial(self, 'P_OGRN', 'A_COD')
        getNumber(self, ['A_COD'])
        getDecimal(self, ['C_KAT'])
        executeIfExist(getDecimal, self, ['C_KATL'])
        getDecimal(self, ['C_PFS'])
        executeIfExist(getDecimal, self, ['PR_REG', 'NOMK_LS', 'C_TRN'])
        etree.SubElement(self.element, 'DATE_BP').text = '1900-01-01'
        executeIfExist(getFull, self, ['D_TYPE'])
        etree.SubElement(self.element, 'RecipeGUID').text = "{{{0}}}".format(uuid1().__str__().upper())
        etree.SubElement(self.element, 'NumExport').text = '0'
        getDateTime(self, ['DATE_OBR'])
        executeIfExist(getDecimal, self, ['OKATO_REG', 'DOZ_ME', 'SL_ALL', 'TYPE_SCHET'])
        getFull(self, ['FO_OGRN'])
        
        
        
class xmlElementPersondlo(xmlElement):
    def generateElement(self):
        self.element = etree.Element('PERSONDLO')
    def generateAttribs(self):
        self.element.attrib['op'] = 'I'

    def attachParams(self):
        getFull(self, ['SS'])
        getSerial(self, 'S_POL', 'SN_POL')
        getNumber(self, 'N_POL', 'SN_POL')
        getFull(self, ['FAM', 'IM', 'OT', 'W'])
        getDate(self, ['DR'])
        getDecimal(self, ['C_KAT'])
        getSerial(self, 'S_DOC', 'SN_DOC')
        getNumber(self, 'N_DOC', 'SN_DOC')
        getDecimal(self, ['C_DOC'])
        getDecimal(self, ['OKATO_OMS'])
        getFull(self, ['QM_OGRN'])
        getDecimal(self, ['OKATO_REG'])
        executeIfExist(getFull, self, ['D_TYPE'])
