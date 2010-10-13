# -*- coding: utf-8 -*-

from common_helpers import *
import sql_helpers
import importer
import datetime
import re
import uuid

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
