#******************************************************************************
# ESO/DFS
#
# "@(#) $Id: PccUtDb2KeyMap.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  05/02/2000  Created
#

"""
Module providing class to handle mapping of FITS keywords into
Sybase table columns and vice versa.
"""
from pcc.pccUt import PccUtUtils, PccUtString
from pcc.pccLog.PccLog import info
from pcc.pccKey import PccKey
from pcc.pccPaf import PccPaf
import string, exceptions

from PccLog import *


class PccUtDb2KeyMap:
    """
    Handles mapping of DB columns into FITS keywords and
    vice versa.

    The mapping is based on a DB To Key Mapping File (DB2KMAP) which is a
    PAF file and has the following format, example::

     DBKMAP.TABLE    isaac_all
     DBKMAP.ATTRIB   lst:LST
     DBKMAP.ATTRIB   tpl_expno:TPL.EXPNO
     DBKMAP.ATTRIB   ins_filt1_name:INS.FILT1.NAME
     DBKMAP.ATTRIB   ins_lamp1_name:INS.LAMP1.NAME

     DBKMAP.TABLE    uves
     DBKMAP.ATTRIB   ins_temp1_mean:INS.TEMP1.MEAN
     DBKMAP.ATTRIB   det_win1_nx:DET.WIN1.NX
     DBKMAP.ATTRIB   tel_az:TEL.AZ
    
     DBKMAP.TABLE    fors
     DBKMAP.ATTRIB   ins_coll_name:INS.COLL.NAME
     DBKMAP.ATTRIB   det_read_clock:DET.READ.CLOCK
     DBKMAP.ATTRIB   dimm_fwhm_avg:DIMM.FWHM.AVG 

     # --- oOo ---

    The DB2KMAP can be perceived as consisting of blocks, with mappings
    for different tables. The keyword "DBKMAP.TABLE" indicates the start
    of such a block, and contains the name of the table. The keywords
    "DBKMAP.ATTRIB" are used to indicate the mapping between the column
    names and the corresponding FITS keyword (written in the Short-FITS
    format). The value of this keyword consist of two fields separated
    with colon: "<column name>:<keyword>".


    **Mapping from DB -> FITS Keywords:** 

    This mapping is done with the method PccUtDb2KeyMap.db2Key(). It takes
    the DB2KMAP file as input parameter, and generates a list of PccKey
    objects, to which a reference is returned.


    **Mapping from FITS/PAF -> DB:**
    
    The mapping from a list of PccKey objects into the DB is done with the
    method PccUtDb2KeyMap.key2Db(). The mapping of keys contained in
    FITS files or in PAF files are done with PccUtDb2KeyMap.key2DbFits()
    and PccUtDb2KeyMap.key2DbPaf().

    _THE MAPPING FROM FITS/PAF TO THE DB IS NOT YET IMPLEMENTED._
    """


    def __init__(self,
                 dbName     = "",
                 dbUser     = "",
                 dbPassword = "",
                 dataBase   = "observations",
                 tolerant   = 0):
        """
        Constructor method to initialize class member variables.

        dbName:     Name of DB (Server).
        
        dbUser:     Name of DB user.
        
        dbPassword: Password for user.
        
        dataBase:   Name of database.
        
        tolerant:   Run in Tolerant Mode. In Tolerant Mode when mapping
                    from DB->Keys a key listed in the DB2KMAP is not
                    found in the DB this is ignored, otherwise an
                    exception is raised. Likewise, if for a keyword
                    found in a FITS or PAF file being mapped into the
                    DB, a corresponding DB2KMAP entry is not found in
                    the mapping table, this is ignored (the key skipped)
                    in Tolerant Mode.
        """
        # Get default values for the DB access.
        if (dbName != ""):
            self.__dbName = dbName
        else:
            self.__dbName = "ESOECF"
        if (dbUser != ""):
            self.__dbUser = dbUser
        else:
            self.__dbUser = "www"
        if (dbPassword != ""):
            self.__dbPassword = dbPassword
        else:
            # Try to get the password from ~/.dbrc.
            self.__dbPassword = PccUtUtils.getDbPwd("ESOECF", "www")

        # Connect to DB + select DB "observations".
        try:
            import Sybase
        except ImportError:
            return

        self.__db = Sybase.connect(self.__dbName, self.__dbUser,
                                   self.__dbPassword)
        self.__query("use " + dataBase)

        self.__tolerant = tolerant


    def __query(self,
                sqlQuery):
        """
        Carry out a DB query and return the result in raw format.

        sqlQuery:    SQL query to carry out.

        Returns:     Result of SQL query in raw format.
        """        
        info(3,"Performing SQL statement: " + sqlQuery)
        res = self.__db.execute(sqlQuery)
        return res


    def db2Key(self,
               selCol,
               selValue,
               dbKeyMapPafObj):
        """
        From the DB2KEY Map PAF File given in as input parameter,
        the method reads the values specified in the DB Key Map
        PAF Object for the row specified by the key + value parameters.
        The values read are stored together with the corresponding FITS
        keywords in a list which is returned.

        Note, only the values of one row are read.

        selCol:           The key (column) in the table used to select
                          the row of interest.

        selValue:         The value of the selection key of the column of
                          interest.
 
        dbKeyMapPafObj:   PccPaf object containing the DB2KEY Map.

        Returns:          List containing PccKey elements.
        """
        keyList = []
        mapTbl = dbKeyMapPafObj
        noOfKeys = mapTbl.getNoOfKeys()
        keyCount = 0
        # Find first "DBKMAP.TABLE" key (start of next 'block').
        key = mapTbl.getKeyObjNo(keyCount)
        while ((key.getKey() != "DBKMAP.TABLE") and (keyCount < noOfKeys)):
            keyCount = keyCount + 1
            key = mapTbl.getKeyObjNo(keyCount)
        if (keyCount == noOfKeys):
            raise exceptions.Exception, \
                  "DB2KEY Mapping file given improperly formatted (no "+\
                  "mapping records)."
                    
        while (keyCount < noOfKeys):
            # Handle the keywords in this block.
            tblName = PccUtString.trimString(key.getValue(), " \"")
            keyCount = keyCount + 1
            key = mapTbl.getKeyObjNo(keyCount)
            while ((key.getKey()=="DBKMAP.ATTRIB") and (keyCount < noOfKeys)):
                # The mapping elements are stored as: "<col nm>:<FITS key>".
                map = string.split(key.getValue(), ":")
                column = PccUtString.trimString(map[0], " \"")
                fitsKey = PccUtString.trimString(map[1], " \"")
                sqlQuery = "SELECT " + column + " FROM " + tblName + \
                           " WHERE " + selCol + "='" + selValue + "'"
                res = self.__query(sqlQuery)

                # If the DB value was found we store it together with the
                # FITS keyword. If not found and tolerant mode is off,
                # we raise an error. Otherwise if tolerant mode is on, we
                # ignore the missing value.
                if (len(res[0]) == 1):
                    if (res[0][0][0] == None):
                        val = ""
                    else:
                        val = res[0][0][0]
                    tmpKey = PccKey.PccKey(fitsKey, val)
                    keyList.append(tmpKey)
                elif (self.__tolerant == 0):
                    raise exceptions.Exception, \
                          "Problem mapping DB address " + tblName + "." +\
                          column + " into FITS key " + fitsKey + "."
                    
                keyCount = keyCount + 1
                if (keyCount < noOfKeys): key = mapTbl.getKeyObjNo(keyCount)
            
        return keyList


    def key2Db(self,
               dbKeyMapPafObj,
               pccKeyList):
        """
        **NOT YET IMPLEMENTED!!** 
        
        Map the keywords stored in a Python list of PccKey objects into the
        DB, based on the mappings given in the DB2KMAP contained in the
        PccPaf object (dbKeyMapPafObj).
        
        dbKeyMapPafObj:   PccPaf object containing the DB2KEY Map.
        
        pccKeyList:       List of PccKey objects.

        Returns:          Void.
        """
        print "NOTE: Not yet supported!"
        
        
    def key2DbFits(self,
                   dbKeyMapPafObj,
                   fitsFileName):
        """
        **NOT YET IMPLEMENTED!!**
        
        Map the keyword values stored in a FITS file into the DB based
        on the mappings defined in the DB2KMAP PAF file loaded into the
        PccPaf object ("dbKeyMapPafObj").
        
        dbKeyMapPafObj:   PccPaf object containing the DB2KEY Map.
        
        fitsFileName:     Name of FITS file to load.

        Returns:          Void.
        """
        print "NOTE: Not yet supported!"


    def key2DbPaf(self,dbKeyMapPafObj,
                  pafFileName):
        """
        **NOT YET IMPLEMENTED!!**
        
        Map the keyword values stored in a PAF file into the DB based
        on the mappings defined in the DB2KMAP PAF file loaded into the
        PccPaf object ("dbKeyMapPafObj").
        
        dbKeyMapPafObj:   PccPaf object containing the DB2KEY Map.
        
        fitsFileName:     Name of PAF file to load.

        Returns:          Void.
        """
        print "NOTE: Not yet supported!"
        

def test():
    """
    Test the methods of this class.
    """
    db2KeyMap = PccPaf.PccPaf(1)
    db2KeyMap.load("Db2KeyMap.dbkm")
    
    mapObj = PccUtDb2KeyMap()
    dbMapRes = mapObj.db2Key("dp_id", "UVES.1999-09-08T01:58:14.977",db2KeyMap)
    for key in dbMapRes:
        key.dump()


if __name__ == '__main__':
    """
    Test main
    function.
    """
    test()


#
# ___oOo___
