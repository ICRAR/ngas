#
#    ALMA - Atacama Large Millimiter Array
#    (c) European Southern Observatory, 2002
#    Copyright by ESO (in the framework of the ALMA collaboration),
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#

#******************************************************************************
#
# "@(#) $Id: ngamsDbBase.py,v 1.20 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/05/2001  Created
#

"""
Contains the ngamsDbBase class.

Variables are defined to query all columns of the tables in a specific order.
These variables are defined below. An example of this is:


  from ngams import *
  from ngamsDbBase import *

  ...
  diskId = diskInfo[ngamsDbBase.NGAS_DISKS_DISK_ID]


In the docmentation of this class the following aliases are used for
the tables:

  nd = NGAS Disks Table (ngas_disks).
  nf = NGAS Files Table (ngas_files).
  nh = NGAS Hosts Table (ngas_hosts).
  ns = NGAS Subscribers Table (ngas_subscribers).
"""

from ngams import *
import ngamsDbCore, ngamsDbNgasCfg, ngamsDbNgasDisks, ngamsDbNgasDisksHist
import ngamsDbNgasFiles, ngamsDbNgasHosts, ngamsDbNgasSubscribers
import ngamsDbMirroring, ngamsDbNgasCache, ngamsDbJoin


# Generate documentation for the table access parameters.
_tblDefFcts = [["ngamsDbCore.getNgasDisksCols()",
                ngamsDbCore.getNgasDisksDef()],
               ["ngamsDbCore.getNgasFilesCols()",
                ngamsDbCore.getNgasFilesDef()],
               ["ngamsDbCore.getNgasHostsCols()",
                ngamsDbCore.getNgasHostsDef()],
               ["ngamsDbCore.getNgasSubscribersCols()",
                ngamsDbCore.getNgasSubscribersDef()],
               ["ngamsDbCore.getNgasSummary1Cols()",
                ngamsDbCore.getNgasSummary1Def()],
               ["ngamsDbCore.getNgasSummary2Cols()",
                ngamsDbCore.getNgasSummary2Def()]]
docStr1 = "\n\nThe function %s returns a reference to a string, which " +\
          "defines all the columns of the NGAS Disks Table. It has the " +\
          "following value:\n\n"
docStr2 = "\n\nWhen quering ngas_disks using %s, the resulting (sub-)list " +\
          "can be accessed by means of the variables:\n\n"
for colsFct, defFct in _tblDefFcts:
    # Update documentation.
    __doc__ += docStr1 % colsFct
    __doc__ += eval(colsFct)
    __doc__ += docStr2 % colsFct
    for colDef in defFct:
        __doc__ += "  ngamsDbCore.%s\n" % colDef[1]


class ngamsDbBase(ngamsDbCore.ngamsDbCore,
                  ngamsDbNgasCache.ngamsDbNgasCache,
                  ngamsDbNgasCfg.ngamsDbNgasCfg,
                  ngamsDbNgasDisks.ngamsDbNgasDisks,
                  ngamsDbNgasDisksHist.ngamsDbNgasDisksHist,
                  ngamsDbNgasFiles.ngamsDbNgasFiles,
                  ngamsDbNgasHosts.ngamsDbNgasHosts,
                  ngamsDbNgasSubscribers.ngamsDbNgasSubscribers,
                  ngamsDbMirroring.ngamsDbMirroring,
                  ngamsDbJoin.ngamsDbJoin):
    """
    Class to handle the connection to the NGAS DB.
    """

    def __init__(self,
                 server,
                 db,
                 user,
                 password,
                 createSnapshot = 1,
                 interface = "ngamsSybase",
                 tmpDir = "/tmp",
                 maxRetries = 10,
                 retryWait = 1.0,
                 parameters = None,
                 multipleConnections = False):
        """
        Constructor method.

        server:              DB server name (string).

        db:                  DB name (string).
        
        user:                DB user (string).
        
        password:            DB password (string).

        createSnapshot:      Indicates if a DB Snapshot (temporary snapshot
                             files) should be created (integer/0|1).

        interface:           NG/AMS DB Interface Plug-In (string).
        
        tmpDir:              Name of NGAS Temporary Directory (string).

        maxRetries:          Max. number of retries in case of failure
                             (integer).

        retryWait:           Time in seconds to wait for next retry (float).

        parameters:          Plug-in parameters for the connection (usually for
                             the NG/AMS DB Driver Plug-In).

        multipleConnections: Allow multiple connections or only one (boolean).
        """
        ngamsDbCore.ngamsDbCore.__init__(self, server, db, user, password,
                                         createSnapshot, interface, tmpDir,
                                         maxRetries, retryWait, parameters,
                                         multipleConnections)


    
if __name__ == '__main__':
    """
    Main routine to make a connection to the DB.
    """
    setLogCond(0, "", 0, "", 5)
    db = ngamsDbBase("TESTSRV", "ngas_dev", "ngas_dbo", "ngas_dbo_pw")
    db.close()
    res1 = []
    try:
        res1 = db.query("SELECT total_disk_write_time FROM ngas_disks")
    except Exception, e:
        print e
    print "res1="+str(res1)
    #print db.diskInDb("DiskId-1-1--")

    
# EOF
