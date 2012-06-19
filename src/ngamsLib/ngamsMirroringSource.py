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
# "@(#) $Id: ngamsMirroringSource.py,v 1.6 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  12/05/2001  Created
#

"""
Contains the implementation of the Mirroring Source Class, used to handle
the information for one Mirroring Source.
"""

from ngams import *
import ngamsDbCore


# Definitions for the handling of Mirroring Sources.
NGAMS_MIR_TYPE_ING_DATE     = "INGESTION_DATE"
NGAMS_MIR_TYPE_ING_DATE_NO  = 1
NGAMS_MIR_TYPE_FILTER_PI    = "FILTER_PLUGIN"
NGAMS_MIR_TYPE_FILTER_PI_NO = 2
_mirTypeDic = {NGAMS_MIR_TYPE_ING_DATE: NGAMS_MIR_TYPE_ING_DATE_NO,
               NGAMS_MIR_TYPE_ING_DATE_NO: NGAMS_MIR_TYPE_ING_DATE,
               NGAMS_MIR_TYPE_FILTER_PI: NGAMS_MIR_TYPE_FILTER_PI_NO,
               NGAMS_MIR_TYPE_FILTER_PI_NO: NGAMS_MIR_TYPE_FILTER_PI}
NGAMS_MIR_ALL_NODES = "ALL"


class ngamsMirroringSource:
    """
    Class to handle the information about one Mirroring Source.
    """

    def __init__(self):
        """
        Constructor.
        """
        # Parameters from the configuration.
        self.__id               = None
        self.__serverList       = None
        self.__period           = 60.0 
        self.__completeSync     = []
        self.__syncType         = NGAMS_MIR_TYPE_ING_DATE
        self.__targetNodes      = None
        self.__filterPlugIn     = None
        self.__filterPlugInPars = None

        # Parameters used to manage a source archive.
        self.__lastSync         = timeRef2Iso8601(0)
        self.__lastComplSyncDic = {}


    def setId(self,
              id):
        """
        Set the ID for the mirroring source.
        
        id:        ID of mirroring source (string).

        Returns:   Reference to object itself.
        """
        self.__id = id
        return self


    def getId(self):
        """
        Get the ID for the mirroring source.

        Returns:   ID of mirroring source (string).
        """
        return self.__id


    def setServerList(self,
                      srvList):
        """
        Set the internal server list member.
        
        srvList:   List of servers to add (string).

        Returns:   Reference to object itself.
        """
        self.__serverList = ngamsDbCore.cleanSrvList(srvList)
        return self


    def getServerList(self):
        """
        Get the internal server list member.

        Returns:   Server list (string).
        """
        return self.__serverList


    def setPeriod(self,
                  period):
        """
        Set the period for checking for availability of new data objects
        at the associated source cluster.
        
        period:    Period for checking for new data objects (float).
        
        Returns:   Reference to object itself.
        """
        try:
            self.__period = float(period)
        except:
            msg = "Illegal value for configuration parameter: " +\
                  "Mirroring.Source.Period. Value given: %s"
            raise Exception, msg % str(period)
        return self
    
    
    def getPeriod(self):
        """
        Get the internal period member.

        Returns:   Period for checking for new data objects (float).
        """
        return self.__period


    def setCompleteSync(self,
                        syncTimes):
        """
        Set the period for checking for availability of new data objects
        at the associated source cluster.
        
        syncTimes:   List with ISO 8601 time references for when to make a
                     complete sync (string/ISO 8601).
        
        Returns:     Reference to object itself.
        """
        try:
            tmpSyncList = []
            syncTimeVector = syncTimes.split(",")
            for syncTime in syncTimeVector:
                syncTime = syncTime.replace(" ", "")
                # Expect: HH:MM:SS
                isoTime2Secs(syncTime)
                tmpSyncList.append(syncTime)
            self.__completeSync = tmpSyncList

            # Put the time stamps for complete syncs in the dictionary, which
            # used to keep track of if each complete sync was done.
            for syncTime in tmpSyncList:
                self.__lastComplSyncDic[syncTime] = None

        except Exception, e:
            msg = "Illegal value for configuration parameter: " +\
                  "Mirroring.Source.CompleteSync. Value given: %s. Details: %s"
            raise Exception, msg % (str(syncTimes), str(e))
        return self

    
    def getCompleteSyncList(self):
        """
        Get the list with HH:MM:SS time stamps for when to carry out a complete
        synchronization.

        Returns:   List with time stamps (seconds since midnight) for when to
                   carry out a complete sync with the source archive
                   (list/integer).
        """
        return self.__completeSync


    def setSyncType(self,
                    syncType):
        """
        Set the synchronization type.
        
        syncType:  Synchronization type:
         
                     - NGAMS_MIR_TYPE_ING_DATE
                     - NGAMS_MIR_TYPE_FILTER_PI  (string).

        Returns:   Reference to object itself.
        """
        if (syncType not in (NGAMS_MIR_TYPE_ING_DATE,
                             NGAMS_MIR_TYPE_FILTER_PI)):
            msg = "Mirroring synchronization type given illegal: %s. " +\
                  "Valid options: %s, %s"
            raise Exception, msg % (str(syncType), NGAMS_MIR_TYPE_ING_DATE,
                                    NGAMS_MIR_TYPE_FILTER_PI)
        self.__syncType = syncType
        return self


    def getSyncType(self):
        """
        Get the synchronization type.

        Returns:   Synchronization type (string).
        """
        return self.__syncType


    def setTargetNodes(self,
                       targetNodes):
        """
        Set the internal list of target nodes.

        targetNodes:  Server list in string format (string).

        Returns:      Reference to object itself.
        """
        self.__targetNodes = ngamsDbCore.cleanSrvList(targetNodes)
        return self
    

    def getTargetNodes(self):
        """
        Get the target nodes.

        Returns:   Target node list (string).
        """
        return self.__targetNodes


    def setFilterPlugIn(self,
                        filterPlugIn):
        """
        Set the name of the filter plug-in to apply.

        filterPlugIn:  Name of the filter plug-in (string).

        Returns:       Reference to object itself.
        """
        self.__filterPlugIn = filterPlugIn
        return self
    

    def getFilterPlugIn(self):
        """
        Get  the name of the filter plug-in to apply.

        Returns:   Name of the filter plug-in (string).
        """
        return self.__filterPlugIn


    def setFilterPlugInPars(self,
                            filterPlugInPars):
        """
        Set the parameters for the filter plug-in. Should normally be given as
        comma separated list of values.

        filterPlugInPars:  Plug-in parameters (string).

        Returns:           Reference to object itself.
        """
        self.__filterPlugInPars = filterPlugInPars
        return self
    

    def getFilterPlugInPars(self):
        """
        Get  the name of the filter plug-in parameters.

        Returns:   Plug-in parameters (string).
        """
        return self.__filterPlugInPars


    def setLastSyncTime(self,
                        lastSync):
        """
        Set the time for the last synchronization.

        lastSync:     ISO 8601 time stamp for last sync (string/ISO 8601).

        Returns:      Reference to object itself.
        """
        self.__lastSync = timeRef2Iso8601(lastSync)
        return self
    

    def getLastSyncTime(self):
        """
        Get the time for the last synchronization.

        Returns:     ISO 8601 time stamp for last sync (string/ISO 8601).
        """
        return self.__lastSync


    def getLastCompleteSyncDic(self):
        """
        Get reference to dictionary with references to last, complete
        synchronizations done.

        Returns:   Dictionary with the time stamps for complete syncs as
                   keys point to the last time it was done (dictionary)
        """
        return self.__lastComplSyncDic


# EOF
