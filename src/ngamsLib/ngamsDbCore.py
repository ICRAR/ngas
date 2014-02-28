#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2012
#    Copyright by UWA (in the framework of the ICRAR)
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
# "@(#) $Id: ngamsDbCore.py,v 1.13 2010/03/29 12:56:17 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/03/2008  Created
#

"""
Core class for the NG/AMS DB interface.
"""

import os, sys, string, time, types, base64, random

import pcc, PccLog, PccUtTime
from   ngams import *
import ngamsDbm, threading


# Global DB Semaphore to protect critical, global DB interaction.
_globalDbSem = threading.Semaphore(1)


# Define lay-out of ngas_disks table
_ngasDisksDef = [["nd.disk_id",               "NGAS_DISKS_DISK_ID"],
                 ["nd.archive",               "NGAS_DISKS_ARCHIVE"],
                 ["nd.logical_name",          "NGAS_DISKS_LOG_NAME"],
                 ["nd.host_id",               "NGAS_DISKS_HOST_ID"],
                 ["nd.slot_id",               "NGAS_DISKS_SLOT_ID"],
                 ["nd.mounted",               "NGAS_DISKS_MOUNTED"],
                 ["nd.mount_point",           "NGAS_DISKS_MT_PT"],
                 ["nd.number_of_files",       "NGAS_DISKS_NO_OF_FILES"],
                 ["nd.available_mb",          "NGAS_DISKS_AVAIL_MB"],
                 ["nd.bytes_stored",          "NGAS_DISKS_BYTES_STORED"],
                 ["nd.type",                  "NGAS_DISKS_TYPE"],
                 ["nd.capacity_mb",           "NGAS_DISKS_CAPACITY_MB"],
                 ["nd.manufacturer",          "NGAS_DISKS_MANUFACTURER"],
                 ["nd.installation_date",     "NGAS_DISKS_INST_DATE"],
                 ["nd.checksum",              "NGAS_DISKS_CHECKSUM"],
                 ["nd.total_disk_write_time", "NGAS_DISKS_TOT_WR_TIME"],
                 ["nd.completed",             "NGAS_DISKS_COMPLETED"],
                 ["nd.completion_date",       "NGAS_DISKS_COMPL_DATE"],
                 ["nd.last_check",            "NGAS_DISKS_LAST_CHECK"],
                 ["nd.last_host_id",          "NGAS_DISKS_LAST_HOST_ID"]]
_ngasDisksCols = ""
idx = 0
for colDef in _ngasDisksDef:
    if (_ngasDisksCols): _ngasDisksCols += ", "
    _ngasDisksCols += colDef[0]
    exec(colDef[1] + "=%d" % idx)
    idx += 1

def getNgasDisksCols():
    """
    Return reference to a string defining the lay-out of the table.

    Returns:   Reference to string listing all columns (string).
    """
    return _ngasDisksCols

def getNgasDisksDef():
    """
    Returns reference to list defining mapping between columns and variables
    defined to refer to each column.

    Returns:   List with mapping between column name and associated variable
               (list).
    """
    return _ngasDisksDef


# Define lay-out of ngas_files table.
#
# NOTE: DON'T CHANGE THE SEQUENCE ALLOCATED - A NEW ENTRY CAN BE ADDED AT THE
#       END OF THE LIST, BUT CHANGING THE EXISTING ENTRIES WILL BREAK THE DB
#       SNAPSHOT FEATURE.
_ngasFilesDef = [["nf.disk_id",                "NGAS_FILES_DISK_ID"],
                 ["nf.file_name",              "NGAS_FILES_FILE_NAME"],
                 ["nf.file_id",                "NGAS_FILES_FILE_ID"],
                 ["nf.file_version",           "NGAS_FILES_FILE_VER"],
                 ["nf.format",                 "NGAS_FILES_FORMAT"],
                 ["nf.file_size",              "NGAS_FILES_FILE_SIZE"],
                 ["nf.uncompressed_file_size", "NGAS_FILES_UNCOMPR_FILE_SIZE"],
                 ["nf.compression",            "NGAS_FILES_COMPRESSION"],
                 ["nf.ingestion_date",         "NGAS_FILES_INGEST_DATE"],
                 ["nf.ignore",                 "NGAS_FILES_IGNORE"],
                 ["nf.checksum",               "NGAS_FILES_CHECKSUM"],
                 ["nf.checksum_plugin",        "NGAS_FILES_CHECKSUM_PI"],
                 ["nf.file_status",            "NGAS_FILES_FILE_STATUS"],
                 ["nf.creation_date",          "NGAS_FILES_CREATION_DATE"],
                 ["nf.io_time",                "NGAS_FILES_IO_TIME"],
                 ]
_ngasFilesCols = ""
_ngasFilesNameMap = {}
idx = 0
for colDef in _ngasFilesDef:
    if (_ngasFilesCols): _ngasFilesCols += ", "
    _ngasFilesCols += colDef[0]
    exec(colDef[1] + "=%d" % idx)
    colName = colDef[0].split(".")[1]
    _ngasFilesNameMap[idx] = colName
    _ngasFilesNameMap[colName] = idx
    idx += 1

def getNgasFilesCols():
    """
    Return reference to a string defining the lay-out of the table.

    Returns:   Reference to string listing all columns (string).
    """
    return _ngasFilesCols

def getNgasFilesDef():
    """
    Returns reference to list defining mapping between columns and variables
    defined to refer to each column.

    Returns:   List with mapping between column name and associated variable
               (list).
    """
    return _ngasFilesDef


# Define lay-out of ngas_hosts table.
_ngasHostsDef = [["nh.host_id",              "NGAS_HOSTS_HOST_ID"],
                 ["nh.domain",               "NGAS_HOSTS_DOMAIN"],
                 ["nh.ip_address",           "NGAS_HOSTS_ADDRESS"],
                 ["nh.mac_address",          "NGAS_HOSTS_MAC_ADDRESS"],
                 ["nh.n_slots",              "NGAS_HOSTS_N_SLOTS"],
                 ["nh.cluster_name",         "NGAS_HOSTS_CLUSTER_NAME"],
                 ["nh.installation_date",    "NGAS_HOSTS_INST_DATE"],
                 ["nh.srv_version",          "NGAS_HOSTS_SRV_VER"],
                 ["nh.srv_port",             "NGAS_HOSTS_SRV_PORT"],
                 ["nh.srv_archive",          "NGAS_HOSTS_SRV_ARCHIVE"],
                 ["nh.srv_retrieve",         "NGAS_HOSTS_SRV_RETRIEVE"],
                 ["nh.srv_process",          "NGAS_HOSTS_SRV_PROCESS"],
                 ["nh.srv_remove",           "NGAS_HOSTS_SRV_REMOVE"],
                 ["nh.srv_data_checking",    "NGAS_HOSTS_SRV_DATA_CHECK"],
                 ["nh.srv_state",            "NGAS_HOSTS_SRV_STATE"],
                 ["nh.srv_suspended",        "NGAS_HOSTS_SRV_SUSP"],
                 ["nh.srv_req_wake_up_srv",  "NGAS_HOSTS_SRV_REQ_WAKE_UP_SRV"],
                 ["nh.srv_req_wake_up_time", "NGAS_HOSTS_SRV_REQ_WAKE_UP_TIM"]]
_ngasHostsCols = ""
_ngasHostsNameMap = {}
idx = 0
for colDef in _ngasHostsDef:
    if (_ngasHostsCols): _ngasHostsCols += ", "
    _ngasHostsCols += colDef[0]
    exec(colDef[1] + "=%d" % idx)
    colName = colDef[0].split(".")[1]
    _ngasHostsNameMap[idx] = colName
    _ngasHostsNameMap[colName] = idx
    idx += 1

def getNgasHostsCols():
    """
    Return reference to a string defining the lay-out of the table.

    Returns:   Reference to string listing all columns (string).
    """
    return _ngasHostsCols

def getNgasHostsDef():
    """
    Returns reference to list defining mapping between columns and variables
    defined to refer to each column.

    Returns:   List with mapping between column name and associated variable
               (list).
    """
    return _ngasHostsDef

def getNgasHostsMap():
    """
    Return the reference to dictionary mapping the column IDs into the the
    associated column name.

    Returns:   Reference to columne ID/name map (dictionary).
    """
    return _ngasHostsNameMap


# Define lay-out of ngas_subscribers table.
_NGS = "NGAS_SUBSCR_"
_ngasSubscribersDef = [["ns.host_id",                   _NGS + "HOST_ID"],
                       ["ns.srv_port",                  _NGS + "SRV_PORT"],
                       ["ns.subscr_prio",               _NGS + "PRIO"],
                       ["ns.subscr_id",                 _NGS + "ID"],
                       ["ns.subscr_url",                _NGS + "URL"],
                       ["ns.subscr_start_date",         _NGS + "START"],
                       ["ns.subscr_filter_plugin",      _NGS + "FILT_PI"],
                       ["ns.subscr_filter_plugin_pars", _NGS + "FILT_PI_PARS"],
                       ["ns.last_file_ingestion_date",  _NGS + "ING_DATE"],
                       ["ns.concurrent_threads",  _NGS + "CONCURR_THRDS"]]
_ngasSubscribersCols = ""
_ngasSubscriberNameMap = {}
idx = 0
for colDef in _ngasSubscribersDef:
    if (_ngasSubscribersCols): _ngasSubscribersCols += ", "
    _ngasSubscribersCols += colDef[0]
    exec(colDef[1] + "=%d" % idx)
    colName = colDef[0].split(".")[1]
    _ngasSubscriberNameMap[idx] = colName
    _ngasSubscriberNameMap[colName] = idx
    idx += 1


def getNgasSubscribersCols():
    """
    Return reference to a string defining the lay-out of the table.

    Returns:   Reference to string listing all columns (string).
    """
    return _ngasSubscribersCols

def getNgasSubscribersDef():
    """
    Returns reference to list defining mapping between columns and variables
    defined to refer to each column.

    Returns:   List with mapping between column name and associated variable
               (list).
    """
    return _ngasSubscribersDef


# Define lay-out of a Summary 1 Query.
_sum1Def = [["nd.slot_id",         "SUM1_SLOT_ID"],
            ["nd.mount_point",     "SUM1_MT_PT"],
            ["nf.file_name",       "SUM1_FILENAME"],
            ["nf.checksum",        "SUM1_CHECKSUM"],
            ["nf.checksum_plugin", "SUM1_CHECKSUM_PI"],
            ["nf.file_id",         "SUM1_FILE_ID"],
            ["nf.file_version",    "SUM1_VERSION"],
            ["nf.file_size",       "SUM1_FILE_SIZE"],
            ["nf.file_status",     "SUM1_FILE_STATUS"],
            ["nd.disk_id",         "SUM1_DISK_ID"],
            ["nf.file_ignore",     "SUM1_FILE_IGNORE"],
            ["nd.host_id",         "SUM1_HOST_ID"]]
_sum1Cols = ""
idx = 0
for colDef in _sum1Def:
    if (_sum1Cols): _sum1Cols += ", "
    _sum1Cols += colDef[0]
    exec(colDef[1] + "=%d" % idx)
    idx += 1

def getNgasSummary1Cols():
    """
    Return reference to a string defining the lay-out of the table.

    Returns:   Reference to string listing all columns (string).
    """
    return _sum1Cols

def getNgasSummary1Def():
    """
    Returns reference to list defining mapping between columns and variables
    defined to refer to each column.

    Returns:   List with mapping between column name and associated variable
               (list).
    """
    return _sum1Def


# Define lay-out of a Summary 2 Query.
_sum2Def = [["nf.file_id",        "SUM2_FILE_ID"],
            ["nd.mount_point",    "SUM2_MT_PT"],
            ["nf.file_name",      "SUM2_FILENAME"],
            ["nf.file_version",   "SUM2_VERSION"],
            ["nf.ingestion_date", "SUM2_ING_DATE"],
            ["nf.format",         "SUM2_MIME_TYPE"],
            ["nd.disk_id",        "SUM2_DISK_ID"]]
_sum2Cols = ""
idx = 0
for colDef in _sum2Def:
    if (_sum2Cols): _sum2Cols += ", "
    _sum2Cols += colDef[0]
    exec(colDef[1] + "=%d" % idx)
    idx += 1

def getNgasSummary2Cols():
    """
    Return reference to a string defining the lay-out of the table.

    Returns:   Reference to string listing all columns (string).
    """
    return _sum2Cols

def getNgasSummary2Def():
    """
    Returns reference to list defining mapping between columns and variables
    defined to refer to each column.

    Returns:   List with mapping between column name and associated variable
               (list).
    """
    return _sum2Def


# Define lay-out of ngas_mirroring_queue table.
NGAS_MIR_QUEUE = "ngas_mirroring_queue"
NGAS_MIR_HIST  = "ngas_mirroring_hist"
_ngasMirQueueDef = [["mq.instance_id",        "NGAS_MIR_Q_INST_ID"],
                    ["mq.file_id",            "NGAS_MIR_Q_FILE_ID"],
                    ["mq.file_version",       "NGAS_MIR_Q_FILE_VERSION"],
                    ["mq.ingestion_date",     "NGAS_MIR_Q_ING_DATE"],
                    ["mq.srv_list_id",        "NGAS_MIR_Q_SRV_LIST_ID"],
                    ["mq.xml_file_info",      "NGAS_MIR_Q_XML_FILE_INFO"],
                    ["mq.status",             "NGAS_MIR_Q_STATUS"],
                    ["mq.message",            "NGAS_MIR_Q_MESSAGE"],
                    ["mq.last_activity_time", "NGAS_MIR_Q_LAST_ACT_TIME"],
                    ["mq.scheduling_time",    "NGAS_MIR_Q_SCHED_TIME"]]
_ngasMirQueueCols = ""
_ngasMirQueueNamesMap = {}
idx = 0
for colDef in _ngasMirQueueDef:
    if (_ngasMirQueueCols): _ngasMirQueueCols += ", "
    _ngasMirQueueCols += colDef[0]
    exec(colDef[1] + "=%d" % idx)
    colName = colDef[0].split(".")[1]
    _ngasMirQueueNamesMap[idx] = colName
    _ngasMirQueueNamesMap[colName] = idx
    idx += 1


def getNgasMirQueueCols():
    """
    Return reference to a string defining the lay-out of the table.

    Returns:   Reference to string listing all columns (string).
    """
    return _ngasMirQueueCols


def getNgasMirQueueCols2():
    """
    Return reference to a string defining the lay-out of the table.

    Returns:   Reference to string listing all columns (string).
    """
    return _ngasMirQueueCols.replace("mq.", "")


def getNgasMirQueueDef():
    """
    Returns reference to list defining mapping between columns and variables
    defined to refer to each column.

    Returns:   List with mapping between column name and associated variable
               (list).
    """
    return _ngasMirQueueDef

def getNgasMirQueueNamesMap():
    """
    Return the reference to dictionary mapping the column IDs into the the
    associated column name.

    Returns:   Reference to columne ID/name map (dictionary).
    """
    return _ngasMirQueueNamesMap


class ngamsDbTimer:
    """
    Small timer class use to measure the time spent for DB access.
    """

    def __init__(self,
                 dbConObj,
                 query):
        """
        Constructor.

        dbConObj:  DB connection object (ngamsDbBase).
        """
        self.__startTime = time.time()
        self.__dbConObj = dbConObj
        self.__query = query


    def __del__(self):
        """
        Destructor. Stop the timer and update the global timer in ngamsDbBase.
        """
        try:
            stopTime = time.time()
            deltaTime = (stopTime - self.__startTime)
            self.__dbConObj.updateDbTime(deltaTime)
            if (PccLog.getVerboseLevel() >= 4):
                msg = "DB-TIME: Time spent for DB query: |%s|: %.6fs" %\
                      (self.__query, deltaTime)
                info(4, msg)
        except:
            pass

def cleanSrvList(srvList):
    """
    Clean the server list given. This means:

    - Removing irrelevant characters.
    - Sorting the servers.

    srvList:   List of servers: '<host>:<port>,...' (string).

    Returns:   Return the cleaned up list (string).
    """
    T = TRACE()

    # Clean up the list, we ensure the servers are always listed in
    # alphabetical order.
    try:
        srvList = srvList.replace(" ", "").split(",")
        srvList.sort()
        srvList = str(srvList)[1:-1].replace("'", "").replace(" ", "")
    except Exception, e:
        msg = "Error cleaning up server list. Error: %s" % str(e)
        raise Exception, msg

    return srvList


class ngamsDbCore:
    """
    Core class for the NG/AMS DB interface.
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
        T = TRACE()

        self.__dbDrv = None

        # Semaphore to protect critical DB interaction.
        self.__dbSem = threading.Lock() # threading.Semaphore(1)

        # Controls if the snapshot of the DB should be created.
        self.__createSnapshot = createSnapshot

        # List of Event Objects which are used to inform other instances
        # about changes in the DB.
        self.__dbChangeEvents = []

        # Timer for analyzing time spent for DB access
        self.__dbAccessTime = 0.0

        # Import the DB Interface Plug-In + create connection.
        self.__dbServer            = server
        self.__dbName              = db
        self.__dbUser              = user
        self.__dbPassword          = password
        self.__parameters          = parameters
        self.__multipleConnections = multipleConnections
        self.__dbSnapshot          = createSnapshot
        self.__dbInterface         = interface
        self.connect(server, db, user, password, interface)

        # Verification/Auto Recover.
        self.__dbVerify      = 1
        self.__dbAutoRecover = 0

        self.__dbTmpDir      = tmpDir

        self.__maxRetries    = maxRetries
        self.__retryWait     = retryWait


    def takeDbSem(self):
        """
        Acquire access to a critical DB interaction.

        Returns:   Reference to object itself.
        """
        if (not self.__multipleConnections):
            if (getVerboseLevel() > 4): info(5, "Taking DB Access Semaphore")
            self.__dbSem.acquire()


    def relDbSem(self):
        """
        Release acquired access to a critical DB interaction.

        Returns:   Reference to object itself.
        """
        if (not self.__multipleConnections):
            if (getVerboseLevel() > 4):
                info(5, "Releasing DB Access Semaphore")
            self.__dbSem.release()


    def takeGlobalDbSem(self):
        """
        Acquire access to a critical, global DB interaction.

        Returns:   Reference to object itself.
        """
        # TODO: Check if really needed with a Global DB Semaphore (if yes
        #       write the reason in the documentation).
        if (getVerboseLevel() > 4):
            info(5, "Taking Global DB Access Semaphore")
        global _globalDbSem
        _globalDbSem.acquire()


    def relGlobalDbSem(self):
        """
        Release acquired access to a critical, global DB interaction.

        Returns:   Reference to object itself.
        """
        # TODO: Check if really needed with a Global DB Semaphore (if yes
        #       write the reason in the documentation).
        if (getVerboseLevel() > 4):
            info(5, "Releasing Global DB Access Semaphore")
        global _globalDbSem
        _globalDbSem.release()


    def updateDbTime(self,
                     dbAccessTime):
        """
        Update the DB access timer.

        dbAccessTime:   DB access time to add in seconds (float).

        Returns:  Reference to object itself.
        """
        self.__dbAccessTime += float(dbAccessTime)
        return self


    def getDbTime(self):
        """
        Return the time spent for the last DB access.

        Returns:    Last DB access time in seconds (float).
        """
        return self.__dbAccessTime


    def resetDbTime(self):
        """
        Reset the Db timer.

        Returns:    Reference to object itself.
        """
        self.__dbAccessTime = 0.0
        return self


    def getDbTimeStr(self,
                     prec = 3):
        """
        Return the time spent for the last DB access as a string.

        Returns:    Last DB access time in seconds as a string (string).
        """
        return str('%.' + str(prec) + 'fs') % self.__dbAccessTime


    def getDbServer(self):
        """
        Returns the name of the DB server.

        Returns:  Name of DB server (string).
        """
        return self.__dbServer


    def getDbName(self):
        """
        Returns the name of the DB.

        Returns:  Name of DB (string).
        """
        return self.__dbName


    def setDbVerify(self,
                    verify):
        """
        Enable/disable DB Verification.

        verify:   0=off, 1=on (integer/0|1).

        Returns:  Reference to object itself.
        """
        self.__dbVerify = int(verify)
        return self


    def getDbVerify(self):
        """
        Get value of DB verification flag.

        Returns:  value of DB verification flag (boolean).
        """
        return self.__dbVerify


    def setDbAutoRecover(self,
                         autoRecover):
        """
        Enable/disable DB Auto Recovering.

        autoRecover:   0=off, 1=on (integer/0|1).

        Returns:       Reference to object itself.
        """
        self.__dbAutoRecover = int(autoRecover)
        return self


    def getDbAutoRecover(self):
        """
        Get value of DB Auto Recover Flag.

        Returns:    value of DB Auto Recover Flag (boolean).
        """
        return self.__dbAutoRecover


    def setDbTmpDir(self,
                    tmpDir):
        """
        Set the DB temporary directory.

        tmpDir:        Temporary directory (string).

        Returns:       Reference to object itself.
        """
        self.__dbTmpDir = tmpDir
        return self


    def getDbTmpDir(self):
        """
        Get the DB temporary directory.

        Returns:   DB temporary directory (string).
        """
        return self.__dbTmpDir


    def getCreateDbSnapshot(self):
        """
        Return the flag indicating if a DB Snapshot should be created or not.

        Returns:  Value of the DB Snapshot Creation Flag (boolean).
        """
        return self.__createSnapshot


    def connect(self,
                server,
                db,
                user,
                password,
                interface = "ngamsSybase"):
        """
        Connect to the DB.

        server:       DB server name (string).

        db:           DB name (string).

        user:         DB user (string).

        password:     DB password (base 64 encoded) (string).

        interface:    NG/AMS DB Interface Plug-In (string).

        Returns:      Void.
        """
        try:
            self.takeDbSem()
            self._connect(server, db, user, password, interface, 0)
            self.relDbSem()
        except Exception, e:
            self.relDbSem()
            raise Exception, e


    def _connect(self,
                 server,
                 db,
                 user,
                 password,
                 interface,
                 recurseLevel):
        """
        Connect to the DB. The method may do this in a recursive manner
        (re-trying) in case it fails in connecting.

        NOTE: The invocation of this method should be semaphore protected.

        server:       DB server name (string).

        db:           DB name (string).

        user:         DB user (string).

        password:     DB password (base 64 encoded) (string).

        interface:    NG/AMS DB Interface Plug-In (string).

        recurseLevel: Recursive depth level (integer).

        Returns:      Void.
        """
        T = TRACE()

        try:
            if (self.__dbDrv != None):
                del self.__dbDrv
                self.__dbDrv = None

            info(4, "Importing DB Driver Interface: %s" % interface)
            exec "import " + interface
            try:
                decryptPassword = base64.decodestring(password)
            except Exception, e:
                errMsg = "Incorrect, encrypted DB password given. Error: " +\
                         str(e)
                raise Exception, errMsg
            creStat = "%s.%s('%s', '%s', '%s', '%s', '%s', '%s')" %\
                      (interface, interface, server, db, user, decryptPassword,
                       "NG/AMS:" + getThreadName(), self.__parameters)
            info(4, "Creating instance of DB Driver Interface/connecting ...")
            info(5, "Command to create DB connection object: %s" % creStat)
            self.__dbDrv = eval(creStat)
            info(3, "DB Driver Interface ID: " + self.__dbDrv.getDriverId())
        except Exception, e:
            errMsg = genLog("NGAMS_ER_DB_COM", ["Problem setting up " +\
                                                "DB connection: " + str(e)])
            errMsg = errMsg.replace("\n", "")

            # Retry up to 5 times.
            if (recurseLevel < 5):
                warning(errMsg)
                msg = "Reconnecting to DB server. Recursive depth: %d/" +\
                      "Test Mode: %d ..."
                notice(msg % (recurseLevel, getTestMode()))
                if (getTestMode()):
                    time.sleep(0.5)
                else:
                    time.sleep(5.0)
                recurseLevel += 1
                self._connect(self.__dbServer, self.__dbName, self.__dbUser,
                              self.__dbPassword, self.__dbInterface,
                              recurseLevel)
                notice("Reconnected to DB server")
            else:
                notice("Abandonned reconnectiong attempt. " +\
                       "Recursive level: %d" % recurseLevel)
                raise Exception, errMsg


    def checkCon(self):
        """
        Method that check if the DB connection seems to be OK. If it is
        not, it will be tried to reconnect. If the connection is down, and
        it cannot reconnect, an exception is raised.

        Returns:    Void.
        """
        T = TRACE()

        # Perform a simply query, the ngamsDbBase.query() method will reconnect
        # automatically if the DB connection is lost
        sqlQuery = "SELECT * from ngas_hosts"
        res = self.query(sqlQuery)


    def close(self):
        """
        Close the DB connection.

        Returns:    Void.
        """
        T = TRACE()

        try:
            self.takeDbSem()
            if (self.__dbDrv): self.__dbDrv.close()
            self.relDbSem()
        except Exception, e:
            self.relDbSem()
            raise Exception, e


    def addDbChangeEvt(self,
                       evtObj):
        """
        Add an Event Object (threading.Event) that will be triggered to
        indicate other threads that DB changes where introdiced.

        evtObj:        Event object (threading.Event).

        Returns:       Reference to object itself.
        """
        self.__dbChangeEvents.append(evtObj)
        return self


    def triggerEvents(self,
                      eventInfo = None):
        """
        Set the Event Objects to inform other threads about DB changes.

        eventInfo:    Piece of information to be transferred from one
                      thread to another (free format).

        Returns:      Reference to object itself.
        """
        try:
            self.takeDbSem()
            for evtObj in self.__dbChangeEvents:
                if (eventInfo): evtObj.addEventInfo(eventInfo)
                if (not evtObj.isSet()): evtObj.set()
            self.relDbSem()
            return self
        except Exception, e:
            self.relDbSem()
            raise Exception, e


    def query(self,
              sqlQuery,
              ignoreEmptyRes = 1,
              maxRetries = None,
              retryWait = None):
        """
        Perform a query in the DB and return the result.

        sqlQuery:         SQL query (string).

        ignoreEmptyRes:   If set to 1, no error will be assumed if a
                          completely empty result is returned (integer/0|1).

        maxRetries:       Max. number of retries in case of failure (integer).

        retryWait:        Time in seconds to wait for next retry (float).

        Returns:          Result of SQL query (list).
        """
        if (not maxRetries): maxRetries = self.__maxRetries
        if (not retryWait): retryWait = self.__retryWait

        dbTimer = ngamsDbTimer(self, sqlQuery)
        try:
            self.takeDbSem()
            res = self._query(sqlQuery, ignoreEmptyRes, 0, maxRetries,
                              retryWait)
            self.relDbSem()
            info(4, "Accumulated DB access time: %.6fs" % self.getDbTime())
            return res
        except Exception, e:
            self.relDbSem()
            raise Exception, e


    def _query(self,
               sqlQuery,
               ignoreEmptyRes,
               recurseLevel,
               maxRetries = 10,
               retryWait = 1.0):
        """
        Perform a query in the DB and return the result. The method may do
        this in a recursive manner (re-trying) in case it fails in executing
        the query. Also, if the connection is lost, it will be tried
        automatically to reconnect.

        NOTE: The invocation of this method should be semaphore protected.

        sqlQuery:         SQL query (string).

        ignoreEmptyRes:   If set to 1, no error will be assumed if a
                          completely empty result is returned (integer/0|1).

        recurseLevel:     Indicates the number of times this method was
                          called recursively (integer).

        maxRetries:       Max. number of retries in case of failure (integer).

        retryWait:        Time in seconds to wait for next retry (float).

        Returns:          Result of SQL query (list).
        """
        T = TRACE(5)

        if (getVerboseLevel() > 4):
            info(5, "Performing SQL query: " + str(sqlQuery))

        #####################################################################
        # Make it possible to control a simulated DB error externally if
        # Test Mode is enabled.
        #####################################################################
        if (getTestMode()):
            fo = None
            try:
                fo = open("/tmp/ngamsDbBaseError.tmp")
                provokeDbErr = int(fo.read())
                fo.close()
            except:
                try:
                    if (fo): fo.close()
                except:
                    pass
                provokeDbErr = 0
            # Simulate a DB communication problem if requested.
            if (provokeDbErr):
                errMsg = genLog("NGAMS_ER_DB_COM",
                                ["Error: connection is not open"])
                raise Exception, errMsg
        #####################################################################

        res = []
        startTime = time.time()
        try:
            res = self.__dbDrv.query(sqlQuery)
        except Exception, e:
            errMsg = genLog("NGAMS_ER_DB_COM", [str(e)])
            error(errMsg)

            # In case of an exception: Try to reconnect if the Recursive
            # Level is not higher than maxRetries.
            if (recurseLevel < maxRetries):
                msg = "Reconnecting to DB server. Recursive depth: %d/" +\
                      "Test Mode: %d ..."
                notice(msg % (recurseLevel, getTestMode()))
                if (getTestMode()):
                    time.sleep(0.2)
                else:
                    time.sleep(retryWait)
                recurseLevel += 1
                self._connect(self.__dbServer, self.__dbName, self.__dbUser,
                              self.__dbPassword, self.__dbInterface, 0)
                notice("Reconnected to DB server")
                notice("Query: %s" % sqlQuery)
                res = self._query(sqlQuery, ignoreEmptyRes, recurseLevel,
                                  maxRetries, retryWait)
            else:
                notice("Abandonned reconnectiong attempt. " +\
                       "Recursive level: %d" % recurseLevel)
                raise Exception, errMsg

        # Return the query result, or try up to 10 times to perform the query
        # with a 1s delay between each attempt. If all attempts to query the DB
        # fail, return an error in case the result was empty and such a result
        # should be reported as an error.
        if (getVerboseLevel() > 4):
            info(5, "Result of SQL query (" + str(sqlQuery) + "): " + str(res))
        if ((res == []) and (not ignoreEmptyRes) and
            (sqlQuery.find("SELECT ") != -1)):
            msg = "Unexpected result returned from SQL query: %s: %s"
            warning(msg % (str(sqlQuery), str(res)))
            if (recurseLevel < 10):
                msg = "Retrying query: %s/Recursive depth: %d/" +\
                      "Test Mode: %d ..."
                notice(msg % (sqlQuery, recurseLevel, getTestMode()))
                if (getTestMode()):
                    time.sleep(0.1)
                else:
                    time.sleep(0.5)
                recurseLevel += 1
                res = self._query(sqlQuery, ignoreEmptyRes, recurseLevel)
            else:
                raise Exception, msg % (str(sqlQuery), str(res))
        return res


    def dbCursor(self,
                 sqlQuery):
        """
        Create a DB Cursor Object (defined in the NG/AMS DB Interface Plug-In)
        on the given query and return the cursor object.

        sqlQuery:        SQL query for the cursor (string).

        Return:          Cursor object instance (<Cursor Object>).
        """
        T = TRACE()

        try:
            if (getVerboseLevel() > 4):
                info(5, "Performing SQL query (using a cursor): " +\
                     str(sqlQuery))
            self.takeDbSem()
            curObj = self.__dbDrv.cursor(sqlQuery)
            self.relDbSem()
        except Exception, e:
            self.relDbSem()
            errMsg = genLog("NGAMS_ER_DB_COM", [str(e)])
            error(errMsg)
            raise Exception, errMsg
        return curObj


    def getNgasFilesMap(self):
        """
        Return the reference to the map (dictionary) containing the mapping
        between the column name and index of the ngas_files table.

        Returns:    Reference to NGAS Files Table name map (dictionary).
        """
        return _ngasFilesNameMap


    def convertTimeStamp(self,
                         timeStamp):
        """
        Convert a timestamp given in one of the following formats:

          1. ISO 8601:  YYYY-MM-DDTHH:MM:SS[.s]
          2. ISO 8601': YYYY-MM-DD HH:MM:SS[.s]
          3. Secs since epoc.

        to a format which can be used to write into the associated RDBMS.

        The actual conversion must be done by the loaded NGAMS DB Driver
        Plug-In.

        timeStamp:    Timestamp (string|integer|float).

        Returns:      Timestamp in format, which can be written into
                      'datetime' column of the DBMS (string).
        """
        return self.__dbDrv.convertTimeStamp(timeStamp)


    def convertTimeStampToMx(self,
                             timeStamp):
        """
        Converts an ISO 8601 timestamp into an mx.DateTime object.

        timeStamp:  ISO 8601 datetime string (string).

        Returns:    Date time object (mx.DateTime).
        """
        T = TRACE()

        return self.__dbDrv.convertTimeStampToMx()


    def addSrvList(self,
                   srvList):
        """
        Add a server list in the NGAS Server List Table and allocate a unique
        server list ID for it.

        srvList:   List of servers to add (string).

        Returns:   New server list ID (integer).
        """
        T = TRACE()

        # Find a free ID.
        srvListId = -1
        while (True):
            srvListId = int(2**31 * random.random())
            if (self.getSrvListFromId(srvListId) == None): break

        # Write the new entry to the list.
        srvlist = cleanSrvList(srvList)
        creationDate = self.convertTimeStamp(timeRef2Iso8601(time.time()))
        sqlQuery = "INSERT INTO ngas_srv_list " +\
                   "(srv_list_id, srv_list, creation_date) VALUES " +\
                   "(%d, '%s', '%s')"
        sqlQuery = sqlQuery % (srvListId, srvlist, creationDate)
        res = self.query(sqlQuery)
        return srvListId


    def getSrvListFromId(self,
                         srvListId):
        """
        Get a server list from its ID. If no list with that ID is found,
        None is returned.

        srvListId:   Server list ID (integer).

        Returns:     Server list associated with the given ID (string|None).
        """
        T = TRACE()

        sqlQuery = "SELECT srv_list FROM ngas_srv_list WHERE " +\
                   "srv_list_id=%d" % srvListId
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if ((len(res) > 0) and (res != [[]])):
            srvList = res[0][0][0]
        else:
            srvList = None
        return srvList


    def getSrvListIdFromSrvList(self,
                                srvList,
                                autoAlloc = True):
        """
        Get the server list ID associated with the server list. If not defined,
        a new can be allocated in the NGAS Servers Table automatically.

        srvList:     Server list ('<host>:<port>,...') (string).

        autoAlloc:   If True and no entry was found, a new entry is
                     automatically created for that server list (boolean).

        Returns:     Server list ID (integer).
        """
        T = TRACE()

        srvList = cleanSrvList(srvList)
        sqlQuery = "SELECT srv_list_id FROM ngas_srv_list WHERE " +\
                   "srv_list='%s'" % srvList
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if ((len(res) > 0) and (res != [[]])):
            srvListId = int(res[0][0][0])
        else:
            srvListId = -1
        if ((srvListId == -1) and autoAlloc):
            srvListId = self.addSrvList(srvList)
        return srvListId


# EOF
