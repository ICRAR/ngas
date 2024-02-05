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

import importlib
import logging
import random
import tempfile
import threading
import time

from DBUtils.PooledDB import PooledDB
import six

from .ngamsCore import toiso8601, fromiso8601

# Global DB Semaphore to protect critical, global DB interaction.
_globalDbSem = threading.Semaphore(1)

logger = logging.getLogger(__name__)

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
                 ["nf.file_ignore",            "NGAS_FILES_IGNORE"],
                 ["nf.checksum",               "NGAS_FILES_CHECKSUM"],
                 ["nf.checksum_plugin",        "NGAS_FILES_CHECKSUM_PI"],
                 ["nf.file_status",            "NGAS_FILES_FILE_STATUS"],
                 ["nf.creation_date",          "NGAS_FILES_CREATION_DATE"],
                 ["nf.io_time",                "NGAS_FILES_IO_TIME"],
                 ["nf.ingestion_rate",         "NGAS_FILES_INGEST_RATE"],
                 ["nf.container_id",           "NGAS_FILES_CONTAINER_ID"],
                 ]
_ngasFilesNameMap = {}
idx = 0
for colDef in _ngasFilesDef:
    exec(colDef[1] + "=%d" % idx)
    colName = colDef[0].split(".")[1]
    _ngasFilesNameMap[idx] = colName
    _ngasFilesNameMap[colName] = idx
    idx += 1

def getNgasFilesCols(file_ignore_columnname):
    """
    Return reference to a string defining the lay-out of the table.

    Returns:   Reference to string listing all columns (string).
    """
    colnames = []
    for colDef in _ngasFilesDef:
        colname = colDef[0]
        if colname == 'nf.file_ignore':
            colnames.append('nf.%s' % (file_ignore_columnname,))
        else:
            colnames.append(colname)
    return ', '.join(colnames)

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
idx = 0
for colDef in _sum1Def:
    exec(colDef[1] + "=%d" % idx)
    idx += 1

def getNgasSummary1Cols(file_ignore_columnname):
    """
    Return reference to a string defining the lay-out of the table.

    Returns:   Reference to string listing all columns (string).
    """
    colnames = []
    for colDef in _sum1Def:
        colname = colDef[0]
        if colname == 'nf.file_ignore':
            colnames.append('nf.%s' % (file_ignore_columnname,))
        else:
            colnames.append(colname)
    return ", ".join(colnames)

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

    def __init__(self, dbConObj, query):
        self.__dbConObj = dbConObj
        self.__query = query

    def __enter__(self):
        self.__startTime = time.time()
        return self

    def __exit__(self, typ, value, traceback):
        deltaTime = (time.time() - self.__startTime)
        self.__dbConObj.updateDbTime(deltaTime)
        logger.debug("DB-TIME: Time spent for DB query: |%s|: %.6fs", self.__query, deltaTime)

def cleanSrvList(srvList):
    """
    Clean the server list given. This means:

    - Removing irrelevant characters.
    - Sorting the servers.

    srvList:   List of servers: '<host>:<port>,...' (string).

    Returns:   Return the cleaned up list (string).
    """
    # Clean up the list, we ensure the servers are always listed in
    # alphabetical order.
    try:
        srvList = srvList.replace(" ", "").split(",")
        srvList.sort()
        srvList = str(srvList)[1:-1].replace("'", "").replace(" ", "")
    except Exception as e:
        msg = "Error cleaning up server list. Error: %s" % str(e)
        raise Exception(msg)

    return srvList


class ngamsDbCursor(object):
    """
    A class representing a cursor over which a query is run and where results
    are extracted from.
    """

    def __init__(self, pool, query, args):
        self.conn = None
        self.cursor = None
        try:
            self.conn = pool.connection()
            self.cursor = self.conn.cursor()
            self.cursor.execute(query, args)
        except:
            self.close()
            raise

    def __del__(self):
        try:
            self.close()
        except: pass

    def fetch(self, howmany):
        """
        Fetches at most ``howmany`` results from the database. If no more
        results are available it returns an empty sequence
        """
        rows = []
        for row in self.cursor.fetchmany(howmany):
            # This is a namedtuple, make a normal one instead
            # TODO: We're doing this because some users of these results
            # serialize them and are having problems while doing it; we ease
            # their life this way, but they should take care of it actually.
            # By users we mean ngamsDataCheckThread:598 for instance
            if isinstance(row, tuple) and hasattr(row, "_fields"):
                row = row[:]
            rows.append(row)
        return rows

    def close(self):
        """Closes the underlying cursor and connection"""
        if self.cursor:
            try:
                self.cursor.close()
            except: pass
        if self.conn:
            try:
                self.conn.close()
            except: pass

class cursor2(ngamsDbCursor):
    """A cursor that yields values and acts as a context manager"""

    def fetch(self, howmany):
        """
        Fetches at most ``howmany`` results from the database at a given time,
        yielding them instead of returning them as a sequence. This makes
        client code simpler to write.
        """
        while True:
            rows = self.cursor.fetchmany(howmany)
            if not rows:
                return
            for row in rows:
                yield row

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

class transaction(object):
    """
    A context manager that allows multiple SQL queries to be executed
    within a single transaction
    """

    def __init__(self, db_core, pool):
        self.db_core = db_core
        self.pool = pool

    def __enter__(self):
        self.conn = self.pool.connection()
        self.cursor = self.conn.cursor()
        return self

    def __exit__(self, typ, *_):

        # React accordingly
        t0 = time.time()
        if not typ:
            self.conn.commit()
        else:
            self.conn.rollback()
        t1 = time.time()
        logger.debug('Committed/rolled back transaction in %.2f [s]', t1 - t0)

        # Always close the cursor and the connection at the end
        for x in (self.cursor, self.conn):
            try:
                x.close()
            except:
                pass
        logger.debug('closed connection/cursor in %.2f [s]', time.time() - t1)

        # Re-raise original exception
        if typ:
            raise

    def execute(self, sql, args=()):
        """Executes `sql` using `args`"""

        # If we are passing down parameters we need to sanitize both the query
        # string (which should come with {0}-style formatting) and the parameter
        # list to cope with the different parameter styles supported by PEP-249
        logger.debug("Performing SQL query with parameters: %s / %r", sql, args)
        sql, args = self.db_core._prepare_query(sql, args)
        cursor = self.cursor

        with ngamsDbTimer(self.db_core, sql):

            # Some drivers complain when an empty argument list/tuple is passed
            # so let's avoid it
            if not args:
                cursor.execute(sql)
            else:
                cursor.execute(sql, args)

            # From PEP-249, regarding .description:
            # This attribute will be None for operations that do not return
            # rows [...]
            # We thus use it to distinguish between those cases when there
            # are results to fetch or not. This is important because fetch*
            # calls can raise Errors if there are no results generated from
            # the last call to .execute*
            res = []
            if cursor.description is not None:
                res = cursor.fetchall()
            return res

class ngamsDbCore(object):
    """
    Core class for the NG/AMS DB interface.
    """

    def __init__(self,
                 interface,
                 parameters = {},
                 createSnapshot = 1,
                 maxpoolcons = 6,
                 use_file_ignore=True,
                 session_sql=None,
                 use_prepared_statement=True):
        """
        Creates a new ngamsDbCore object using ``interface`` as the underlying
        PEP-249-compliant database connection driver. Connections creation
        parameters are given via ``parameters``.

        This object maintains a pool of connections to avoid connection
        creation overheads. The maximum amount of connections held in the pool
        is set via ``maxpoolcons``.

        Finally, some combinations of old versions of NGAS and database engines
        used a different column name for the same field in the "ngas_files"
        table. ``use_file_ignore`` controls this behavior to provide
        backwards-compatibility. If true, the code will use "file_ignore" for
        the column name as opposed to "ignore".
        """
        self.__dbSem = threading.Lock()

        # Controls if the snapshot of the DB should be created.
        self.__createSnapshot = createSnapshot

        # List of Event Objects which are used to inform other instances
        # about changes in the DB.
        self.__dbChangeEvents = []

        # Timer for analyzing time spent for DB access
        self.__dbAccessTime = 0.0

        # Import the DB Interface Plug-In (PEP-249 compliant)
        logger.info("Importing DB Module: %s", interface)
        self.module_name = interface
        self.__dbModule = importlib.import_module(interface)
        logger.info("DB Module param style: %s", self.__dbModule.paramstyle)
        logger.info("DB Module API Level: %s", self.__dbModule.apilevel)
        self.__paramstyle = self.__dbModule.paramstyle

        logger.info('Preparing database pool with %d connections. Initial SQL: %s', maxpoolcons, session_sql)
        self.__pool = PooledDB(self.__dbModule,
                                maxshared = maxpoolcons,
                                maxconnections = maxpoolcons,
                                blocking = True,
                                setsession=session_sql,
                                **parameters)

        self.__dbTmpDir      = "/tmp"

        self._use_file_ignore = use_file_ignore
        self._file_ignore_columnname = 'file_ignore' if use_file_ignore else 'ignore'
        self._use_prepared_statement = use_prepared_statement

    @property
    def file_ignore_columnname(self):
        return self._file_ignore_columnname

    def takeGlobalDbSem(self):
        """
        Acquire access to a critical, global DB interaction.

        Returns:   Reference to object itself.
        """
        # TODO: Check if really needed with a Global DB Semaphore (if yes
        #       write the reason in the documentation).
        logger.debug("Taking Global DB Access Semaphore")
        global _globalDbSem
        _globalDbSem.acquire()


    def relGlobalDbSem(self):
        """
        Release acquired access to a critical, global DB interaction.

        Returns:   Reference to object itself.
        """
        # TODO: Check if really needed with a Global DB Semaphore (if yes
        #       write the reason in the documentation).
        logger.debug("Releasing Global DB Access Semaphore")
        global _globalDbSem
        _globalDbSem.release()


    def updateDbTime(self, dbAccessTime):
        """
        Update the DB access timer.

        dbAccessTime:   DB access time to add in seconds (float).

        Returns:  Reference to object itself.
        """
        self.__dbAccessTime += dbAccessTime
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


    def setDbTmpDir(self,
                    tmpDir):
        """
        Set the DB temporary directory.

        tmpDir:        Temporary directory (string).

        Returns:       Reference to object itself.
        """
        self.__dbTmpDir = tmpDir


    def genTmpFile(self, fname):
        return tempfile.mktemp(fname, dir=self.__dbTmpDir)


    def getCreateDbSnapshot(self):
        """
        Return the flag indicating if a DB Snapshot should be created or not.

        Returns:  Value of the DB Snapshot Creation Flag (boolean).
        """
        return self.__createSnapshot


    def close(self):
        """
        Close the DB pool.

        Returns:    Void.
        """
        self.__pool.close()


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
            self.__dbSem.acquire()
            for evtObj in self.__dbChangeEvents:
                if eventInfo:
                    evtObj.addEventInfo(eventInfo)
                if not evtObj.isSet():
                    evtObj.set()
            return self
        finally:
            self.__dbSem.release()


    def _named_marker(self, i):
        # We know that the Sybase module uses @ named markers
        # Everyone else (so far) is pretty sensible
        if self.module_name == 'Sybase':
            return '@n%d' % i
        return ':n%d' % i

    def _named_key(self, i):
        # See above
        if self.module_name == 'Sybase':
            return '@n%d' % i
        return 'n%d' % i


    def _markers(self, howMany):
        # Depending on the different vendor, we need to write the parameters in
        # the SQL calls using different notations. This method will produce an
        # array containing all the parameter _references_ in the SQL statement
        #
        # qmark     Question mark style, e.g. ...WHERE name=?
        # numeric   Numeric, positional style, e.g. ...WHERE name=:1
        # named     Named style, e.g. ...WHERE name=:name
        # format    ANSI C printf format codes, e.g. ...WHERE name=%s
        # pyformat  Python extended format codes, e.g. ...WHERE name=%(name)s
        #
        s = self.__paramstyle
        if s == 'qmark':    return ['?'                   for i in range(howMany)]
        if s == 'numeric':  return [':%d'%(i)             for i in range(howMany)]
        if s == 'named':    return [self._named_marker(i) for i in range(howMany)]
        if s == 'format':   return ['%s'                  for i in range(howMany)]
        if s == 'pyformat': return ['%%(n%d)s'%(i)        for i in range(howMany)]
        raise Exception('Unknown paramstyle: %s' % (s))

    def _format_query(self, sql, args):
        return sql.format(*self._markers(len(args)))

    def _data_to_bind(self, data):
        if self.__paramstyle == 'named':
            return {self._named_key(i): d for i,d in enumerate(data)}
        elif self.__paramstyle == 'pyformat':
            return {'n%d'%(i): d for i,d in enumerate(data)}
        return data

    def _prepare_query(self, sql, args):

        if not self._use_prepared_statement:
            markers = ["'{}'" if isinstance(arg, str) else "{}" for arg in args]
            return sql.format(*markers).format(*args), ()

        # Depending on the database vendor and its declared paramstyle
        # we will need to escape '%' literals so they are not considered
        # a parameter in the query
        if self.__paramstyle in ('format', 'pyformat') and '%' in sql:
            sql = sql.replace('%', '%%')

        if args:
            sql = self._format_query(sql, args)
            args = self._data_to_bind(args)

        return sql, args

    def transaction(self):
        """Creates a new transaction object and return it"""
        return transaction(self, self.__pool)

    def query2(self, sqlQuery, args = ()):
        """Takes an SQL query and a tuple of arguments to bind to the query"""
        with self.transaction() as t:
            return t.execute(sqlQuery, args)


    def dbCursor(self, sqlQuery, args=()):
        """
        Create a cursor on the given query and return the cursor object.
        """

        logger.debug("Performing SQL query (using a cursor): %s / %r", sqlQuery, args)
        sqlQuery, args = self._prepare_query(sqlQuery, args)
        return cursor2(self.__pool, sqlQuery, args)

    def getNgasFilesMap(self):
        """
        Return the reference to the map (dictionary) containing the mapping
        between the column name and index of the ngas_files table.

        Returns:    Reference to NGAS Files Table name map (dictionary).
        """
        return _ngasFilesNameMap

    def convertTimeStamp(self, t):
        """
        Convert a timestamp given in one of the following formats:
        """
        if isinstance(t, six.string_types):
            return t
        else:
            return toiso8601(t, local=True)

        # TODO: we can only start using the code below once we finish porting
        # all the code that calls this method; until then we have to keep
        # returning a simple string
        #((year, mon, mday, hour, mins, sec, _, _, _), _) = ts.mjdToTm(ts.getMjd())
        #return self.__dbModule.Timestamp(year , mon , mday , hour , mins, sec)

    def asTimestamp(self, t):
        """
        Returns `None` if timestamp is `None`, otherwise calls convertTimeStamp
        """
        if t is None:
            return None
        return self.convertTimeStamp(t)

    def fromTimestamp(self, timestamp):
        """
        Converts a database timestamp into a number of seconds from the epoch.
        This is the reverse of `asTimestamp`.
        """
        if timestamp is None:
            return None
        return fromiso8601(timestamp, local=True)

    def addSrvList(self,
                   srvList):
        """
        Add a server list in the NGAS Server List Table and allocate a unique
        server list ID for it.

        srvList:   List of servers to add (string).

        Returns:   New server list ID (integer).
        """
        # Find a free ID.
        srvListId = -1
        while True:
            srvListId = int(2**31 * random.random())
            if self.getSrvListFromId(srvListId) is None:
                break

        # Write the new entry to the list.
        srvlist = cleanSrvList(srvList)
        creationDate = self.asTimestamp(time.time())
        sql = "INSERT INTO ngas_srv_list (srv_list_id, srv_list, creation_date) " +\
              "VALUES ({0}, {1}, {2})"
        self.query2(sql, args=(srvListId, srvlist, creationDate))
        return srvListId


    def getSrvListFromId(self,
                         srvListId):
        """
        Get a server list from its ID. If no list with that ID is found,
        None is returned.

        srvListId:   Server list ID (integer).

        Returns:     Server list associated with the given ID (string|None).
        """
        sql = "SELECT srv_list FROM ngas_srv_list WHERE srv_list_id={0}"
        res = self.query2(sql, args=(srvListId,))
        if res:
            return res[0][0]
        return None


    def getSrvListIdFromSrvList(self, srvList):
        """
        Get the server list ID associated with the server list. If not defined,
        a new can be allocated in the NGAS Servers Table automatically.

        srvList:     Server list ('<host>:<port>,...') (string).

        Returns:     Server list ID (integer).
        """
        srvList = cleanSrvList(srvList)
        sql = "SELECT srv_list_id FROM ngas_srv_list WHERE srv_list={0}"
        res = self.query2(sql, args=(srvList,))
        if res:
            return int(res[0][0])

        return self.addSrvList(srvList)
