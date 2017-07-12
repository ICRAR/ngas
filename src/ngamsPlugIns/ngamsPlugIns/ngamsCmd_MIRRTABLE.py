#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) European Southern Observatory, 2009
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
# "@(#) $Id: ngamsCmd_MIRRTABLE.py,v 1.6 2010/06/22 18:55:14 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jagonzal  2009/12/14  Created
#
"""
NGAS Command Plug-In, implementing a command to fill the mirroring_bookkeping_table

NOTES: By default only the last version found in the source cluster is synchronized
       to the target cluster. To synchronize all existing versions specify all_versions=1.
       In case of remote data-base mirroring it is necessary to enable data-base links
       to the source/target data-bases in the local data-base. If the target data-base
       is the local one is not neccesary to specify a target data-base link, (by default
       the local data base is the target data base)

PARAMETERS:
	-source_cluster	[mandatory] source cluster name
	-target_cluster	[mandatory] target cluster name
	-source_dbl	[optional] source archive data base link (remote data base mirroring)
	-target_dbl	[optional] target archive data base link (remote data base mirroring)
	-all_versions	[optional] (=1) mirror all missing versions of each file, default (=0)
	-archive_cmd	[optional] custom archive command, default (=MIRRARCHIVE)
	-retriev_cmd	[optional] custom retrieve command, default (=RETRIEVE)

EXAMPLES:
	- Local data base mirroring with custom ARCHIVE command
		http://ngas05.hq.eso.org:7778/MIRRTABLE?target_cluster=ngas05:7778&source_cluster=ngas02:7778&archive_cmd=QARCHIVE?

"""

import logging
import re
import thread
import time

from ngamsLib import ngamsHttpUtils
from ngamsLib.ngamsCore import toiso8601, FMT_DATETIME_NOMSEC
from .alma.almaMirroringTargetNodeAssigner import TargetVolumeAssigner

logger = logging.getLogger(__name__)

def handleCmd(srvObj,
              reqPropsObj,
              httpRef):
    """
    Handle Command MIRRTABLE to populate bookkeeping table in target cluster

    INPUT:
        srvObj:         ngamsServer, Reference to NG/AMS server class object

        reqPropsObj:    ngamsReqProps, Request Property object to keep track
                        of actions done during the request handling

       httpRef:        ngamsHttpRequestHandler, Reference to the HTTP request
                        handler object

    RETURNS:        Void.
    """

    # Get command parameters.
    target_cluster = srvObj.getCfg().getVal("Mirroring[1].target_cluster")
    if (reqPropsObj.hasHttpPar("target_cluster")):
        target_cluster = reqPropsObj.getHttpPar("target_cluster")
    source_cluster = srvObj.getCfg().getVal("Mirroring[1].source_cluster")
    if (reqPropsObj.hasHttpPar("source_cluster")):
        source_cluster = reqPropsObj.getHttpPar("source_cluster")
    source_dbl = "@" + srvObj.getCfg().getVal("Mirroring[1].source_dbl")
    if (reqPropsObj.hasHttpPar("source_dbl")):
        source_dbl = "@" + reqPropsObj.getHttpPar("source_dbl")
    target_dbl = ""
    if (reqPropsObj.hasHttpPar("target_dbl")):
        target_dbl = "@" + reqPropsObj.getHttpPar("target_dbl")
    all_versions = srvObj.getCfg().getVal("Mirroring[1].all_versions")
    if (reqPropsObj.hasHttpPar("all_versions")):
        all_versions = int(reqPropsObj.getHttpPar("all_versions"))

    # what was the date of the last succesful mirroring iteration? We will only compare files
    # from local and remote databases from that date (the start of the iteration, just to allow
    # some error margin)
    siteId = getCurrentSite(srvObj)
    validArcs = ['EU', 'EA', 'NA', 'SCO', 'OSF']
    if not (siteId in validArcs): raise Exception("Can not mirror, The table ngas_cfg_pars_properties does not contain an element 'siteId' with one of these values: " + str(validArcs))

    # we're overlapping mirroring iterations now. First check is to see if there are any spare threads
    # for this iteration to use
    numAvailableDownloadThreads = getNumAvailableDownloadThreads(srvObj)
    if numAvailableDownloadThreads <= 0:
        logger.info("All the available download threads are busy. Skipping this mirroring iteration.")
        return
    logger.info("total num threads available for mirroring in the cluster: %d", numAvailableDownloadThreads)

    baselineDate = getMirroringBaselineDate(srvObj)
    if baselineDate is None or baselineDate == "None":
        startDate = "None"
    else:
        # work out the time window to compare files. We use the last succesful iteration and then subtract 100 for safety
        startDate = findDateOfLastSuccessfulMirroringIteration(srvObj)
        # however, if this is the first mirroring iteration of a new day then we perform a complete mirror
        lastIteration = findDateOfLastMirroringIteration(srvObj)
        if (lastIteration is None or lastIteration == "None" or lastIteration[8:10] != time.strftime('%d', time.localtime())):
            logger.info('performing a full mirroring')
            startDate = "None"
        # unless, of course, the baselineDate has been set. We never ever extend beyond that.
        if startDate == "None" and baselineDate:
            logger.info('using the baseline data from the config table: %s', baselineDate)
            startDate = baselineDate + "T00:00:00:000"
    rows_limit = getIterationFileLimit(srvObj)

    # Get source cluster active nodes
    source_active_nodes = get_cluster_active_nodes(source_dbl, source_cluster, srvObj)

    if len(source_active_nodes) == 0:
        logger.warning("there are no active source nodes. Skipping this iteration.")
    else:
        # Construct sub-query for source cluster
        source_query = generate_source_files_query(srvObj, startDate, source_dbl, source_cluster, all_versions)
        logger.debug("SQL sub-query to get source cluster files-hosts information: %s", source_query)

        # Construct sub-query for target cluster
        target_query = generate_target_files_query(srvObj, startDate, target_cluster, all_versions)
        logger.debug("SQL sub-query to get target cluster files-hosts information: %s", target_query)

        # Construct sub-query for  table
        diff_query = generate_diff_ngas_files_query(source_query, target_query)
        logger.debug("SQL sub-query to get diff between source and target files: %s", diff_query)

        # Get iteration nmumber
        iteration = get_mirroring_iteration(srvObj)

        # Populate book keeping table
        logger.debug("Populating ngas_mirroring_bookkeeping_table, source_cluster=%s , target_cluster=%s, all_versions=%s",
             source_cluster + source_dbl, target_cluster + target_dbl, str(all_versions))
        populate_mirroring_bookkeeping_table(diff_query, startDate, target_dbl, target_cluster, iteration, srvObj)

        # grab any toresume fetches for this iteration
        reassign_broken_downloads(iteration, srvObj)

        # de-schedule any files which have been blocked from the SCO
        deschedule_exclusions(iteration, 'EU', source_dbl, srvObj);

        # limit the number of files that we will fetch in a single iteration
        if rows_limit is not None and rows_limit != 'None':
            limit_mirrored_files(srvObj, iteration, rows_limit)

        # remove the source nodes which do not have any files for mirroring during this iteration
        working_source_nodes = remove_empty_source_nodes(iteration, source_active_nodes, target_cluster, srvObj)

        # Assign book keeping table entries
        logger.info("Updating entries in ngas_mirroring_bookkeeping_table to assign target nodes")
        totalFilesToMirror = assign_mirroring_bookkeeping_entries(iteration, working_source_nodes, target_cluster, srvObj)

        logger.info("There are %d files are to be mirrored in iteration %d", totalFilesToMirror, iteration)
        if totalFilesToMirror > 0:
            thread.start_new_thread(executeMirroring, (srvObj, iteration))
    return

def executeMirroring(srvObj, iteration):
    logger.info('executeMirroring for iteration %d', iteration)
    try:
        rx_timeout = 30 * 60
        if srvObj.getCfg().getVal("Mirroring[1].rx_timeout"):
            rx_timeout = int(srvObj.getCfg().getVal("Mirroring[1].rx_timeout"))

        pars = {
            'mirror_cluster': 2,
            'iteration': str(iteration),
            'rx_timeout': rx_timeout,
            'n_threads': getNumberOfSimultaneousFetchesPerServer(srvObj)
        }
        host, port = srvObj.get_endpoint()
        ngamsHttpUtils.httpGet(host, port, 'MIRREXEC', pars=pars, timeout=rx_timeout)
        # TODO look at the response code

    except Exception:
        logger.exception("Mirroring failed")
    finally:
        failRemainingTransfers(srvObj, iteration);
        logger.info('executeMirroring for iteration %d complete', iteration)

    # remove some of the older bookkeeping entries
    clean_mirroring_bookkeeping_entries(srvObj)

def getNumAvailableDownloadThreads(srvObj):
    sql = "select numHosts * fetchesPerServer - currentFetches as availableFetches"
    sql += " from"
    sql += " ( select count(distinct(c.host_id)) as numHosts"
    sql += "   from ngas_cfg_pars p"
    sql += "     inner join ngas_hosts h on h.host_id = p.cfg_val"
    sql += "     inner join ngas_hosts c on h.cluster_name = c.cluster_name"
    sql += "     inner join ngas_disks d on d.last_host_id = c.host_id"
    sql += "   where p.cfg_par = 'masterNode' and d.completed = 0),"
    sql += " ( select count(*) as currentFetches"
    sql += "   from ngas_mirroring_bookkeeping"
    sql += "   where status = 'FETCHING'),"
    sql += " ( select cfg_val as fetchesPerServer"
    sql += "   from ngas_cfg_pars"
    sql += "   where cfg_par = 'numParallelFetches')"

    res = srvObj.getDb().query2(sql)
    numAvailable = int(res[0][0])
    return numAvailable

def failRemainingTransfers(srvObj, iteration):
    logger.info('making sure there are no LOCKED or READY entries left for iteration %d', iteration)
    sql = "update ngas_mirroring_bookkeeping set status = 'FAILURE' where status in ('LOCKED', 'READY') and iteration = {0}"
    srvObj.getDb().query2(sql,args=(iteration,))


def reassign_broken_downloads(currentIteration, srvObj):
    sql = "insert into ngas_mirroring_bookkeeping"
    sql += " (file_id, file_version, file_size, disk_id, host_id, format, status,"
    sql += " target_cluster, target_host, source_host, "
    sql += " ingestion_date, ingestion_time, iteration, checksum, staging_file, attempt, source_ingestion_date)"
    sql += " select file_id, file_version, file_size, disk_id, host_id, format, 'READY' as status,"
    sql += " target_cluster, target_host, source_host,"
    sql += " ingestion_date, ingestion_time, {0} as iteration, checksum, staging_file, attempt, source_ingestion_date"
    sql += " from ngas_mirroring_bookkeeping "
    sql += " where status = 'TORESUME'"
    srvObj.getDb().query2(sql, args=(currentIteration,))

    # we've grabbed
    sql = "update ngas_mirroring_bookkeeping"
    sql += " set status = 'FAILURE',staging_file = null"
    sql += " where status = 'TORESUME' and iteration < {0}"
    srvObj.getDb().query2(sql, args=(currentIteration,))

def clean_mirroring_bookkeeping_entries(srvObj):
    # TBD parameterise the time period
    logger.info('cleaning up mirroring bookkeeping table - removing entries older than 60 days')

    query = "delete from ngas_mirroring_bookkeeping where iteration < ("
    query += "select max(iteration) from ("
    query += "select iteration from ngas_mirroring_bookkeeping"
    query += " group by iteration having min(status) = 'SUCCESS'"
    query += " and substr(max(ingestion_date), 1, 10) < to_char(sysdate - 60, 'YYYY-MM-DD')))"

    srvObj.getDb().query2(query)

def findDateOfLastMirroringIteration(srvObj):
    query = "select min(ingestion_date)"
    query += " from ngas_mirroring_bookkeeping"
    query += " where iteration = ("
    query += " select max(iteration) from ngas_mirroring_bookkeeping)"

    # Execute query
    res = srvObj.getDb().query2(query)
    timestamp = str(res[0][0])

    # Log info
    logger.info("the date of the last mirroring iteration was %s", timestamp)

    # Return void
    return timestamp

def findDateOfLastSuccessfulMirroringIteration(srvObj):
    query = "select min(ingestion_date)"
    query += " from ngas_mirroring_bookkeeping"
    query += " where iteration = ("
    query += " select max(iteration) - 100 from ("
    query += " select iteration from ngas_mirroring_bookkeeping"
    query += " group by iteration having count(distinct(status)) = 1"
    query += " and min(status) = 'SUCCESS'))"

    # Execute query
    res = srvObj.getDb().query2(query)
    timestamp = str(res[0][0])

    # Log info
    logger.info("The start of the sliding window is %s", timestamp)

    # Return void
    return timestamp

def getNumberOfSimultaneousFetchesPerServer(srvObj):
    query = "select cfg_val"
    query += " from ngas_cfg_pars"
    query += " where cfg_par = 'numParallelFetches'"

    res = srvObj.getDb().query2(query)
    n_threads = str(res[0][0])
    if n_threads == None or n_threads == "None":
        n_threads = 5
    return n_threads

def getMirroringBaselineDate(srvObj):
    query = "select cfg_val"
    query += " from ngas_cfg_pars"
    query += " where cfg_par = 'baselineDate'"

    res = srvObj.getDb().query2(query)
    baselineDate = str(res[0][0])

    logger.info("mirroring baseline date is %s", baselineDate)
    return baselineDate

def getIterationFileLimit(srvObj):
    query = "select cfg_val"
    query += " from ngas_cfg_pars"
    query += " where cfg_par = 'iterationFileLimit'"

    res = srvObj.getDb().query2(query)
    limit = str(res[0][0])
    return limit

def getCurrentSite(srvObj):
    query = "select cfg_val"
    query += " from ngas_cfg_pars"
    query += " where cfg_par = 'siteId'"

    res = srvObj.getDb().query2(query)
    siteId = str(res[0][0])
    if siteId == None or siteId == "None":
        siteId = '?'
    return siteId

def generate_source_files_query(srvObj,startDate, db_link,
                                       cluster_name,
                                       all_versions):
    """
    Specify an alias table that extends ngas_files
    table including host_id/domain/srv_port information

    INPUT:
        db_link        string, Name for the data base link hosting the cluster
    cluster_name    string, Name of the cluster involved in the operation
    all_versions    int, Parameter to determine if all-versions mode is desired

    RETURNS:
        query       string, Sub-Query to be aliased in a whole query
    """

    # Query create table statement (common)
    query = "(select nf.file_id file_id, nf.file_version file_version, "
    query += "min(ingestion_date) ingestion_date, "
    query += "substr(min(nf.disk_id || nh.host_id || nh.srv_port),1,length(min(nd.disk_id))) disk_id, "
    query += "substr(min(nf.disk_id || nh.host_id || nh.srv_port),1+length(min(nd.disk_id)),length(min(nh.host_id))) host_id, "
    query += "substr(min(nf.disk_id || nh.host_id || nh.srv_port),1+length(min(nd.disk_id))+length(min(nh.host_id)),length(min(nh.srv_port))) srv_port, "
    query += "min(nf.format) format, min(nf.file_size) file_size, min(nf.checksum) checksum, min(nh.domain) domain "
    query += "from "
    # Depending on all_versions parameter we select only last version or not
    if all_versions:
        query += "ngas_files" + db_link + " nf inner join ngas_disks" + db_link + " nd on nd.disk_id = nf.disk_id "
        query += "inner join ngas_hosts" + db_link + " nh on nh.host_id = nd.host_id "
    else:
        query += "(select file_id, max(file_version) file_version from ngas_files" + db_link + " group by file_id) nflv "
        query += "inner join ngas_files" + db_link + " nf on nf.file_id = nflv.file_id and nf.file_version = nflv.file_version "
        query += "inner join ngas_disks" + db_link + " nd on nd.disk_id = nf.disk_id "
        query += "inner join ngas_hosts" + db_link + " nh on nh.host_id = nd.host_id "
    # for clarity,I used to insert all entries and then remove them according to the exclusion rules in a second step.
    # The problem with that (in the dev environment) is the performance. It brings over way more rows from the source
    # DB than is required.
    query += " left join alma_mirroring_exclusions c "
    # replace(ingestion_date, ':60.000', ':59.999'):
    # some "timestamps" (stored as varchar2 in Oracle) have 60 as the seconds value due to an
    # earlier NGAS bug. We can't convert that to an Oracle timestamp so have to hack it.
    query += " on file_id like c.file_pattern and to_timestamp(replace(ingestion_date, ':60.000', ':59.999'), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF') between c.ingestion_start and c.ingestion_end and arc = 'HERE'"
    query += "where "
    # ignore certain mime types
    query += "nf.format not in ('application/octet-stream', 'text/log-file', 'unknown') and "
    # no longer ignore files where the file_status is not 0. Always replicate to the ARCs.
    query += "nf.ignore=0 and "
    # Query join conditions to reach host_id (common) and check cluster name
    query += "nh.cluster_name='" + cluster_name + "' "
    if startDate is not None and startDate != "None":
        query += " and ingestion_date > {0} "
    query += " group by nf.file_id,nf.file_version"
    query += " having (max(rule_type) is null or max(rule_type) <> 'EXCLUDE')"
    query += ")"

    # Return query
    return query

def generate_target_files_query(srvObj, startDate,
                                cluster_name,
                                all_versions):
    """
    Specify an alias table that extends ngas_files
    table including host_id/domain/srv_port information

    INPUT:
        db_link        string, Name for the data base link hosting the cluster
    cluster_name    string, Name of the cluster involved in the operation
    all_versions    int, Parameter to determine if all-versions mode is desired

    RETURNS:
        query       string, Sub-Query to be aliased in a whole query
    """

    # Query create table statement (common)
    query = "(select nf.file_id file_id, nf.file_version file_version "
    query += "from "
    # Depending on all_versions parameter we select only last version or not
    if all_versions:
        query += "ngas_files nf inner join ngas_disks nd on nd.disk_id = nf.disk_id "
        query += "inner join ngas_hosts nh on nh.host_id = nd.last_host_id "
    else:
        query += "(select file_id, max(file_version) file_version from ngas_files group by file_id) nflv "
        query += "inner join ngas_files nf on nf.file_id = nflv.file_id and nf.file_version = nflv.file_version "
        query += "inner join ngas_disks nd on nd.disk_id = nf.disk_id "
        query += "inner join ngas_hosts nh on nh.host_id = nd.last_host_id "
    query += "where "
    # ICT-1988 - cannot take file_status into consideration
    # query += "nf.ignore=0 and nf.file_status=0 and "
    colname = 'file_ignore' if srvObj.getCfg().getDbUseFileIgnore() else 'ignore'
    query += "nf.%s=0 and " % (colname,)
    # Query join conditions to reach host_id (common) and check cluster name
    query += "nh.cluster_name='" + cluster_name + "' "
    if startDate is not None and startDate != "None":
        query += "and ingestion_date > {0} "
    query += "group by nf.file_id,nf.file_version"
    # these are the files that are currently downloading. We include them in the union so
    # that they wil not be re-considered for mirroring
    query += " union all"
    # query += " select nf.file_id file_id, nf.file_version file_version from"
    # query += " (select file_id, file_version, target_host from ngas_mirroring_bookkeeping "
    # query += " where status in ('READY', 'TORESUME', 'FETCHING')) nf"
    # query += " inner join ngas_hosts h on replace(h.host_id, ':', '.' || domain || ':') = nf.target_host"
    # query += " group by nf.file_id, nf.file_version))"
    query += " select file_id, file_version from ngas_mirroring_bookkeeping "
    query += " where status in ('READY', 'TORESUME', 'FETCHING')"
    query += " group by file_id, file_version)"

    # Return query
    return query


def generate_diff_ngas_files_query(source_ext_ngas_files_query,
                                   target_ext_ngas_files_query):
    """
    Specify an alias table to handle the result
    of the left join (diff) query between source
    and target extended ngas files sub-queries

    INPUT:
        source_ext_ngas_files_query    string, Sub-Query defining ngas_files information in source cluster
        target_ext_ngas_files_query    string, Sub-Query defining ngas_files information in target cluster

    RETURNS:
        query               string, Sub-Query to be aliased in a whole query
    """

    # Query create table statement (common)
    query = "(select source.file_id, source.file_version, source.disk_id,"
    query += " source.format, source.file_size, source.checksum,"
    query += " source.host_id, source.domain, source.srv_port,"
    # replace: some "timestamps" (stored as varchar2 in Oracle) have 60 as the seconds value due to an
    # earlier NGAS bug. We can't convert that to an Oracle timestamp so have to hack it.
    query += " to_timestamp(replace(source.ingestion_date, ':60.000', ':59.999'), 'YYYY-MM-DD\"T\"HH24:MI:SS:FF') as ingestion_date"
    query += " from "

    # Query left join condition
    query += source_ext_ngas_files_query + " source left join " + target_ext_ngas_files_query + " target on "
    query += "target.file_id = source.file_id and target.file_version = source.file_version "
    # Get no-matched records
    query += "where target.file_id is null)"

    # Log info
    logger.debug("SQL sub-query to generate extended ngas_files table: %s", query)

    # Return query
    return query


def get_mirroring_iteration(srvObj):

    """
    Get iteration number for next mirroring loop
    """

    # Construct query
    query = "select max(iteration)+1 from ngas_mirroring_bookkeeping"

    # Execute query
    res = srvObj.getDb().query2(query)
    iteration = res[0][0] or 1

    logger.info("next mirroring iteraion: %d", iteration)

    # Return void
    return iteration


def populate_mirroring_bookkeeping_table(diff_ngas_files_query,
                                         startDate,
                                         dbLink,
                                         cluster_name,
                                         iteration,
                                         srvObj):
    """
    Populate mirroring book keeping table with the
    diff between the source and target tables.

    INPUT:
        diff_ngas_files_query    string, Sub-Query defining the diff between the ngas_files
                    in the source and target clusters
        dbLink            string, Name for the data base link hosting the target cluster
    cluster_name        string, Name of the target cluster
        iteration        string, Iteration number for next mirroring loop
    srvObj            ngamsServer, Reference to NG/AMS server class object

    RETURNS:        Void.
    """

    ## First dump information from diff_ngas_files into book keeping table

    # Insert query statement
    query = "insert into ngas_mirroring_bookkeeping" + dbLink + " "
    # Fields to be filled
    query += "(file_id, file_version, disk_id, host_id, "
    query += "file_size, format, status, target_cluster, "
    query += "source_host, ingestion_date, "
    query += "iteration, checksum, source_ingestion_date)"
    query += "select "
    # file_id: Direct from diff_ngas_files table
    query += "d.file_id, "
    # file_version: Direct from diff_ngas_files table
    query += "d.file_version, "
    # disk_id: Direct from diff_ngas_files table
    query += "d.disk_id, "
    # host_id: Direct from diff_ngas_files table
    query += "d.host_id, "
    # file_size: Direct from diff_ngas_files table
    query += "d.file_size, "
    # format: Direct from diff_ngas_files table
    query += "d.format, "
    # status: Must be filled with 0 in case of no-ready entry
    query += "'LOCKED',"
    # target_host: We can temporary use the name of the target cluster
    #              rather than the target host to lock the table entry
    query += "'" + cluster_name + "', "
    # source_host: We concatenate host_id (without port)
    query += "substr(d.host_id,0,instr(d.host_id || ':',':')-1) || '.' || "
    #              With domain name and port number
    query += "d.domain || ':' || d.srv_port, "
    # ingestion_date: We have to assign it a default value because it is part of the primary key
    query += "{1}, "
    # iteration: In order to distinguish from different runs
    query += "{2}, "
    # the checksum from the source node
    query += "d.checksum, d.ingestion_date"
    # All this info comes from the diff table
    query += " from " + diff_ngas_files_query + " d"
    #if rows_limit is not None and rows_limit != "None":
    #    query += " where rownum <= " + str(rows_limit)

    # args are startDate, ingestionTime and iteration
    args = (startDate, toiso8601(fmt=FMT_DATETIME_NOMSEC) + ":000", iteration)
    srvObj.getDb().query2(query, args=args)

    # Return void
    return


def get_cluster_active_nodes(db_link,
                             cluster_name,
                             srvObj):
    """
    Return active nodes in a NG/AMS cluster

    INPUT:
        dbLink        string, Name for the data base link of the cluster
        cluster_name    string, Name of the cluster to check
        srvObj         ngamsServer, Reference to NG/AMS server class object

    RETURNS:
        active_nodes    list[strings], List of the active nodes in the cluster
    """

    # Construct query
    # TODO but only if there is a disk available....
    query = "select substr(host_id,0,instr(host_id || ':',':')-1) || '.' || domain || ':' || srv_port "
    query += "from ngas_hosts" + db_link + " where "
    query += "cluster_name='" + cluster_name + "' and srv_state='ONLINE'"

    # Execute query
    active_nodes = [x[0] for x in srvObj.getDb().query2(query)]

    # Log info
    logger.info("Active nodes found in cluster %s: %s", cluster_name + db_link, str(active_nodes))

    # Return active nodes list
    return active_nodes


def remove_empty_source_nodes(iteration,
                              source_active_nodes,
                              cluster_name,
                              srv_obj):
    """
    Remove source active nodes that don't contain any file to mirror

    INPUT:
        source_active_nodes    string, List of active nodes in the source cluster
        cluster_name        string, Name of the target_cluster
        srvObj              ngamsServer, Reference to NG/AMS server class object

    RETURNS:
        source_nodes    list[strings], List of the active source nodes
    """

    # Construct query
    query = "select source_host from ngas_mirroring_bookkeeping where target_cluster={0} "
    query += " and (status='LOCKED' or status = 'READY') and iteration = {1} group by source_host"

    source_nodes_object = srv_obj.getDb().query2(query, args=(cluster_name, iteration))

    # Re-dimension query results array
    source_nodes = [x[0] for x in source_nodes_object]
    logger.info("source nodes: %r", source_nodes)

    # Compute the intersection of both lists
    working_nodes = []
    for node in source_active_nodes:
        if source_nodes.count(node) > 0:
            working_nodes.append(node)
    logger.info("active source nodes: %r", working_nodes)

    # Return working nodes list
    return working_nodes


def deschedule_exclusions(iteration, arc, db_link, srv_obj):
    sql = "delete from ngas_mirroring_bookkeeping b where b.rowid in ("
    sql += "select min(f.rowid) from ngas_mirroring_bookkeeping f"
    sql += " left join alma_mirroring_exclusions" + db_link + " c on f.file_id like c.file_pattern"
    sql += " and f.source_ingestion_date between c.ingestion_start and c.ingestion_end"
    sql += " and arc = {0} and iteration = {1}"
    sql += " group by f.file_id, f.file_version having max(rule_type) = 'EXCLUDE')"

    logger.info('removing SCO exclusions')
    try:
        srv_obj.getDb().query2(sql, args=(arc, iteration))
    except Exception:
        # this clause should never be reached
        logger.exception("Fetch failed in an unexpected way")


def limit_mirrored_files(srv_obj, iteration, file_limit):
    # this is just used in development. I recommend not to use it in production, otherwise
    # we risk losing partially downloaded file.
    logger.info('limiting the number of files to fetch to %d', file_limit)
    sql = "delete from ngas_mirroring_bookkeeping where rowid in ("
    sql += "select myrowid from ("
    sql += "select rowid as myrowid, rownum as myrownum from ngas_mirroring_bookkeeping where iteration = {0}"
    # prefer files which are already downloading - otherwise we risk losing the data that has already been downloaded
    sql += " order by staging_file"
    sql += ") where myrownum > {1})"

    srv_obj.getDb().query2(sql, args=(iteration, file_limit))


def get_files_to_mirror_from_node(iteration, source_node, srv_obj):
    # type: (object, object, object) -> object
    # Construct query, for every update the nodes left are n_nodes-i
    query = "select file_id, file_version, file_size"
    query += " from ngas_mirroring_bookkeeping"
    query += " where iteration = {0}"
    query += " and SOURCE_HOST = {1}"
    query += " and status = 'LOCKED'"
    logger.info("SQL to obtain all files to be mirrored from host %s: %s", source_node, query)
    return srv_obj.getDb().query2(query, args=(iteration, source_node))


def get_target_node_disks(srv_obj, cluster_name):
    """I know this algorithm is still vague and can easily fail. We can get into race conditions, or
    an operator can manually ingest a large fie and mess up the space calculations. That's fine. We'll
    recover from such situations later. At this point we just don't want to assign files to disks where
    we know there is clearly not enough space to store them"""

    # Construct query, for every update the nodes left are n_nodes-i
    query = "select replace(h.host_id, ':', '.' || h.domain || ':'), d.mount_point, d.available_mb * (1024 * 1024)"
    query += " from ngas_hosts h"
    query += "   inner join ngas_disks d"
    query += "     on d.LAST_HOST_ID = h.host_id"
    query += " where h.cluster_name = {0}"
    query += " and h.srv_archive = 1 and srv_state = 'ONLINE'"
    query += " and d.mounted = 1 and d.completed = 0"
    query += " order by h.host_id, d.mount_point"
    logger.info("SQL to obtain all target disks %s", query)
    rs = srv_obj.getDb().query2(query, args=(cluster_name,))
    disks = {}
    for row in rs:
        host_id = row[0]
        mount_point = row[1]
        available_bytes = row[2]
        disks.setdefault(host_id, {})[mount_point] = available_bytes

    return disks


def get_target_node_disks_scheduled(srv_obj):
    # type: (object) -> object
    # Construct query, for every update the nodes left are n_nodes-i
    query = "select target_host, substr(staging_file, 0, instr(staging_file, '/', 1, 3) - 1), sum(file_size), count(file_size)"
    query += " from ngas_mirroring_bookkeeping"
    query += " where target_host is not null"
    query += " and status in ('READY', 'LOCKED', 'FETCHING', 'TORESUME')"
    query += " group by target_host, substr(staging_file, 0, instr(staging_file, '/', 1, 3) - 1)"
    logger.info("SQL to obtain all target disks %s", query)
    rs = srv_obj.getDb().query2(query)
    disks = {}
    for row in rs:
        host_id = row[0]
        mount_point = row[1]
        used_bytes = row[2]
        num_files = row[3]
        disks[(host_id, mount_point)] = (used_bytes, num_files)

    return disks


def assign_files_to_target_nodes(files, assigner, volumes_to_files):
    logger.info('assigning %d files to host volums', len(files))
    for (index, next_file_to_be_mirrored) in enumerate(files):
        target_host, target_volume = assigner.get_next_target_with_space_for(next_file_to_be_mirrored[2])
        logger.info("%s / %s: assigning %s to host %s, volume %s", index, len(files), next_file_to_be_mirrored[0], target_host, target_volume)
        volumes_to_files.setdefault((target_host, target_volume), []).append(next_file_to_be_mirrored)


def assign_mirroring_bookkeeping_entries(iteration,
                                         source_cluster_active_nodes,
                                         cluster_name,
                                         srv_obj):
    """
    Update target_cluster field in the book keeping
    table in order to assign entries to each node.

    The entries are assigned to target cluster nodes
    making sure that the file-size distribution is
    balanced. To do it so the entries to be assigned
    are first file-size sorted and then the 1st entry
    is assigned to the first node, the 2nd entry to
    the 2nd node and so on until all the nodes have
    been assigned one entry and then it starts again.

    At the end all the nodes should have been assigned
    the same number of files, the same total load in Mb
    and the same distribution of files in terms of file size.

    INPUT:
        target_cluster_active_nodes    list[strings], List of the active nodes in the target cluster
        target_cluster_source_nodes     list[strings], List of the active nodes in the source cluster
        db_link                string, Name for the data base link hosting the target cluster
        cluster_name            string, Name of the target cluster
        srvObj                ngamsServer, Reference to NG/AMS server class object
    RETURNS:            Void.
    """

    num_threads_per_host = getNumberOfSimultaneousFetchesPerServer(srv_obj)
    target_cluster_active_nodes = get_target_node_disks(srv_obj, cluster_name)
    target_cluster_assigned_files = get_target_node_disks_scheduled(srv_obj)
    required_free_space = srv_obj.getCfg().getFreeSpaceDiskChangeMb()
    assigner = TargetVolumeAssigner(target_cluster_active_nodes, target_cluster_assigned_files, required_free_space)
    available_target_hosts = assigner.remove_hosts_without_available_threads(num_threads_per_host)
    if not available_target_hosts:
        # in theory we shouldn't have reached this point if there are no threads available. However, the way I calculate
        # if threads are available is open to errors, and it can be that we only discover at this point that no threads
        # are actually available.
        logger.info("There are no hosts available with threads available for mirroring")
        return 0

    assigned_files = {}
    for source_node in source_cluster_active_nodes:
        logger.info('assigning files from source host %s to target nodes / volumes', source_node)

        # part1: get the required info from the DB
        files = get_files_to_mirror_from_node(iteration, source_node, srv_obj)

        # part2: assign
        assign_files_to_target_nodes(files, assigner, assigned_files)

    # part3: persist the assignments
    logger.info('persisting the assignments of files to hosts / volumes')
    total_files_to_mirror = 0
    for host, volume in assigned_files:
        file_list = assigned_files[(host, volume)]
        if host is None:
            logger.error("there is insufficient space available to mirror the files %r", file_list)
            continue
        logger.info("updating mirroring table with %d files which are assigned to %s, %s", len(file_list), host, volume)
        total_files_to_mirror += len(file_list)
        # TODO this is going to take a while. If we update 10K rows with a DB roundtrip of 0.01s
        # then the whole update will take about 2 minutes. Therefore we have to limit the sizes
        # of each iteration so that we can be receving files while we are fetching the next one.
        sql = "update ngas_mirroring_bookkeeping set status = 'READY', target_host = :1, staging_file = :2 "
        sql += " where iteration = :3 and file_id = :4 and file_version = :5"
        for next_file in file_list:
            parameters = [host, volume, iteration, next_file[0], next_file[1]]
            srv_obj.getDb().query(sql, maxRetries=1, retryWait=0, parameters=parameters)

    # extra part - moved from ngamsCmd_MIRRARCHIVE as a performance optimisation - we already have the information we
    # need here to check if we should mark a disk as complete
    # Check if the disk is completed.
    logger.info('Checking the available disk space remaining after this iteration')
    for host, volume in assigned_files:
        if host is None:
            continue
        available_mb = assigner.get_available_bytes(host, volume) / (1024 * 1024)
        logger.info("available bytes after this iteration for node %s, disk mounted at %s: %s MB", host, volume, available_mb)
        # Check if the disk is completed.
        if available_mb < srv_obj.getCfg().getFreeSpaceDiskChangeMb():
            logger.info('disk on host %s at mount point %s has less than %s mb - marking as complete', host, volume, srv_obj.getCfg().getFreeSpaceDiskChangeMb())
            sql = "update ngas_disks set completed = 1 where last_host_id = :1 and mount_point = :2"
            unqualified_host_name = re.sub(r"\..*:", ":", host)
            srv_obj.getDb().query(sql, maxRetries=1, retryWait=0, parameters=[unqualified_host_name, volume])

    return total_files_to_mirror

# EOF