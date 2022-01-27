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
# *****************************************************************************
#
# "@(#) $Id: ngamsCmd_MIRRTABLE.py,v 1.6 2010/06/22 18:55:14 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jagonzal  2009/12/14  Created
#
"""
NGAS Command Plug-In, implementing a command to fill the mirroring_bookkeeping_table

NOTES
------
By default only the last version found in the source cluster is synchronized to the target cluster. To synchronize all
existing versions specify all_versions=1. In case of remote data-base mirroring it is necessary to enable data-base
links to the source/target data-bases in the local data-base. If the target data-base is the local one is not
necessary to specify a target data-base link, (by default the local data base is the target data base)

PARAMETERS
----------
* source_cluster [mandatory] source cluster name
* target_cluster [mandatory] target cluster name
* source_dbl [optional] source archive data base link (remote data base mirroring)
* target_dbl [optional] target archive data base link (remote data base mirroring)
* all_versions [optional] (=1) mirror all missing versions of each file, default (=0)
* archive_cmd [optional] custom archive command, default (=MIRRARCHIVE)
* retriev_cmd [optional] custom retrieve command, default (=RETRIEVE)

EXAMPLES
--------
Local data base mirroring with custom ARCHIVE command
    http://ngas05.hq.eso.org:7778/MIRRTABLE?target_cluster=ngas05:7778&source_cluster=ngas02:7778&archive_cmd=QARCHIVE?
"""

import logging
import re
import threading
import time

from ngamsLib import ngamsHttpUtils
from ngamsLib.ngamsCore import toiso8601, FMT_DATETIME_NOMSEC
from .alma.almaMirroringTargetNodeAssigner import TargetVolumeAssigner

logger = logging.getLogger(__name__)

def handleCmd(ngams_server, request_properties, http_reference=None):
    """
    Handle Command MIRRTABLE to populate bookkeeping table in target cluster
    :param ngams_server: ngamsServer, Reference to NG/AMS server class object
    :param request_properties: ngamsReqProps, Request properties to keep track of actions done during request handling
    :param http_reference: HTTP reference
    """
    # Get command parameters
    target_cluster = ngams_server.getCfg().getVal("Mirroring[1].target_cluster")
    if request_properties.hasHttpPar("target_cluster"):
        target_cluster = request_properties.getHttpPar("target_cluster")

    source_cluster = ngams_server.getCfg().getVal("Mirroring[1].source_cluster")
    if request_properties.hasHttpPar("source_cluster"):
        source_cluster = request_properties.getHttpPar("source_cluster")

    source_dbl = "@" + ngams_server.getCfg().getVal("Mirroring[1].source_dbl")
    if request_properties.hasHttpPar("source_dbl"):
        source_dbl = "@" + request_properties.getHttpPar("source_dbl")

    target_dbl = ""
    if request_properties.hasHttpPar("target_dbl"):
        target_dbl = "@" + request_properties.getHttpPar("target_dbl")

    all_versions = ngams_server.getCfg().getVal("Mirroring[1].all_versions")
    if request_properties.hasHttpPar("all_versions"):
        all_versions = int(request_properties.getHttpPar("all_versions"))

    # What was the date of the last successful mirroring iteration? We will only compare files from local and remote
    # databases from that date (the start of the iteration, just to allow some error margin)
    site_id = get_current_site(ngams_server)
    valid_arcs = ['EU', 'EA', 'NA', 'SCO', 'OSF']
    if not (site_id in valid_arcs):
        raise Exception("Cannot mirror, the table ngas_cfg_pars_properties does not contain an element 'siteId' " 
                        "with one of these values: " + str(valid_arcs))

    # We're overlapping mirroring iterations now. First check is to see if there are any spare threads for this
    # iteration to use
    num_available_download_threads = get_num_available_download_threads(ngams_server)
    if num_available_download_threads <= 0:
        logger.info("All the available download threads are busy. Skipping this mirroring iteration.")
        return
    logger.info("Total num threads available for mirroring in the cluster: %d", num_available_download_threads)

    baseline_date = get_mirroring_baseline_date(ngams_server)
    if baseline_date is None or baseline_date == "None":
        start_date = "None"
    else:
        # Work out the time window to compare files. We use the last succesful iteration and then subtract 100 for safety
        start_date = find_date_of_last_successful_mirroring_iteration(ngams_server)
        # However, if this is the first mirroring iteration of a new day then we perform a complete mirror
        last_iteration = find_date_of_last_mirroring_iteration(ngams_server)
        if last_iteration is None or last_iteration == "None" or last_iteration[8:10] != time.strftime('%d', time.localtime()):
            logger.info('performing a full mirroring')
            start_date = "None"
        # Unless, of course, the baseline_date has been set. We never ever extend beyond that.
        if start_date == "None" and baseline_date:
            logger.info('using the baseline data from the config table: %s', baseline_date)
            start_date = baseline_date + "T00:00:00:000"
    rows_limit = get_iteration_file_limit(ngams_server)

    # Get source cluster active nodes
    source_active_nodes = get_cluster_active_nodes(source_dbl, source_cluster, ngams_server)

    if len(source_active_nodes) == 0:
        logger.warning("There are no active source nodes. Skipping this iteration.")
    else:
        # Construct sub-query for source cluster
        source_query = generate_source_files_query(ngams_server, start_date, source_dbl, source_cluster, all_versions)
        logger.debug("SQL sub-query to get source cluster files-hosts information: %s", source_query)

        # Construct sub-query for target cluster
        target_query = generate_target_files_query(ngams_server, start_date, target_cluster, all_versions)
        logger.debug("SQL sub-query to get target cluster files-hosts information: %s", target_query)

        # Construct sub-query for  table
        diff_query = generate_diff_ngas_files_query(source_query, target_query)
        logger.debug("SQL sub-query to get diff between source and target files: %s", diff_query)

        # Get iteration number
        iteration = get_mirroring_iteration(ngams_server)

        # Populate book keeping table
        logger.debug("Populating ngas_mirroring_bookkeeping_table, source_cluster=%s , target_cluster=%s, all_versions=%s",
                     source_cluster + source_dbl, target_cluster + target_dbl, str(all_versions))
        populate_mirroring_bookkeeping_table(diff_query, start_date, target_dbl, target_cluster, iteration,
                                             ngams_server)

        # Grab any TORESUME fetches for this iteration
        reassign_broken_downloads(iteration, ngams_server)

        # De-schedule any files which have been blocked from the SCO
        # FIXME: should this be hard coded as 'EU' ???
        deschedule_exclusions(iteration, 'EU', source_dbl, ngams_server)

        # Limit the number of files that we will fetch in a single iteration
        if rows_limit is not None and rows_limit != 'None':
            limit_mirrored_files(ngams_server, iteration, rows_limit)

        # Remove the source nodes which do not have any files for mirroring during this iteration
        working_source_nodes = remove_empty_source_nodes(iteration, source_active_nodes, target_cluster, ngams_server)

        # Assign book keeping table entries
        logger.info("Updating entries in ngas_mirroring_bookkeeping_table to assign target nodes")
        total_files_to_mirror = assign_mirroring_bookkeeping_entries(iteration, working_source_nodes,
                                                                     target_cluster, ngams_server)

        logger.info("There are %d files are to be mirrored in iteration %d", total_files_to_mirror, iteration)
        if total_files_to_mirror > 0:
            t = threading.Thread(target=execute_mirroring, args=(ngams_server, iteration))
            t.start()
    return


def execute_mirroring(ngams_server, iteration):
    logger.info('executeMirroring for iteration %d', iteration)
    try:
        rx_timeout = 30 * 60
        if ngams_server.getCfg().getVal("Mirroring[1].rx_timeout"):
            rx_timeout = int(ngams_server.getCfg().getVal("Mirroring[1].rx_timeout"))

        pars = {
            'mirror_cluster': 2,
            'iteration': str(iteration),
            'rx_timeout': rx_timeout,
            'n_threads': get_num_simultaneous_fetches_per_server(ngams_server)
        }
        host, port = ngams_server.get_self_endpoint()
        # TODO: look at the HTTP response code
        ngamsHttpUtils.httpGet(host, port, 'MIRREXEC', pars=pars, timeout=rx_timeout)
    except Exception:
        logger.exception("MIRREXEC command for iteration %d has failed", iteration)
    finally:
        fail_remaining_transfers(ngams_server, iteration)
        logger.info("MIRREXEC command for iteration %d has successfully completed", iteration)

    # Remove some of the older bookkeeping entries
    clean_mirroring_bookkeeping_entries(ngams_server)


def get_num_available_download_threads(ngams_server):
    sql = "select numHosts * fetchesPerServer - currentFetches as availableFetches from"
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

    result = ngams_server.getDb().query2(sql)
    num_available = int(result[0][0])
    return num_available


def fail_remaining_transfers(ngams_server, iteration):
    logger.info('Making sure there are no LOCKED or READY entries left for iteration %d', iteration)
    sql = "update ngas_mirroring_bookkeeping set status = 'FAILURE' " \
          "where status in ('LOCKED', 'READY') and iteration = {0}"
    ngams_server.getDb().query2(sql, args=(iteration,))


def reassign_broken_downloads(current_iteration, ngams_server):
    sql = "insert into ngas_mirroring_bookkeeping " \
          "(file_id, file_version, file_size, disk_id, host_id, " \
          "format, status, target_cluster, target_host, source_host, " \
          "ingestion_date, ingestion_time, iteration, checksum, checksum_plugin, " \
          "staging_file, attempt, source_ingestion_date) " \
          "select file_id, file_version, file_size, disk_id, host_id, " \
          "format, 'READY' as status, target_cluster, target_host, source_host, " \
          "ingestion_date, ingestion_time, {0} as iteration, checksum, checksum_plugin, " \
          "staging_file, attempt, source_ingestion_date " \
          "from ngas_mirroring_bookkeeping where status = 'TORESUME'"
    ngams_server.getDb().query2(sql, args=(current_iteration,))

    # We've grabbed
    sql = "update ngas_mirroring_bookkeeping set status = 'FAILURE', staging_file = null " \
          "where status = 'TORESUME' and iteration < {0}"
    ngams_server.getDb().query2(sql, args=(current_iteration,))


def clean_mirroring_bookkeeping_entries(ngams_server):
    # TBD parameterize the time period
    logger.info('Cleaning up mirroring bookkeeping table - removing entries older than 60 days')
    sql = "delete from ngas_mirroring_bookkeeping where iteration < " \
          "(select max(iteration) from " \
          "(select iteration from ngas_mirroring_bookkeeping" \
          " group by iteration having min(status) = 'SUCCESS'" \
          " and substr(max(ingestion_date), 1, 10) < to_char(sysdate - 60, 'YYYY-MM-DD')))"
    ngams_server.getDb().query2(sql)


def find_date_of_last_mirroring_iteration(ngams_server):
    sql = "select min(ingestion_date) from ngas_mirroring_bookkeeping " \
          "where iteration = (select max(iteration) from ngas_mirroring_bookkeeping)"
    result = ngams_server.getDb().query2(sql)
    timestamp = str(result[0][0])
    logger.info("The date of the last mirroring iteration was %s", timestamp)
    return timestamp


def find_date_of_last_successful_mirroring_iteration(ngams_server):
    sql = "select min(ingestion_date) from ngas_mirroring_bookkeeping " \
          "where iteration = " \
          "(select max(iteration) - 100 from " \
          "(select iteration from ngas_mirroring_bookkeeping " \
          " group by iteration having count(distinct(status)) = 1 and min(status) = 'SUCCESS'))"

    result = ngams_server.getDb().query2(sql)
    timestamp = str(result[0][0])
    logger.info("The start of the sliding window is %s", timestamp)
    return timestamp


def get_num_simultaneous_fetches_per_server(ngams_server):
    sql = "select cfg_val from ngas_cfg_pars where cfg_par = 'numParallelFetches'"
    result = ngams_server.getDb().query2(sql)
    value = str(result[0][0])
    if not value or value == "None":
        num_threads = 5
    else:
        num_threads = int(value)
    return num_threads


def get_mirroring_baseline_date(ngams_server):
    sql = "select cfg_val from ngas_cfg_pars where cfg_par = 'baselineDate'"
    result = ngams_server.getDb().query2(sql)
    baseline_date = str(result[0][0])
    logger.info("Mirroring baseline date is %s", baseline_date)
    return baseline_date


def get_iteration_file_limit(ngams_server):
    sql = "select cfg_val from ngas_cfg_pars where cfg_par = 'iterationFileLimit'"
    result = ngams_server.getDb().query2(sql)
    limit = str(result[0][0])
    return limit


def get_current_site(ngams_server):
    sql = "select cfg_val from ngas_cfg_pars where cfg_par = 'siteId'"
    result = ngams_server.getDb().query2(sql)
    site_id = str(result[0][0])
    if site_id is None or site_id == "None":
        site_id = '?'
    return site_id


def generate_source_files_query(ngams_server, start_date, db_link, cluster_name, all_versions):
    """
    Specify an alias table that extends ngas_files table including host_id/domain/srv_port information
    :param ngams_server: ngamsServer, Reference to NG/AMS server class object
    :param start_date: Start date
    :param db_link: string, Name for the data base link hosting the cluster
    :param cluster_name: string, Name of the cluster involved in the operation
    :param all_versions: int, Parameter to determine if all-versions mode is desired
    :return: string, Sub-Query to be aliased in a whole query
    """
    # Query create table statement (common)
    sql = "(select nf.file_id file_id, nf.file_version file_version, "
    sql += "min(ingestion_date) ingestion_date, "
    sql += "substr(min(nf.disk_id || nh.host_id || nh.srv_port),1,length(min(nd.disk_id))) disk_id, "
    sql += "substr(min(nf.disk_id || nh.host_id || nh.srv_port),1+length(min(nd.disk_id)),length(min(nh.host_id))) host_id, "
    sql += "substr(min(nf.disk_id || nh.host_id || nh.srv_port),1+length(min(nd.disk_id))+length(min(nh.host_id)),length(min(nh.srv_port))) srv_port, "
    sql += "min(nf.format) format, min(nf.file_size) file_size, min(nf.checksum) checksum, min(nf.checksum_plugin) checksum_plugin, min(nh.domain) domain "
    sql += "from "
    # Depending on all_versions parameter we select only last version or not
    if all_versions:
        sql += "ngas_files" + db_link + " nf inner join ngas_disks" + db_link + " nd on nd.disk_id = nf.disk_id "
        sql += "inner join ngas_hosts" + db_link + " nh on nh.host_id = nd.host_id "
    else:
        sql += "(select file_id, max(file_version) file_version from ngas_files" + db_link + " group by file_id) nflv "
        sql += "inner join ngas_files" + db_link + " nf on nf.file_id = nflv.file_id and nf.file_version = nflv.file_version "
        sql += "inner join ngas_disks" + db_link + " nd on nd.disk_id = nf.disk_id "
        sql += "inner join ngas_hosts" + db_link + " nh on nh.host_id = nd.host_id "
    # For clarity, I used to insert all entries and then remove them according to the exclusion rules in a second step.
    # The problem with that (in the dev environment) is the performance. It brings over way more rows from the source
    # DB than is required.
    sql += " left join alma_mirroring_exclusions c "
    # Replace(ingestion_date, ':60.000', ':59.999'):
    # some "timestamps" (stored as varchar2 in Oracle) have 60 as the seconds value due to an earlier NGAS bug.
    # We can't convert that to an Oracle timestamp so have to hack it.
    sql += " on file_id like c.file_pattern and to_timestamp(replace(ingestion_date, ':60.000', ':59.999'), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF') between c.ingestion_start and c.ingestion_end and arc = 'HERE'"
    sql += "where "
    # Ignore certain mime types
    sql += "nf.format not in ('application/octet-stream', 'text/log-file', 'unknown') and "
    # No longer ignore files where the file_status is not 0. Always replicate to the ARCs.
    column_name = 'file_ignore' if ngams_server.getCfg().getDbUseFileIgnore() else 'ignore'
    sql += "nf.%s=0 and " % column_name
    # Query join conditions to reach host_id (common) and check cluster name
    sql += "nh.cluster_name='" + cluster_name + "' "
    if start_date is not None and start_date != "None":
        sql += " and ingestion_date > {0} "
    sql += " group by nf.file_id,nf.file_version"
    sql += " having (max(rule_type) is null or max(rule_type) <> 'EXCLUDE')"
    sql += ")"
    return sql


def generate_target_files_query(ngams_server, start_date, cluster_name, all_versions):
    """
    Specify an alias table that extends ngas_files table including host_id/domain/srv_port information
    :param ngams_server: ngamsServer, Reference to NG/AMS server class object
    :param cluster_name: string, Name of the cluster involved in the operation
    :param all_versions: int, Parameter to determine if all-versions mode is desired
    :return: string, Sub-Query to be aliased in a whole query
    """
    # Query create table statement (common)
    sql = "(select nf.file_id file_id, nf.file_version file_version "
    sql += "from "
    # Depending on all_versions parameter we select only last version or not
    if all_versions:
        sql += "ngas_files nf inner join ngas_disks nd on nd.disk_id = nf.disk_id "
        sql += "inner join ngas_hosts nh on nh.host_id = nd.last_host_id "
    else:
        sql += "(select file_id, max(file_version) file_version from ngas_files group by file_id) nflv "
        sql += "inner join ngas_files nf on nf.file_id = nflv.file_id and nf.file_version = nflv.file_version "
        sql += "inner join ngas_disks nd on nd.disk_id = nf.disk_id "
        sql += "inner join ngas_hosts nh on nh.host_id = nd.last_host_id "
    sql += "where "
    # ICT-1988 - cannot take file_status into consideration
    # query += "nf.ignore=0 and nf.file_status=0 and "
    column_name = 'file_ignore' if ngams_server.getCfg().getDbUseFileIgnore() else 'ignore'
    sql += "nf.%s=0 and " % column_name
    # Query join conditions to reach host_id (common) and check cluster name
    sql += "nh.cluster_name='" + cluster_name + "' "
    if start_date is not None and start_date != "None":
        sql += "and ingestion_date > {0} "
    sql += "group by nf.file_id,nf.file_version"
    # These are the files that are currently downloading. We include them in the union so that they wil not be
    # re-considered for mirroring
    sql += " union all"
    # sql += " select nf.file_id file_id, nf.file_version file_version from"
    # sql += " (select file_id, file_version, target_host from ngas_mirroring_bookkeeping "
    # sql += " where status in ('READY', 'TORESUME', 'FETCHING')) nf"
    # sql += " inner join ngas_hosts h on replace(h.host_id, ':', '.' || domain || ':') = nf.target_host"
    # sql += " group by nf.file_id, nf.file_version))"
    sql += " select file_id, file_version from ngas_mirroring_bookkeeping "
    sql += " where status in ('READY', 'TORESUME', 'FETCHING')"
    sql += " group by file_id, file_version)"
    return sql


def generate_diff_ngas_files_query(source_ext_ngas_files_query, target_ext_ngas_files_query):
    """
    Specify an alias table to handle the result of the left join (diff) query between source and target extended NGAS
    files sub-queries
    :param source_ext_ngas_files_query: string, Sub-Query defining ngas_files information in source cluster
    :param target_ext_ngas_files_query: string, Sub-Query defining ngas_files information in target cluster
    :return" string, Sub-Query to be aliased in a whole query
    """
    # Query create table statement (common)
    sql = "(select source.file_id, source.file_version, source.disk_id,"
    sql += " source.format, source.file_size, source.checksum, source.checksum_plugin,"
    sql += " source.host_id, source.domain, source.srv_port,"
    # Replace: some "timestamps" (stored as varchar2 in Oracle) have 60 as the seconds value due to an earlier NGAS bug.
    # We can't convert that to an Oracle timestamp so have to hack it.
    sql += " to_timestamp(replace(source.ingestion_date, ':60.000', ':59.999'), 'YYYY-MM-DD\"T\"HH24:MI:SS:FF') as ingestion_date"
    sql += " from "
    # Query left join condition
    sql += source_ext_ngas_files_query + " source left join " + target_ext_ngas_files_query + " target on "
    sql += "target.file_id = source.file_id and target.file_version = source.file_version "
    # Get no-matched records
    sql += "where target.file_id is null)"
    logger.debug("SQL sub-query to generate extended ngas_files table: %s", sql)
    return sql


def get_mirroring_iteration(ngams_server):
    """
    Get iteration number for next mirroring loop
    """
    sql = "select max(iteration) + 1 from ngas_mirroring_bookkeeping"
    result = ngams_server.getDb().query2(sql)
    value = result[0][0]
    if value is None:
        iteration = 1
    else:
        iteration = int(value)
    logger.info("Next mirroring iteration: %d", iteration)
    return iteration


def populate_mirroring_bookkeeping_table(diff_ngas_files_query, start_date, db_link, cluster_name,
                                         iteration, ngams_server):
    """
    Populate mirroring book keeping table with the difference between the source and target tables.
    :param diff_ngas_files_query: string, Sub-Query defining the diff between ngas_files in source and target clusters
    :param start_date: Start date
    :param db_link: string, Name for the data base link hosting the target cluster
    :param cluster_name: string, Name of the target cluster
    :param iteration: string, Iteration number for next mirroring loop
    :param ngams_server: ngamsServer, Reference to NG/AMS server class object
    """
    # First dump information from diff_ngas_files into book keeping table
    # Insert query statement
    sql = "insert into ngas_mirroring_bookkeeping" + db_link + " "
    # Fields to be filled
    sql += "(file_id, file_version, disk_id, host_id, "
    sql += "file_size, format, status, target_cluster, "
    sql += "source_host, ingestion_date, "
    sql += "iteration, checksum, checksum_plugin, source_ingestion_date)"
    sql += "select "
    # file_id: Direct from diff_ngas_files table
    sql += "d.file_id, "
    # file_version: Direct from diff_ngas_files table
    sql += "d.file_version, "
    # disk_id: Direct from diff_ngas_files table
    sql += "d.disk_id, "
    # host_id: Direct from diff_ngas_files table
    sql += "d.host_id, "
    # file_size: Direct from diff_ngas_files table
    sql += "d.file_size, "
    # format: Direct from diff_ngas_files table
    sql += "d.format, "
    # status: Must be filled with 0 in case of no-ready entry
    sql += "'LOCKED',"
    # target_host: We can temporary use the name of the target cluster rather than the target host to lock table entry
    sql += "'" + cluster_name + "', "
    # source_host: We concatenate host_id (without port)
    sql += "substr(d.host_id,0,instr(d.host_id || ':',':')-1) || '.' || "
    # With domain name and port number
    sql += "d.domain || ':' || d.srv_port, "
    # ingestion_date: We have to assign it a default value because it is part of the primary key
    sql += "{1}, "
    # iteration: In order to distinguish from different runs
    sql += "{2}, "
    # The checksum from the source node
    sql += "d.checksum, d.checksum_plugin, d.ingestion_date"
    # All this info comes from the diff table
    sql += " from " + diff_ngas_files_query + " d"
    # if rows_limit is not None and rows_limit != "None":
    #    query += " where rownum <= " + str(rows_limit)

    # args are startDate, ingestionTime and iteration
    args = (start_date, toiso8601(fmt=FMT_DATETIME_NOMSEC) + ":000", iteration)
    ngams_server.getDb().query2(sql, args=args)
    return


def get_cluster_active_nodes(db_link, cluster_name, ngams_server):
    """
    Return active nodes in a NG/AMS cluster
    :param db_link: string, Name for the data base link of the cluster
    :param cluster_name: string, Name of the cluster to check
    :param ngams_server: ngamsServer, Reference to NG/AMS server class object
    :return: list[strings], List of the active nodes in the cluster
    """
    # Construct query
    # TODO: but only if there is a disk available....
    sql = "select substr(host_id, 0, instr(host_id || ':',':')-1) || '.' || domain || ':' || srv_port " \
          "from ngas_hosts" + db_link + \
          " where cluster_name = '" + cluster_name + "' and srv_state = 'ONLINE'"
    active_nodes = [x[0] for x in ngams_server.getDb().query2(sql)]
    logger.info("Active nodes found in cluster %s: %s", cluster_name + db_link, str(active_nodes))
    return active_nodes


def remove_empty_source_nodes(iteration, source_active_nodes, cluster_name, ngams_server):
    """
    Remove source active nodes that don't contain any file to mirror
    :param iteration: Iteration
    :param source_active_nodes: string, List of active nodes in the source cluster
    :param cluster_name: string, Name of the target_cluster
    :param ngams_server: ngamsServer, Reference to NG/AMS server class object
    :return: list[strings], List of the active source nodes
    """
    sql = "select source_host from ngas_mirroring_bookkeeping " \
          "where target_cluster = {0}  and (status='LOCKED' or status = 'READY') and iteration = {1} " \
          "group by source_host"
    source_nodes_object = ngams_server.getDb().query2(sql, args=(cluster_name, iteration))

    # Re-dimension query results array
    source_nodes = [x[0] for x in source_nodes_object]
    logger.info("Source nodes: %r", source_nodes)

    # Compute the intersection of both lists
    working_nodes = []
    for node in source_active_nodes:
        if source_nodes.count(node) > 0:
            working_nodes.append(node)
    logger.info("Active source nodes: %r", working_nodes)
    return working_nodes


def deschedule_exclusions(iteration, arc, db_link, ngams_server):
    sql = "delete from ngas_mirroring_bookkeeping b where b.rowid in ("
    sql += "select min(f.rowid) from ngas_mirroring_bookkeeping f"
    sql += " left join alma_mirroring_exclusions" + db_link + " c on f.file_id like c.file_pattern"
    sql += " and f.source_ingestion_date between c.ingestion_start and c.ingestion_end"
    sql += " and arc = {0} and iteration = {1}"
    sql += " group by f.file_id, f.file_version having max(rule_type) = 'EXCLUDE')"

    logger.info('Removing SCO exclusions')
    try:
        ngams_server.getDb().query2(sql, args=(arc, iteration))
    except Exception:
        # This clause should never be reached
        logger.exception("Fetch failed in an unexpected way")


def limit_mirrored_files(ngams_server, iteration, file_limit):
    # This is just used in development. I recommend not to use it in production, otherwise we risk losing partially
    # downloaded file.
    logger.info('Limiting the number of files to fetch to %s', file_limit)
    sql = """
        delete from ngas_mirroring_bookkeeping d
        where exists (
          select file_id
          from (
            select file_id, file_version, iteration, source_ingestion_Date, status, rownum as myrownum
            from (
              select file_id, file_version, iteration, source_ingestion_date, status
              from ngas_mirroring_bookkeeping i
              where iteration = {0}
              order by source_ingestion_date
            )
          )
          where file_id = d.file_id
          and file_version = d.file_version
          and iteration = d.iteration
          and myrownum > {1}
       )
    """
    ngams_server.getDb().query2(sql, args=(iteration, file_limit))


def get_files_to_mirror_from_node(iteration, source_node, ngams_server):
    # type: (object, object, object) -> object
    # Construct query, for every update the nodes left are n_nodes-i
    sql = "select file_id, file_version, file_size " \
          "from ngas_mirroring_bookkeeping " \
          "where iteration = {0} and SOURCE_HOST = {1} and status = 'LOCKED'"
    logger.info("SQL to obtain all files to be mirrored from host %s: %s", source_node, sql)
    return ngams_server.getDb().query2(sql, args=(iteration, source_node))


def get_target_node_disks(ngams_server, cluster_name):
    """
    I know this algorithm is still vague and can easily fail. We can get into race conditions, or an operator can
    manually ingest a large fie and mess up the space calculations. That's fine. We'll recover from such situations
    later. At this point we just don't want to assign files to disks where we know there is clearly not enough space
    to store them
    """
    # Construct query, for every update the nodes left are n_nodes-i
    sql = "select replace(h.host_id, ':', '.' || h.domain || ':'), h.domain, d.mount_point, d.available_mb * (1024 * 1024)"
    sql += " from ngas_hosts h"
    sql += "   inner join ngas_disks d"
    sql += "     on d.LAST_HOST_ID = h.host_id"
    sql += " where h.cluster_name = {0}"
    sql += " and h.srv_archive = 1 and srv_state = 'ONLINE'"
    sql += " and d.mounted = 1 and d.completed = 0"
    sql += " order by h.host_id, d.mount_point"
    logger.info("SQL to obtain all target disks %s", sql)
    result = ngams_server.getDb().query2(sql, args=(cluster_name,))
    disks = {}
    for row in result:
        host_id = row[0]

        # TODO: this is an ugly fix for an ugly situation in which the host_id already contains the domain name
        #  (but the SQL code assumes that it doesn't). The hostId of a server is simply hostname:port. Depending on
        #  how the machine is configured, the hostname will be a fully- qualified hostname or not, and therefore the
        #  host_id returned by the query will contain the domain twice. Let's solve that
        domain = row[1]
        double_domain_start = host_id.find(domain + "." + domain)
        if double_domain_start != -1:
            host_id = host_id[:double_domain_start] + host_id[double_domain_start + len(domain) + 1:]

        mount_point = row[2]
        available_bytes = row[3]
        disks.setdefault(host_id, {})[mount_point] = available_bytes

    return disks


def get_target_node_disks_scheduled(ngams_server):
    # type: (object) -> object
    # Construct query, for every update the nodes left are n_nodes-i
    sql = "select target_host, substr(staging_file, 0, instr(staging_file, '/', 1, 3) - 1), sum(file_size), count(file_size)"
    sql += " from ngas_mirroring_bookkeeping"
    sql += " where target_host is not null"
    sql += " and status in ('READY', 'LOCKED', 'FETCHING', 'TORESUME')"
    sql += " group by target_host, substr(staging_file, 0, instr(staging_file, '/', 1, 3) - 1)"

    logger.info("SQL to obtain all target disks %s", sql)
    result = ngams_server.getDb().query2(sql)

    disks = {}
    for row in result:
        host_id = row[0]
        mount_point = row[1]
        used_bytes = row[2]
        num_files = row[3]
        disks[(host_id, mount_point)] = (used_bytes, num_files)

    return disks


def assign_files_to_target_nodes(files, assigner, volumes_to_files):
    logger.info('Assigning %d files to host volumes', len(files))
    for (index, next_file_to_be_mirrored) in enumerate(files):
        target_host, target_volume = assigner.get_next_target_with_space_for(next_file_to_be_mirrored[2])
        logger.info("%s / %s: assigning %s to host %s, volume %s", index, len(files), next_file_to_be_mirrored[0],
                    target_host, target_volume)
        volumes_to_files.setdefault((target_host, target_volume), []).append(next_file_to_be_mirrored)


def assign_mirroring_bookkeeping_entries(iteration, source_cluster_active_nodes, cluster_name, ngams_server):
    """
    Update target_cluster field in the book keeping table in order to assign entries to each node.

    The entries are assigned to target cluster nodes making sure that the file-size distribution is balanced. To do it
    so the entries to be assigned are first file-size sorted and then the 1st entry is assigned to the first node, the
    2nd entry to the 2nd node and so on until all the nodes have been assigned one entry and then it starts again.

    At the end all the nodes should have been assigned the same number of files, the same total load in MB and the
    same distribution of files in terms of file size.
    :param iteration: Iteration
    :param source_cluster_active_nodes: list[strings], List of the active nodes in the source cluster
    :param cluster_name: string, Name of the target cluster
    :param ngams_server: ngamsServer, Reference to NG/AMS server class object
    :return: Total number of files to be mirrored
    """
    num_threads_per_host = get_num_simultaneous_fetches_per_server(ngams_server)
    target_cluster_active_nodes = get_target_node_disks(ngams_server, cluster_name)
    target_cluster_assigned_files = get_target_node_disks_scheduled(ngams_server)
    required_free_space = ngams_server.getCfg().getFreeSpaceDiskChangeMb()
    assigner = TargetVolumeAssigner(target_cluster_active_nodes, target_cluster_assigned_files, required_free_space)
    available_target_hosts = assigner.remove_hosts_without_available_threads(num_threads_per_host)
    if not available_target_hosts:
        # In theory we shouldn't have reached this point if there are no threads available. However, the way I
        # calculate if threads are available is open to errors, and it can be that we only discover at this point
        # that no threads are actually available.
        logger.info("There are no hosts available with threads available for mirroring")
        return 0

    assigned_files = {}
    for source_node in source_cluster_active_nodes:
        logger.info('Assigning files from source host %s to target nodes / volumes', source_node)
        # part1: get the required info from the DB
        files = get_files_to_mirror_from_node(iteration, source_node, ngams_server)
        # part2: assign
        assign_files_to_target_nodes(files, assigner, assigned_files)

    # part3: persist the assignments
    logger.info('Persisting the assignments of files to hosts / volumes')
    total_files_to_mirror = 0
    for host, volume in assigned_files:
        file_list = assigned_files[(host, volume)]
        if host is None:
            logger.error("There is insufficient space available to mirror the files %r", file_list)
            continue
        logger.info("Updating mirroring table with %d files which are assigned to %s, %s", len(file_list), host, volume)
        total_files_to_mirror += len(file_list)
        # TODO: This is going to take a while. If we update 10K rows with a DB round trip of 0.01s then the whole
        #  update will take about 2 minutes. Therefore we have to limit the sizes of each iteration so that we can be
        #  receving files while we are fetching the next one.
        sql = "update ngas_mirroring_bookkeeping set status = 'READY', target_host = {0}, staging_file = {1} " \
              "where iteration = {2} and file_id = {3} and file_version = {4}"
        for next_file in file_list:
            args = [host, volume, iteration, next_file[0], next_file[1]]
            ngams_server.getDb().query2(sql, args=args)

    # Extra part - moved from ngamsCmd_MIRRARCHIVE as a performance optimisation - we already have the information we
    # need here to check if we should mark a disk as complete
    # Check if the disk is completed.
    logger.info('Checking the available disk space remaining after this iteration')
    for host, volume in assigned_files:
        if host is None:
            continue
        available_mb = assigner.get_available_bytes(host, volume) / (1024 * 1024)
        logger.info("Available bytes after this iteration for node %s, disk mounted at %s: %s MB", host, volume,
                    available_mb)
        # Check if the disk is completed.
        if available_mb < ngams_server.getCfg().getFreeSpaceDiskChangeMb():
            logger.info('Disk on host %s at mount point %s has less than %s mb - marking as complete', host, volume,
                        ngams_server.getCfg().getFreeSpaceDiskChangeMb())
            sql = "update ngas_disks set completed = 1 where last_host_id = {0} and mount_point = {1}"
            unqualified_host_name = re.sub(r"\..*:", ":", host)
            ngams_server.getDb().query2(sql, args=(unqualified_host_name, volume))

    return total_files_to_mirror
