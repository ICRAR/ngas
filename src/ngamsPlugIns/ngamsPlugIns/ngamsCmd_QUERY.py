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
# "@(#) $Id: ngamsCmd_QUERY.py,v 1.6 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  15/07/2006  Created
#
"""
Dynamic loadable command to query the DB associated with the NG/AMS instance.
"""

import decimal
import io
import json
import logging
import os

import six
from six.moves import cPickle  # @UnresolvedImport
from six.moves import reduce  # @UnresolvedImport

from ngamsLib import ngamsDbm, ngamsDbCore
from ngamsLib.ngamsCore import NGAMS_TMP_FILE_EXT, TRACE, NGAMS_TEXT_MT, rmFile


logger = logging.getLogger(__name__)

CURSOR_IDX           = "__CURSOR_IDX__"
NGAMS_PYTHON_LIST_MT = "application/python-list"
NGAMS_PYTHON_PICKLE_MT = "application/python-pickle"
NGAMS_JSON_MT = "application/json"

# Dirty trick to get the simple columnnames from these tables
NGAMS_FILES_COLS = map(lambda x: x[0].split('.')[1], ngamsDbCore._ngasFilesDef)
NGAMS_DISKS_COLS = map(lambda x: x[0].split('.')[1], ngamsDbCore._ngasDisksDef)
NGAMS_SUBSCR_COLS = map(lambda x: x[0].split('.')[1], ngamsDbCore._ngasSubscribersDef)
NGAMS_HOST_COLS = map(lambda x: x[0].split('.')[1], ngamsDbCore._ngasHostsDef)

#creation_date could be different from ingestion_date if it is a mirrored archive
# ingestion_date is when the original copy was ingested in the system,
# creation_date is when the replicated copy appears on the mirrored archive
_LOCATION_COLS = ('file_full_path', 'file_version', 'creation_date', 'ingestion_date')
_FILES_LOCATION_SQL = ("SELECT a.host_id, a.mount_point || '/' || b.file_name AS file_full_path, b.file_version, b.ingestion_date FROM ngas_disks a, ngas_files b "
                       "WHERE a.disk_id = b.disk_id AND "
                       "b.file_id LIKE {0} "
                       "ORDER BY b.file_id")
_LASTVER_LOCATION_SQL = ("SELECT a.host_id, a.mount_point || '/' || b.file_name AS file_full_path, b.file_version, b.creation_date, b.ingestion_date FROM ngas_disks a, ngas_files b, "
                         "(SELECT file_id, MAX(file_version) AS max_ver FROM ngas_files WHERE file_id like {0} GROUP BY file_id) c "
                         "WHERE a.disk_id = b.disk_id and "
                         "b.file_id LIKE {0} AND "
                         "b.file_id = c.file_id and b.file_version = c.max_ver "
                         "ORDER BY b.file_id")

# query_name: (column_list, SQL statement)
queries = {
    "files_list":
        (NGAMS_FILES_COLS,
        "select * from ngas_files"),
    "subscribers_list":
        (NGAMS_SUBSCR_COLS,
        "select * from ngas_subscribers"),
    "subscribers_like":
        (NGAMS_SUBSCR_COLS,
        "select * from ngas_subscribers where host_id like {0}"),
    "disks_list":
        (NGAMS_DISKS_COLS,
        "select * from ngas_disks"),
    "hosts_list":
        (NGAMS_HOST_COLS,
        "select * from ngas_hosts"),
    "files_like":
        (NGAMS_FILES_COLS,
        "select * from ngas_files where file_id like {0}"),
    "files_location":
        (_LOCATION_COLS,
         _FILES_LOCATION_SQL),
    "lastver_location":  (
        _LOCATION_COLS,
        _LASTVER_LOCATION_SQL),
    "files_between":
        (NGAMS_FILES_COLS,
         "select * from ngas_files where ingestion_date between {0} and {1}"),
    "files_stats":
        (('Number of files', 'Total volume [MB]'),
         "select count(*),sum(uncompressed_file_size)/1048576. as MB from ngas_files"),
    "files_list_recent":
        (('file_id', 'file_name', 'file_size', 'ingestion_date'),
         "select file_id, file_name, file_size, ingestion_date from ngas_files order by ingestion_date desc limit 300"),
}



def encode_decimal(obj):
    """
    Just a little helper function for JSON serialisation
    """
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(repr(obj) + " is not JSON serializable")


def formatAsList(resultSet, colnames):
    """
    Format the query result as a list.

    resultSet:      Result returned from the SQL interface (list).
    header:         column names in the correct order (tuple)

    Returns:  Result formatted as a list (string).
    """

    # Go through the results, find the longest result per column and use
    # that as basis for the column.
    rows = list(resultSet)

    max_col_lens = [reduce(max, map(len, map(str, r))) for r in zip(colnames, *rows)]
    lines = [b'-' * l for l in max_col_lens]
    col_fmts = ['{:%d}' % l for l in max_col_lens]

    # Write upper set of lines, column names, and bottom lines
    buf = io.BytesIO()
    buf.write(b' '.join(lines))
    buf.write(b'\n')
    buf.write(b' '.join((six.b(fmt.format(c)) for fmt, c in zip(col_fmts, colnames))))
    buf.write(b'\n')
    buf.write(b' '.join(lines))

    # Write row values using the corresponding format string
    for r in rows:
        buf.write(b'\n')
        buf.write(b' '.join(fmt.format(val) for fmt, val in zip(col_fmts, r)))

    return buf.getvalue()


def genCursorDbmName(rootDir,
                     cursorId):
    """
    Generate a QUERY Cursor DBM filename.

    rootDir:   NGAS Root Directory (string).

    cursorId:  Cursor ID (string).

    Returns:   Name of Cursor DBM File corresponding to the Cursor ID (string).
    """
    path = "%s/%s/QUERY_CURSOR_%s.bsddb" %\
           (rootDir, NGAMS_TMP_FILE_EXT, cursorId)
    return os.path.normpath(path)


def handleCmd(srvObj,
              reqPropsObj,
              httpRef):
    """
    Handle Command QUERY to query the DB system used.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """
    T = TRACE()

    # Get command parameters.
    if not 'query' in reqPropsObj:
        raise Exception("No query specified. Valid queries are: %s" % (queries.keys(),))
    query = reqPropsObj.getHttpPar("query").lower()
    if query not in queries.keys():
        raise Exception("Invalid query specified. Valid queries are: %s" % (queries.keys(),))

    out_format = None
    if 'format' in reqPropsObj:
        out_format = reqPropsObj["format"]

    cursorId = None
    if (reqPropsObj.hasHttpPar("cursor_id")):
        cursorId = reqPropsObj.getHttpPar("cursor_id")

    fetch = None
    if (reqPropsObj.hasHttpPar("fetch")):
        fetch = int(reqPropsObj.getHttpPar("fetch"))

    # Select the SQL statement + pars to execute
    colnames, sql = queries[query]
    args = ()
    if query in ('subscribers_like', 'files_like', 'files_location', 'lastver_location'):
        param = '%'
        if (reqPropsObj.hasHttpPar("like")):
            param = reqPropsObj.getHttpPar("like")
        args = (param,)
    elif query == 'files_between':
        param1 = param2 = ''
        if 'start' in reqPropsObj:
            param1 = reqPropsObj["start"]
        if 'end' in reqPropsObj:
            param2 = reqPropsObj["end"]
        if param1 and param2:
            args = (param1, param2)
        elif param1:
            sql = 'select * from ngas_files where ingestion_date >= {0}'
            args = (param1,)
        else:
            sql = queries['files_list']

    # Execute the query.
    if not cursorId:

        # TODO: Make possible to return an XML document
        # TODO: Potential problem with very large result sets.
        #       Implement streaming result directly3
        # rtobar, 2018 Feb: for the streaming result functionality to work
        #                   correctly we need to support sending content with
        #                   chunked transfer encoding first. This is not
        #                   difficult to add on the server side, but needs
        #                   to be also added on the client side (which I suppose
        #                   means all languages, not only python, if we really
        #                   need to maintain those).
        #                   We already support proper cursors at the database
        #                   access level, so it's not difficult to change
        #                   the query below to use a cursor and work as a
        #                   generator instead of returning the full list of
        #                   results in one go.
        res = srvObj.getDb().query2(sql, args=args)

        if out_format in ("list", 'text'):
            finalRes = formatAsList(res, colnames)
            mimeType = NGAMS_TEXT_MT
        elif out_format == "pickle":
            finalRes = cPickle.dumps([res])
            mimeType = NGAMS_PYTHON_PICKLE_MT
        elif out_format == "json":
            results = [{colname: val for colname, val in zip(colnames, row)} for row in res]
            finalRes = six.b(json.dumps(results, default=encode_decimal))
            mimeType = NGAMS_JSON_MT
        else:
            finalRes = six.b(str(list(res)))
            mimeType = NGAMS_PYTHON_LIST_MT

        # Return the data and good bye.
        httpRef.send_data(finalRes, mimeType)
        return


    # TODO:
    #
    #
    #
    # The rest seems to be very old functionality, which we probably
    # want to drop at some point. I'm still keeping it here for the time being
    # to be a good citizen and not break any (very potential) user.
    #
    #
    #
    elif (fetch):
        cursorDbmFilename = genCursorDbmName(srvObj.getCfg().\
                                             getRootDirectory(), cursorId)
        if (not os.path.exists(cursorDbmFilename)):
            logger.error("Illegal Cursor ID: %s or cursor expired", cursorId)
            return []
        try:
            cursorDbm = ngamsDbm.ngamsDbm(cursorDbmFilename, writePerm=1)
            count = 0
            resSet = []
            cursorIdx = cursorDbm.get(CURSOR_IDX)
            while (count < fetch):
                cursorIdx += 1
                if (cursorDbm.hasKey(str(cursorIdx))):
                    res = cursorDbm.get(str(cursorIdx))
                    resSet.append(res)
                else:
                    # -- no more results to return.
                    break
                count += 1
            cursorDbm.add(CURSOR_IDX, cursorIdx)
            cursorDbm.sync()
            del cursorDbm
            # If all entries have been fetched, we delete the cursor DBM.
            if (count < fetch):
                rmFile(cursorDbmFilename + "*")

            # Return the data.
            # TODO: Make it possible to return ASCII List + XML.
            httpRef.send_data(str(resSet), NGAMS_PYTHON_LIST_MT)

        except Exception as e:
            msg = "Error fetching from cursor with ID: %s. Error: %s"
            raise Exception(msg % (cursorId, str(e)))
    elif (query and cursorId):
        logger.debug("Creating new cursor with ID: %s, query: %s",
                     cursorId, query)
        cursorDbmFilename = genCursorDbmName(srvObj.getCfg().\
                                             getRootDirectory(), cursorId)
        cursorDbm = ngamsDbm.ngamsDbm(cursorDbmFilename, writePerm=1)

        # Make the query in a cursor and dump the results into the DBM.
        curObj = srvObj.getDb().dbCursor(query, args=args)
        with curObj:
            for res in curObj.fetch(1000):
                cursorDbm.addIncKey(res)
        cursorDbm.add(CURSOR_IDX, 0)
        cursorDbm.sync()
        del cursorDbm
        # TODO: In this case no reply is generated??
    else:
        msg = "Error illegal combination of parameters. Correct syntax is: " +\
              "QUERY?query=<Query>[&out_format=<Format (list)>] or " +\
              "QUERY?query=<Query>&cursor_id=<ID> followed by N calls to " +\
              "QUERY?cursor_id=<ID>&fetch=<Number of Elements>"
        raise Exception(msg)


# EOF
