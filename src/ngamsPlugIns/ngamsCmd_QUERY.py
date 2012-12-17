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
# "@(#) $Id: ngamsCmd_QUERY.py,v 1.6 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  15/07/2006  Created
#

"""
Dynamic loadable command to query the DB associated with the NG/AMS instance.
"""

import pydoc
import pcc, PccUtTime
from ngams import *
import ngamsLib, ngamsStatus, ngamsDbm

CURSOR_IDX           = "__CURSOR_IDX__"
NGAMS_PYTHON_LIST_MT = "application/python-list"

valid_queries = {"files_list":"select * from ngas_files",
                  "disks_list":"select * from ngas_disks", 
                  "hosts_list":"select * from ngas_hosts"}


def formatAsList(resultSet):
    """
    Format the query result as a list.

    resultSet:      Result returned from the SQL interface (list).

    Returns:  Result formatted as a list (string).
    """
    # Go through the results, find the longest result per column and use
    # that as basis for the column.
    formatStrDic = {}
    for res in resultSet[0]:
        col = 0
        for subRes in res:
            if (not formatStrDic.has_key(col)): formatStrDic[col] = 0
            if (len(str(subRes)) > formatStrDic[col]):
                formatStrDic[col] = len(str(subRes))
            col += 1

    # Build up format string.
    formatStr = ""
    col = 0
    while (True):
        if (not formatStrDic.has_key(col)): break
        formatStr += "%%-%ds" % (formatStrDic[col] + 3)
        col += 1
    formatStr += "\n"

    # Now, generate the list.
    listBuf = ""
    for res in resultSet[0]:
        valList = []
        for subRes in res:
            valList.append(str(subRes))
        listBuf += formatStr % tuple(valList)

    return listBuf


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
    query = None
    if (reqPropsObj.hasHttpPar("query")):
        query = reqPropsObj.getHttpPar("query")
        if query.lower() in valid_queries.values():
            query = valid_queries[query.lower()]
        else:
            msg = "Invalid query specified. Valid queries are: %s" %\
            valid_queries.keys()
            
        raise Exception, msg

    out_format = None
    if (reqPropsObj.hasHttpPar("format")):
        out_format = reqPropsObj.getHttpPar("format")    
    cursorId = None
    if (reqPropsObj.hasHttpPar("cursor_id")):
        cursorId = reqPropsObj.getHttpPar("cursor_id")
    fetch = None
    if (reqPropsObj.hasHttpPar("fetch")):
        fetch = int(reqPropsObj.getHttpPar("fetch"))

    # Execute the query.
    if (not cursorId):
        info(3, "Executing SQL query: %s" % str(query))
        res = srvObj.getDb().query(query, maxRetries=1, retryWait=0)

        # TODO: Make possible to return an XML document
        # TODO: Potential problem with very large result sets.
        #       Implement streaming result directly.
        if (out_format == "list"):
            finalRes = formatAsList(res)
            mimeType = NGAMS_TEXT_MT
        else:
            finalRes = str(res)
            mimeType = NGAMS_PYTHON_LIST_MT

        # Return the data.
        srvObj.httpReplyGen(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                            finalRes, 0, mimeType)
    elif (fetch):
        cursorDbmFilename = genCursorDbmName(srvObj.getCfg().\
                                             getRootDirectory(), cursorId)
        if (not os.path.exists(cursorDbmFilename)):
            if (getMaxLogLevel() >= 4):
                msg = "Illegal Cursor ID: %s or cursor expired" % cursorId
                notice(msg)
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
            srvObj.httpReplyGen(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                                str(resSet), 0, NGAMS_PYTHON_LIST_MT)
        except Exception, e:
            msg = "Error fetching from cursor with ID: %s. Error: %s"
            raise Exception, msg % (cursorId, str(e))                       
    elif (query and cursorId):
        info(4, "Creating new cursor with ID: %s, query: %s" %\
             (cursorId, query))
        cursorDbmFilename = genCursorDbmName(srvObj.getCfg().\
                                             getRootDirectory(), cursorId)
        cursorDbm = ngamsDbm.ngamsDbm(cursorDbmFilename, writePerm=1)

        # Make the query in a cursor and dump the results into the DBM.
        curObj = srvObj.getDb().dbCursor(query)
        while (True):
            resSet = curObj.fetch(1000)
            if (not resSet): break
            for res in resSet:
                cursorDbm.addIncKey(res)
        cursorDbm.add(CURSOR_IDX, 0)
        cursorDbm.sync()
        del curObj
        del cursorDbm
        # TODO: In this case no reply is generated??
    else:
        msg = "Error illegal combination of parameters. Correct syntax is: " +\
              "QUERY?query=<Query>[&out_format=<Format (list)>] or " +\
              "QUERY?query=<Query>&cursor_id=<ID> followed by N calls to " +\
              "QUERY?cursor_id=<ID>&fetch=<Number of Elements>"
        raise Exception, msg

    return


# EOF
