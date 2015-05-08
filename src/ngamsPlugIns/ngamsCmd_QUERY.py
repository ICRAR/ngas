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

from ngams import *
import ngamsDbm, ngamsDbCore
import cPickle, json, decimal

# import markup TODO: This is for HTML formatting

CURSOR_IDX           = "__CURSOR_IDX__"
NGAMS_PYTHON_LIST_MT = "application/python-list"
NGAMS_PYTHON_PICKLE_MT = "application/python-pickle"
NGAMS_JSON_MT = "application/json"
NGAMS_FILES_COLS = map(lambda x:x[1],ngamsDbCore._ngasFilesDef)
NGAMS_DISKS_COLS = map(lambda x:x[1],ngamsDbCore._ngasDisksDef)

#creation_date could be different from ingestion_date if it is a mirrored archive 
# ingestion_date is when the original copy was ingested in the system, 
# creation_date is when the replicated copy appears on the mirrored archive
LASTVER_LOCATION = "SELECT a.host_id, a.mount_point || '/' || b.file_name as file_full_path, b.file_version, b.creation_date, b.ingestion_date FROM ngas_disks a, ngas_files b, " +\
                    "(SELECT file_id, MAX(file_version) AS max_ver FROM ngas_files WHERE file_id like '{0}' GROUP BY file_id) c " +\
                    "WHERE a.disk_id = b.disk_id and " +\
                    "b.file_id like '{0}' and " +\
                    "b.file_id = c.file_id and b.file_version = c.max_ver " +\
                    "order by b.file_id"

valid_queries = {"files_list":"select * from ngas_files",
                  "disks_list":"select * from ngas_disks", 
                  "hosts_list":"select * from ngas_hosts",
                  "files_like":"select * from ngas_files where file_id like '{0}'",
                  "files_location":"select a.host_id, a.mount_point || '/' || b.file_name as file_full_path, b.file_version, b.ingestion_date from ngas_disks a, ngas_files b where a.disk_id = b.disk_id and b.file_id like '{0}' order by b.file_id",
                  "lastver_location":LASTVER_LOCATION,
                  "files_between":"select * from ngas_files where ingestion_date between '{0}' and '{1}'",
                  "files_stats":"select count(*),sum(uncompressed_file_size)/1048576. as MB from ngas_files",
                  "files_list_recent":"select file_id, file_name, file_size, ingestion_date from ngas_files order by ingestion_date desc limit 300",
                }



def encode_decimal(obj):
    """
    Just a little helper function for JSON serialisation
    """
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(repr(obj) + " is not JSON serializable")

def createJsonObj(resultSet, queryKey):
    """
    Format the query result as an object that is json friendly, 

    resultSet:      Result returned from the SQL interface (list).

    Returns:  i.e. a list of dic, each of which is a record (List[{fieldname, fieldvalue}]
    """
    jsobj = {}
    listResult = []
    for res in resultSet[0]:
        col = 1
        record = {}
        for subRes in res:
            colName = 'col' + str(col)
            colVal = str(subRes)
            record[colName] = colVal
            col += 1
        listResult.append(record)
    jsobj[queryKey] = listResult
    return jsobj

def formatAsList(resultSet, header = None):
    """
    Format the query result as a list.

    resultSet:      Result returned from the SQL interface (list).
    header:         column names in the correct order (tuple)

    Returns:  Result formatted as a list (string).
    """
    # Go through the results, find the longest result per column and use
    # that as basis for the column.
    formatStrDic = {}
    reList = resultSet[0]
    if (header):
        reList = [header] + reList
    for res in reList:
        col = 0
        for subRes in res:
            if (not formatStrDic.has_key(col)): formatStrDic[col] = 0
            if (len(str(subRes)) > formatStrDic[col]):
                formatStrDic[col] = len(str(subRes))
            col += 1

    # Build up format string.
    formatStr = ""
    col = 0
    if (header):
        headers = ()
    while (True):
        if (not formatStrDic.has_key(col)): break
        formatStr += "%%-%ds" % (formatStrDic[col] + 3)
        if (header):
            headers += ('-' * formatStrDic[col],)
        col += 1
    formatStr += "\n"

    # Now, generate the list.
    listBuf = ""
    
    if (header):
        reList = [headers] + [header] + [headers] + reList[1:]
    for res in reList:
        valList = []
        for subRes in res:
            valList.append(str(subRes))
        listBuf += formatStr % tuple(valList)

    return listBuf


# TODO: Add proper markup module
#def formatAsHTML(resultSet):
#    """
#    Format the result as an HTML table
#    
#    resultSet:    (list) result returned from the SQL interface
#    
#    Returns:      (string) Result formatted as HTML
#    """
#    resultHTML = resultSet
#    title = "NGAS"
#    header = "Some information at the top, perhaps a menu."
#    footer = "Dynamic page created by NGAS server: {0}".format(time.strftime('%Y-%M-%dT%H:%m:%S'))
#    styles = ( 'layout.css', 'alt.css', 'images.css' )
#    
#    page = markup.page( )
#    page.init( css=styles, title=title, header=header, footer=footer )
#    page.br( )
#    
#    page.table(border="1")
#    page.thead()
#    page.th()
#    page.td()
#    page.td.close()
#    page.th.close()
#    page.thead.close()
#    page.tbody()
#    page.tr()
#    for row in resultSet:
#        for cell in row:
#            page.td(cell)
#        page.tr.close()
#    page.tbody.close()
#    page.table.close()
#    
#    return page.__str__()


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
        qkey = query
        if query.lower() in valid_queries.keys():
            query = valid_queries[query.lower()]
        else:
            msg = "Invalid query specified. Valid queries are: %s" %\
            valid_queries.keys()
            raise Exception, msg
        
        if reqPropsObj.getHttpPar("query") == 'files_like' or reqPropsObj.getHttpPar("query") == 'files_location' or reqPropsObj.getHttpPar("query") == 'lastver_location':
            param = '%'
            if (reqPropsObj.hasHttpPar("like")):
                param = reqPropsObj.getHttpPar("like")
            query = query.format(param)
        if reqPropsObj.getHttpPar("query") == 'files_between':
            param1 = param2 = ''
            if (reqPropsObj.hasHttpPar("start")):
                param1 = reqPropsObj.getHttpPar("start")
            if (reqPropsObj.hasHttpPar("end")):
                param2 = reqPropsObj.getHttpPar("end")
            if param1 and param2:
                query = query.format(param1, param2)
            elif param1:
                query = 'select * from ngas_files where ingestion_date >= "{0}"'.format(param1)
            else:
                query = valid_queries['files_list']
    else:
        msg = "No query specified. Valid queries are: %s" %\
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
            header = None
            if reqPropsObj.getHttpPar("query") not in ['files_stats', 'files_list_recent', 'files_location', 'lastver_location']:
                if query.find('ngas_files') >=0:
                    header = NGAMS_FILES_COLS
                elif query.find('ngas_disks') >= 0:
                    header = NGAMS_DISKS_COLS
            elif reqPropsObj.getHttpPar("query") == 'files_stats':
                header = ['Number of files', 'Total volume [MB]']
            elif reqPropsObj.getHttpPar("query") == 'files_list_recent':
                header = ['file_id', 'file_name', 'file_size', 'ingestion_date']
            elif reqPropsObj.getHttpPar("query") == 'files_location':
                header = ['host_id', 'file_full_path', 'file_version', 'ingestion_date']
            elif reqPropsObj.getHttpPar("query") == 'lastver_location':
                header = ['host_id', 'file_full_path', 'file_version', 'creation_date', 'ingestion_date']
            finalRes = formatAsList(res, header=header)
            """
            if query.find('ngas_files') >=0:
                header = NGAMS_FILES_COLS
            elif query.find('ngas_disks') >= 0:
                header = NGAMS_DISKS_COLS
            else:
                header = None
            finalRes = formatAsList(res, header=header)
            """
            mimeType = NGAMS_TEXT_MT
        elif (out_format == "pickle"):
            finalRes = cPickle.dumps(res)
            mimeType = NGAMS_PYTHON_PICKLE_MT
        elif (out_format == "json"):
            jsobj = createJsonObj(res, qkey)
            finalRes = json.dumps(jsobj, default=encode_decimal)
            mimeType = NGAMS_JSON_MT
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
