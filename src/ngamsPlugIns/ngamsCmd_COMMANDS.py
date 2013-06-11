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
Dynamic loadable command to query the commands available with the NG/AMS instance.

Usage URL: http://<ngas>:<port>/COMMANDS{?reload=1}
"""

from ngams import *
import cPickle, json, decimal, glob, imp

# import markup TODO: This is for HTML formatting

CURSOR_IDX           = "__CURSOR_IDX__"
NGAMS_PYTHON_LIST_MT = "application/python-list"
NGAMS_PYTHON_PICKLE_MT = "application/python-pickle"
NGAMS_JSON_MT = "application/json"


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

    commands = glob.glob(imp.find_module('ngamsPlugIns')[1]+'/ngamsCmd_*.py')
    commands = map(lambda x:os.path.basename(x), commands)
    commands = map(lambda x:x.split('.')[0].split('_')[1], commands)

    srvObj.httpReplyGen(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS,
                                '\n'.join(commands), 0, NGAMS_PYTHON_LIST_MT)

    return


# EOF

