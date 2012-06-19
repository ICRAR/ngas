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
# "@(#) $Id: ngamsHttpDbInterface.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  2007-03-09  Created
#

"""
This module contains two classed:

The ngamsHttpDbInterface class is an NG/AMS DB Interface Plug-In to be used for
interfacing with an HTTP server via the QUERY Command.

The ngamsHttpDbInterfaceCursor class, which implements a cursor object based on
the NG/AMS Cursor Object API definition.
"""

import time, re

from mx import DateTime

import pcc, PccUtTime

from   ngams import *
#import ngamsPClient
import ngamsLib, ngamsStatus

 
class ngamsHttpDbInterface:
    """
    Class to handle the connection to the NGAS DB via the NG/AMS HTTP
    based query interface (command: QUERY).
    """
  
    def __init__(self,
                 server,
                 db = "",
                 user = "",
                 password = "",
                 application = "",
                 parameters = ""):
        """
        Constructor method.

        server:          DB server name: <Node>:<Port> (string).

        db:              Ignored (string).
        
        user:            Ignored (string).
        
        password:        Ignored (string).

        application:     Name of application (ID string) (string).

        parameters:      Parameters for the connection object (string).
        """
        T = TRACE()
        
        if (not application):
            raise Exception, "Please specify an application name"
        try:
            self.__dbModVer  = str(getNgamsVersion())
        except:
            self.__dbModVer  = "-1"
        self.__server        = server
        self.__host, tmpPort = server.split(":")
        self.__port          = int(tmpPort)
        self.__db            = db
        self.__user          = user
        self.__password      = password
        self.__application   = application
        self.__parameters    = parameters
        

    def getDriverId(self):
        """
        Return DB driver ID.

        Return:    Driver version (string).
        """
        return "NG/AMS_HTTP_DB_INTERFACE_" + self.__dbModVer
    

    def connect(self,
                db,
                application):
        """
        Method to create connection to the DB server.

        db:              DB name (string).

        application:     application name (string).
 
        Returns:         Reference to object itself.
        """
        # Connect is not used.
        return self

        
    def close(self):
        """
        Close the DB connection.

        Returns:    Reference to object itself.
        """
        # Close has no meaning in this context.
        return self


    def _execute(self,
                 query):
        """
        Execute a query in the DB,

        query:       Query to execute (string).

        Returns:     Result (list).
        """
        # In the python SQLite module there is no direct execute, everything
        # is done with cursors.
        parameters = [["query", query]]
        #stat = ngamsPClient.ngamsPClient().sendCmdGen(self.__host,
        #                                              self.__port, "QUERY",
        #                                              pars = parameters)
        #try:
        #    return eval(stat.getData())
        #except:
        #    return [[]]
        code, msg, hdrs, data = ngamsLib.httpGet(self.__host, self.__port,
                                                 "QUERY", pars = parameters,
                                                 timeOut = 30.0)
        try:
            return eval(data)
        except Exception, e:
            return [[]]


    def query(self,
              query):
        """
        Perform a query in the DB and return the result. The result will
        be returned as a list with the following format:

          [[<col1>, <col2>, <col3>, ...], ...]

        An empty list ([]) may be returned if there were no matches to the
        SQL query.

        query:         SQL query (string).

        Returns:       Result of SQL query (list).
        """
        try:
            res = self._execute(query)
            return res
        except Exception, e:
            msg = "Exception in ngamsHttpDbInterface DB Driver Interface: %s"
            error(msg % str(e))
            raise e

 
    def cursor(self,
               query):
        """
        Create a DB cursor with the same connection properties for the
        given SQL query and return a cursor handle.

        query:       SQL query (string).

        Returns:     Cursor object (<NG/AMS Cursor Object API>.
        """
        cur = ngamsHttpDbInterfaceCursor(self.__server, self.__application,
                                         query)
        return cur


    def convertTimeStamp(self,
                         timeStamp):
        """
        The time-stamp given is already in the proper format when using
        this NG/AMS DB Driver.

        Note: This implementation returns an ISO-8601 string. If this is not
        appropriate for the (R)DBMS used, derive a sub-class from this class,
        and overwrite this method.

        timeStamp:    Timestamp (string)

        Returns:      Timestamp in format, which can be written into a
                      'datetime' column of the DBMS (string).
        """
        T = TRACE(5)
         
        if (str(timeStamp).find(":") != -1):
            if (timeStamp[10] != "T"): timeStamp[10] = "T"
            ts = timeStamp
            ts = PccUtTime.TimeStamp().\
                 initFromTimeStamp(timeStamp).getTimeStamp()
        else:
            ts = PccUtTime.TimeStamp().\
                 initFromSecsSinceEpoch(timeStamp).getTimeStamp()
        return ts

        
class ngamsHttpDbInterfaceCursor:
    """
    Cursor class used to fetch sequentially the result of an SQL query.
    """

    def __init__(self,
                 db,
                 application,
                 query = None):
        """
        Constructor method creating a cursor connection to the DBMS.
 
        db:           DB name (string).

        query:        Query to execute (string/SQL).

        application:  Application name (string).
        """
        self.__host, tmpPort = db.split(":")
        self.__port = int(tmpPort)
        self.__cursorId = genUniqueId()
        parameters = [["query", query],
                      ["cursor_id",  self.__cursorId ]]
#         stat = ngamsPClient.ngamsPClient().sendCmdGen(self.__host,
#                                                       self.__port, "QUERY",
#                                                       pars=parameters)
#         if (stat.getStatus() != NGAMS_SUCCESS):
#             msg = "ngamsHttpDbInterfaceCursor: Error creating cursor on " +\
#                   "server: %s, query: %s. Error: %s"
#             raise Exception, msg % (db, query, stat.getMessage())
        
        code, msg, hdrs, data = ngamsLib.httpGet(self.__host, self.__port,
                                                 "QUERY", pars = parameters,
                                                 timeOut = 30.0)
        if (code != NGAMS_HTTP_SUCCESS):
            stat = ngamsStatus.ngamsStatus().unpackXmlDoc(data, 1)
            msg = "ngamsHttpDbInterfaceCursor: Error creating cursor on " +\
                  "server: %s, query: %s. Error: %s"
            raise Exception, msg % (db, query, stat.getMessage())


    def _fetch(self,
               maxEls):
        """
        Fetch the next N elements via the cursor.


        maxEls:   Maximum number of elements to return (integer).

        Returns:  Query result (list/list).
        """
        parameters = [["cursor_id", self.__cursorId],
                      ["fetch", str(maxEls)]]
        #stat = ngamsPClient.ngamsPClient().sendCmdGen(self.__host,
        #                                              self.__port,
        #                                              "QUERY",
        #                                              pars=parameters)
        #return stat.getData()
        
        code, msg, hdrs, data = ngamsLib.httpGet(self.__host, self.__port,
                                                 "QUERY", pars = parameters,
                                                 timeOut = 30.0)
        try:
            return eval(data)        
        except Exception, e:
            return []

        
    def fetch(self,
              maxEls):
        """
        Fetch a number of elements from the query and return this.
        The result will be returned as a list with the following format:

          [[<col1>, <col2>, <col3>, ...], ...]

        An empty list ([]) may be returned if there were no matches to the
        SQL query.
        
        query:      string containing the SQL statement to be executed.

        maxEls:     Maximum number of elements/rows to return (integer).

        Return:     List containing tuples with the values queried
                    (list/list).
        """
        T = TRACE()

        res = self._fetch(maxEls)
        try:
            return eval(res)
        except:
            return res


if __name__ == '__main__':
    """
    Main function to test the module.
    """
    pass

# ___oOo___
