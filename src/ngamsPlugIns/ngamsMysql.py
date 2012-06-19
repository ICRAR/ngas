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
# "@(#) $Id: ngamsMysql.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# awicenec  2005-02-28  Created
#

"""
This module contains two classed:

The ngamsMysql class is an NG/AMS DB Interface Plug-In to be used for
interfacing with MySql DB.

The ngamsMysqlCursor class, which implements a cursor object based on
the NG/AMS Cursor Object API definition.
"""

import time, re
import MySQLdb
import pcc,PccUtTime
from   ngams import *
from   mx import DateTime

 
class ngamsMysql:
    """
    Class to handle the connection to the NGAS DB when MySQL is
    used as DBMS.
    """
  
    def __init__(self,
                 server,
                 db,
                 user,
                 password,
                 application,
                 parameters):
        """
        Constructor method.

        server:          DB server name (string).

        db:              DB name (string).
        
        user:            DB user (string).
        
        password:        DB password (string).

        application:     Name of application (ID string) (string).

        parameters:      Parameters for the connection object (string).
        """
        T = TRACE()

        try:
            self.__dbModVer = str(MySQLdb.__version__)
        except:
            self.__dbModVer = "-1"
        self.connect(server, db, user, password, application, parameters)


    def getDriverId(self):
        """
        Return DB driver ID.

        Return:    Driver version (string).
        """
        T = TRACE()
        
        return "NG/AMS_MySQL_" + self.__dbModVer
    

    def connect(self,
                server,
                db,
                user,
                password,
                application,
                parameters):
        """
        Method to create connection to the DB server.

        server:          DB server name (string).

        db:              DB name (string).
        
        user:            DB user (string).
        
        password:        DB password (string).

        application:     Name of application (ID string) (string).

        parameters:      Parameters for the connection object (string).
 
        Returns:         Reference to object itself.
        """
        T = TRACE()
        
        # Set up DB connection.
        self.__dbDrv = ngamsMySqlCursor(server, db, user, password,
                                        application, parameters)
               
        # Store connection parameters.
        self.__server      = server
        self.__db          = db
        self.__user        = user
        self.__password    = password
        self.__application = application
        self.__parameters  = parameters

        return self

        
    def close(self):
        """
        Close the DB connection.

        Returns:    Reference to object itself.
        """
        T = TRACE()
        
        del(self.__dbDrv)
        return close


    def execute(self,
                query):
        """
        in the python MySQLdb module there is no direct execute,
        everything goes through cursors, thus we emulate the db.excute()
        method here.
        """
        T = TRACE(5)
        
        cur = self.cursor(query)
        res = cur.fetchall()
        del(cur)
        if (len(res) > 0):
            return [res]
        else:
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
        T = TRACE(5)
        
        try:
            res = self.execute(query)
            return res
        except Exception, e:
            for n in range(10):
                try:
                    pass
                except Exception, e:
                    time.sleep(0.1)
                break

            # Try to reconnect once if the connection is not available
            # - maybe it was lost.
            if ((str(e).find("connection is not open") != -1) or
                (str(e).find("connection has been marked dead") != -1) or
                (str(e).find("operation terminated due to disconnect") != -1)):
                time.sleep(2.0)
                self.connect(self.__server, self.__db, self.__user,
                             self.__password, self.__parameters)
                info(1,"Reconnected to DB - performing SQL query: " +\
                     sqlQuery)
                res = self.__dbDrv.execute(query)
                return [res]
            else:
                raise e

 
    def cursor(self,
               query):
        """
        Create a DB cursor with the same connection properties for the
        given SQL query and return a cursor handle.

        query:       SQL query (string).

        Returns:     Cursor object (<NG/AMS Cursor Object API>.
        """
        T = TRACE()

        return self.__dbDrv.initQuery(query)


    def convertTimeStamp(self,
                         timeStamp):
        """
        Convert a timestamp in one of the following representations to
        a timestamp string, which can be used to set a column of the DBMS
        of type 'datetime'.

        timeStamp:    Timestamp represented in one of the following formats:

                        1. ISO 8601:  YYYY-MM-DDTHH:MM:SS[.s]
                        2. ISO 8601': YYYY-MM-DD HH:MM:SS[.s]
                        3. Secs since epoc.
                                                        (string|integer|float).

        Returns:      Timestamp in format, which can be written into
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

        
    def convertTimeStampToMx(self,
                             timeStamp):
        """
        Converts an ISO 8601 timestamp into an mx.DateTime object.
        
        timeStamp:  ISO 8601 datetime string (string).
        
        Returns:    Datetime object (mx.DateTime).
        """
        T = TRACE(5)
        
        dt = DateTime.ISO.ParseDateTime(timeStamp)
        return dt


class ngamsMySqlCursor:
    """
    Cursor class used to fetch sequentially the result of an SQL query.
    """

    def __init__(self,
                 server,
                 db,
                 user,
                 password,
                 application,
                 parameters,
                 query = None):
        """
        Constructor method creating a cursor connection to the DBMS.

        server:       DB server name (string).
 
        db:           DB name (string).
        
        user:         DB user (string).
        
        password:     DB password (string).


        application:  Application name (string).

        parameters:   Parameters for the connection object (string).

        query:        Query to execute (string/SQL).
        """
        T = TRACE()

        self.__cursorObj = None
        self.__dbDrv = None
        self.__dbDrv = MySQLdb.connect(host = server, user = user,
                                       passwd = password, db = db)
        self.__cursorObj = self.__dbDrv.cursor()
        if ((query != None) and (len(query) != 0)): self.initQuery(query)


    def __del__(self):
        """
        Destructor method free'ing the internal DB connection + cursor objects.
        """
        T = TRACE()
        
        if (self.__cursorObj): del self.__cursorObj
        if (self.__dbDrv): del self.__dbDrv
        
                     
    def initQuery(self,
                  query):
        """
        Initialize the query.
        
        query:    The query to execute (string)
        
        Returns pointer to itself.
        """
        T = TRACE(5)
        
        # query replace to catch DB specifics
        pquery = self.queryRewrite(query)

        info(4, "Executing query:" + pquery)
        type = self.__cursorObj.execute(pquery)
        return self
        

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
        T = TRACE(5)
        
        if (self.__cursorObj):
            res = self.__cursorObj.fetchmany(maxEls)
            if len(res) > 0:
                return res
            else:
                return []
        else:
            return []


    def fetchAll(self):
        """
        Fetch all elements from the query and return this.
        The result will be returned as a list with the following format:

          [[<col1>, <col2>, <col3>, ...], ...]

        An empty list ([]) may be returned if there were no matches to the
        SQL query.
        
        query:      string containing the SQL statement to be executed.

        Return:     List containing tuples with the values queried
                    (list/list).
        """
        T = TRACE()
        
        if (self.__cursorObj):
            res = self.__cursorObj.fetchall()
            if len(res) > 0:
                return res
            else:
                return []
        else:
            return []


    def queryRewrite(self,query):
        """
        Method holds query replacements to catch differences between the SQL
        queries as coded in ngamsDb and the actual SQL query as supported by
        the DB.

        query:    The query as send by ngamsDb (string)
        
        Returns the modified query string.
        """
        T = TRACE(5)
        
        # The following block replaces the ignore column name
        # (reserved word in MySQL) with fignore.
        regex1 = re.compile("ignore")
        pquery = regex1.sub("fignore", query)

        # Remove the Sybase specific noholdlock keyword
        regex2 = re.compile("noholdlock")
        pquery = regex2.sub("", pquery)

        return pquery


if __name__ == '__main__':
    """
    Main function to test the module.
    """
    pass


# EOF
