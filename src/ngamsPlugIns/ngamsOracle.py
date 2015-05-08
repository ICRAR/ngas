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
# "@(#) $Id: ngamsOracle.py,v 1.15 2009/12/02 23:19:08 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# awicenec  2005-03-15  Created
# jknudstr  2008-02-11  Removed usage of _queryRewrite(), not needed anymore.
#

"""
This module contains two classes:

The ngamsOracle class is an NG/AMS DB Interface Plug-In to be used for
interfacing with Oracle DB.

The ngamsOracleCursor class, which implements a cursor object based on
the NG/AMS Cursor Object API definition.
"""

import time, re
import cx_Oracle
import pcc, PccUtTime
from   ngams import *
from   mx import DateTime


class ngamsOracle:
    """
    Class to handle the connection to the NGAS DB when Oracle is used as DBMS.
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
        try:
            self.__dbModVer = str(cx_Oracle.__version__)
        except:
            self.__dbModVer = "-1"
        self.connect(server, db, user, password, application, parameters)


    def getDriverId(self):
        """
        Return DB driver ID.

        Return:    Driver version (string).
        """
        T = TRACE()

        return "NG/AMS_Oracle_" + self.__dbModVer


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
        tns = db   # the tns string is used by Oracle as part of the connection
                   # protocol. There is a mapping defined between the tns and
                   # the actual server and DB in the file /etc/tnsnames.ora
        self.__dbDrv = cx_Oracle.connect(user, password, tns, threaded = 1)

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


    def _execute(self,
                 query):
        """
        In the python Oracle module there is no direct execute, everything
        goes through cursors, thus we emulate the db.excute() method here.
        """
        T = TRACE(5)

        cur = self.__dbDrv.cursor()
        info(4, "Executing query: |%s|" % query)
        try:
           dum = cur.execute(str(query))
        except Exception, e:
            if str(e).find('ORA-00001'): #unique constraint violated
              errMsg = genLog("NGAMS_ER_DB_UNIQUE", [str(e)])
              error(errMsg)
            else:
                error(str(e))
        res = self._fetchAll(cur)
        del(cur)
        if (len(res) > 0):
            info(5, "Leaving _execute() with results")
            return [res]
        else:
            info(5, "Leaving _execute() without results")
            return [[]]


    def _fetchAll(self, cursor):
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

        startTime = time.time()

        try:
            res = cursor.fetchall()
        except Exception, e:
            if (str(e).find("not a query") != -1):
                self.__dbDrv.commit()
                info(4, "Leaving fetchAll() without results")
                return []
            else:
                self.__dbDrv.rollback()
                errMsg = "Leaving _fetchAll() after exception and " +\
                         "rollback: %s" % str(e)
                info(4, errMsg)
                return []
        deltaTime = (time.time() - startTime)

        if (len(res) > 0):
            info(4, "Leaving _fetchAll() with results. Time: %.4fs" %\
                 deltaTime)
            return res
        else:
            info(4, "Leaving _fetchAll() without results. Time: %.4fs" %\
                 deltaTime)
            return []



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
        T = TRACE()
        startTime = time.time()
        try:
            res = self._execute(query)
            deltaTime = (time.time() - startTime)
            info(4, "Leaving query() Time: %.4fs" % deltaTime)
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
                             self.__password)
                info(1,"Reconnected to DB - performing SQL query: " + query)
                res = self._execute(query)
                return res
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

        return ngamsOracleCursor(self.__server, self.__db, self.__user,
                                 self.__password,
                                 self.__application + ":" +\
                                 threading.currentThread().getName(),
                                 self.__parameters, query = query)


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

        timeStamp:  ISO 8601 Datetime string (string/ISO 8601).

        Returns:    Date time object (mx.DateTime).
        """
        T = TRACE(5)

        return DateTime.ISO.ParseDateTime(timeStamp)


class ngamsOracleCursor:
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

        query:        Query to execute (string/SQL).

        application:  Application name (string).

        parameters:   Parameters for the connection object (string).
        """
        T = TRACE()

        tns = db
        self.__cursorObj = None
        self.__dbDrv = cx_Oracle.connect(user, password, tns, threaded = 1)
        if ((query != None) and (len(query) != 0)): self._initQuery(query)


    def __del__(self):
        """
        Destructor method free'ing the internal DB connection + cursor objects.
        """
        T = TRACE()

        if (self.__cursorObj): del self.__cursorObj
        if (self.__dbDrv): del self.__dbDrv


    def _initQuery(self,
                   query):
        """
        Initialize the query.

        query:    The query to execute (string)

        Returns pointer to itself.
        """
        T = TRACE()

        # Query replace to catch DB specifics.
        #query = self._queryRewrite(query)
        self.__cursorObj = self.__dbDrv.cursor()
        info(4, "Executing query: |%s|" % query)
        self.__cursorObj.execute(str(query))
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
        T = TRACE()

        if (self.__cursorObj):
            res = self.__cursorObj.fetchmany(maxEls)
            if len(res) > 0:
                info(4, "Leaving fetch with %d results" % len(res))
                return res
            else:
                info(4, "Leaving fetch without results")
                return []
        else:
            info(4, "Leaving fetch without results (no valid cursor object)")
            return []



    def _queryRewrite(self,
                      query):
        """
        Method holds query replacements to catch differences between the SQL
        queries as coded in ngamsDb and the actual SQL query as supported by
        the DB.

        query:    The query as send by ngamsDb (string)

        Returns the modified query string.
        """
        T = TRACE()

        # The following block replaces the ignore column name (reserved word
        # in mySQL) with file_ignore.
        info(5, "Original query: %s" % query)
        regex1 = re.compile('nf.ignore')
        pquery = regex1.sub('nf.file_ignore',query)

        # Remove the Sybase specific noholdlock keyword
        regex2 = re.compile('noholdlock')
        pquery = regex2.sub('', pquery)

        #regex1 = re.compile('max\(right\(logical\_name, 6\)\)')
        #pquery = str(regex1.sub('max(substr(logical_name, -6))', pquery))
        info(5, "Rewritten query: %s" % pquery)
        return pquery


if __name__ == '__main__':
    """
    Main function to test the module.
    """
    pass

# EOF
