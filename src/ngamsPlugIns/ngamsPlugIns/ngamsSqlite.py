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
# "@(#) $Id: ngamsSqlite.py,v 1.7 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# awicenec  2005-02-28  Created
# jknudstr  2006-02-04  Updated
#
"""
This module contains two classed:

The ngamsSqlite class is an NG/AMS DB Interface Plug-In to be used for
interfacing with Sqlite DB.

The ngamsSqliteCursor class, which implements a cursor object based on
the NG/AMS Cursor Object API definition.
"""

import os

from mx import DateTime
from ngamsLib.ngamsCore import TRACE, alert, getMaxLogLevel, info, error
from pccUt import PccUtTime




try:
    import sqlite3 as sqlite
except:
    from pysqlite2 import dbapi2 as sqlite



def _queryRewrite(sqlQuery):
    """
    Function to adapt the SQL query for execution in SQLite.

    sqlQuery:    SQL query (string)

    Returns:     Modified query string.
    """
    T = TRACE(5)

    # The following block replaces the ignore column name
    # (reserved word in Sqlite) with file_ignore.
    sqlQuery = sqlQuery.replace("nf.ignore", "nf.file_ignore")

    # Remove the Sybase specific noholdlock keyword
    sqlQuery = sqlQuery.replace("noholdlock", "")

    return sqlQuery


class ngamsSqlite:
    """
    Class to handle the connection to the NGAS DB when Sqlite is used as DBMS.
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
        T = TRACE(5)

        # Check that the specified DB exists. If not, bail out.
        if (not os.path.exists(db)):
            msg = "The specified SQLite NGAS DB: %s does not exist!"
            msg = msg % str(db)
            alert(msg)
            raise Exception, msg

        try:
            self.__dbModVer = str(sqlite.version)
        except:
            self.__dbModVer = "-1"
        self.__server      = server
        self.__db          = db
        self.__user        = user
        self.__password    = password
        self.__application = application
        self.__parameters  = parameters
        self.__dbDrv       = None

        self.connect(db, application)


    def getDriverId(self):
        """
        Return DB driver ID.

        Return:    Driver version (string).
        """
        return "NG/AMS_Sqlite_" + self.__dbModVer


    def connect(self,
                db,
                application):
        """
        Method to create connection to the DB server.

        db:              DB name (string).

        application:     application name (string).

        Returns:         Reference to object itself.
        """
        T = TRACE(5)

        # Store connection parameters. Connect is not used, a new connection
        # is created for each query.
        self.__db          = db
        self.__application = application

        #self.__dbDrv = sqlite.connect(db, check_same_thread = False)

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
        T = TRACE(5)

        # In the python SQLite module there is no direct execute, everything
        # is done with cursors.
        cur = self.cursor(query)
        res = cur._fetchall()
        del(cur)

        ############################################
        # TODO: Try to use directly Sqlite.execute()
        #       Must create one connection to make
        #       this work.
        ############################################
        #query = _queryRewrite(query)
        #cur = self.__dbDrv.execute(query)
        #res = cur.fetchall()
        #cur.close()
        #del(cur)
        ############################################

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
            res = self._execute(query)
            #self.postQueryAction(query)
            return res
        except Exception, e:
            error("Exception in ngamsSqlite DB Driver Interface: %s" % str(e))
            raise e


    def cursor(self,
               query):
        """
        Create a DB cursor with the same connection properties for the
        given SQL query and return a cursor handle.

        query:       SQL query (string).

        Returns:     Cursor object (<NG/AMS Cursor Object API>.
        """
        cur = ngamsSqliteCursor(self.__db, self.__application, query)
        return cur


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


class ngamsSqliteCursor:
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
        T = TRACE(5)

        self.__dbDrv = None
        self.__cursorObj = None

        if (getMaxLogLevel() > 4):
            info(5, "Creating cursor in NGAS SQLite DB ...")
        self.__dbDrv = sqlite.connect(db)
        if (getMaxLogLevel() > 4):
            info(5, "Created cursor in NGAS SQLite DB ...")
        self.__cursorObj = self.__dbDrv.cursor()
        if ((query != None) and (len(query) > 0)): self._init(query + ";")


    def __del__(self):
        """
        Destructor method free'ing the internal DB connection + cursor objects.
        """
        if (self.__cursorObj):
            self.__cursorObj.close()
            del self.__cursorObj
        if (self.__dbDrv):
            self.__dbDrv.commit()
            self.__dbDrv.close()
            del self.__dbDrv


    def _init(self,
              query):
        """
        Initialize the query.

        query:    The query to execute (string)

        Returns pointer to itself.
        """
        T = TRACE(5)

        query = _queryRewrite(query)
        if (getMaxLogLevel() > 3): info(4, "Executing query: %s" % query)
        type = self.__cursorObj.execute(query)
        return self


    def _cleanRes(self,
                  res):
        """
        Have strange results like this:

          [[(u'NGAS-StorageSet1-Main-1', u'ESO-ARCHIVE',...

        - remove the u's.

        res:      Result from SQL query (list).

        Returns:  Cleaned result list (list).
        """
        tmpRes = str(res)
        tmpRes = tmpRes.replace(", u'", ", '")
        tmpRes = tmpRes.replace("(u'", "('")
        return eval(tmpRes)


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
            res = self.__cursorObj.fetchmany(int(maxEls))
            if len(res) > 0:
                retVal = self._cleanRes(res)
            else:
                retVal = []
        else:
            retVal = []
        return retVal


    def _fetchall(self):
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
        T = TRACE(5)

        if (self.__cursorObj):
            res = self.__cursorObj.fetchall()
            if len(res) > 0:
                return self._cleanRes(res)
            else:
                return []
        else:
            return []


if __name__ == '__main__':
    """
    Main function to test the module.
    """
    pass

# EOF
