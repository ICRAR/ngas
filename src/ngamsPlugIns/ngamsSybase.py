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
# "@(#) $Id: ngamsSybase.py,v 1.7 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/05/2001  Created
#

"""
This module contains two classes:

The ngamsSybase class is an NG/AMS DB Interface Driver to be used for
interfacing with Sybase ASE.

The ngamsSybaseCursor class, which implements a cursor object based on
the NG/AMS Cursor Object API definition.

Note: A mechanism for simulating problems in the interaction with remote
      DB server. The following problems are simluated:

        1. Broken connection (2.5%).
        2. Problems creating connection (2.5%).
        3. Empty query result returned (10%).

This feature is only enabled if the global Test Mode Flag is set to one.
See functions setTestMode()/getTestMode() in the main NG/AMS module.
"""

import time, threading, random
import Sybase

import pcc, PccUtTime
from   ngams import *

# Probability for different, simulated errors.
if (1):
    ERR_BROKEN_CON = 0.0
    ERR_CREAT_CON  = 0.0
    ERR_EMPTY_RES  = 0.0
else:
    ERR_BROKEN_CON = 0.025
    ERR_CREAT_CON  = 0.025
    ERR_EMPTY_RES  = 0.10


def _provokeErr(prop = 0.5):
    """
    Internal function to generate randomly an indication whether or not the
    DB Driver should provoke an error or not.
   
    prop:     Probability for requesting to produce error (0.5 = 50%)
              (float/[0.0; 1.0]).
    Returns:  Indication if error should be produced or not (integer/0|1).
    """
    if (getTestMode()):
        if (random.randint(1, 10) <= (10 * prop)):
            return 1
        else:
            return 0
    else:
        return 0

 
class ngamsSybase:
    """
    Class to handle the connection to the NGAS DB when Sybase ASE is
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
            self.__sybModVer = str(Sybase.__version__)
        except:
            self.__sybModVer = "-1"
        self.connect(server, db, user, password, application, parameters)


    def getDriverId(self):
        """
        Return DB driver ID.

        Return:    Driver version (string).
        """
        T = TRACE()
        
        return "NG/AMS_Sybase_" + self.__sybModVer
    

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
        
        # Provoke problem creating connection.
        if (_provokeErr(ERR_BROKEN_CON)):
            raise Exception, "Layer: 5, Origin: 3 ct_connect(): " +\
                  "network packet layer: internal net library error: " +\
                  "Net-Lib protocol driver call to connect two " +\
                  "endpoints failed (TEST)"

        # Set up DB connection.
        self.__dbDrv = Sybase.connect(server, user, password,delay_connect = 1)
        self.__dbDrv.set_property(Sybase.CS_HOSTNAME, getHostId())
        self.__dbDrv.set_property(Sybase.CS_APPNAME, application)
        self.__dbDrv.connect()
        self.__dbDrv.execute("use " + db)
               
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
        
        self.__dbDrv.close()
        return close


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
            # Provoke broken connection.
            if (_provokeErr(ERR_BROKEN_CON)):
                raise Exception, "Layer: 1, Origin: 1 ct_cmd_drop(): " +\
                      "user api layer: external error: The connection " +\
                      "has been marked dead (TEST)"

            # Normal execution.
            res = self.__dbDrv.execute(query)
            self.__dbDrv.execute("commit transaction")

            # Provoke empty query result. This comes after actually executing
            # the query, in case the query was an INSERT or UPDATE.
            if (_provokeErr(ERR_EMPTY_RES)): return []

            return res
        except Exception, e:
            self.__dbDrv.execute("commit transaction")
            # Reset the connection.
            for n in range(10):
                try:
                    stat = self.__dbDrv._conn.ct_cancel(Sybase.CS_CANCEL_ALL)
                except Exception, e2:
                    time.sleep(0.1)
                break
            raise Exception, e

 
    def cursor(self,
               query):
        """
        Create a DB cursor with the same connection properties for the
        given SQL query and return a cursor handle.

        query:       SQL query (string).

        Returns:     Cursor object (<NG/AMS Cursor Object API>.
        """
        T = TRACE()

        return ngamsSybaseCursor(self.__server, self.__db, self.__user,
                                 self.__password,
                                 self.__application + ":" +\
                                 threading.currentThread().getName(),
                                 self.__parameters, query)


    def convertTimeStamp(self,
                         timeStamp):
        """
        Convert a timestamp in one of the following representations to
        a timestamp string, which can be used to set a column of the DBMS
        of type 'datetime' (Sybase type name).

        timeStamp:    Timestamp represented in one of the following formats:

                        1. ISO 8601:  YYYY-MM-DDTHH:MM:SS[.s]
                        2. ISO 8601': YYYY-MM-DD HH:MM:SS[.s]
                        3. Secs since epoc.
                                                        (string|integer|float).

        Returns:      Timestamp in format, which can be written into a
                      'datetime' column of the DBMS (string).
        """
        T = TRACE(5)
        
        if (str(timeStamp).find(":") != -1):
            if (timeStamp[10] != "T"): timeStamp[10] = "T"
            ts = PccUtTime.TimeStamp().\
                 initFromTimeStamp(timeStamp).getSybaseTimeStamp()
        else:
            ts = PccUtTime.TimeStamp().\
                 initFromSecsSinceEpoch(timeStamp).getSybaseTimeStamp()
        return ts


    def convertTimeStampToMx(self,
                             timeStamp):
        """
        The Sybase module uses the mx.DateTime module internally thus no
        need for any conversion.
        
        timeStamp:    Date time object (mx.DateTime).
        
        Returns:      Date time object (mx.DateTime).
        """
        T = TRACE(5)

        return timeStamp


class ngamsSybaseCursor:
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
                 query):
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
        
        info(5, "Creating cursor with SQL query: " + str(query))
        self.__cursorObj = None
        self.__dbDrv = None
        self.__dbDrv = Sybase.connect(server, user, password,
                                      delay_connect = 1)
        self.__dbDrv.set_property(Sybase.CS_HOSTNAME, getHostId())
        self.__dbDrv.set_property(Sybase.CS_APPNAME, application)
        self.__dbDrv.connect()
        self.__dbDrv.execute("use " + db)
        self.__cursorObj = self.__dbDrv.cursor()
        self.__cursorObj.execute(query)


    def __del__(self):
        """
        Destructor method free'ing the internal DB connection + cursor objects.
        """
        T = TRACE()
        
        if (self.__cursorObj): del self.__cursorObj
        if (self.__dbDrv): del self.__dbDrv
        
                     
    def fetch(self,
              maxEls):
        """
        Fetch a number of elements from the query and return this.
        The result will be returned as a list with the following format:

          [[<col1>, <col2>, <col3>, ...], ...]

        An empty list ([]) may be returned if there were no matches to the
        SQL query.
        
        maxEls:     Maximum number of elements/rows to return (integer).

        Return:     List containing tuples with the values queried
                    (list/list).
        """
        T = TRACE(5)
        
        # Simulate error in DB communication:
        #   - Broken connection.
        if (_provokeErr(ERR_BROKEN_CON)):
            raise Exception, "Layer: 1, Origin: 1 ct_cmd_drop(): " +\
                  "user api layer: external error: The connection " +\
                  "has been marked dead (TEST)"
        #   - Empty query result.
        if (_provokeErr(ERR_EMPTY_RES)): return []

        # Normal execution.
        if (self.__cursorObj):
            return self.__cursorObj.fetchmany(maxEls)
        else:
            return []


if __name__ == '__main__':
    """
    Main function to test the module.
    """
    pass


# EOF
