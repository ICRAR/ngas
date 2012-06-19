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
# "@(#) $Id: ngamsDbConPool.py,v 1.4 2009/06/02 07:46:19 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  30/04/2008  Created
#

"""
The ngamsDbConPool is an NG/AMS DB Interface Driver, which implements a 
DB connection pool. The actual connections in the pool are 'normal' NG/AMS
DB Driver Plug-Ins. The name of these should be given within the input 
parameters.
"""

import threading, Queue

import pcc, PccUtTime
from   ngams import *
import ngamsPlugInApi
 


def _getObjId(obj):
    """
    Return an ID for the object (valid for this session).

    Returns:  ID for the referenced object (string).
    """
    return str(obj).split(" ")[-1][:-2]
    
    

class ngamsDbConPool:
    """
    Class to manage a pool of Db connections.
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

        parameters:      Parameters for the connection pool. Should be given
                         as:

                           'Driver=(Driver Name),Connections=(#)'     (string).
        """
	T = TRACE()

        self.__server      = server
        self.__db          = db
        self.__user        = user
        self.__password    = password
        self.__application = application
	self.__parameters  = parameters
	parDic = ngamsPlugInApi.parseRawPlugInPars(parameters)
	try:
	    self.__driver      = parDic["Driver"]
	    self.__connections = int(parDic["Connections"])
	except Exception, e:
	    msg = "Incorrect input parameters for ngamsDbConPool DB " +\
                  "Driver. Must specify: Driver=(Driver Name),Connection=(#)"
	    raise Exception, msg
	self.__conCount    = 0
	self.__poolSem     = threading.Lock()
	self.__idleCons    = Queue.Queue()
	# Create and insert one connection in the queue, usually at least one 
	# connection will be needed. This connection is at the same time used
	# as utility connection, although no queries are done through this 
	# connection. Amongst other, it is used as DB cursor factory.
	self.__utilDrvObj  = self._allocConnection()


    def getDriverId(self):
        """
        Return DB driver ID.

        Return:    Driver version (string).
        """
	T = TRACE()

        return self.__utilDrvObj.getDriverId()
    

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

        parameters:      Parameters for the connection pool (string).
  
        Returns:         Reference to object itself.
        """
	T = TRACE()

	# The connect() method does not have a meaning in the case of the
	# connection pool class, as the connections are managed per 
	# DB connection and not globally.
        return self

        
    def close(self):
        """
        Close the DB connection.

        Returns:    Reference to object itself (ngamsSybase).
        """
	T = TRACE()

	# The close() method does not have a meaning in the case of the
	# connection pool class, as the connections are managed per 
	# DB connection and not globally.
	return self


    def _allocConnection(self):
	"""
	Allocate a new connection and add it in the pool. This is only done
	if the maximum specified number of connections have not already been
	allocated.

	Returns:    Reference to the allocated connection object 
                    (NG/AMS DB Driver Plug-In Class).
	"""
	T = TRACE()

	conObj = None
	try:
	    self.__poolSem.acquire()

	    # Check if the maximum number of connections have been created.
	    if (self.__conCount == self.__connections): 
		self.__poolSem.release()
		return None

	    # OK, go ahead and create the connection.
	    exec "import " + self.__driver
            creStat = "%s.%s('%s', '%s', '%s', '%s', '%s', '%s')" %\
                      (self.__driver, self.__driver, self.__server, self.__db,
		       self.__user, self.__password, "NG/AMS:" + getHostId(),
		       self.__parameters)
            info(4, "Creating instance of DB Driver Interface/connecting ...")
            conObj = eval(creStat)
	    self.__conCount += 1
	    self.__idleCons.put(conObj)
            info(4, "Created instance of DB Driver Interface/connecting")
   
	    self.__poolSem.release()
	    return conObj
	except Exception, e:
	    self.__poolSem.release()
	    msg = "Error allocating DB connection. Error: %s" % str(e)
	    raise Exception, msg


    def _getConnection(self):
	"""
	Get a connection from the pool. If no connection is free, the 
	method waits for the next connection to become available/

	Returns:  Reference to next connection object (NG/AMS DB Driver
                  Object).
	"""
	T = TRACE(5)

	# Allocate a new connection if not the maximum number of connections
	# has been reached.
	if (self.__conCount < self.__connections): self._allocConnection()
	# Wait for the next free connection.
	conObj = self.__idleCons.get()
        info(5, "Got connection: %s" % _getObjId(conObj))
	return conObj


    def _releaseConnection(self,
			   conObj):
	"""
	Release a connection, previously given, to the pool, making it 
	available for other threads.
	
	conObj:    Connection object to be freed (NG/AMS DB Driver object).
	
	Returns:   Reference to object itself.
	"""
	T = TRACE(5)
	
	# Put the referenced connection object back into the idle queue.
        info(5, "Releasing connection: %s" % _getObjId(conObj))
	self.__idleCons.put(conObj)
	return self


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
        
        conObj = None
        try:
            conObj = self._getConnection()
            res = conObj.query(query)
            self._releaseConnection(conObj)
            return res
        except Exception, e:
            if (conObj):
		              # If the query produces an exception, just reconnect.
                try:
                   conObj.connect(self.__server, self.__db, self.__user,
				            self.__password, self.__application,
				            self.__parameters)
                except:
		          pass
                  # Delete the connection.
                self._deleteConnection(conObj)
                del conObj
                raise Exception, e

 
    def cursor(self,
               query):
        """
        Create a DB cursor with the same connection properties for the
        given SQL query and return a cursor handle.

        query:       SQL query (string).

        Returns:     Cursor object (NG/AMS Cursor Object).
        """
	T = TRACE()

        return self.__utilDrvObj.cursor(query)


    def convertTimeStamp(self,
                         timeStamp):
        """
        Convert a timestamp in one of the following representations to
        a timestamp string, which can be used to set a column of the DBMS
        of type 'datetime' (Sybase type name).

        timeStamp:    Timestamp represented in one of the following formats:

                        1. ISO8601:  YYYY-MM-DDTHH:MM:SS[.s]
                        2. ISO8601': YYYY-MM-DD HH:MM:SS[.s]
                        3. Secs since epoc.
                                                        (string|integer|float).

        Returns:      Timestamp in format, which can be written into a
                      'datetime' column of the DBMS (string).
        """
	T = TRACE()

	return self.__utilDrvObj.convertTimeStamp(timeStamp)


    # TODO: Possible try to change this such that the native Python
    #       time object is always used.
    def convertTimeStampToMx(self,
                             timeStamp):
        """
        Converts an ISO 8601 timestamp into an mx.DateTime object.
        
        timeStamp:  ISO 8601 Datetime string (string/ISO 8601).
        
        Returns:    Date time object (mx.DateTime).
        """
        T = TRACE(5)
        
        return self.__utilDrvObj.convertTimeStampToMx(timeStamp)
    

if __name__ == '__main__':
    """
    Main function to test the module.
    """
    pass


# EOF
