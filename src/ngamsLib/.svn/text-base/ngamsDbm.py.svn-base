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
# "@(#) $Id: ngamsDbm.py,v 1.9 2010/03/25 14:47:19 jagonzal Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  25/03/2002  Created
#

"""
Contains definition of class for handling a DBM DB (BSDDB).
"""

# TODO: Stop using bsddb. Use only gdbm (i.e. remove ngamsDbm and rename
#       ngamsDbm2 to ngamsDbm).

import sys, os, cPickle, random

try:
    import bsddb
except:
    import bsddb3 as bsddb
import gdbm

from ngams import *


class ngamsDbm:
    """
    Class implementing interface to DBM DB.
    """

    def __init__(self,
                 dbmName,
                 cleanUpOnDestr = 0,
                 writePerm = 0,
                 autoSync = 100):
        """
        Constructor method.
        
        dbmName:           Name of the DBM DB file (string).
        
        cleanUpOnDestr:    If set to 1, the DBM file will be removed on
                           object destruction (integer/0|1).

        writePerm:         Open with write permission (integer/0|1).

        autoSync:          Synchonize the DB after the specified number
                           of changes have been introduced (integer).
        """
        T = TRACE()

        # Add proper extension if not already added.
        tmpName, tmpExt = os.path.splitext(dbmName)
        if (tmpExt != "." + NGAMS_DBM_EXT): dbmName += "." + NGAMS_DBM_EXT

        self.__dbmObj         = None
        self.__dbmName        = dbmName
        self.__cleanUpOnDestr = cleanUpOnDestr
        self.__writePerm      = writePerm
        self.__keyPtr         = None
        self.__sem            = threading.Semaphore(1)
        self.__autoSync       = autoSync
        self.__changeCount    = 0
        if (not os.path.exists(dbmName)):
            info(4,"DBM file: %s does not exist - creating ..." % dbmName)
            perm = "c"
        elif (writePerm):
            info(4,"DBM file: %s being opened for writing ..." % dbmName)
            perm = "w"
        else:
            info(4,"DBM file: %s being opened for reading ..." % dbmName)
            perm = "r"
        info(4,"Opening/creating DBM: " + dbmName)
        self.__dbmObj = bsddb.hashopen(dbmName, perm)
        if (perm == "c"):
            self.__dbmObj[NGAMS_FILE_DB_COUNTER] = cPickle.dumps(0, 1)
            self.__dbmObj.sync()
        self.__dbmOpen = 1
        info(4,"Opened/created DBM: " + dbmName)


    def __del__(self):
        """
        Destructor method cleaning up.
        """
        T = TRACE()

        if (self.__dbmObj): self.__dbmObj.sync()
        if (self.__cleanUpOnDestr): rmFile(self.__dbmName)


    def getDbmName(self):
        """
        Return the filename of the DBM.

        Returns:   DBM filename (string).
        """
        return self.__dbmName
    

    def cleanUp(self):
        """
        Close a possible open DB connection + remove the DB file from the disk.

        Returns:   Reference to object itself.
        """
        T = TRACE()
        
        if (self.__dbmObj): self.__dbmObj.sync()
        rmFile(self.__dbmName)
        return self


    def _incrDbCount(self,
                     val):
        """
        Increment (or decrement) the DBM counter.

        val:      Value with which to increment/decrement the counter
                  (integer).

        Returns:  Reference to object itself.
        """
        T = TRACE(5)
        
        newVal = (cPickle.loads(self.__dbmObj[NGAMS_FILE_DB_COUNTER]) + val)
        self.__dbmObj[NGAMS_FILE_DB_COUNTER] = cPickle.dumps(newVal, 1)
        return self


    def add(self,
            key,
            object,
            sync = 0):
        """
        Add an element in the DBM.

        key:       Key in DBM (string).

        object:    Object to store in connection with key (<Object>).

        sync:      Synchronize the DB to disk after adding the object
                   (integer/0|1).

        Returns:   Reference to object itself.
        """
        T = TRACE(5)
        
        try:
            self.__sem.acquire()
            dbVal = cPickle.dumps(object, 1)
            if (not self.__dbmObj.has_key(key)): self._incrDbCount(1)
            self.__dbmObj[key] = dbVal
            self.__changeCount += 1
            if (sync):
                self.__dbmObj.sync()
            elif (self.__changeCount >= self.__autoSync):
                self.__dbmObj.sync()
                self.__changeCount = 0
            self.__sem.release()
            return self
        except Exception, e:
            self.__sem.release()
            raise e


    def addIncKey(self,
                  object,
                  sync = 0):
        """
        Add an element in the DBM, generate the key automatically
        (incrementally).

        object:    Object to store in connection with key (<Object>).

        sync:      Synchronize the DB to disk after adding the object
                   (integer/0|1).

        Returns:   Reference to object itself.
        """
        T = TRACE(5)
        
        newKey = str(self.getCount() + 1)
        return self.add(newKey, object, sync)


    def rem(self,
            key):
        """
        Remove an element from the DBM. If the referred element is not
        contained in the DB, an exception is thrown.

        key:       Name of key (string).

        Returns:   Reference to object itself.
        """
        T = TRACE(5)
        
        try:
            self.__sem.acquire()
            del self.__dbmObj[key]
            self._incrDbCount(-1)
            self.__sem.release()
            return self
        except Exception, e:
            self.__sem.release()
            raise e


    def keys(self):
        """
        Return the keys in the DBM. Internal keys (of the form __<Key>__)
        are not returned.

        Returns:   List with the keys in the DBM (list).
        """
        T = TRACE(5)
        
        keyList = self.__dbmObj.keys()
        for idx in range((len(keyList) - 1), -1, -1):
            if (keyList[idx].find("__") == 0): del keyList[idx]
        return keyList
        
        
    def hasKey(self,
               key):
        """
        Return 1 if the given key is in the DBM.

        key:      Key (string).

        Returns:  1 = key in DB otherwise 0 is returned (integer/0|1).
        """
        T = TRACE(5)
        
        try:
            self.__sem.acquire()
            hasKey = self.__dbmObj.has_key(key)
            self.__sem.release()
            return hasKey
        except Exception, e:
            self.__sem.release()
            raise e       
        

    def sync(self):
        """
        Synchronize the DBm to disk.

        Returns:    Reference to object itself.
        """
        T = TRACE(5)
        
        try:
            self.__sem.acquire()
            self.__dbmObj.sync()
            self.__sem.release()
            return self
        except Exception, e:
            self.__sem.release()
            raise e       
        

    def get(self,
            key,
            pop = 0):
        """
        Get a specific element from the DBM.

        key:       Name of the key referring to the element (string).

        Returns:   Element or None if not available (<Object>).
        """
        T = TRACE(5)
        
        if (self.__dbmObj.has_key(key)):
            return cPickle.loads(self.__dbmObj[key])
        else:
            return None


    def initKeyPtr(self):
        """
        Initialize the internal key pointer. Subsequent calls to
        getNext() will then start from the beginning.

        Returns:   Reference to object itself.
        """
        T = TRACE(5)
        
        self.__keyPtr = None
        return self
        

    def getNext(self,
                pop = 0):
        """
        Get the keys + objects sequentially from the DB. If pop=1,
        the entry will be removed from the DB before returning.

        Entries for keys of the format: '__<Key>__' are skipped.

        pop:        Remove the element from the DB after retrieving it
                    (integer/0|1).

        Returns:    Tuple with key + value (unpickled) (tuple).
        """
        T = TRACE(5)
        
        try:
            self.__sem.acquire()
            if (self.__keyPtr):
                while (self.__keyPtr):
                    try:
                        self.__keyPtr, dbVal = self.__dbmObj.next()
                    except:
                        self.__keyPtr, dbVal = (None, None)
                    if (str(self.__keyPtr).find("__") != 0): break
            else:
                try:
                    self.__keyPtr, dbVal = self.__dbmObj.first()
                    while (str(self.__keyPtr).find("__") == 0):
                        self.__keyPtr, dbVal = self.__dbmObj.next()
                except Exception, e:
                    self.__keyPtr, dbVal = (None, None)   
            if (self.__keyPtr and pop):
                del self.__dbmObj[self.__keyPtr]
                self._incrDbCount(-1)
                self.__changeCount += 1
                if (self.__changeCount >= self.__autoSync):
                    self.__dbmObj.sync()
                    self.__changeCount = 0
            self.__sem.release()
            if (not self.__keyPtr):
                return (None, None)
            else:
                return (self.__keyPtr, cPickle.loads(dbVal))
        except Exception, e:
            self.__sem.release()
            raise e


    def getRandom(self,
                  pop = 0):
        """
        Return a random element from the DB.

        pop:      Remove element from DBM (integer/0|1).

        Returns:  Tuple with key + value (unpickled) (tuple).
        """
        T = TRACE(5)
        
        try:
            self.__sem.acquire()       
            keys = self.__dbmObj.keys()
            idx = int((random.random() * (len(keys) - 1)) + 0.5)
            key = keys[idx]
            del keys
            
            key, val = self.__dbmObj[key]
            val = cPickle.loads(val)
            if (pop):
                del self.__dbmObj[key]
                self._incrDbCount(-1)
                self.__changeCount += 1
                if (self.__changeCount >= self.__autoSync):
                    self.__dbmObj.sync()
                    self.__changeCount = 0
            self.__sem.release()
            return (key, val)
        except Exception, e:
            self.__sem.release()
            raise e

        
    def getCount(self):
        """
        Return the number of elements stored in the DBM.

        Returns:    Number of elements stored in the DBM (integer).
        """
        return cPickle.loads(self.__dbmObj[NGAMS_FILE_DB_COUNTER])


    def iteritems(self):
        """
        jagonzal: This method is needed in order to avoid looping strategies
                  based on next() that is prone to corrupt the hash table object
                  when used at the end of the table. In fact a DB_NOTFOUND 
                  exception is raised in that case, and our conclusion is that 
                  it is not handled properly in either Berkeley API layer or the
                  bsddb Python extension. 

        Return an iterator over the dictionary's (key, value) pairs.          

        Returns:  An iterator over the dictionary's (key, value) pairs.
        """

        return self.__dbmObj.iteritems()


class ngamsDbm2:
    """
    Class implementing interface to DBM DB.
    """

    def __init__(self,
                 dbmName,
                 cleanUpOnDestr = 0,
                 writePerm = 0,
                 autoSync = 100):
        """
        Constructor method.
        
        dbmName:           Name of the DBM DB file (string).
        
        cleanUpOnDestr:    If set to 1, the DBM file will be removed on
                           object destruction (integer/0|1).

        writePerm:         Open with write permission (integer/0|1).

        autoSync:          Synchonize the DB after the specified number
                           of changes have been introduced (integer).
        """
        T = TRACE()

        self.__dbmExt = "gdbm"
        
        # Add proper extension if not already added.
        tmpName, tmpExt = os.path.splitext(dbmName)
        if (tmpExt != "." + self.__dbmExt): dbmName += "." + self.__dbmExt

        self.__dbmObj         = None
        self.__dbmName        = dbmName
        self.__cleanUpOnDestr = cleanUpOnDestr
        self.__writePerm      = writePerm
        self.__keyPtr         = None
        self.__sem            = threading.Semaphore(1)
        self.__autoSync       = autoSync
        self.__changeCount    = 0
        if (not os.path.exists(dbmName)):
            info(4,"DBM file: %s does not exist - creating ..." % dbmName)
            perm = "c"
        elif (writePerm):
            info(4,"DBM file: %s being opened for writing ..." % dbmName)
            perm = "w"
        else:
            info(4,"DBM file: %s being opened for reading ..." % dbmName)
            perm = "r"
        info(4,"Opening/creating DBM: " + dbmName)
        self.__dbmObj = gdbm.open(dbmName, perm)
        if (perm == "c"):
            self.__dbmObj[NGAMS_FILE_DB_COUNTER] = cPickle.dumps(0, 1)
            self.__dbmObj.sync()
        self.__dbmOpen = 1
        info(4,"Opened/created DBM: " + dbmName)


    def __del__(self):
        """
        Destructor method cleaning up.
        """
        T = TRACE()
        
        if (self.__dbmObj): self.__dbmObj.sync()
        if (self.__cleanUpOnDestr): rmFile(self.__dbmName)


    def _getDbmObj(self):
        """
        Returns the reference to the internal DBM object.

        Returns:   Reference to DBM object.
        """
        return self.__dbmOpen


    def getDbmName(self):
        """
        Return the filename of the DBM.

        Returns:   DBM filename.
        """
        return self.__dbmName
    

    def cleanUp(self):
        """
        Close a possible open DB connection + remove the DB file from the disk.

        Returns:   Reference to object itself.
        """
        T = TRACE()
        
        if (self.__dbmObj): self.__dbmObj.sync()
        rmFile(self.__dbmName)
        return self


    def _incrDbCount(self,
                     val):
        """
        Increment (or decrement) the DBM counter.

        val:      Value with which to increment/decrement the counter
                  (integer).

        Returns:  Reference to object itself.
        """
        T = TRACE(5)
        
        newVal = (cPickle.loads(self.__dbmObj[NGAMS_FILE_DB_COUNTER]) + val)
        self.__dbmObj[NGAMS_FILE_DB_COUNTER] = cPickle.dumps(newVal, 1)
        return self


    def add(self,
            key,
            object,
            sync = 0):
        """
        Add an element in the DBM.

        key:       Key in DBM (string).

        object:    Object to store in connection with key (<Object>).

        sync:      Synchronize the DB to disk after adding the object
                   (integer/0|1).

        Returns:   Reference to object itself.
        """
        T = TRACE(5)
        
        try:
            self.__sem.acquire()
            dbVal = cPickle.dumps(object, 1)
            if (not self.__dbmObj.has_key(key)): self._incrDbCount(1)
            self.__dbmObj[key] = dbVal
            self.__changeCount += 1
            if (sync):
                self.__dbmObj.sync()
            elif (self.__changeCount >= self.__autoSync):
                self.__dbmObj.sync()
                self.__changeCount = 0
            self.__sem.release()
            return self
        except Exception, e:
            self.__sem.release()
            raise e


    def addIncKey(self,
                  object,
                  sync = 0):
        """
        Add an element in the DBM, generate the key automatically
        (incrementally).

        object:    Object to store in connection with key (<Object>).

        sync:      Synchronize the DB to disk after adding the object
                   (integer/0|1).

        Returns:   Reference to object itself.
        """
        T = TRACE(5)
        
        newKey = str(self.getCount() + 1)
        return self.add(newKey, object, sync)


    def rem(self,
            key):
        """
        Remove an element from the DBM. If the referred element is not
        contained in the DB, an exception is thrown.

        key:       Name of key (string).

        Returns:   Reference to object itself.
        """
        T = TRACE(5)
        
        try:
            self.__sem.acquire()
            del self.__dbmObj[key]
            self._incrDbCount(-1)
            self.__sem.release()
            return self
        except Exception, e:
            self.__sem.release()
            raise e


    def keys(self):
        """
        Return the keys in the DBM. Internal keys (of the form __<Key>__)
        are not returned.

        Returns:   List with the keys in the DBM (list).
        """
        T = TRACE(5)
        
        keyList = self.__dbmObj.keys()
        for idx in range((len(keyList) - 1), -1, -1):
            if (keyList[idx].find("__") == 0): del keyList[idx]
        return keyList
        
        
    def hasKey(self,
               key):
        """
        Return 1 if the given key is in the DBM.

        key:      Key (string).

        Returns:  1 = key in DB otherwise 0 is returned (integer/0|1).
        """
        T = TRACE(5)
        
        try:
            self.__sem.acquire()
            hasKey = self.__dbmObj.has_key(key)
            self.__sem.release()
            return hasKey
        except Exception, e:
            self.__sem.release()
            raise e       
        

    def sync(self):
        """
        Synchronize the DBm to disk.

        Returns:    Reference to object itself.
        """
        T = TRACE(5)
        
        try:
            self.__sem.acquire()
            self.__dbmObj.sync()
            self.__sem.release()
            return self
        except Exception, e:
            self.__sem.release()
            raise e       
        

    def get(self,
            key,
            pop = 0):
        """
        Get a specific element from the DBM.

        key:       Name of the key referring to the element (string).

        Returns:   Element or None if not available (<Object>).
        """
        T = TRACE(5)
        
        if (self.__dbmObj.has_key(key)):
            return cPickle.loads(self.__dbmObj[key])
        else:
            return None


    def initKeyPtr(self):
        """
        Initialize the internal key pointer. Subsequent calls to
        getNext() will then start from the beginning.

        Returns:   Reference to object itself.
        """
        T = TRACE(5)
        
        self.__keyPtr = None
        return self
        

    def getNext(self,
                pop = 0):
        """
        Get the keys + objects sequentially from the DB. If pop=1,
        the entry will be removed from the DB before returning.

        Entries for keys of the format: '__<Key>__' are skipped.

        pop:        Remove the element from the DB after retrieving it
                    (integer/0|1).

        Returns:    Tuple with key + value (unpickled) (tuple).
        """
        T = TRACE(5)
        
        try:
            self.__sem.acquire()
            if (self.__keyPtr):
                while (self.__keyPtr):
                    try:
                        self.__keyPtr = self.__dbmObj.nextkey(self.__keyPtr)
                        if (self.__keyPtr):
                            dbVal = self.__dbmObj[self.__keyPtr]
                    except:
                        self.__keyPtr, dbVal = (None, None)
                    if (str(self.__keyPtr).find("__") != 0): break
            else:
                try:
                    self.__keyPtr = self.__dbmObj.firstkey()
                    if (self.__keyPtr): dbVal = self.__dbmObj[self.__keyPtr]
                    while (str(self.__keyPtr).find("__") == 0):
                        self.__keyPtr = self.__dbmObj.nextkey(self.__keyPtr)
                        if (self.__keyPtr):
                            dbVal = self.__dbmObj[self.__keyPtr]
                except Exception, e:
                    self.__keyPtr, dbVal = (None, None)   
            if (self.__keyPtr and pop):
                del self.__dbmObj[self.__keyPtr]
                self._incrDbCount(-1)
                self.__changeCount += 1
                if (self.__changeCount >= self.__autoSync):
                    self.__dbmObj.sync()
                    self.__changeCount = 0
            self.__sem.release()
            if (not self.__keyPtr):
                return (None, None)
            else:
                return (self.__keyPtr, cPickle.loads(dbVal))
        except Exception, e:
            self.__sem.release()
            raise e


    def getRandom(self,
                  pop = 0):
        """
        Return a random element from the DB.

        pop:      Remove element from DBM (integer/0|1).

        Returns:  Tuple with key + value (unpickled) (tuple).
        """
        T = TRACE(5)

        # TODO: Change this to avoid creating a list in memory with all keys,
        #       just get the number of elements in the DBM and pick out a
        #       random of these.
        try:
            self.__sem.acquire()       
            keys = self.__dbmObj.keys()
            idx = int((random.random() * (len(keys) - 1)) + 0.5)
            key = keys[idx]
            del keys
            
            key, val = self.__dbmObj[key]
            val = cPickle.loads(val)
            if (pop):
                del self.__dbmObj[key]
                self._incrDbCount(-1)
                self.__changeCount += 1
                if (self.__changeCount >= self.__autoSync):
                    self.__dbmObj.sync()
                    self.__changeCount = 0
            self.__sem.release()
            return (key, val)
        except Exception, e:
            self.__sem.release()
            raise e

        
    def getCount(self):
        """
        Return the number of elements stored in the DBM.

        Returns:    Number of elements stored in the DBM (integer).
        """
        return cPickle.loads(self.__dbmObj[NGAMS_FILE_DB_COUNTER])




if __name__ == '__main__':
    """
    Main function invoking the function to dump the contents of a DBM.
    """
    import Sybase
    
    if (len(sys.argv) != 2):
        print "\nCorrect usage:\n\n" +\
              "> python ngamsDbm <DBM File>\n\n"
        sys.exit(1)

    dbm = ngamsDbm(sys.argv[1], 0, 0)
    print 80 * "="
    print "Dumping contents of NG/AMS DBM:"
    print 80 * "-"
    print "Name:     %s" % dbm.getDbmName()
    print "Elements: %d\n" % dbm.getCount()
    print "Contents:"
    print 80 * "-"
    while (1):
        key, contents = dbm.getNext(0)
        if (not key): break
        print "%s:" % key
        print str(contents)
        print 80 * "-"
    print 80 * "=" + "\n"
    

# EOF
