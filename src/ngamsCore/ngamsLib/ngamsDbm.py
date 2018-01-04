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
# "@(#) $Id: ngamsDbm.py,v 1.9 2010/03/25 14:47:19 jagonzal Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  25/03/2002  Created
#

"""
Contains definition of class for handling a DBM DB (BSDDB).
"""

import cPickle
import functools
import logging
import os
import threading

from ngamsCore import NGAMS_DBM_EXT, NGAMS_FILE_DB_COUNTER, rmFile


logger = logging.getLogger(__name__)

try:
    import bsddb
except:
    import bsddb3 as bsddb


class DbRunRecoveryError(Exception):
    pass

def translated(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except bsddb.db.DBRunRecoveryError:
            raise DbRunRecoveryError
    return wrapper

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
        # Add proper extension if not already added.
        _, tmpExt = os.path.splitext(dbmName)
        if (tmpExt != "." + NGAMS_DBM_EXT): dbmName += "." + NGAMS_DBM_EXT

        self.__dbmObj         = None
        self.__dbmName        = dbmName
        self.__cleanUpOnDestr = cleanUpOnDestr
        self.__keyPtr         = None
        self.__sem            = threading.Lock()
        self.__autoSync       = autoSync
        self.__changeCount    = 0
        if (not os.path.exists(dbmName)):
            logger.debug("DBM file: %s does not exist - creating ...", dbmName)
            perm = "c"
        elif (writePerm):
            logger.debug("DBM file: %s being opened for writing ...", dbmName)
            perm = "w"
        else:
            logger.debug("DBM file: %s being opened for reading ...", dbmName)
            perm = "r"
        logger.debug("Opening/creating DBM: %s", dbmName)
        self.__dbmObj = bsddb.hashopen(dbmName, perm)
        if (perm == "c"):
            self.__dbmObj[NGAMS_FILE_DB_COUNTER] = cPickle.dumps(0, 1)
            self.__dbmObj.sync()
        self.__dbmOpen = 1
        logger.debug("Opened/created DBM: %s", dbmName)


    def __del__(self):
        """
        Destructor method cleaning up.
        """
        if (self.__dbmObj): self.__dbmObj.sync()
        if (self.__cleanUpOnDestr): rmFile(self.__dbmName)


    def getDbmName(self):
        """
        Return the filename of the DBM.

        Returns:   DBM filename (string).
        """
        return self.__dbmName


    @translated
    def cleanUp(self):
        """
        Close a possible open DB connection + remove the DB file from the disk.

        Returns:   Reference to object itself.
        """
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
        newVal = (cPickle.loads(self.__dbmObj[NGAMS_FILE_DB_COUNTER]) + val)
        self.__dbmObj[NGAMS_FILE_DB_COUNTER] = cPickle.dumps(newVal, 1)
        return self


    @translated
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
        with self.__sem:
            dbVal = cPickle.dumps(object, 1)
            if (not self.__dbmObj.has_key(key)): self._incrDbCount(1)
            self.__dbmObj[key] = dbVal
            self.__changeCount += 1
            if (sync):
                self.__dbmObj.sync()
            elif (self.__changeCount >= self.__autoSync):
                self.__dbmObj.sync()
                self.__changeCount = 0
            return self


    @translated
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
        newKey = str(self.getCount() + 1)
        return self.add(newKey, object, sync)


    @translated
    def rem(self,
            key):
        """
        Remove an element from the DBM. If the referred element is not
        contained in the DB, an exception is thrown.

        key:       Name of key (string).

        Returns:   Reference to object itself.
        """
        with self.__sem:
            del self.__dbmObj[key]
            self._incrDbCount(-1)
            return self


    @translated
    def keys(self):
        """
        Return the keys in the DBM. Internal keys (of the form __<Key>__)
        are not returned.

        Returns:   List with the keys in the DBM (list).
        """
        keyList = self.__dbmObj.keys()
        for idx in range((len(keyList) - 1), -1, -1):
            if (keyList[idx].find("__") == 0): del keyList[idx]
        return keyList


    @translated
    def hasKey(self,
               key):
        """
        Return 1 if the given key is in the DBM.

        key:      Key (string).

        Returns:  1 = key in DB otherwise 0 is returned (integer/0|1).
        """
        with self.__sem:
            return self.__dbmObj.has_key(key)

    # suppor for "k in dbm" syntax
    def __contains__(self, k):
        return self.hasKey(k)

    @translated
    def sync(self):
        """
        Synchronize the DBm to disk.

        Returns:    Reference to object itself.
        """
        with self.__sem:
            self.__dbmObj.sync()
            return self


    @translated
    def get(self,
            key,
            pop = 0):
        """
        Get a specific element from the DBM.

        key:       Name of the key referring to the element (string).

        Returns:   Element or None if not available (<Object>).
        """

        if (self.__dbmObj.has_key(key)):
            return cPickle.loads(self.__dbmObj[key])
        else:
            return None


    @translated
    def initKeyPtr(self):
        """
        Initialize the internal key pointer. Subsequent calls to
        getNext() will then start from the beginning.

        Returns:   Reference to object itself.
        """
        self.__keyPtr = None
        return self


    @translated
    def getNext(self):
        """
        Get the keys + objects sequentially from the DB. If pop=1,
        the entry will be removed from the DB before returning.

        Entries for keys of the format: '__<Key>__' are skipped.

        pop:        Remove the element from the DB after retrieving it
                    (integer/0|1).

        Returns:    Tuple with key + value (unpickled) (tuple).
        """
        with self.__sem:
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
                except Exception:
                    self.__keyPtr, dbVal = (None, None)

        if (not self.__keyPtr):
            return (None, None)
        else:
            return (self.__keyPtr, cPickle.loads(dbVal))


    @translated
    def getCount(self):
        """
        Return the number of elements stored in the DBM.

        Returns:    Number of elements stored in the DBM (integer).
        """
        return cPickle.loads(self.__dbmObj[NGAMS_FILE_DB_COUNTER])


    @translated
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

# EOF