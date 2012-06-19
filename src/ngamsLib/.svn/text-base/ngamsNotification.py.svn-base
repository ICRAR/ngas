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
# "@(#) $Id: ngamsNotification.py,v 1.7 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  22/09/2003  Created
#

"""
Contains functions to handle the Email Notification.
"""

import os, threading, time, getpass
import pcc, PccUtTime
from ngams import *
import ngamsLib, ngamsHighLevelLib


# Internal reference to Notification Message Retain Buffer. This is a global
# variable. The format is:
#
# {"<Msg ID>": [<Last Emission Time (secs)>, <Retention Start Time (secs)>,
#               <Type>, <Subject>, <Receiver List>,
#               [[<Retention Time (ISO 8601)>, <Msg>], ...]],
#  "<Msg ID>": [...], ...}
retentionBuf_ = None

# Semaphore to protect operations on the Notification Retention Buffer
# to avoid inonsistencies/conflicts.
notifSem_ = threading.Semaphore(1)


def _getNotifRetBufPickleFile(ngamsCfgObj):
    """
    Generate the Notification Pickle Buffer filename.

    ngamsCfgObj:    Reference to object containing NG/AMS
                    Configuration (ngamsConfig).

    Returns:        Name of Retention Buffer Pickle File (string).
    """
    mtRootDir = ngamsCfgObj.getRootDirectory()
    ngasId    = ngamsHighLevelLib.genNgasId(ngamsCfgObj)
    return os.path.normpath(mtRootDir + "/cache/" + ngasId +\
                            "-NOTIFICATION." + NGAMS_PICKLE_FILE_EXT)


def _sendNotifMsg(ngamsCfgObj,
                  type,
                  subject,
                  msg,
                  recList = [],
                  contentType = None,
                  attachmentName = None,
                  dataInFile = 0):
    """
    Function, which actually sends the Email Notification Message.

    Parameters:    See notify().

    Returns:       Void.
    """
    T = TRACE()

    lst = []
    if (recList):
        lst = recList 
    elif (type == NGAMS_NOTIF_ALERT):
        lst = ngamsCfgObj.getAlertNotifList()
    elif (type == NGAMS_NOTIF_ERROR):
        lst = ngamsCfgObj.getErrorNotifList()
    elif (type == NGAMS_NOTIF_DISK_SPACE):
        lst = ngamsCfgObj.getDiskSpaceNotifList()
    elif (type == NGAMS_NOTIF_DISK_CHANGE):
        lst = ngamsCfgObj.getDiskChangeNotifList()
    elif (type == NGAMS_NOTIF_NO_DISKS):
        lst = ngamsCfgObj.getNoDiskSpaceNotifList()
    elif (type == NGAMS_NOTIF_DATA_CHECK):
        lst = ngamsCfgObj.getDataCheckNotifList()
    else:
        pass

    if ((contentType == None) and (attachmentName == None) and (msg != "")):
        msg = "Notification Message:\n\n" + msg + "\n\n\n" +\
              "Note: This is an automatically generated message"
    if (lst != []):
        subject = ngamsHighLevelLib.genNgasId(ngamsCfgObj) + ": " + subject
        fromField = ngamsCfgObj.getSender()
        for recipient in lst:
            # Small trick to avoid to distribute emails when default values
            # contained in the configuration.
            if (recipient.find(NGAMS_DEFINE) != -1): continue
            info(2, "Sending Notification Message to: " + recipient + ". " +\
                 "Subject: " + subject)
            try:
                smtpHost = ngamsCfgObj.getNotifSmtpHost()
                ngamsHighLevelLib.sendEmail(ngamsCfgObj, smtpHost, subject,
                                            [recipient], fromField, msg,
                                            contentType, attachmentName,
                                            dataInFile)
            except Exception, e:
                pass


def _checkSendNotifMsg(ngamsCfgObj,
                       msgId,
                       retBuf,
                       flush = 0,
                       flushMsg = None):
    """
    Send out a retained Notification Message referenced by its ID if
    the time is ready for this.

    ngamsCfgObj:    Reference to object containing NG/AMS
                    configuration file (ngamsConfig).

    msgId:          Message ID within the Retention Buffer (string).

    retBuf:         Reference to Retention Buffer (dictionary/list).

    flush:          Force a flush (emission) of all retained Notification
                    Messages (integer/0|1).

    flushMsg:       Possible message to be contained in the Email
                    Notification Messages (string).

    Returns:        Void.
    """
    T = TRACE()

    # Check if the time past since retaining the first message in the
    # Retention Buffer with this Message ID is larger than the Max.
    # Retention Time. If this is the case, all the messages retained
    # with the given Message ID will be packed into one message, which
    # subsequently is send in one go.
    timeNow    = time.time()
    maxRetTime = isoTime2Secs(ngamsCfgObj.getMaxRetentionTime())
    maxRetNo   = ngamsCfgObj.getMaxRetentionSize()
    if (retBuf[msgId][1]):
        retTime    = (timeNow - retBuf[msgId][1])
    else:
        retTime = 0.0
    type       = retBuf[msgId][2]
    subject    = retBuf[msgId][3]
    recList    = retBuf[msgId][4]
    retBufLen  = len(retBuf[msgId][5])
    msg        = None
    format     = "Check if Retention Buffer with ID: %s should be emptied. " +\
                 "Retention Buffer Size/Maximum Size: %d/%d. " +\
                 "Retention Time/Maximum Time: %.0fs/%.0fs"
    info(4, format % (msgId, retBufLen, maxRetNo, retTime, maxRetTime))
    if ((retBufLen >= maxRetNo) or ((retTime >= maxRetTime) and retBufLen) or
        (retBufLen and flush)):
        info(3,"Sending out retained Notification Messages for Message ID: " +\
             msgId + ". Number of messages retained: " + str(retBufLen))

        # Generate message with all messages retained.
        msgCount = 1
        msg = "\nACCUMULATED NOTIFICATION MESSAGES:\n"
        if (flushMsg): msg += "\n" + flushMsg + "\n"
        for m in retBuf[msgId][5]:
            countStr = padString(str(msgCount), 6, "0")
            msg += "\n--MESSAGE#%s/%s----------\n" % (countStr, m[0])
            msg += str(str(m[1]))
            msgCount += 1
        msg += "\n--END-----------------------------------------"

        # Delete the entry for that Message ID and regenerate the Retention
        # Buffer Pickle File.
        #
        # Point of discussion:
        # It can be dicussed if the pickle file should be deleted after
        # having (tried to) send the Notification Messages or before.
        # If something goes wrong during sending, it might be that some
        # messages are lost.
        del retBuf[msgId]
        retBuf[msgId] = [timeNow, None, type, subject, recList, []]
        pickleObjFile = _getNotifRetBufPickleFile(ngamsCfgObj)
        ngamsLib.createObjPickleFile(pickleObjFile, retBuf)

        # Send the Notification Message.
        _sendNotifMsg(ngamsCfgObj, type, subject, msg, recList)
    else:
        # There is nothing to send.
        pass


def notify(ngamsCfgObj,
           type,
           subject,
           dataRef,
           recList = [],
           force = 0,
           contentType = None,
           attachmentName = None,
           dataInFile = 0):
    """
    Send an Notification Email to the subscriber(s) about an event happening.

    ngamsCfgObj:    Reference to object containing NG/AMS
                    configuration file (ngamsConfig).
    
    type:           Type of Notification (See NGAMS_NOTIF_* in ngams).
    
    subject:        Subject of message (string).
    
    dataRef:        Message to send or filename containing data (string).

    recList:        List with recipients that should receive the Notification
                    Message (list).

    force:          Force distribution of Notification Message even though
                    this was disabled in the configuration (integer/0|1).

    contentType:    Mime-type of message (string).
    
    attachmentName: Name of attachment in mail (string).

    dataInFile:     Indicates that data to be send is stored in a file
                    (integer/0|1).

    Returns:        Void.
    """
    T = TRACE()

    if ((not force) or (ngamsCfgObj.getNotifActive() != 1)):
        info(4, "Leaving notify() with no action (disabled/force=0)")
        return

    # Force emission if data is contained in a file and the (Notification
    # Service is activated).
    if (dataInFile): force = 1

    global notifSem_
    try:
        # Take Notification Semaphore.
        notifSem_.acquire()

        # Load a possible pickled Retention Buffer.
        pickleObjFile = _getNotifRetBufPickleFile(ngamsCfgObj)
        global retentionBuf_
        if (not retentionBuf_):
            # Check if pickled Retention Object exists.
            if (os.path.exists(pickleObjFile)):
                retentionBuf_ = ngamsLib.loadObjPickleFile(pickleObjFile)
            else:
                retentionBuf_ = {}

        # The Retention Buffer is handled the following way:
        #
        # * If the Retention Buffer does not have an entry for that Message ID,
        #   create a new entry for that Message ID + store the Last Emission
        #   Time.
        #
        # * If the time since the Last Emission Time is less than the Max.
        #   Retention Time from the configuration, we buffer this message
        #   in the Retention Buffer.
        msgId      = type + ":" + subject
        timeNow    = time.time()
        maxRetTime = isoTime2Secs(ngamsCfgObj.getMaxRetentionTime())
        maxRetNo   = ngamsCfgObj.getMaxRetentionSize()
        if ((not force) and (not retentionBuf_.has_key(msgId))):
            # We simply create a new entry for this Message ID and initialize
            # the Last Emission Time. This message will be sent out.
            retentionBuf_[msgId] = [timeNow, None, type, subject, recList, []]
            ngamsLib.createObjPickleFile(pickleObjFile, retentionBuf_)
            _sendNotifMsg(ngamsCfgObj, type, subject, dataRef, recList,
                          contentType, attachmentName)
        elif (not force):
            info(4,"Appending Notification Message with ID: " + msgId +\
                 " in Notification Retention Buffer")
            # Set the Retention Start Time if not set.
            if (not retentionBuf_[msgId][1]): retentionBuf_[msgId][1] = timeNow
            # Append the new element containing the information about the
            # message to be retained.
            isoTime = PccUtTime.TimeStamp().initFromSecsSinceEpoch(timeNow).\
                      getTimeStamp()
            retentionBuf_[msgId][5].append([isoTime, dataRef])
            # Update the Notification Retention Buffer Pickle File and check
            # if there are messages to send out.
            ngamsLib.createObjPickleFile(pickleObjFile, retentionBuf_)
            _checkSendNotifMsg(ngamsCfgObj, msgId, retentionBuf_)
        else:
            # Emission of the Notification Message is forced. Just go ahead
            # and send it out.
            _sendNotifMsg(ngamsCfgObj, type, subject, dataRef, recList,
                          contentType, attachmentName, dataInFile)

        # Release Notification Semaphore.
        notifSem_.release()
    except Exception, e:
        # Release Notification Semaphore.
        notifSem_.release()
        raise Exception, e
    

def checkNotifRetBuf(ngamsCfgObj,
                     flush = 0,
                     flushMsg = None):
    """
    Check the Notification Retention Buffer if there are messages
    that should be send out.

    ngamsCfgObj:    Reference to object containing NG/AMS
                    configuration file (ngamsConfig).

    flush:          Force a flush (emission) of all retained Notification
                    Messages (integer/0|1).

    flushMsg:       Possible message to be contained in the Email
                    Notification Messages (string).

    Returns:        Void.
    """
    T = TRACE()

    global notifSem_
    try:
        pickleObjFile = _getNotifRetBufPickleFile(ngamsCfgObj)

        # Take Notification Semaphore.
        notifSem_.acquire()

        # Load a possible pickled Retention Buffer.
        global retentionBuf_
        if (not retentionBuf_):
            # Check if pickled Retention Object exists.
            if (os.path.exists(pickleObjFile)):
                retentionBuf_ = ngamsLib.loadObjPickleFile(pickleObjFile)
            else:
                retentionBuf_ = {}

        # Loop over the entries in the Retention Buffer and send out
        # entries to be emitted.
        for msgId in retentionBuf_.keys():
            _checkSendNotifMsg(ngamsCfgObj, msgId, retentionBuf_, flush,
                               flushMsg)

        # Release Notification Semaphore.
        notifSem_.release()
    except Exception, e:
        # Release Notification Semaphore.
        notifSem_.release()
        raise Exception, e
            

# EOF
