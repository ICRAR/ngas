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
# "@(#) $Id: ngamsDbNgasDisksHist.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/03/2008  Created
#

"""
Contains queries for accessing the NGAS Disks History Table.

This class is not supposed to be used standalone in the present implementation.
It should be used as part of the ngamsDbBase parent classes.
"""

import logging
import re
import time

from ngamsCore import TRACE, NGAMS_XML_MT, toiso8601
import ngamsDbCore


logger = logging.getLogger(__name__)

class ngamsDbNgasDisksHist(ngamsDbCore.ngamsDbCore):
    """
    Contains queries for accessing the NGAS Disks History Table.
    """

    def addDiskHistEntry(self,
                         hostId,
                         diskId,
                         synopsis,
                         descrMimeType = None,
                         descr = None,
                         origin = None,
                         date = None):
        """
        Add an entry in the NGAS Disks History Table (ngas_disks_hist)
        indicating a major action or event occurring in the context of a disk.

        dbConObj:       Instance of NG/AMS DB class (ngamsDbBase).

        diskId:         Disk ID for the disk concerned (string).

        synopsis:       A short description of the action
                        (string/max. 255 char.s).

        descrMimeType:  The mime-type of the contents of the description
                        field. Must be specified when a description is
                        given (string).

        descr:          An arbitrary long description of the action or event
                        in the life-time of the disk (string).

        origin:         Origin of the history log entry. Can either be the
                        name of an application or the name of an operator.
                        If not specified (= None) it will be set to
                        'NG/AMS - <host name>' (string).

        date:           Date for adding the log entry. If not specified (set
                        to None), the function takes the current date and
                        writes this in the new entry (string/ISO 8601).

        Returns:        Void.
        """
        T = TRACE()

        try:
            if (origin == None):
                origin = "NG/AMS@" + hostId

            now = time.time()
            if (date == None):
                histDate = self.convertTimeStamp(now)
            else:
                histDate = self.convertTimeStamp(date)

            if (descr != None):
                if (descrMimeType == None):
                    errMsg = "Mime-type must be specified for entry in the "+\
                             "NGAS Disks History when a Description is given!"
                    raise Exception(errMsg)
                if (descrMimeType == NGAMS_XML_MT):
                    descr = re.sub("\n", "", descr)
                    descr = re.sub("> *<", "><", descr)
                    descr = re.sub(">\t*<", "><", descr)
                descr = descr
            else:
                descr = "None"
            if (descrMimeType != None):
                mt = descrMimeType
            else:
                mt = "None"

            sqlQuery = ("INSERT INTO ngas_disks_hist "
                       "(disk_id, hist_date, hist_origin, hist_synopsis, "
                       "hist_descr_mime_type, hist_descr) VALUES "
                       "({0}, {1}, {2}, {3}, {4}, {5})")

            self.query2(sqlQuery, args = (diskId, histDate, origin, synopsis, mt, descr))

            logger.info("Added entry in NGAS Disks History Table - Disk ID: %s - Date: %s - " + \
                        "Origin: %s - Synopsis: %s - Description Mime-type: %s - Description: %s",
                        diskId, toiso8601(now, local=True), origin, synopsis, str(mt), str(descr))
            self.triggerEvents()
        except:
            raise
