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

from   ngams import *
import ngamsDbCore


class ngamsDbNgasDisksHist(ngamsDbCore.ngamsDbCore):
    """
    Contains queries for accessing the NGAS Disks History Table.
    """
       
    def addDiskHistEntry(self,
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
            if (origin == None): origin = "NG/AMS@" + getHostId()
            tsObj = PccUtTime.TimeStamp()
            try:
                self.takeDbSem()
                if (date == None):
                    histDate = self.convertTimeStamp(tsObj.getTimeStamp())
                else:
                    histDate = self.convertTimeStamp(date)
                self.relDbSem()
            except Exception, e:
                self.relDbSem()
                raise Exception, e    
            if (descr != None):
                if (descrMimeType == None):
                    errMsg = "Mime-type must be specified for entry in the "+\
                             "NGAS Disks History when a Description is given!"
                    raise Exception, errMsg
                if (descrMimeType == NGAMS_XML_MT):
                    descr = re.sub("\n", "", descr)
                    descr = re.sub("> *<", "><", descr)
                    descr = re.sub(">\t*<", "><", descr)
                descr = "'" + descr + "'"
            else:
                descr = "None"
            if (descrMimeType != None):
                mt = "'" + descrMimeType + "'"
            else:
                mt = "None"
            sqlQuery = "INSERT INTO ngas_disks_hist " +\
                       "(disk_id, hist_date, hist_origin, hist_synopsis, " +\
                       "hist_descr_mime_type, hist_descr) VALUES (" +\
                       "'" + diskId + "', " +\
                       "'" + histDate + "', " +\
                       "'" + origin + "', " +\
                       "'" + synopsis + "', " +\
                       mt + ", " +\
                       descr + ")"
            res = self.query(sqlQuery)
            info(2,"Added entry in NGAS Disks History Table - Disk ID: " +\
                 diskId + " - Date: " + tsObj.getTimeStamp() + " - Origin: " +\
                 origin + " - Synopsis: " + synopsis +\
                 " - Description Mime-type: " + str(mt) + " - Description: " +\
                 str(descr))        
            self.triggerEvents()
        except Exception, e:   
            raise e



# EOF
