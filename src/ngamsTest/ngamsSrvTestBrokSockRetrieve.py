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
# "@(#) $Id: ngamsSrvTestBrokSockRetrieve.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/10/2004  Created
#

"""
Special version of the NG/AMS Server class used to send back a non-sense
(corrupted/illegal) HTTP response.
"""

import os, sys, time, BaseHTTPServer
from   ngams import *
import ngamsServer
from   ngamsTestLib import *

def cleanUpAfterProc(dummy):
    pass

def genReplyRetrieveFail(srvObj,
                         reqPropsObj,
                         httpRef,
                         statusObjList):
    """
    Used to override ngamsRetrieveCmd.genReplyRetrieve(). This implementation
    simluates a broken socket connection while data is being sent back to the
    client while handling a Retrieve Request.

    See ngamsRetrieveCmd.genReplyRetrieve() for further details. The two
    methods are (and should continue to be identical apart from the section
    indicated below with 'TEST:').
    """
    T = TRACE()
    
    # Send back reply with the result queried.
    try:
        # TODO: Make possible to send back several results - use multipart
        #       mime-type message -- for now only one result is sent back.
        resObj = statusObjList[0].getResultObject(0)

        mimeType = resObj.getMimeType()
        dataSize = resObj.getDataSize()
        refFilename = resObj.getRefFilename()
        info(3,"Sending data back to requestor. Reference filename: " +\
             refFilename + ". Size: " + str(dataSize))
        srvObj.httpReplyGen(reqPropsObj, httpRef, NGAMS_HTTP_SUCCESS, None, 0,
                            mimeType, dataSize)
        contDisp = "attachment; filename=\"" + refFilename + "\""
        info(4,"Sending header: Content-disposition: " + contDisp)
        httpRef.send_header('Content-disposition', contDisp)
        httpRef.wfile.write("\n")
        
        # Send back data from the memory buffer, from the result file, or
        # from HTTP socket connection.
        if (resObj.getObjDataType() == NGAMS_PROC_DATA):
            info(3,"Sending data in buffer to requestor ...")
            httpRef.wfile.write(resObj.getDataRef())
        elif (resObj.getObjDataType() == NGAMS_PROC_FILE):
            info(3,"Reading data block-wise from file and sending " +\
                 "to requestor ...")
            fd = open(resObj.getDataRef())
            dataSent = 0
            dataToSent = getFileSize(resObj.getDataRef())
            while (dataSent < dataToSent):
                tmpData = fd.read(srvObj.getCfg().getBlockSize())
                os.write(httpRef.wfile.fileno(), tmpData)
                dataSent += len(tmpData)

                #############################################################
                # TEST: SIMULATE BROKEN SOCKET BY TERMINATING WHEN HALF OF
                #       THE DATA HAS BEEN SENT.
                #############################################################
                if (dataSent > (dataToSent / 2)):
                    srvObj.killServer()
                    sys.exit(0)
                #############################################################
                
        else:
            # NGAMS_PROC_STREAM - read the data from the File Object in
            # blocks and send it directly to the requestor.
            info(3,"Routing data from foreign location to requestor ...")
            dataSent = 0
            dataToSent = dataSize
            while (dataSent < dataToSent):
                tmpData = resObj.getDataRef().\
                          read(srvObj.getCfg().getBlockSize())
                os.write(httpRef.wfile.fileno(), tmpData)
                dataSent += len(tmpData)

        info(4,"HTTP reply sent to: " + str(httpRef.client_address))
        reqPropsObj.setSentReply(1)
    
        cleanUpAfterProc(statusObjList)
    except Exception, e:
        cleanUpAfterProc(statusObjList)
        raise e


class ngamsServerTestBrokSockRetrieve(ngamsServer.ngamsServer):
    """
    Special version of the NG/AMS Server class used to send back a non-sense
    (corrupted/illegal) HTTP response.
    """

    def __init__(self):
        """
        Constructor method.
        """
        ngamsServer.ngamsServer.__init__(self)
        # Replace the function ngamsRetrieveCmd.genReplyRetrieve() used to
        # reply to the RETRIEVE Command with the test version provoking a
        # broken socket situation.
        import ngamsRetrieveCmd
        ngamsRetrieveCmd.genReplyRetrieve = genReplyRetrieveFail

        
if __name__ == '__main__':
    """
    Main program executing the special test NG/AMS Server
    """
    ngamsTestSrv = ngamsServerTestBrokSockRetrieve()
    ngamsTestSrv.init(sys.argv)

# EOF
