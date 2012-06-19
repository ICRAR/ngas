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
# "@(#) $Id: ngamsExtractAlmaMultipart.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# awicenec  2005-04-04  Created
#

"""
Contains a DDPI which is used to extract one part of a multipart/related
message file.
"""

from ngams import *
import ngamsPlugInApi, ngamsDppiStatus
import MultipartHandler


def extractData(result, resourceId, verbose=0):
    """
    INPUT:
        result:    list of strings containing multipart messages
    
    RETURNS:
        xyData:    dictionary,
    """
    T = TRACE(3)

    if (verbose >= 3):
        print "Entering extractData with parameters " + \
              "type(result): %s, resourceId: %s" % (str(type(result)),
                                                    resourceId)
    eP = MultipartHandler.Parser()
    xData = []
    yData = []
    xyData = {}
    if len(result) == 0:
        errMsg = "[ERROR]: No data returned. Check whether resourceId is correct: %s" % resourceId
        error( errMsg)
        raise Exception, errMsg
    resultClean = []
    for res in result:
        if len(res) != 0:
            resultClean.append(res)
        else:
            waring( "Empty document detected and ignored!")
    for res in resultClean:
        try:
            msg = eP.parsestr(res) # and parse the string
        except Exception, e:
            errMsg = "email parsing failed: %s" % str(e)
            error( errMsg)
            raise Exception, errMsg
    
        # get the first xml part of the email...
        xmlParts = MultipartHandler.getMimeParts(msg, 'text/xml')
        if len(xmlParts) == 0:
            errMsg = "MonitorDataCli.extractData: No text/xml part found!"
            error( errMsg)
            raise Exception, errMsg
        else:
            xml = xmlParts[0].get_payload()  # There should only be one!
        
    
        # ...and parse it
        try:
            root = MultipartHandler.VOTable.parseString(xml)
        except Exception, e:
    #        print xml
            errMsg = "MonitorDataCli.extractData: XML parsing failed: %s " % str(e)
            error( errMsg)
            raise Exception, errMsg
    
        try:
            (res, cidI, cids) = MultipartHandler.interpretVotable(root, selection=resourceId, verbose=verbose)
        except Exception, e:
            errMsg = "ERROR interpreting VOTABLE: %s" % str(e)
            error( errMsg)
            raise Exception, errMsg
        if len(res) != 1:
            errMsg = "The resource with ID %s has been found %d times! Expecting 1!" \
                % (resourceId, len(res))
            waring( errMsg)

        elif len(cids) != 2:
            errMsg = "The reource %s has %d values. Expecting 2!" % (resourceId, len(cid))
            waring( errMsg)
        
        else:
            xName = res[0].getTable()[0].getField()[0].getId()
            yName = res[0].getTable()[0].getField()[1].getId()
            xUnit = res[0].getTable()[0].getField()[0].getUnit()
            yUnit = res[0].getTable()[0].getField()[1].getUnit()

            try:
                xPart = MultipartHandler.getPartId(msg, cids[0])[0]
            except Exception, e:
                errMsg = "ERROR: Interpretation of x-part failed: %s" % str(e)
                error( errMsg)
                raise Exception, errMsg
            try:
                yPart = MultipartHandler.getPartId(msg, cids[1])[0]
            except Exception, e:
                errMsg = "ERROR: Interpretation of y-part failed: %s" % str(e)
                error( errMsg)
                raise Exception, errMsg

            xDataPart = list(MultipartHandler.\
                             interpretBinary(xPart, cidI[cids[0]], endian='>'))
            yDataPart = list(MultipartHandler.\
                             interpretBinary(yPart, cidI[cids[1]], endian='>'))

            if len(xDataPart) != len(yDataPart):
                errMsg = "The number of x and y datapoints differs (x,y): (%d,%d)" % (len(xData), len(yData))
                errMsg += "\nData skipped!"
                error( errMsg)
            else:
                xData += xDataPart
                yData += yDataPart

    if xData: 
        xyData = {xName:xData, yName:yData, 'xName':xName, 'xUnit':xUnit, 'yName':yName, 'yUnit': yUnit}
    else:
        errMsg = "No x-axis data found!"
        error( errMsg)
        raise Exception, errMsg
    
    info(4, "Leaving extractData")

    return xyData
 


def ngamsExtractAlmaMultipart(srvObj,
                            reqPropsObj,
                            file_id):
    """
    This DPPI extracts one part of a multipart/related message
    requested from the ALMA Archive.

    srvObj:        Reference to instance of the NG/AMS Server
                   class (ngamsServer).
    
    reqPropsObj:   NG/AMS request properties object (ngamsReqProps).
    
    file_id:      Name of file to process (string).

    Returns:       DPPI return status object (ngamsDppiStatus).

    Side effect:   This DPPI works directly on the archived file, since it is
                   read-only access.

    SPECIFIC DOCUMENTATION:

        This DPPI extracts one part of an ALMA multipart related message and
        returns a complete, self-consistent message containing a XML header
        and the requested cid. This version deals with VOTable headers and
        returns a new VOTable header containing only the RESOURCE element 
        describing the requested (by cid) part. If no XML header can be found
        or if the XML header is not a VOTable header this plugin returns just the
        requested part, without any header.

    Example URL (single line):

    http://ngasdev1:7777/RETRIEVE
        ?file_id=X01/X7/X42&
        processing=ngamsAlmaMultipart&
        processing_pars='cid=<cid>'
    

    """
    statusObj = ngamsDppiStatus.ngamsDppiStatus()

    cpart = 1
    pars = 0    # initialize pars
    if (reqPropsObj.hasHttpPar("processing_pars")): 
        pars = ngamsPlugInApi.parseRawPlugInPars(\
        reqPropsObj.getHttpPar("processing_pars"))
    if pars and not pars.has_key('cid'):
        ext = '.cid'
        cpart = 1    # first part only by default
    elif pars and pars.has_key('cid'):  # if processing_par 'cid' exists check its contents
        
        pass
    else:
        pass

    resFilename = file_id + ext
    try:
        mimeType = ngamsPlugInApi.determineMimeType(srvObj.getCfg(),\
         resFilename)
    except:
        pass
    if ext == '.xml': 
        mimeType = 'text/xml'
    else:
        mimeType = 'multipart/related'
    
    resObj = ngamsDppiStatus.ngamsDppiResult(NGAMS_PROC_DATA, mimeType,
                                             head, resFilename, '')
    statusObj.addResult(resObj)

    return statusObj

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print "Usage: ngamsExtractAlmaMultipart.py <test_file> cid"
        sys.exit()
    try:
        fo = open(sys.argv[1],'r')
        (file_id,fileName, type) = specificTreatment(fo)
        print file_id, fileName, type
    except:
        raise
                    
    
#
# ___oOo___
