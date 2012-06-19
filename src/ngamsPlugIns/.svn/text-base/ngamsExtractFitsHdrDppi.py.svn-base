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
# "@(#) $Id: ngamsExtractFitsHdrDppi.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# awicenec  26/09/2002  Created
# awicenec  31/03/2004  Support for extraction of certain headers
#

"""
Contains a DDPI which is used to extract the main header from FITS files.
"""

from ngams import *
import ngamsPlugInApi, ngamsDppiStatus
#import printhead
import os, re
from commands import getstatusoutput
from sets import Set

def constructCommand(file, head=0, struct=0, skey='END', tsv=0, \
                     xmlfl='', mode=1, check=0):
    """
    """
    cmd = '/opsw/packages/bin/printhead'
    extCmd = ''
    if head != 0: extCmd += '-H %d' % head
    if struct != 0: extCmd += ' -S'
    if skey != 'END': extCmd += ' -s %s' % skey
    if tsv != 0: extCmd += ' -t'
    if xmlfl != '': extCmd += ' -x %s' % xmlfl
    if mode != 1: extCmd += ' -m %d' % mode
    if check != 0: extCmd += ' -c'
    
    cmd = '%s %s %s' % (cmd, extCmd, file)

    return cmd

def ngasExtractFitsHdrDppi(srvObj,
                           reqPropsObj,
                           filename):
    """
    This DPPI extracts the main header from a FITS file
    requested from the ESO Archive.

    srvObj:        Reference to instance of the NG/AMS Server
                   class (ngamsServer).
    
    reqPropsObj:   NG/AMS request properties object (ngamsReqProps).
    
    filename:      Name of file to process (string).

    Returns:       DPPI return status object (ngamsDppiStatus).

    Side effect:   This DPPI works directly on the archived file, since it is
                   read-only access.

    SPECIFIC DOCUMENTATION:
    This DPPI controls the call to the printhead module. If the 

    Example URL (single line):

	http://ngasdev1:7777/RETRIEVE
		?file_id=MIDI.2004-02-11T04:16:04.528&
		processing=ngasExtractFitsHdrDppi&
		processing_pars=header%3D99
	
    The header parameter is optional, if not specified the primary header
    is returned.

    Valid values for the header parameter are numbers between 0 and 99. If
    numbers are specified which are either outside the range or bigger than
    the number of headers (including the primary) the primary header will be
    returned. Headers are counted from 0 starting with the primary header. 99
    is a special value as it returns all headers concatenated in a single file.

    If 'xml=vo' is specified headers are returned using a slightly modified
    VOTable (XML) format. If 'xml=xfits' is specified headers are returned
    using the XFits (XML) format. struct=1 returns the structure of the FITS
    file. tsv=1 returns the headers in a tab separated format suitable for 
    direct ingest into the header repository.
    """
    T = TRACE()

    statusObj = ngamsDppiStatus.ngamsDppiStatus()
    
    if (reqPropsObj.hasHttpPar("processing_pars")): 
        pars = ngamsPlugInApi.\
               parseRawPlugInPars(reqPropsObj.getHttpPar("processing_pars"))
    else:
        # default is to extract the primary header
        pars = {'header':0}
    
    info(2, "ngasExtractFitsHdrDppi: " + filename+"  " + pars.__repr__())

    PARS = Set(['header', 'xml', 'skey', 'struct', 'tsv', 'check'])
    
    # initial settings for printhead
    xtract = 0
    parse = 0
    xmlVals = ['xfits','vo']
    xmlfl = ''
    skeyfl = 0
    skey = 'END'
    show = 0
    struct = 0
    tsv = 0
    check = 0
    mergefl = 0
    hfl = 0
    mode = 1
    
    result = ''
    err = ''
    ext = 'hdr'
    
    
    if pars.has_key('header'):
        # extract a certain header: if value == 99 all headers are extracted,
        # for any other value that header is extracted. headers are
        # counted from 0
        hfl = 1
        struct = 0
        show = pars['header']
        try:
            head = int(show)
        except:
            err = "ngasExtractFitsHdrDppi: Invalid type for header " +\
                  "parameter. Should be int"
        if head < 0 or head > 99:
            err = "ngasExtractFitsHdrDppi: Invalid value specified for " +\
                  "header parameter."
 
    if pars.has_key('xml'):
        # if this key exists we do a conversion to XFits XML.
        struct = 0
        if pars['xml'] in xmlVals:
            xmlfl = pars['xml']
        else:
            err = "ngasExtractFitsHdrDppi: Invalid value for xml " +\
                  "parameter. Should be 'vo|xfits': "+ pars['xml']
        ext = 'xml'
        
    if pars.has_key('skey'):
        # extract just one keyword. CAUTION: No checking done!
        skey = pars['skey'].strip()
        skeyfl = 1
        keyParts = skey.split()
        ext = 'txt'
        head = int(head)
        if head < 0: head = 0
        if ((not re.match('[a-zA-Z]', skey[0])) or
            (len(keyParts) > 1 and keyParts[0] != 'HIERARCH') or
            (len(keyParts[0]) > 8)):
            err = "ngasExtractFitsHdrDppi: Invalid value for skey " +\
                  "parameter specified. Must be a valid FITS keyword:",\
                  skey

    if pars.has_key('struct'):
        # return only the structure of the FITS file. Value of the
        # parameter is ignored
        head = -99
        struct = 1
        ext = 'txt'

    if pars.has_key('tsv'):
        # extract header in tsv format. Parameter value is ignored
        struct = 1
        tsv = 1
        ext = 'txt'
        head = int(head)
        if head < 0: head = 0

    if pars.has_key('check'):
        # head structure and calculate the checksum of the data part.
        head = -99
        struct = 1
        check = 1

    # printhead supports a list of files, but here we only use one
    fils = [filename] 
    base = os.path.basename(filename)
    pos = base.rfind('.fits')
    file_id = base[:pos]

    if len(Set(pars) - PARS) != 0:  # detect unsupported parameters
        xpars = Set(pars) - PARS
        err   = "ngasExtractFitsHdrDppi: Unsupported option(s): %s\n" + \
                "Valid options are:\n" + \
                "header=<number> where number is an integer between 0 " +\
                "and max(extension)-1 or 99 (for all)\n" +\
                "xml=<format>, where format is [vo|xfits]\n" +\
                "struct=1\n" +\
                "skey=<FITS keyword>, should be a valid keyword, crude " +\
                "checking is done\n" + \
                "tsv=1\n\n" + \
                "combinations are allowed and should be separated by " +\
                "a comma character.\n"
        err = err % xpars
        ext = 'txt'
        file_id = 'error'

    result = ''  # initialize result string

    if err != '':
        result = err

    if result == '':
        for f in fils:
            cmd = constructCommand(f, head, struct, skey, tsv, xmlfl, mode, check)
            info(4,'Executing command: %s' % cmd)
            (stat, result) = getstatusoutput(cmd)
            if stat != 0:
                errMsg = "Processing of header for file %s failed: %s" % (filename, result)
                raise Exception, errMsg
    
    resFilename = file_id + "." + ext
    try:
        # mime-type guessing does not work sometimes, we force it in that case.
        mimeType = ngamsPlugInApi.determineMimeType(srvObj.getCfg(),
                                                    resFilename)
    except:
        if ext == 'xml': 
           mimeType = 'text/xml'
        else:
            mimeType = 'text/ascii'
	
    resObj = ngamsDppiStatus.ngamsDppiResult(NGAMS_PROC_DATA, mimeType,
                                             result, resFilename, '')
    statusObj.addResult(resObj)

    return statusObj


# EOF
