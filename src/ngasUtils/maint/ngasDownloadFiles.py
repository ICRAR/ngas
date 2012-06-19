

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
# "@(#) $Id: ngasDownloadFiles.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  02/03/2005  Created
#

"""
The tool is used to download files from NGAS Nodes via 

  RETRIEVE?internal=<Filename>

requests.

It is possible to specify a pattern for the filename to be retrieved and
to specify a list of nodes from which to retrieve the files.

If the specified location contains sub-foldes, the sub-folders will be
traversed recursively.

It is also possible to only list the files, which would be concerned by the
pattern given, by specifying the --list parameter.
"""

# IMPL: - If --list is specified don't download the file.
#       - Write file system information about file.

import sys, os, time

from ngams import *
import ngamsDb, ngamsLib, ngamsStatus, ngamsFileInfo, ngamsDiskInfo
import ngamsPClient
import ngasUtils, ngasUtilsLib


def isNgasXmlStatusDoc(doc):
    """
    Check if a document is an NGAS XML Status Document.

    doc:      Document to test (string).

    Returns:  1 if document is an NGAS XML Status Document (integer/0|1).
    """
    if ((doc.strip().find("<?xml version=") == 0) and
        (doc.find("RETRIEVE?internal=ngamsStatus.dtd") != -1)):
        return 1
    else:
        return 0
        

def storeFile(hdrs,
              data,
              outDir):
    """
    Store data contained in HTTP response.

    hdrs:       HTTP headers (string).
    
    data:       Data contained in HTTP response (string).
    
    outDir:     Requested output directory (string).

    Returns:    Name of target file (string).
    """
    hdrDic = ngamsLib.httpMsgObj2Dic(hdrs)
    tmpLine = hdrDic["content-disposition"]
    outFile = string.split(string.split(tmpLine, ";")[1], "=")[1]
    outFileCompl = os.path.normpath("%s/%s" % (outDir, outFile))
    fo = open(outFileCompl, "w")
    fo.write(data)
    fo.close()
    info(1,"Generated output file: %s" % outFileCompl)


def downloadFiles(host,
                  port,
                  srcHost,
                  path,
                  outputDir,
                  list = 0):
    """
    Download the files matching the given pattern via the host specified,
    from the host(s) indicated.

    The files downloaded will be stored in a directory structure of the
    form:

      <Output Dir>/<Source Host>/<File Path>

    I.e., the filepath of the file is preserved, and the source host name is
    also kept.

    host:       Name of host to be contacted (string).
    
    port:       Port number of NG/AMS Server (integer).
    
    srcHost:    List of hosts to be contacted (<Host 1>,<Host 2>,...).
                Can also be a single host, wildcards is allowed (string).
    
    path:       File pattern of file(s) to be retrieved. If this is a
                directory, the files in the directory will be retrieved
                (string).

    outputDir:  Output directory. If not given, the current working point
                will be chosen (string).
                
    list:       If set to 1 and the path given is a directory, rather than
                retrieving the file in the remote directory, the names of
                the remote files are listed on stdout (integer/0|1).

    Returns:    Void.
    """
    if (not list): commands.getstatusoutput("mkdir -p %s" % outputDir)
    reply, msg, hdrs, data = ngamsLib.httpGet(host, port, NGAMS_RETRIEVE_CMD,
                                              pars=[["internal", path],
                                                    ["host_id", srcHost]],
                                              timeOut=10)
    if (isNgasXmlStatusDoc(data)):
        stat = ngamsStatus.ngamsStatus().unpackXmlDoc(data, 1)
        if (stat.getStatus() == NGAMS_FAILURE):
            msg = "Problem handling request: %s" % stat.getMessage()
            raise Exception, msg
    else:
        stat = None
    if (data.find("<?xml version=") == -1):
        if (not list):
            storeFile(hdrs, data, outputDir)
        else:
            print path
        return

    # The reply is a file list. Retrieve the file in question.
    remDir = stat.getFileListList()[0].getComment().split(": ")[-1]
    remDir = os.path.normpath(remDir)
    outputDir = os.path.normpath("%s/%s" % (outputDir, remDir.split("/")[-1]))
    if (not list): commands.getstatusoutput("mkdir -p %s" % outputDir)
    client = ngamsPClient.ngamsPClient()
    for fileInfo in stat.getFileListList()[0].getFileInfoObjList():
        # If we should only list the contents and if the entry is a file,
        # we only display its information.
        if (list):
            print "%s %-8s %-8s %-10s %-16s %s" %\
                  (fileInfo.getPermissions(), fileInfo.getOwner(),
                   fileInfo.getGroup(), str(fileInfo.getFileSize()),
                   fileInfo.getModDate(), fileInfo.getFilename())
            # If the entry is a file and we should only list, we go to the
            # next item.
            if (fileInfo.getPermissions()[0] != "d"): continue

        # Handle/download the item.
        fn = fileInfo.getFilename()
        basename = os.path.basename(fn)
        trgFile = os.path.normpath("%s/%s" % (outputDir, basename))
        if (not list): info(1,"Requesting: %s:%s ..." % (srcHost, fn))
        reply2, msg2, hdrs2, data2 = \
                ngamsLib.httpGet(host, port, NGAMS_RETRIEVE_CMD,
                                 pars=[["internal", fn],
                                       ["host_id", srcHost]], timeOut=10)

        # If the remote object specified is a file, retrieve it, otherwise if a
        # directory, call this function recursively to retrieve the contents of
        # the sub-folder(s).
        if (isNgasXmlStatusDoc(data2)):
            stat2 = ngamsStatus.ngamsStatus().unpackXmlDoc(data2, 1)
            if (stat2.getStatus() == NGAMS_FAILURE):
                msg = "Problem handling request: %s" % stat2.getMessage()
                raise Exception, msg
            # It seems to be another File List, we retrieve the contents
            # of this recursively.
            if (not list): info(1,"Remote object found: %s" % fn)
            downloadFiles(host, port, srcHost, fn, outputDir, list)
        else:
            # Just retrieve the file.
            storeFile(hdrs2, data2, outputDir)

    return


def correctUsage():
    """
    Return the usage/online documentation in a string buffer.

    Returns:  Man-page (string).
    """
    buf = "\nCorrect usage is:\n\n" +\
          "> ngasDownloadFiles.py --host=<Host Name> --port=<Port> " +\
          "--srcHost=<Source Host> --path=<Filename (Pattern)> --list\n\n"
    return buf

  
if __name__ == '__main__':
    """
    Main function to execute the tool.
    """
    setLogCond(0, "", 0, "", 1)

    # Parse input parameters.
    host      = None
    port      = None
    srcHost   = None
    path      = None
    outputDir = None
    list      = 0
    idx = 1
    while idx < len(sys.argv):
        parOrg = sys.argv[idx]
        par    = parOrg.upper()
        try:
            if (par.find("--HOST") == 0):
                host = sys.argv[idx].split("=")[-1]
            elif (par.find("--PORT") == 0):
                port = int(sys.argv[idx].split("=")[-1])
            elif (par.find("--SRCHOST") == 0):
                srcHost = sys.argv[idx].split("=")[-1]
            elif (par.find("--PATH") == 0):
                path = sys.argv[idx].split("=")[-1]
            elif (par.find("--OUTPUTDIR") == 0):
                outputDir = sys.argv[idx].split("=")[-1]
            elif (par == "--LIST"):
                list = 1
            else:
                raise Exception, "Unknown parameter: %s" % parOrg
            idx += 1
        except Exception, e:
            print "\nProblem executing the tool: %s\n" % str(e)
            print correctUsage()  
            sys.exit(1)
    if ((not host) and (not port) or (not path)):
        print correctUsage()  
        raise Exception, "Incorrect/missing command line parameter(s)!"
    if (not srcHost): srcHost = host
    try:
        if (not outputDir): outputDir = os.getcwd()
        outputDir = os.path.normpath("%s/%s" % (outputDir, srcHost))
        downloadFiles(host, port, srcHost, path, outputDir, list)
        if (not list): info(1,"Finished downloading specified path: %s" % path)
    except Exception, e:
        print "Problem encountered handling request:\n\n%s\n" % str(e)
        sys.exit(1)

# EOF
