

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
# "@(#) $Id: ngasPrepareVolume.py,v 1.4 2008/12/12 14:28:33 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  26/02/2007  Created
#

_doc =\
"""
The ngasPrepareVolume Tool is used to prepare a 'HW independent' NGAS
volume. The volume must be mounted already. The tool will generate a file,
NGAS Volume Info File (<Volume Path>/.ngas_volume_info), which contains the
relevant parameters for the volume.

The name of the path for the volume, should be of the type:

<NGAS Root>/<Volumes Directory>/<Volume Name>

- whereby <NGAS Root> is the value of the Server.RootDirectory from the
NGAS configuration, and the <Volumes Directory> the value of the
Server.VolumesDiretory.

The tool will only accept to be executed as user root. The resulting
NGAS Volume Info File will be owned by user root and will be read-only for
all.

%s

"""

import sys, os, time, getpass, md5

import pcc, PccUtTime

from ngams import *
import ngamsDb, ngamsLib
from ngamsGenericPlugInLib import *
import ngasUtils
from ngasUtilsLib import *
#import ngasUtilsLib


# Definition of predefined command line parameters.
_options = [\
    ["path", [], None, NGAS_OPT_MAN, "=<Path of Volume>",
     "The absolute path of the volume as mounted on the system."],
    ["force", [], False, NGAS_OPT_OPT, "",
     "Force the execution of the tool."],
    ["new", [], False, NGAS_OPT_OPT, "", "Create a new NGAS Volume Info " +\
     "File (delete a possible existing one)"],
    ["type", [], None, NGAS_OPT_OPT, "=<Disk Type>", "Type of the volume"],
    ["manufacturer", [], 0, NGAS_OPT_OPT, "=<Manufacturer>",
     "Name of the disk manufacturer."],
    ["silent", [], False, NGAS_OPT_OPT, "",
     "No questions asked (DANGEROUS!!)"]
    ]
_optDic, _optDoc = genOptDicAndDoc(_options)
__doc__ = _doc % _optDoc


def getOptDic():
    """
    Return reference to command line options dictionary.

    Returns:  Reference to dictionary containing the command line options
              (dictionary).
    """
    return _optDic


def correctUsage():
    """
    Return the usage/online documentation in a string buffer.

    Returns:  Man-page (string).
    """
    return __doc__


def checkGenPars(optDic,
                 volInfoDic,
                 forceQuery):
    """
    Check/generate the parameters.

    optDic:      Options dictionary (dictionary).

    volInfoDic:  Dictionary with parameters from the Volume Info File
                 (dictionary).

    forceQuery:  If True, query the parameters (boolean).

    Returns:     Void.
    """
    # Generate the Volume ID (=Disk ID) if not defined.
    if (not volInfoDic.has_key(NGAS_VOL_INFO_ID)):
        # Generate key as md5 checksum of:
        # <Host Name> + <Volume Path> + <Time Microsecond Res>
        pat = "%s_%s_%.6f" % (getHostName(), optDic["path"], time.time())
        volInfoDic[NGAS_VOL_INFO_ID] = md5.new(pat).hexdigest()

    # NGAS_VOL_INFO_TYPE
    if ((not volInfoDic.has_key(NGAS_VOL_INFO_TYPE)) or
        (volInfoDic[NGAS_VOL_INFO_TYPE] == NGAS_VOL_INFO_UNDEF) or
        forceQuery):
        if (forceQuery and (volInfoDic[NGAS_VOL_INFO_TYPE] !=
                            NGAS_VOL_INFO_UNDEF)):
            type = volInfoDic[NGAS_VOL_INFO_TYPE]
        else:
            type = NGAS_VOL_INFO_UNDEF
        msg = "Enter disk type [%s]" % type
        newType = input(msg)
        if (newType != ""): type = newType
        volInfoDic[NGAS_VOL_INFO_TYPE] = type

    # NGAS_VOL_INFO_MANUFACT
    if ((not volInfoDic.has_key(NGAS_VOL_INFO_MANUFACT)) or
        (volInfoDic[NGAS_VOL_INFO_MANUFACT] == NGAS_VOL_INFO_UNDEF) or
        forceQuery):
        if (forceQuery and (volInfoDic[NGAS_VOL_INFO_MANUFACT] !=
                            NGAS_VOL_INFO_UNDEF)):
            manufact = volInfoDic[NGAS_VOL_INFO_MANUFACT]
        else:
            manufact = NGAS_VOL_INFO_UNDEF
        msg = "Enter manufacturer [%s]" % manufact
        newManufact = input(msg)
        if (newManufact != ""): manufact = newManufact
        volInfoDic[NGAS_VOL_INFO_MANUFACT] = manufact


def dumpPars(optDic,
             volInfoDic):
    """
    Dump the parameters in a string buffer.

    optDic:      Options dictionary (dictionary).
  
    volInfoDic:  Dictionary with parameters from the Volume Info File
                 (dictionary).

    Returns:     String buffer with parameter status (string).
    """
    buf = ""
    buf += "Disk parameters for disk:\n\n"
    buf += "Path:         %s\n" % optDic["path"][NGAS_OPT_VAL]
    buf += "Disk ID:      %s\n" % volInfoDic[NGAS_VOL_INFO_ID]
    buf += "Type:         %s\n" % volInfoDic[NGAS_VOL_INFO_TYPE]
    buf += "Manufacturer: %s\n" % volInfoDic[NGAS_VOL_INFO_MANUFACT]
    buf += "\n"
    return buf


def execute(optDic):
    """
    Execute the tool

    optDic:    Dictionary containing the options (dictionary).

    Returns:   Void.
    """
    info(4,"Entering execute() ...")
    if (optDic["help"][NGAS_OPT_VAL]):
        print correctUsage()
        sys.exit(0)

    # Check if the given path already has an NGAS Volume Info File.
    if (os.path.exists(optDic["path"][NGAS_OPT_VAL]) and
        (not optDic.has_key("force"))):
        msg ="Specified volume path: %s already contains an NGAS Volume " +\
              "Info File. Use --force to force execution."
        raise Exception, msg % optDic["path"][NGAS_OPT_VAL]

    # Load an existing NGAS Volume Info File first.
    volInfoFile = os.path.normpath(optDic["path"][NGAS_OPT_VAL] + os.sep +\
                                   NGAS_VOL_INFO_FILE)
    # Delete a possible existing NGAS Volume Info File.
    if (optDic["new"][NGAS_OPT_VAL]): rmFile(volInfoFile)
    if (os.path.exists(volInfoFile)):
        volInfoDic = loadVolInfoFile(volInfoFile)
    else:
        volInfoDic = {}
    
    if optDic.has_key("type"):
        volInfoDic[NGAS_VOL_INFO_TYPE] = optDic["type"][NGAS_OPT_VAL]
    if optDic.has_key("manufacturer"):
        volInfoDic[NGAS_VOL_INFO_MANUFACT] = optDic["manufacturer"][NGAS_OPT_VAL]
       
    # Generate/check parameters, get user confirmation.
    forceQuery = False
    while (True):
        checkGenPars(optDic, volInfoDic, forceQuery)
        print "\n\n%s" % dumpPars(optDic, volInfoDic)
        if optDic["silent"][NGAS_OPT_VAL]:
            break
        else:
            choice = input("Are these parameters correct (Y(es)/N(o)) [N]?")
            if ((choice.upper() == "YES") or (choice.upper() == "Y")):
                break
            print "\n"
            forceQuery = True

    # Write/update the NGAS Volume Info File.
    print "Writing/updating NGAS Volume Info File: %s ..." %\
          optDic["path"][NGAS_OPT_VAL]
    writeVolInfoFile(volInfoFile, volInfoDic)
    print "Wrote/updated NGAS Volume Info File: %s" %\
          optDic["path"][NGAS_OPT_VAL]

    # Ensure ownership of volume is ngas:ngas and 
    commands.getstatusoutput("chown root.root %s" % optDic["path"])
    commands.getstatusoutput("chmod 755 %s" % optDic["path"])


    # Ensure ownership is root + make it readonly for all.
    commands.getstatusoutput("chown root.root %s" % volInfoFile)
    commands.getstatusoutput("chmod 644 %s" % volInfoFile)

    info(4,"Leaving execute()")


if __name__ == '__main__':
    """
    Main function to execute the tool.
    """
    try:
        if (os.getuid() != 0):
            raise Exception, "This tool must be executed as user root!"
        optDic = parseCmdLine(sys.argv, getOptDic())
    except Exception, e:
        print "\nProblem executing the tool:\n\n%s\n" % str(e)
        sys.exit(1)
    setLogCond(0, "", 0, "", 1)
    execute(optDic)

# EOF
