

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
# "@(#) $Id: ngasMvSpuriousFiles.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  23/07/2003  Created
#

"""
This script is used to remove spurious files from NGAS Data Disks.

The files must be listed in files, which must have one of the two following
formats:

Format 1:

<FileStatus FileName="<complete filename 1>"/>
<FileStatus FileName="<complete filename 2>"/>
...
<FileStatus FileName="<complete filename N>"/>


Format 2:

<complete filename 1>
<complete filename 2>
...
<complete filename N>
"""

import sys, os, commands


if __name__ == '__main__':
    """
    Main function.
    """
    print "\nWARNING: THIS TOOL IS ONLY FOR AN NGAS EXPERT USER!!!\n"
    print "Are you sure you're supposed to execute this tool?"
    answer = sys.stdin.readline().upper()
    if (answer != "JKNUDSTR\n"):
        print "\n\nSorry, you're not allowed to execute this tool!!\n\n"
        sys.exit(0)
    else:
        print "\n\n-- hope you're doing the right thing ..."

    if (len(sys.argv) != 2):
        print "\n\nCorrect usage is:\n"
        print "\nngasMvSpuriousFiles.py <file list>\n\n"
        sys.exit(1)


    fileList = sys.argv[1]
    targDir = "/home/ngasmgr/REMDISK_SPURIOUS_FILES/"

    fo = open(fileList)
    lines = fo.readlines()
    fo.close()

    for line in lines:
        line = line.strip()
        if (not line): continue
        if (line.find("=") != -1):
            complFilename = line.split("=")[1][1:-3]
        else:
            complFilename = line.strip()

        # Check if this file is available.
        if (not os.path.exists(complFilename)):
            filePat = complFilename[12:]
            res = commands.getoutput("find /NGAS/ |grep \"" + filePat + "\"")
            if (res.find(filePat) != -1):
                print "File: " + complFilename + " not found. Move file: " +\
                      res + " instead (Y/N) [Y]?"
                answer = sys.stdin.readline()[0:-1].upper()
                if ((answer == "Y") or (answer == "")):
                    complFilename = res
                else:
                    print "Skipping handling of file: " + complFilename
                    continue

        print "Handling file:", complFilename
        path = os.path.dirname(complFilename)
        targPath = os.path.normpath(targDir + "/" + path)
        filename = os.path.basename(complFilename)
        cmd = "mkdir -p " + targPath
        print cmd
        os.system(cmd)
        cmd = "mv " + complFilename + " " + targPath
        print cmd
        os.system(cmd)
        print "\n\n"


# EOF
