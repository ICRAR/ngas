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
# "@(#) $Id: ngamsEscaladeUtils.py,v 1.9 2008/08/19 20:43:56 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  10/05/2001  Created,
#

"""
This module contains tools for interacting with the Escalade 6800 controller.
"""

import string, urllib, commands
from   ngams import *
import ngamsLib, ngamsPhysDiskInfo


def parseHtmlInfo(url,
                  rootMtPt,
                  slotIds = ["*"]):
    """
    Query the information about the disks as an HTML page, and parse the
    page and return the information in the format as defined for the
    NG/AMS Disk Dictionary.
    
    url:         URL from where to query the disk status (string).

    rootMtPt:    Root mount point to be used (string)

    slotIds:     List containing the Slot IDs to take into account. If
                 given as '*' all slot are considered (list/string).

    Returns:     Dictionary containing objects which are instances of
                 the class ngamsPhysDiskInfo (dictionary).
    """
    T = TRACE()
    
    # Check if the 3ware WEB server can be accessed.
    cmd = "wget -T 2 -t 1 %s" % url
    stat, out = commands.getstatusoutput(cmd)
    if (stat):
        warning("Problem contacting local 3ware WEB server via URL: " + url)
        return {}
    
    fd = urllib.urlopen(url)
    #fd = open(url)
    buf = fd.read()
    fd.close()
    lines = string.split(buf, "\n")
    idx = 0
    length = len(lines)
    takeAllSlots = 0
    if (len(slotIds)):
        if (slotIds[0] == '*'):
            takeAllSlots = 1
    diskInfoDic = {}
    while (idx < length):
        if (string.find(lines[idx], "tech_drive_header\">Port") >= 0):
            # We expect the following output:
            #
            # <tr><td class="tech_drive_header">Port 0</td></tr>
            # <tr><td class="tech_drive_info">
            # <table>
            # <tr><td class="tech_unit_field">Status:</td>\
            #   <td class="tech_unit_value">\
            #   <font color="#3333FF">OK</font></td></tr>
            # <tr><td class="tech_unit_field">Capacity:\
            #   </td><td class="tech_unit_value">\
            #   38.34 GB (80418240 blocks)</td></tr>
            # <tr><td class="tech_unit_field">\
            #   Model:</td><td class="tech_unit_value">\
            #   IBM-DTLA-305040                         </td></tr>
            # <tr><td class="tech_unit_field">Serial number:\
            #   </td><td class="tech_unit_value">         YJ0YJ070913</td>\
            #   </tr>
            # <tr><td class="tech_unit_field">Unit number:</td>\
            #   <td class="tech_unit_value">0</td></tr>
            # <li>Physical drive (port) 7
            # <ul>
            # <li>Status: <font color="#3333FF">OK</font>
            # <li>Capacity: 41.17 GB (80418240 blocks)
            # <li>Model: IBM-DTLA-305040                         
            # <li>Serial number: YJ0YJ070913
            # <li>Unit number: 7
            portNo = int(string.split(string.split(lines[idx], " ")[2],"<")[0])

            # Check if this slot should be taken into account, i.e., if the
            # current Slot ID is in the list given as input to the function.
            slotId = str(int(portNo) + 1)
            if ((not takeAllSlots) and
                (not ngamsLib.elInList(slotIds, slotId))):
                idx = idx + 1
                continue

            idx = idx + 3
            status = string.split(string.split(lines[idx], ">")[5], "<")[0]
            idx = idx + 1
            capacityGb = float(string.split(string.split(lines[idx], ">")[4],
                                            " ")[0])
            capacityBlocks = int(string.split(string.split(string.\
                                                           split(lines[idx],
                                                                 ">")[4],
                                                           "(")[1], " ")[0])
            idx = idx + 1
            model = string.split(string.split(lines[idx], ">")[4], " ")[0]
            diskType = "MAGNETIC DISK/ATA"
            manufacturer = string.split(model, "-")[0]
            if (manufacturer[0:2] == "IC"): manufacturer = "IBM"
            idx = idx + 1
            serialNo = trim(string.split(string.split(lines[idx],
                                                      ">")[4], "<")[0], " ")
            idx = idx + 1
            unitNo = int(string.split(string.split(lines[idx],">")[4], "<")[0])
            diskId = model + "-" + serialNo
            mtPt = rootMtPt + "/data" + str(slotId)                 
            deviceName = "/dev/sd" + chr(97 + len(diskInfoDic)) + '1'
            diskInfoDic[str(slotId)] = ngamsPhysDiskInfo.ngamsPhysDiskInfo().\
                                       setPortNo(portNo).\
                                       setSlotId(slotId).\
                                       setMountPoint(mtPt).\
                                       setStatus(status).\
                                       setCapacityGb(capacityGb).\
                                       setModel(model).\
                                       setSerialNo(serialNo).\
                                       setType(diskType).\
                                       setManufacturer(manufacturer).\
                                       setDiskId(diskId).\
                                       setDeviceName(deviceName)
        idx = idx + 1

    return diskInfoDic


def getControllers():
    """
    Query the controllers available, and return this in a list.

    Returns:   List with available 3ware controllers (list).
    """
    T = TRACE()

    cmd = "sudo /usr/local/sbin/tw_cli info"
    stat, out = commands.getstatusoutput(cmd)
    if (stat):
        raise Exception, "Error invoking 3ware Command Line Tool: " + str(out)
    contList = []
    for line in out.split("\n"):
        line = line.strip()
        if (line):
            if (line.find("Controller") == 0):
                # "Controller 1: 8506-4LP (4)"
                contNo = line.split(" ")[1].split(":")[0]
                contList.append(contNo)
            elif (line[0] == "c"):
                contNo = int(line.split(" ")[0][1:])
                contList.append(contNo)
    contList.sort()
    return contList


def parseGen1Controller(rootMtPt,
                        devStartIdx,
                        contOut,
                        oldFormat):
    """
    The output of the 1st generation 3ware Command Line Utility used for NGAS
    look like this:


    Controller 0
    -------------
    Model:    7810
    FW:       X 1.05.00.034
    BIOS:     BE7X 1.08.00.038
    Monitor:  ME7X 1.01.00.035
    Serial #: F06802A1220097
    PCB:      Rev3
    PCHIP:    V1.30
    ACHIP:    V3.20

    # of units:  2
    Unit  4: JBOD 41.17 GB (80418240 blocks): OK
    Unit  5: JBOD 30.73 GB (60036480 blocks): OK

    # of ports:  8
    Port  0: DRIVE NOT PRESENT
    Port  1: DRIVE NOT PRESENT
    Port  2: DRIVE NOT PRESENT
    Port  3: DRIVE NOT PRESENT
    Port  4: IC35L040AVER07-0 SXPTX093675 41.17 GB (80418240 blocks): \
    OK (unit  4)
    Port  5: IBM-DTLA-307030 YKEYKNJ6131 30.73 GB (60036480 blocks): \
    OK (unit  5)
    Port  6: DRIVE NOT PRESENT
    Port  7: DRIVE NOT PRESENT    

   
    The entries for each port can also be formatted in the following way:


    Port  1: WDC WD2000JB-00EVA0 WD-WMAEH2143360 200.4 GB (390721968 blocks):\
    OK (unit  1)


    I.e., manufacturer and model are not concatenated.

    contOut:    Output from 3ware controller (string).

    Returns:    Dictionary with info about the disks (dictionary).
    """
    T = TRACE()

    # Parse the output from the 3ware Command Line Tool.
    diskInfoDic = {}
    slotCount   = 0   # Not used for the moment.
    contCount   = 0   # Controller counter.
    tmpPorts    = 0   # Number of ports current controller.
    portsSum    = 0   # Total sum of ports parsed.
    devIdx      = ord(devStartIdx)
    for line in contOut.split("\n"):
        line = line.strip()
        if (line.find("Controller") == 0):
            contCount += 1
            portsSum += tmpPorts
            tmpPorts = 0
        if (line.find("Port ") != -1): slotCount += 1
        if (line.find("# of ports:") == 0):
            tmpPorts += int(line.split(" ")[-1])
        if ((line.find("Port ") != -1) and \
            (line.find("DRIVE NOT PRESENT") == -1)):
            # Take this line. Generate the information for the disk.
            diskType = "MAGNETIC DISK/ATA"
            lineEls1 = line.split(":")
            portNo   = int(lineEls1[0].split(" ")[-1])
            slotId = str((((contCount - 1) > 0) * portsSum) + (portNo + 1))
            lineEls2 = lineEls1[1].strip().split(" ")
            if (len(lineEls2) == 6):
                model = lineEls2[0]
                idx = 1
            else:
                if (lineEls2[0].find("Maxtor") != -1):
                    model = lineEls2[0] + "-" + lineEls2[1]
                elif (oldFormat):
                    model = lineEls2[0]
                else:
                    model = lineEls2[0] + "-" + lineEls2[1]
                idx = 2
            serialNo = lineEls2[idx]
            capGb    = int(float(lineEls2[idx + 1]) + 0.5)
            manufact = string.split(model, "-")[0]
            if (manufact[0:2] == "IC"): manufact = "IBM"
            diskId   = model + "-" + serialNo
            mtPt     = rootMtPt + "/data" + slotId
            devName  = "/dev/sd" + chr(devIdx + len(diskInfoDic)) + "1"
            status   = lineEls1[-1].strip().split(" ")[0]
            diskInfoDic[str(slotId)] = ngamsPhysDiskInfo.ngamsPhysDiskInfo().\
                                       setPortNo(portNo).\
                                       setSlotId(slotId).\
                                       setMountPoint(mtPt).\
                                       setStatus(status).\
                                       setCapacityGb(capGb).\
                                       setModel(model).\
                                       setSerialNo(serialNo).\
                                       setType(diskType).\
                                       setManufacturer(manufact).\
                                       setDiskId(diskId).\
                                       setDeviceName(devName)

    return diskInfoDic

   
def parseGen2Controller(rootMtPt,
                        contList,
                        devStartIdx,
                        ngasStartIdx,
                        contOut):
    """
    The output of the 2nd generation 3ware Command Line Utility used for NGAS
    look like this:


    Unit  UnitType  Status      %Cmpl  Stripe  Size(GB)  Cache  AVerify  IgnECC
    ---------------------------------------------------------------------------

    Port   Status           Unit   Size        Blocks        Serial
    ---------------------------------------------------------------
    p0     NOT-PRESENT      -      -           -             -
    p1     OK               -      372.61 GB   781422768     KRFS06RAGGXTAC
    p2     NOT-PRESENT      -      -           -             -
    p3     OK               -      372.61 GB   781422768     KRFS06RAGBND5C
    p4     OK               -      372.61 GB   781422768     KRFS06RAGGWD7C
    p5     OK               -      372.61 GB   781422768     KRFS06RAGGXTAC
    p6     OK               -      372.61 GB   781422768     KRFS06RAGGXUWC
    p7     OK               -      372.61 GB   781422768     KRFS06RAGBND5C

    Unit  UnitType  Status      %Cmpl  Stripe  Size(GB)  Cache  AVerify  IgnECC
    ---------------------------------------------------------------------------
    u0    RAID-1    OK           -      -       65.1826   ON     OFF      OFF  
    u1    RAID-1    OK           -      -       372.519   ON     OFF      OFF  

    Port   Status           Unit   Size        Blocks        Serial
    ---------------------------------------------------------------
    p0     OK               u0     69.25 GB    145226112     WD-WMAKE1317691
    p1     OK               u0     69.25 GB    145226112     WD-WMAKE1317543
    p2     OK               u1     372.61 GB   781422768     KRFS06RAGBNB1C
    p3     OK               u1     372.61 GB   781422768     KRFS06RAGBGP0C


    rootMtPt:     Root mount point (string).

    contList:     List with controllers in the system (list/integer).
    
    devStartIdx:  Start index of disk array (string/a|b|c...).
                        
    ngasStartIdx: Startindex of first disk to be used by NGAS (string/a|b|c...)
    
    contOut:      Output from 3ware controller (string).

    Returns:      Dictionary with info about the disks (dictionary).
    """
    T = TRACE()
  
    # Parse the output from the 3ware Command Line Tool.
    diskInfoDic = {}
    slotCount   = 0   # Not used for the moment.
    contCount   = 0   # Controller counter.
    tmpPorts    = 0   # Number of ports current controller.
    portsSum    = 0   # Total sum of ports parsed.
    devIdx      = ord(devStartIdx)
    if ngasStartIdx < devStartIdx: ngasStartIdx = devStartIdx
    idx         = 0
    bufLines    = contOut.split("\n")
    bufLen      = len(bufLines)
    while (idx < bufLen):
        line = bufLines[idx].strip()
        info(5,"Parsing 3ware status line: %s" % line)
        # Next controller?
        if ((line.find("Unit ") != -1) and (line.find("UnitType ") != -1)):
            unitTypeDic = {}
            contNo = contList[contCount]
            contCount += 1
            portsSum += tmpPorts
            tmpPorts = 0

            # Look for 1st unit - check if a JBOD or RAID unit.
            raid = 0
            while (idx < bufLen):
                idx += 1
                line = bufLines[idx].strip()
                if (len(line)):
                    if ((line.find("Port") != -1) and
                        (line.find("Status") != -1)):
                        break
                    elif (line[0].strip() == "u"):
                        cfg = cleanList(line.split(" "))
                        if (cfg[1].find("RAID") != -1):
                            unitTypeDic[cfg[0]] = "RAID"
                        else:
                            unitTypeDic[cfg[0]] = "JBOD"

            # Get the info for each JBOD disk. Parse until the start of the
            # next controller status or till the end of the status output.
            idx += 1
            while (idx < bufLen):
                line = bufLines[idx].strip()
                info(5,"Parsing 3ware status line: %s" % line)
                if (line == ""):
                    idx += 1
                    continue
                if ((line.find("Unit ") != -1) and
                    (line.find("UnitType ") != -1)):
                    break    # Break to outer loop
                # Take the line if it contains disk info.
                if (line[0] == "p"):
                    tmpPorts += 1
                    diskInfo = cleanList(line.split(" "))
                    if ((diskInfo[1] != "NOT-PRESENT") and
                        (unitTypeDic[diskInfo[2]] == "JBOD")):
                        # A disk is found, get the info.
                        portNo = int(diskInfo[0][1:])
                        slotId = str((((contCount - 1) > 0) * portsSum) +\
                                     (portNo + 1))
                        # Get status, model ID, serial number and capacity.
                        cmd = "sudo /usr/local/sbin/tw_cli info c%d p%d " +\
                              "status model serial capacity"
                        cmd = cmd % (int(contNo), int(portNo))
                        stat, out = commands.getstatusoutput(cmd)
                        outLines = out.split("\n")
                        status   = cleanList(outLines[0].split(" "))[3]
                        model    = cleanList(outLines[1].split(" "))[3]
                        serialNo = cleanList(outLines[2].split(" "))[3]
                        capGb    = int(float(cleanList(outLines[3].\
                                                       split(" "))[3]) + 0.5)
                        diskType = "MAGNETIC DISK/ATA"
                        if (model.find("HDS") == 0):
                            manufact = "Hitachi Data Systems"
                        elif (model.find("WDC") == 0):
                            manufact = "Western Digital Corporation"
                        else:
                            manufact = "UNKNOWN"
                        diskId   = model + "-" + serialNo
                        mtPt     = rootMtPt + "/volume" + slotId
                        devName  = "/dev/sd" + chr(devIdx +\
                                                   len(diskInfoDic)) + "1"
                        diskInfoDic[str(slotId)] = ngamsPhysDiskInfo.\
                                                   ngamsPhysDiskInfo().\
                                                   setPortNo(portNo).\
                                                   setSlotId(slotId).\
                                                   setMountPoint(mtPt).\
                                                   setStatus(status).\
                                                   setCapacityGb(capGb).\
                                                   setModel(model).\
                                                   setSerialNo(serialNo).\
                                                   setType(diskType).\
                                                   setManufacturer(manufact).\
                                                   setDiskId(diskId).\
                                                   setDeviceName(devName)
                idx += 1
            idx -= 1   # Decrease count to avoid index exceeding buffer length.
        idx += 1    # Main loop buffer index
    # AWI: added to fix ATF problem [AW]
    diskInfoDicUsed = {}
    for di in diskInfoDic:
        if diskInfoDic[di].getDeviceName() >= '/dev/sd' + ngasStartIdx + '1':
            diskInfoDicUsed.update({di:diskInfoDic[di]})
    # AWI
    return diskInfoDicUsed


def getContInfo(contList):
    """
    Get the dump about each controller, concatenate this and return it.

    contList:     List with controllers in the system (list/integer).

    Returns:      Accumulated ASCII output of info about controllers (string).
    """
    T = TRACE()
    
    out = ""
    for contId in contList:
        cmd = "sudo /usr/local/sbin/tw_cli info c%s" % str(contId)
        info(4,"Executing 3ware client tool: %s" % cmd)
        stat, outTmp = commands.getstatusoutput(cmd)
        info(4,"Executed 3ware client tool. Status: %d" % stat)
        if (stat):
            raise Exception, "Error invoking 3ware Command Line Tool: " +\
                  str(out)
        else:
            out += outTmp
        out += "\n"
    return out


def exportCont(contId):
    """
    Export all units found on the controller. The function does not complain
    if errors occur.

    contId:    ID (number) of the controller (integer).

    Returns:   Void.
    """
    return # We don't want this to be executed ...

    T = TRACE()

    # Unit  UnitType  Status         %Cmpl  Stripe  Size(GB)  Cache  AVerify  IgnECC
    # ------------------------------------------------------------------------------
    # u0    RAID-5    OK             -      256K    1862.59   ON     OFF      OFF      
    # u1    RAID-5    OK             -      256K    1490.07   ON     OFF      OFF      
    # u2    SPARE     OK             -      -       372.603   -      OFF      -        
    
    # Port   Status           Unit   Size        Blocks        Serial
    # ---------------------------------------------------------------
    # p0     OK               u0     372.61 GB   781422768     KRFS02RAGWWHKC
    # p1     OK               u0     372.61 GB   781422768     KRFT06RAGW9ZAC
    # p2     OK               u0     372.61 GB   781422768     KRFS26RAGWB7ZC
    # ...
    cmd = "sudo /usr/local/sbin/tw_cli info c%s" % str(contId)
    stat, out = commands.getstatusoutput(cmd)
    # Just look for lines starting with "u".
    unitList = []
    for line in out.split("\n"):
        line = line.strip()
        if (line == ""): continue
        if (line[0] == "u"): unitList.append(line.split(" ")[0])
    for unit in unitList:
        cmd = "sudo /usr/local/sbin/tw_cli /c%d/%s export quiet" %\
              (int(contId), unit)
        info(4,"Invoking command to export 3ware unit: %s ..." % cmd)
        stat, out = commands.getstatusoutput(cmd)
        info(4,"Result of command: %s to export 3ware unit: %d" % (cmd, stat))


def rescanCont(contId):
    """
    Rescan a 3ware controller.

    contId:    ID (number) of the controller (integer).

    Returns:   Void.
    """
    return # Don't want to execute this
    
    T = TRACE()
    
    cmd = "sudo /usr/local/sbin/tw_cli /c%d rescan" % (int(contId))
    info(3,"Invoking command to rescan 3ware unit: %s ..." % cmd)
    stat, out = commands.getstatusoutput(cmd)
    info(3,"Result of command: %s to rescan 3ware unit: %d" % (cmd, stat))


def parseCmdLineInfo(rootMtPt,
                     controllers = None,
                     oldFormat = 0,
                     slotIds = ["*"],
                     devStartIdx = "a",
                     ngasStartIdx = "a",
                     rescan = 1):
    """
    Execute the 3ware command line utility and extract the information
    about the disks connected to the system.
                     
    rootMtPt:      Root mount point to be used (string)

    oldFormat:    Old format producing 'wrong' Disk ID for WDC/Maxtor disks
                  (integer/0|1).

    controllers:  List of Number of 3ware Controllers installed (integer).

    slotIds:      List containing the Slot IDs to take into account. If
                  given as '*' all slot are considered (list/string).

    buf:          Buffer containing output from 3ware Controller (string).

    devStartIdx:  Device starting index. I.e., a /dev/sda1 (string).

    rescan:       If 1 the 3ware controller is re-initialized (export + rescan)
                  (integer/0|1).

    Returns:      Dictionary containing objects which are instances of
                  the class ngamsPhysDiskInfo (dictionary).
    """
    T = TRACE()
    
    out = ""
    if (controllers):
        contList = controllers.split("/")
    else:
        contList = getControllers()
    contList.sort()
    contOut = getContInfo(contList)

    # Check if we're dealing with 3ware 9000 series/JBOD, if yes, we export +
    # rescan the 3ware controllers. Query the controller info again.
    #     if (rescan):
    #         if (contOut.find("Controller") == -1):
    #             for contId in contList:
    #                 exportCont(contId)
    #                 rescanCont(contId)
    #             contOut = getContInfo(contList)

    # Determine the type of controller.
    if (contOut.find("Controller") != -1):
        # 1st Generation Controller.
        diskInfoDic = parseGen1Controller(rootMtPt, devStartIdx, contOut,
                                          oldFormat)
    else:
        # 2nd Generation Controller.
        diskInfoDic = parseGen2Controller(rootMtPt, contList, devStartIdx,
                                          ngasStartIdx, contOut)

    return diskInfoDic


if __name__ == '__main__':
    """
    Main function.
    """
    gen1Ctrl = """\
    Controller 0
    -------------
    Model:    7810
    FW:       1.05.00.063
    BIOS:     BE7X 1.08.00.048
    Monitor:  ME7X 1.01.00.038
    Serial #: 2 `8
    PCB:      Rev4
    PCHIP:    1.30-66
    ACHIP:    3.20
    
    # of units:  3
    Unit  1: JBOD 200.4 GB (390721968 blocks): OK
    Unit  2: JBOD 300.0 GB (585940320 blocks): OK
    Unit  3: JBOD 200.4 GB (390721968 blocks): OK
    
    # of ports:  8
    Port  0: DRIVE NOT PRESENT
    Port  1: WDC WD2000JB-00EVA0 WD-WMAEH2143360 200.4 GB (390721968 blocks):\
             OK (unit  1)
    Port  2: Maxtor 5A300J0 A8151Q7E 300.0 GB (585940320 blocks): OK (unit  2)
    Port  3: WDC WD2000JB-00EVA0 WD-WMAEH1993650 200.4 GB (390721968 blocks):\
             OK (unit  3)
    Port  4: DRIVE NOT PRESENT
    Port  5: DRIVE NOT PRESENT
    Port  6: DRIVE NOT PRESENT
    Port  7: DRIVE NOT PRESENT
    """

    gen2Ctrl = """\
    Unit  UnitType  Status         %Cmpl  Stripe  Size(GB)  Cache  AVerify  \
    IgnECC
    ---------------------------------------------------------------------------
    
    Port   Status           Unit   Size        Blocks        Serial
    ---------------------------------------------------------------
    p0     NOT-PRESENT      -      -           -             -
    p1     OK               -      372.61 GB   781422768     KRFS06RAGGXTAC
    p2     NOT-PRESENT      -      -           -             -
    p3     OK               -      372.61 GB   781422768     KRFS06RAGBND5C
    p4     OK               -      372.61 GB   781422768     KRFS06RAGGWD7C
    p5     OK               -      372.61 GB   781422768     KRFS06RAGGXTAC
    p6     OK               -      372.61 GB   781422768     KRFS06RAGGXUWC
    p7     OK               -      372.61 GB   781422768     KRFS06RAGBND5C

    Unit  UnitType  Status      %Cmpl  Stripe  Size(GB)  Cache  AVerify  IgnECC
    ---------------------------------------------------------------------------
    u0    RAID-1    OK             -      -       65.1826   ON     OFF    OFF  
    u1    RAID-1    OK             -      -       372.519   ON     OFF    OFF  

    Port   Status           Unit   Size        Blocks        Serial
    ---------------------------------------------------------------
    p0     OK               u0     69.25 GB    145226112     WD-WMAKE1317691
    p1     OK               u0     69.25 GB    145226112     WD-WMAKE1317543
    p2     OK               u1     372.61 GB   781422768     KRFS06RAGBNB1C
    p3     OK               u1     372.61 GB   781422768     KRFS06RAGBGP0C
    """
    diskDic = parseCmdLineInfo("/NGAS", devStartIdx="b",ngasStartIdx='f')
    slotIds = diskDic.keys()
    slotIds.sort()
    for diskId in slotIds:
        physDiskObj = diskDic[diskId]
        print "\n\n" + physDiskObj.dumpBuf()


# EOF

