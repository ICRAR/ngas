#!/usr/bin/env python
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
#
# Script dumps the primary header of FITS files to stdout or creates
# header files
# It supports compressed files (.gz and .Z)
#
# for more details just call the usage function or run the script
# without parameters
#
#
# A.W. [ESO]: 2002-02-28
#             2003-03-18
#             2003-04-16
#             2003-05-21
#             2003-07-04  Fixed bug with COMMENT and HISTORY keywords
#             2003-07-08  CRC32 datasum implemented
#             2003-07-14  Several small changes and fixes
#             2003-08-04  New class structure
#             2003-09-02  Bug fix in fitsHead (calculation of blankCards)
#             2003-12-15  Bug fix in HeadDict in the creation of new header keywords
#             2003-12-15  Bug fix in HeadDict in the creation COMMENT and HISTORY keys
#             2005-06-03  Bug fix in getKeyword and restructure of HeadDict class.
#             2005-07-20  New method FitsHead.parseFitsHead2TupleList and
#                         FitsHead.getKeyType
#             2005-07-21  Added possibility to pass file and cStringIO objects to
#                         FitsHead.__init__

__version__ = "4.0"
import sys,os, types, subprocess
import string,re
from glob import glob
from zlib import crc32
from math import ceil

class FitsHead:
    """
    Class parses headers of FITS files and creates a memory data structure
    or creates header files. It supports compressed files
    (.gz and .Z)
    for more details just call the usage function or run the
    script without parameters.
    """
    def __init__(self,filename,skey='END',struct=0,show=0,check=0, verbose=0, mode=1):
        """
        """
        self.verbose = int(verbose)
        self.nbytes = 0             # number of bytes read so far
        self.POS = []                # position of headers
        self.SIZE = []
        self.datasum = []            # datasum of headers if check!=0
        self.show = int(show)        # print the header if show!=0
        self.struct = int(struct)    # examine the structure of the file
        self.check = int(check)      # calculate datasums
        self.Extension = []          # list of HeadDict instances
        self.Mode = mode             # if 0 it is assumed that the input does
                                     # not contain data (.hdr file)
        self.KKeys = ['SIMPLE','EXTEND','NAXIS[0-9]{0,2}','BITPIX','XTENSION', 'END',]
        if skey != 'END': self.KKeys.append(skey)
        (self.fd, self.size) = self.openFile(filename)
        if self.size == -1:
            errMsg = "*** File %s does not exists ****" % filename
            raise Exception(errMsg)
        self.ID = self.fd.name
        self.name = self.fd.name
        self.HEAD = []               # list of list(s) of header cards
        self.analyzeStruct()




    def analyzeStruct(self):
        """
        Method does minimal parsing of the headers in order to derive the structure
        of the FITS file. It fills the string array self.STRUCT and the string array
        self.HEAD, which contains the plain header cards. The mandatory keywords
        are parsed into the HD dictionaries for each extension.
        """
        self.STRUCT = []
        HH = self.dumpHead()
        hcount = 1
        headfl = 1
        if self.struct > 0:
            while len(HH) > 0 :
                self.HEAD.append(HH)
                if self.Mode:
                    self.skipData(header=-1)
                naxis = int(self.Extension[-1].getKeyword('NAXIS')[1])
                if headfl == 1:
                    stmp = "# HDR  NAXIS  "
                    for na in range(1,3):
                        stmp += "NAXIS%2d  " % na
                    stmp += '    POS            DATASUM'
                    self.STRUCT.append(stmp)
                    self.STRUCT.append(70*'-')
                    headfl = 0
                if self.check:
                    datasum = self.datasum[-1]
                else:
                    datasum = -1
                stmp = "%3d  %3d    " % (len(self.HEAD), naxis)
                for na in range(1,naxis+1):
                    lna = int(self.Extension[-1].getKeyword('NAXIS'+str(na))[1])
                    stmp += "%6d   " % lna
                if naxis > 0:
                    stmp += "%10d    %12d" % (self.POS[-1][0],datasum)
                self.STRUCT.append(stmp)
                if self.show == len(self.HEAD)-1 and self.show != 99:
                    break
                else:
                    HH = self.dumpHead()
                    hcount += 1
        else:
            self.HEAD = [HH]



    def dumpHead(self):
        """
        Read all header blocks starting at current position.

        Output: HEAD: array of header strings
        """

        _BLOCKSIZE_ = 2880
        skey = self.KKeys[-1]
        rkkeys = self.KKeys[0]
        for kkey in self.KKeys[1:]:
            rkkeys = rkkeys + '|' + kkey
        rq = re.compile(rkkeys)

        index = 0
        number = len(self.Extension)
        endfl = 0
        skfl = 0
        keys=[]
        block = self.fd.read(_BLOCKSIZE_).decode()
        self.nbytes = self.nbytes + _BLOCKSIZE_
        if len(block) > 0 and not block[0:8] == 'XTENSION' and not block[0:6] == 'SIMPLE':
            return ''
        if block:
            self.POS.append([self.nbytes - _BLOCKSIZE_,0])
            HD = HeadDict(number=number, pos = self.nbytes - _BLOCKSIZE_)
        HEAD=block
        sline = ''
        while block:
            kkeys=[]
            for ind in range(0,_BLOCKSIZE_,80):
                if block[ind] != ' ':
                    pkey = block[ind:ind+8].strip()
                    if pkey == 'END':
                        endfl = 1
                        key = pkey
                    elif pkey == 'HIERARCH':
                        eqind = block[ind:ind+80].find('=')
                        key = block[ind:ind+eqind].strip()
                    else:
                        key = pkey
                    kkeys.append(key)
                    if rq.match(key):
                        LineTuple = self.parseFitsCard(block[ind:ind+80])
                        sline = block[ind:ind+80].strip()
                        if skey != 'END' and LineTuple[0] == skey:
                            HEAD = sline
                            skfl = 1
                        LineDict = HD.keyTuple2Dict(LineTuple)
                        LineDict['index']={index:LineTuple[0]}
                        HD.updateKeyword(LineDict)
                index += 1

            keys.append(kkeys)
            if endfl == 1:
#               stat=self.fd.close()
                break
            block=self.fd.read(_BLOCKSIZE_).decode()
            self.nbytes = self.nbytes + _BLOCKSIZE_
            if skfl == 0: HEAD = HEAD + block


        if block or index > 0:
            HD.setHeaderSize(self.nbytes - self.POS[-1][0])
            HD.setDataSize()
            self.Extension.append(HD)
            self.POS[-1][1] = self.nbytes

        return HEAD


    def skipData(self,header=-1):
        """
        skipData method for multiple extension files. Contains also the calculation of the
        data checksum. If the file object is not created from a ordinary file, like a socket or
        a pipe then the method does not skip but rather read through the data.
        """
        (siz,nblocks) = self.Extension[header].DATASIZE
        rr = siz % 2880
        checksum = -1
        if (siz > 0):
            if dir(self.fd).count('name') != 0 and (not self.check) and \
                self.fd.name[1:-1] != 'fdopen':    #this fd.name means pipe, i.e. no seek
                if siz != 0: self.fd.seek(siz,1)     #skip over data
                if rr  != 0: self.fd.seek(2880-rr,1) #and rest of card
            else:
                datasiz = siz
                if rr!=0: datasiz = datasiz + (2880-rr)
                data = self.fd.read(datasiz)
                checksum = -1
                checksum = crc32(data)

            self.nbytes = self.nbytes + siz
            if rr != 0: self.nbytes = self.nbytes+(2880-rr)
        else:
            datasiz = 0
            checksum = -1

        self.datasum.append(checksum)
        self.SIZE.append(siz)
        return 0



    def getData(self,header=0,ofile='',blfl=1):
        """
        Method reads and optionally writes the data part at the current position
        in a FITS file.
        If blfl is 0 the actual data size as given in the header is read, else
        the number of complete FITS blocks are read.
        """
        if self.size>0:   # positioning does not work for streams
            self.fd.seek(self.POS[header][0]+self.POS[header][1],0)
        else:
            header = -1   # force header to be last one
        (siz,nblocks) = self.Extension[header].DATASIZE
        wfl = 0
        if len(ofile) > 0:
            try:
                of = open(ofile,'w')
                wfl = 1
            except:
                print("Problem opening output file:",ofile)
                return
        if wfl:
            for ii in range(nblocks):
                block = self.fd.read(2880)
                of.write(block)
            del(block)
            return -1
        else:
            if blfl == 0:
                rsiz = siz
            else:
                rsiz = nblocks*2880
            data = self.fd.read(rsiz)
            return data


    def openFile(self,file):
        """
        Opens the file or a pipe if the file is compressed and returns
        a file-descriptor and the size of the file.
        """
        flist = glob(file)        #try to find the file
        if len(flist) == 0:            # don't open new one if it does not exist
            return (-1,-1)
        else:
            base = os.path.basename(file)
            ID, ext = os.path.splitext(base)
            if ext == '.Z' or ext == '.gz':
                (fd,STDIN,STDOUT) = subprocess.Popen(['/usr/bin/gunzip','-qc',file],0)
                size = -2   # size is not available in a pipe, but this is not a problem
                self.name = file
                self.ID, ext = os.path.splitext(ID)
            else:
                fd=open(file,'rb')
                fd.seek(0,2)
                size = fd.tell()
                fd.seek(0,0)
                self.name = fd.name
                self.ID = base

        return (fd,size)



    def parseFitsHead(self):

        """
        Method parses self.HEAD into a HeadDict dictionary.
        """
        exts = []
        for ii in range(len(self.HEAD)):
            HD = HeadDict()
            for ind in range(0,len(self.HEAD[ii]),80):
                h = self.HEAD[ii][ind:ind+80]
                LineTuple = self.parseFitsCard(h)
                key = LineTuple[0]
                if key in ['COMMENT', 'HISTORY', 'ESO-LOG']:
                    LineDict = {'index':-1,'nodes':{key:{'Value':LineTuple[1],\
                                'Comment':LineTuple[2],'Type':''}}}
                else:
                    LineDict = HD.keyTuple2Dict(LineTuple)
                if len(key) > 0:
                    LineDict.update({'index':{ind/80:LineTuple[0]}})
                    HD.updateKeyword(LineDict)

            HD.setNumber(ii)
            HD.setPos(self.Extension[ii].POS)
            HD.setDataSize()
            exts.append(HD)
        self.Extension = exts
        return


    def parseFitsHead2TupleList(self, forceString = 1):

        """
        Method parses self.HEAD into a list containing
        tuples of the form (fileId, ext_ind, key_ind, key, value, comment, type).
        If abs(forceString) == 2 then the DBCM format is produced which contains
        in addition for the numeric types a kw_value_numeric and for certain
        keywords a kw_value_datetime.

        INPUT: forceString    int, optional parameter, if > 0 all entries are converted to strings
                              if < 0 the HISTORY and COMMENT and ESO-LOG keys are not converted.
                              if abs(forceString) == 1: standard format
                              if abs(forceString) == 2: DBCM format
        RETURN: list, list of line tuples.
        """
        if forceString == 0: forceString = 1
        # dateTimeKeys = ['DATE', 'DATE-OBS', 'HIERARCH ESO OBS START', 'HIERARCH ESO TPL START', \
        #                'HIERARCH ESO TEL DATE', 'HIERARCH ESO INS DATE']
        tupleList = []
        for ii in range(len(self.HEAD)):
            tupleList.append([])
            for ind in range(0,len(self.HEAD[ii]),80):
                h = self.HEAD[ii][ind:ind+80]
                LineTuple = self.parseFitsCard(h, index=ind/80)
                key = LineTuple[0]
                LineList = []
                if len(key) > 0:
                    if key in ['COMMENT', 'HISTORY', 'ESO-LOG']:
                        LineList = [self.ID, str(ii), str(LineTuple[4]), LineTuple[0], LineTuple[1][0],\
                             '',LineTuple[3]]
                    else:
                        LineList = [self.ID, str(ii), str(LineTuple[4]), LineTuple[0], LineTuple[1], LineTuple[2], \
                        LineTuple[3]]
                    if abs(forceString) == 2:
                        if LineTuple[3] not in ['C','B','R','T']:
                            kw_value_numeric = LineTuple[1]
                        else:
                            kw_value_numeric = ''
                        if LineTuple[3] == 'T':
                            kw_value_datetime = LineTuple[1][:23].replace('T', ' ')
                        else:
                            kw_value_datetime = ''
                        dotPos = self.ID[2:].find('.') + 2    # make sure that a '.' in the first two characters is ignored
                        if dotPos > 10: dotPos = 10           # and limit the prefix to the first 10 characters
                        LineList = [self.ID[:dotPos]] + LineList + [kw_value_numeric, kw_value_datetime]
                    if forceString > 0 or (key not in ['COMMENT', 'HISTORY', 'ESO-LOG']):
                            tupleList[ii].append(tuple(LineList))
        return tupleList


    def parseFitsCard(self,line, index=-1):
        """
        Method to parse a single FITS header card.

        INPUT: string(80), One line of a FITS header
        RETURN: tuple, (key, value, comment, type, index)
        """

        key = ''
        value = ''
        comment = ''
        typ = ''
        sexpr = re.compile('^COMMENT|HISTORY|END|ESO-LOG')  # these are the special keywords
        qexpr = re.compile("'(''|[^'])*'")  # this allows to catch crazy keyword values like "'o''Neill'"


        if line[0] != ' ' and not sexpr.match(line):
            (key,rest) = line.split('=',1)
            key = key.strip()
            rest = rest.strip()
            if rest[0] == "'":
                try:
                    m = qexpr.match(rest)
                    value = m.group()[1:-1].strip()
                    vind = m.end()
                    typ = 'C'
                except Exception:
                    errMsg = "Could not match expression '(''|[^'])*' against %s\n" % (rest)
                    errMsg += "FITS value of type string not properly quoted! Suspect card is:\n"
                    errMsg += line
                    raise Exception(errMsg)
                if rest[vind:].find("/") > -1:
                    comment = rest[vind:].split("/",1)[1]
                else:
                    comment = ''
            else:
                if rest.find("/") > -1:
                    (value,comment) = rest.split("/",1)
                else:
                    comment = ''
                    value = rest

            value = value.strip()
            comment = comment.strip()
        elif sexpr.match(line):
            key = sexpr.match(line).group()
            rest = sexpr.split(line)
            comment = ''
            if key in ['COMMENT', 'HISTORY', 'ESO-LOG']:
                value = [rest[1].strip()]
            else:
                value = ''

        return self.getKeyType((key,value,comment,typ,index))




    def xmlHead(self, format='vo', outfile = '', pretty = 1, head=0):
        """
        Method takes Extension and creates a list of XML strings or writes
        the XML strings to <outfile>. If <pretty> is 1 (default) then the
        XML file is nicely indented.
        """

        if head == 99:
            heads = self.Extension
        else:
            heads = [self.Extension[head]]
        XmlHead = []

        level = 0
        indent = '   '
        XmlHead.append('<?xml version="1.0" encoding="ISO-8859-1"?>')

        if format == 'vo':

            XmlHead.append('<VOTABLE version="1.1">')
            level = 1 * pretty
            XmlHead.append(level*indent + '<INFO name="Creator" value=' + \
                           '"ESO printhead tool"/>')
            XmlHead.append(level*indent + '<INFO name="Version" value="' +\
                           __version__ + '"/>')
            XmlHead.append(level*indent + '<INFO name="Compatibility" value=' +\
                           '"FITS"/>')
            XmlHead.append(level*indent + '<DESCRIPTION>')
            level +=1

            XmlHead.append(level*indent + 'VOTable file created from FITS file')
            XmlHead.append(level*indent + self.name)

            level -=1
            XmlHead.append(level*indent + '</DESCRIPTION>')
        elif format == 'xfits':
            level = 1 * pretty
            XmlHead.append('<?xml-stylesheet type="text/xml" href="XMLmenu.xsl"?>')
            XmlHead.append('<XFits>')
        else:
            XmlHead.append("<ERROR>Invalid format specified. Should be vo or xfits</ERROR>")
            return XmlHead
        for HD in heads:
            if format == 'vo':
                XmlHead.append(HD.VotableSerialize(level=level, indent=indent))
            else:
                XmlHead.append(HD.XfitsSerialize(level=level, indent=indent))

# close the root element

        if format == 'vo':
            XmlHead.append('\n</VOTABLE>')
        else:
            XmlHead.append('\n</XFits>')


# put the XML into the object

        return XmlHead






    def walkDict(self,level,dict):
        """
        Helper method to recursivly walk-through a dictionary and serialize it.
        """

        for k in list(dict.keys()):
                if type(dict[k]) == type({}):
                    level += 1
                    self.XHead.append((level*"   ") + "<" + k + ">")
                    self.walkDict(level,dict[k])
                    self.XHead.append((level*"   ") + "</" + k + ">")
                else:
                    level += 1
                    if len(dict[k]) > 0:
                        if dict[k][0] == "'" or k == 'Comment':
                            xtype = ' type="string"'
                            self.XHead.append((level*"   ") + "<" + k + xtype + ">" + \
                                      dict[k][1:-1] + "</" + k + ">")
                        else:
                            xtype = ' type="numeric"'
                            self.XHead.append((level*"   ") + "<" + k + xtype + ">" + \
                                              dict[k] + "</" + k + ">")
                    else:
                            self.XHead.append((level*"   ") + "<" + k + "/>")



                level -= 1
        return


    def xmlHead_WalkDict(self,outfile = ''):
        """
        Method takes HeadDict and creates a list of XML strings or writes
        the XML strings to <outfile>.
        This version does not keep the internal order of the HIERARCH keywords, but
        is a lot nicer code than xmlHead and uses the walkDict method recursively.
        """

        self.XHead = []

        self.XHead.append('<?xml version="1.0" encoding="ISO-8859-1"?>')
        self.XHead.append('<XFits>')
        hflag = 0
        for HD in self.Extension:
            for ii in range(len(self)):
                key = self[ii][0:8]
                if key not in ['HIERARCH','COMMENT','HISTORY', 'ESO-LOG']:
                    self.XHead.append('   <' + key + '>')
#                    hkeys = self.walkDict(1,HD[key])
                    self.XHead.append('   </' + key + '>')

                elif key == 'HIERARCH' and hflag == 0:
                    hflag = 1
                    self.XHead.append('   <HIERARCH>')
#                    hkeys = self.walkDict(1,HD['HIERARCH'])
                    self.XHead.append('   </HIERARCH>')

                elif key == 'COMMENT':
                    self.XHead.append('   <COMMENT>')
                    self.XHead = self.XHead + HD['COMMENT']
                    self.XHead.append('   </COMMENT>')

                elif key == 'HISTORY':
                    self.XHead.append('   <HISTORY>')
                    self.XHead = self.XHead + HD['HISTORY']
                    self.XHead.append('   </HISTORY>')
                elif key == 'ESO-LOG':
                    self.XHead.append('   <ESO-LOG>')
                    self.XHead = self.XHead + HD['ESO-LOG']
                    self.XHead.append('   </ESO-LOG>')


        self.XHead.append('</XFits>')

        if len(outfile) > 0:
            try:
                o = open(outfile,'w')
            except:
                print("ERROR: Unable to open ",outfile)
                return 1

            for xml in self.XHead:
                if type(xml) == type(''):
                    o.write(xml + "\n")
                elif type(xml) == type([]):
                    for x in xml:
                        o.write(x + "\n")

            o.close()
        return 0


    def getAllFHeads(self):
        """
        Just for convenience. Not very useful
        """
        self.FHead = []
        for HD in self.Extension:
            self.FHead.append(HD.Serialize())

        return



    def getFitsCard(self,key,header=0):
        """
        Method takes a keyword <key> and returns the original FITS
        card.
        The value of the header parameter can be used to select a
        specific header (counted from 0).

        INPUT:  key, string      Name of the keyword to be searched for
                header,int       number of header to be searched (def. 0)

        OUTPUT: string

        If the keyword does not exist the output string is empty.
        """
        ind = self.Extension[header].getKeyPos(key)
        if ind > -1:
            return self.HEAD[header][ind*80:ind*80+80]
        else:
            return ''


    def getKeyType(self,lineTuple):
        """
        Method tries to guess the type of a keyword value,
        where <type> is one out of ['B','C','U', 'S', 'I','L','F','P', 'R','T']
        and updates the lineTuple on input.

        types:
            'B':    boolean
            'C':    character
            'U':    unsigned short (0 >= value < 256)
            'S':    short (-65536 < value < +65536)
            'I':    integer (32 bit int)
            'L':    long (unlimited)
            'F':    float (if len(value) <= 10)
            'D':    double (if len(value) > 10)
            'T':    datetime string (ISO)

        INPUT: tuple, lineTuple
        RETURN: lineTuple
        """
        # regexp for dateTime type
        dtRx = re.compile(\
          "(19\d{2}|2\d{3})\-(0\d|1[012])\-([012]\d|3[01])" + \
          "([T ]([01]\d|2[0-3])\:[0-5]\d\:[0-5]\d(\.\d+)?)?\s*$")

        # deal with reserved words, which would lead to the wrong type...
        reserved = ['INFINITY', 'INF', 'NAN']
        val = lineTuple[1]
        typ = lineTuple[3]

        # check if type is defined already or if one of the reserved words is used.
        if typ == 'C' or (isinstance(val, str) and val.upper() in reserved):
            typ = 'C'
        else:
            try:
              float(val)
              value = float(val)
              if value != 0 and (abs(value) > 1.0e15 or value < 1e-15):
                  typ = 'R'
                  value = None
              dotpos = val.find('.')
              if dotpos < 0 and typ != 'R':
                  try:
                     iv = int(val)
                     if iv < 256 and iv >= 0:
                        typ = 'U'
                     elif iv < 65536:
                        typ = 'S'
                     else:
                        typ = 'I'
                  except:
                     typ = 'L'
              elif typ != 'R':
                  epos = val.upper().find('E')
                  if epos == -1:
                     epos = len(val)
                  else:
                     ex = int(val[epos+1:])
                  if dotpos >= 0:
                     typ = 'F'
                     if len(val[dotpos+1:epos]) > 15:
                        typ = 'P'
            except:
                if val == 'T' or val == 'F':
                    typ = 'B'
                else:
                    typ = 'C'
        if isinstance(type(val), str) and typ == 'C' and dtRx.match(val):
            # check for datetime format
            typ = 'T'

        return (lineTuple[0], lineTuple[1], lineTuple[2], typ, lineTuple[4])




    def setVerbose(self, value):
        """
        Set verbosity of output.
        """
        self.verbose = value

    def getVerbose(self):
        """
        Return the setting of the verbosity flag.
        """
        return self.verbose


class HeadDict(dict):
    """
    This class defines the data structure used by FitsHead. Essentially the data structure
    consists of nested dictionaries (hash tables) with the following structure:
        {'index':{<index1>:<keyword1>,<index2>:<keyword2>,...},'nodes':{<keyword1>:{'Value':<value1>,'Comment':<comment1>,'Type':<type1>},
                                                                        <keyword2>:{'Value':<value2>,'Comment':<comment2>,'Type':<type2>},...}}

    The 'index' dictionary keeps the sorting information of the keywords, where the key
    is the location of the keyword in the header and the value is the name of the keyword.

    The 'nodes' dictionary contains the keyword name as the key. The value of a single node key
    is again a dictionary. For normal standard keywords the value contains another dictionary,
    (keyval dictionary) which has the three keys 'Value', 'Comment' and 'Type'. For HIERARCH keywords it contains
    the next level in the hierarchy, where the leaf node contains finally a standard keyval
    dictionary as described above for normal keywords.
    """

    def __init__(self, number=0, pos=0):
        """
        """
        self.update({'index':{},'nodes':{}})
        self.POS = pos
        self.NUMBER = number
        self.HEADERSIZE = -1
        self.DATASIZE = (-1,-1)
        self.XmlHead = []


    def setHeaderSize(self, size=-1):
        """
        Set the SIZE variable, which contains the size in bytes of the header.

        INPUT:     size, long
        RETURN:    1 if successful, 0 else
        """
        if type(size) == int:
            self.HEADERSIZE = int(size)
            return 1
        else:
            return 0


    def setPos(self, position=-1):
        """
        Set the POS variable, which contains the position in bytes of the header.

        INPUT:     position, long
        RETURN:    1 if successful, 0 else
        """
        if type(position) == int:
            self.POS = int(position)
            return 1
        else:
            return 0


    def setNumber(self, number=0):
        """
        Set the POS variable, which contains the number of the header counted from 0.

        INPUT:     number, int or long
        RETURN:    1 if successful, 0 else
        """
        if type(number) == int:
            self.NUMBER = int(number)
            return 1
        else:
            return 0


    def setDataSize(self):
        """
        Calculate and set the DATASIZE variable.

        INPUT:     none
        OUTPUT:    int tuple, (datasize, <number of blocks>)
        """
        naxis = int(self.getKeyword('NAXIS')[1])
        siz = 0
        nblocks = 0
        if (naxis > 0):
            siz = 1
            ii  = 1
            while (ii <= naxis):
                kk="NAXIS" + str(ii)
                siz = siz * int(self.getKeyword(kk)[1])
                ii += 1


            siz = siz * abs(int(self.getKeyword('BITPIX')[1]))/8     #calculate data size
            nblocks = int(siz/2880.+0.5)

        self.DATASIZE = (siz,nblocks)

        return (siz,nblocks)




    def keyTuple2Dict(self,keyTuple,force=0):
        """
        Method takes a keyTuple on input and returns a dictionary of the HeadDict
        form for that keyword. If the keyword does not exist in self or
        force=1 the dictionary is created from scratch.

        INPUT:     keyTuple, defining a single keyword
        OUTPUT:    keyDictionary
        """

        try:
            (key,value,comment,typ,index) = keyTuple
            hkeys = key.split()

            fullInd = ''
            for hk in hkeys:
                fullInd += "['"+hk+"']"

        except:
            return 0

        existKey = self.getKeyDict(key)
        maxInd = self.getKeyIndex('END')
        newInd = 0

        if list(existKey['index'].keys())[0] == -1:    # keyword not found, create!
            testKey = existKey.copy()
            lenInd = len(self['index'])
#           print maxInd,lenInd,self['index'].has_key(lenInd-1)
            if lenInd > 0:
                maxInd = max(self['index'].keys())

            if maxInd > (lenInd-1) and not (lenInd - 1) in self['index']:
                newInd = lenInd-1
            elif maxInd > (lenInd-1)  and not lenInd in self['index']:
                newInd = maxInd
            elif maxInd > 0 and self['index'][maxInd] == 'END'  and maxInd == lenInd-1:
                newInd = maxInd
                del(self['index'][maxInd])
                maxInd = maxInd+1
                self['index'].update({maxInd:'END'})
            eval("testKey['nodes']"+fullInd+".update({'Comment':comment,'Value':value,'Type':typ})")
            testKey.update({'index':{newInd:key}})
        elif force == 1:
            testKey = existKey.copy()
            eval("testKey['nodes']"+fullInd+".update({'Comment':comment,'Value':value,'Type':typ})")
        else:
            testKey = existKey


        return testKey


    def getNode(self,key=''):
        """
        Return node of HD dictionary for a certain keyword.

        INPUT:     string, keyword
        OUTPUT:    node of HD dictionary.
        """
        hkeys = key.split()
        fullInd = ''
        node = self['nodes']
        for hk in hkeys:
            node = node[hk]
        return node


    def getElementType(self,key=''):
        """
        Method returns the type of an element ['node'|'leaf'].
        """
        if self.getDescendantElements(key=key) == ('Comment','Type','Value'):
            return 'leaf'
        else:
            return 'node'



    def getDescendantElements(self,key=''):
        """
        Method returns the names of the descendant elements of a <key> as a tuple.

        INPUT:     string, keyword
        OUTPUT:    string tuple, list of descendant nodes
        """
        nodes = list(self.getNode(key=key).keys())
        return tuple(nodes)



    def getKeyDict(self,key,desc=0,inst=0):
        """
        Method takes a keyword <key> and returns a dictionary of the HeadDict
        form for that key only.
        If desc is different from 0 only the descendant nodes will be returned.
        If inst is different from 0 an HeadDict instance is returned.

        INPUT:     string, keyword
                   int attribute desc, 0 or 1, 0 is default, optional
                   int attribute inst, 0 or 1, 0 is default, optional
        """
        hkeys = key.split()

        fullInd = ''
        curkey = ''
        exists = 0
        keyDict = HeadDict()
        node = self['nodes'].copy()
        for hk in hkeys:
            if hk in node and type(node[hk]) == type({}):
                keys = list(node[hk].keys())
                exists = 1
                node = node[hk]
                if keys != ('Comment','Value','Type') and hk != hkeys[-1]:
                    if desc == 0:
                        keyDict.getNode(curkey).update({hk:{}})
#                        eval("keyDict['nodes']"+fullInd+".update({hk:{}})")
                elif keys == ('Comment','Value','Type') or hk == hkeys[-1]:
                    if desc == 0:
                        keyDict.getNode(curkey).update({hk:node})
#                        eval("keyDict['nodes']"+fullInd+".update({hk:node})")
                    else:
                        keyDict['nodes'].update(node)

            elif hk in node and type(node[hk]) != type({}):
                node = node[hk]
                if desc == 0:
                    keyDict.getNode(curkey).update({hk:node})
#                    eval("keyDict['nodes']"+fullInd+".update({hk:node})")
                else:
                    keyDict['nodes'].update({hk:node})

            else:
                exists = 0
                if hk == hkeys[-1]:
                    node = {hk:{'Comment':'','Value':'','Type':''}}
                else:
                    node = {hk:{}}
                if desc == 0:
                    keyDict.getNode(curkey).update(node)
#                    eval("keyDict['nodes']"+fullInd+".update(node)")
                else:
                    keyDict['nodes'].update(node)

            fullInd += "['"+hk+"']"
            curkey = (curkey+" "+hk).strip()

        if self.getKeyIndex(key) >=0:
            keyDict['index'].update({self.getKeyIndex(key):key})
        else:
            keyDict['index'].update({-1:key})

        if inst==1:
            return keyDict
        else:
            return keyDict

    def getRegexpKey(self,key):
        """
        Method takes a keyword regular expression string on input and returns
        a list of keywords matching the expression.

        INPUT:     string, keyword regexp
        OUTPUT:    string list, keyword names
        """
        res=[]
        reg=re.compile(key)
        keyList=list(self['index'].values())
        for k in keyList:
            if reg.match(k):
                res.append(k)
        return res



    def getKeyIndex(self,key):
        """
        Return the index of a keyword. Method tests for existence.

        INPUT:    none
        OUTPUT    integer, index of the keyword or -1 if keyword does not exist
        """

        ind = -1
        try:
            list(self['index'].values()).index(key)
            exist = 1
        except:
            exist = 0

        if exist:
            for ind in list(self['index'].keys()):
                if self['index'][ind] == key:
                    return ind

        return ind


    def filter(self,keyexp):
        """
        Remove keywords matching the regular expression <keyexp>

        INPUT:     keyword regular expression
        OUTPUT:    none
        """
        try:
            recomp = re.compile(keyexp)
        except Exception as e:
            return e
        matchlist = list(map(lambda x,y:(re.match(recomp,x) != None)*(y+1),\
            list(self['index'].values()),\
            list(self['index'].keys())))
        indlist = [x-1 for x in [x for x in matchlist if x>0]]
        list(map(lambda x:self['index'].pop(x), indlist))


    def setKeyIndex(self,newind,key):
        """
        Set the index of a <key> to <newind>.

        INPUT:     int, new index for key
                   string, keyword
        OUTPUT:    1 if succesful, 0 else

        NOTE: This method is pretty destructive, i.e. it really sets the
              index to the keyword given no matter whether this index is
              already occupied or not. It should only be used when creating a
              header from scratch.
        """

        try:
            pos = list(self['index'].values()).index(key)
            self['index'].remove(pos)
            self['index'].update({newind:key})
        except:
            return 0

        return 1


    def deleteKeyIndex(self,key):
        """
        Method takes a keyword <key> and deletes the index in the index
        dictionary of the HeadDict instance. It returns the deleted index.

        INPUT:     string, keyword
        OUTPUT:    int, index of keyword
        """
        ind = self.getKeyIndex(key)
        del(self['index'][ind])
        return ind



    def getKeyword(self,key,check=0):
        """
        Method takes a keyword <key> and returns a tuple of the form
        (<key>,<value>,<comment>,<type>).

        INPUT:  key, string      Name of the keyword to be searched for
        OUTPUT: tuple of strings: (<key>,<value>,<comment>,<type>)

        If the keyword does not exist the strings in the tuple are empty.

        If check is set to 1 and the keyword does not exists, the function
        returns None instead.
        """
        hkeys = key.split()

        fullInd = ''
        for hk in hkeys:
            fullInd += "['"+hk+"']"


        try:
            val = eval("self['nodes']"+fullInd+"['Value']")
        except:
            if check == 1:
                return None
            else:
                val = ''
        try:
            com = eval("self['nodes']"+fullInd+"['Comment']")
        except:
            com = ''
        try:
            if len(str(val)) > 0 and not eval("'Type' in self['nodes']"+fullInd):
                typ = self.getKeyType(key)
            elif eval("'Type' in self['nodes']"+fullInd):
                typ = eval("self['nodes']"+fullInd+"['Type']")
                if typ == '':
                   typ = self.getKeyType(key)
            else:
                typ = ''
        except:
            typ = ''
        return key,val,com,typ,-1



    def updateKeyword(self,keyDict,force=0):
        """
        Method takes a keyDict and updates HeadDict accordingly.

        INPUT:  keyDict, Dictionary of HeadDict structure
                force, optional. If 1 existing keywords are overwritten.
        OUTPUT: 1 for success, 0 otherwise

        """
        keys = list(keyDict['index'].values())
        inds = list(keyDict['index'].keys())

        for ii in range(len(keys)):
            key = keys[ii]
            ind = inds[ii]
            hkeys = key.split()

            fullInd = ''
            curkey = ''

            node = keyDict['nodes']
            oDict = self.getKeyDict(key,inst=1)
            for hk in hkeys:
                if 'Value' in oDict.getNode(key=curkey)[hk]:
                    test = len(oDict.getNode(key=curkey)[hk]['Value'])
                else:
                    test = hk in self.getNode(key=curkey)
                node = node[hk]
                if not test:
                    self.getNode(key=curkey).update({hk:node})
                elif key in ['COMMENT', 'HISTORY', 'ESO-LOG']:
                    if key not in self['nodes']:
                        self['nodes'].update({key:node})
                    self['nodes'][key]['Value'].\
                         append(keyDict['nodes'][key]['Value'][0])
                fullInd += "['"+hk+"']"
                curkey = (curkey+" "+hk).strip()

            if list(self['index'].values()).count(key):
                dind = self.getKeyIndex(key)
                del(self['index'][dind])
            self['index'].update({ind:key})
        return 1


    def getKeyPos(self,key):
        """
        Method takes a keyword <key> and returns the position in the original FITS
        header.

        INPUT:  key, string, Name of the keyword to be searched for
        OUTPUT: int, position of the keyword card in original header

        If the keyword does not exist the output is -1
        """
        values = list(self['index'].values())
        keys = list(self['index'].keys())
        try:
            return keys[values.index(key)]
        except:
            return -1



    def getKeyType(self,key):
        """
        Method updates the keyword dictionary with the derived type
        {<key>:{'Value':<value>,'Comment':<comment>, 'Type':<type>}}
        where <type> is one out of ['B','C','I','L','F','P', 'R' 'T']

        INPUT:     string, keyword
        OUTPUT:    string, derived type or blank string if type could not be derived
        """
        dtRx = re.compile(\
          "(19\d{2}|2\d{3})\-(0\d|1[012])\-([012]\d|3[01])" + \
          "([T ]([01]\d|2[0-3])\:[0-5]\d\:[0-5]\d(\.\d+)?)?\s*$")

        hkeys = key.split()

        fullInd = ''
        for hk in hkeys:
            fullInd += "['"+hk+"']"


        if eval("self['nodes']"+fullInd+".has_key('Type')"):
            typ = eval("self['nodes']"+fullInd+".has_key('Type')")
        else:
            typ = ""
        if eval("self['nodes']"+fullInd+".has_key('Value')"):

            val = eval("self['nodes']"+fullInd+"['Value']")
            if typ == 'C' or (type(val) == bytes and val.upper() in reserved):
                typ = 'C'
            else:
                try:
                  float(val)
                  value = float(val)
                  if value != 0 and (abs(value) > 1.0e15 or abs(value) < 1e-15):
                      typ = 'R'
                      value = None
                  dotpos = val.find('.')
                  if dotpos < 0 and typ != 'R':
                      try:
                         iv = int(val)
                         if iv < 256 and iv >= 0:
                            typ = 'U'
                         elif iv < 65536:
                            typ = 'S'
                         else:
                            typ = 'I'
                      except:
                         typ = 'L'
                  elif typ != 'R':
                      epos = val.upper().find('E')
                      if epos == -1:
                         epos = len(val)
                      else:
                         ex = int(val[epos+1:])
                      if dotpos >= 0:
                         typ = 'F'
                         if len(val[dotpos+1:epos]) > 15:
                            typ = 'P'
                except:
                    if val == 'T' or val == 'F':
                        typ = 'B'
                    else:
                        typ = 'C'
            if type(val) == bytes and typ == 'C' and dtRx.match(val):
                # check for datetime format
                typ = 'T'


            exec("self['nodes']"+fullInd+".update({'Type':typ})")
            return typ
        else:
            exec("self['nodes']"+fullInd+".update({'Type':''})")

            return ''


    def sortKeys(self):
        """
        Just started a method to do the proper sorting of keywords....
        """

        pk = {'SIMPLE':0,'XTENSION':0,'BITPIX':1,'NAXIS':2,'NAXIS1':3,\
              'NAXIS2':4,'NAXIS3':5,'NAXIS4':6}

        rpk = re.compile('|'.join(list(pk.keys())))
        keys = list(self['index'].values())
        self['index'] = {}
        keys.sort()
        KeyDict = {}
        kind = -1
        hind = -1
        maxk = -1

        for k in keys:
            if len(k) > 8:
                hind += 1
                KeyDict.update({k:[-2,hind]})
            elif rpk.match(k):
                maxk = max([maxk,pk[k]])
                KeyDict.update({k:[-1,0]})
            elif k != 'END':
                kind += 1
                KeyDict.update({k:[-2,kind]})

        for k in keys:
            if k != 'END':
                if KeyDict[k][0] == -1: KeyDict[k][0] = KeyDict[k][1] + maxk
                if KeyDict[k][0] == -2: KeyDict[k][0] = KeyDict[k][1] + kind + maxk
                self['index'].update({KeyDict[k][0]:k})

        maxk = max(self['index'].keys())
        self['index'].update({maxk+1:'END'})

        return

    def Serialize(self,  dataFl=-1):
        """
        Method creates a list of FITS header cards from the HeadDict dictionary.

        INPUT:     none
        OUTPUT:    string list, FITS header cards

        NOTE: The Dictionary has to be formatted like one of the self
              parts, i.e. self[0]
        """

        specialKeys = ['HIERARCH','COMMENT','HISTORY','ESO-LOG','END']
        self.FHead = []
        FHead = []

        comhist = {'COMMENT':-1,'HISTORY':-1, 'ESO-LOG':-1}

        newind = list(self['index'].keys())
        newind.sort()

        for ind in newind:

                key = self['index'][ind]
# treat all 'normal' keywords

                if key[0:8] not in specialKeys:
                    fitsLine = key + (8 - len(key))*' '
                    value = str(self['nodes'][key]['Value'])
                    comment = self['nodes'][key]['Comment']
                    typ = self['nodes'][key]['Type']

                    if len(value) > 0:
                        fitsLine += '= '
                        if typ != 'C':
                            fitsLine = fitsLine + (30 - len(fitsLine) - \
                                                   len(value)) * ' '
                        fitsLine = fitsLine + value
                        fitsLine = fitsLine + (39 - len(fitsLine)) * ' '

                    if len(comment) > 0 and len(fitsLine) + 3 < 80:
                        fitsLine = fitsLine + ' / ' + comment

                    if len(fitsLine) > 80: fitsLine = fitsLine[:80]

                    fitsLine = fitsLine + (80 - len(fitsLine)) * ' '
                    if key == 'END':
                        eInd = max(self['index'].keys())+1
                        lInd = len(self)
#                        for ii in range(lInd,eInd):
#                            FHead.append(80*' ')
                        FHead.append((eInd-lInd)*80*' ')
                    FHead.append(fitsLine)

# COMMENT and HISTORY and ESO-LOG keywords

                elif key in ['COMMENT', 'HISTORY', 'ESO-LOG']:
                    comhist[key] += 1
                    if type(self['nodes'][key]['Value']) == type([]):
                        fitsLine = key + self['nodes'][key]['Value'][comhist[key]]
                        fitsLine = fitsLine + (80-len(fitsLine))*' '
                        FHead.append(fitsLine)
                    elif type(self['nodes'][key]['Value']) == type(''):
                        fitsLine = key + self['nodes'][key]['Value']
                        fitsLine = fitsLine + (80-len(fitsLine))*' '
                        FHead.append(fitsLine)



# HIERARCH keywords are reconstructed from the hierarchy in the dict.

                elif key[0:8] == 'HIERARCH':

                    hind = ""
                    hkeys = key.split()

                    fitsLine = key
                    fitsLine = fitsLine + (29 - len(fitsLine)) * ' ' + '= '
                    for hk in hkeys:
                        hind = hind + "['" + hk + "']"

                        kkeys = eval("self['nodes']"+hind+".keys()")

                    if kkeys.count("Value") > 0:
                        value = str(eval("self['nodes']"+hind+"['Value']"))
                        comment = eval("self['nodes']"+hind+"['Comment']")
                        typ = eval("self['nodes']"+hind+"['Type']")
                        if typ != 'C':
                            fitsLine = fitsLine + (43 - len(fitsLine) - \
                                                   len(value)) * ' '
                        fitsLine = fitsLine + value
                        fitsLine = fitsLine + (43 - len(fitsLine)) * ' '
                        if len(fitsLine) + 3 < 80:
                            fitsLine = fitsLine + ' / ' + comment
                        if len(fitsLine) > 80: fitsLine = fitsLine[:80]
                    else:
                        pass

                    fitsLine = fitsLine + (80 - len(fitsLine)) * ' '
                    FHead.append(fitsLine)

                elif key[0:3] == 'END':

                    hlen = len(FHead)
                    blankCards = 36 - ((hlen+1) % 36)
#                    for ii in range(blankCards):
#                        FHead.append(80 * ' ')
                    FHead.append(blankCards*80*' ')
                    FHead.append('END' + 77*' ')

        return FHead




    def XfitsSerialize(self, level=0, indent='   ', pretty=1):
        """
        Method serializes the HD dictionary into a string array. The format is XFits.

        INPUT:     none mandatory
                   int attribute level, >=0 defines the initial indentation level
                                        default 0, optional
                   string attribute indent, whitespace, defines the amount of indentation
                                            per level. default '   ', optional
        OUTPUT:    string list, XML (XFits) formatted header
        """
        if len(self.XmlHead) > 1 and self.XmlHead[0].strip()[:15] == "<HEADER number=":
            return self.XmlHead
        openTags = []
        hflag = 0
        chlist = {'COMMENT':-1,'HISTORY':-1, 'ESO-LOG':-1}
        XmlHead = []
        XmlHead.append(level*indent + '<HEADER number="' + str(self.NUMBER) + '" position="' + \
                           str(self.POS) + '" datasize="' + str(self.DATASIZE[0]) + '">')
        level += 1    # indent all the rest...
        for key in list(self['index'].values()):

# treat all 'normal' keywords
            rkey = key[0:8].strip()
            if rkey not in ['HIERARCH', 'COMMENT', 'HISTORY', 'ESO-LOG']:
                if hflag:
                    for ot in openTags:
                        XmlHead.append(level*indent + '</'+ot+'>')
                        if ot != 'HIERARCH': level = (level - 1) * pretty
                    openTags = []
                    hflag = 0
                if openTags and (openTags[0] in ['COMMENT', 'HISTORY', 'ESO-LOG']):
                    XmlHead.append(level*indent + '</' + openTags[0] + '>')
                openTags = [rkey]
                XmlHead.append(level*indent + '<' + rkey + '>')
                XmlHead.append((level+1)*indent*pretty + '<Value>' + \
                               self.getKeyword(rkey)[1] + '</Value>')
                XmlHead.append((level+1)*indent*pretty + '<Comment>' + \
                               self.getKeyword(rkey)[2] + '</Comment>')
                XmlHead.append(level*indent + '</' + rkey + '>')

# COMMENT and HISTORY keywords

            elif rkey in ['COMMENT', 'HISTORY', 'ESO-LOG']:
                chlist[rkey] += 1
                if hflag:
                    for ot in openTags:
                        level = (level - 1) * pretty
                        XmlHead.append(level*indent + '</'+ot+'>')
                    openTags = []
                    hflag = 0
                if openTags and openTags[0] != rkey:
                    if len(openTags[0])>0:
                        XmlHead.append(level*indent + '</' + openTags[0] + '>')
                    openTags = [rkey]
                    XmlHead.append(level*indent + '<' + rkey + '>')
                if type(self.getKeyDict(rkey)) == type({}):
                    XmlHead.append(self.getKeyword(rkey)[1])
                elif type(self.getKeyDict(rkey)) == type([]):
                    XmlHead.append(self.getKeyDict(rkey)[chlist[rkey]][1].strip())



# HIERARCH keywords are placed in a real XML hierarchy

            elif rkey == 'HIERARCH':


# COMMENT and HISTORY and ESO-LOG elements are kept open until another element is found
# Close COMMENT or HISTORY here if open
                if openTags and  (openTags[0] in ['COMMENT', 'HISTORY', 'ESO-LOG']):
                    XmlHead.append(level*indent + '</' + openTags[0] + '>')


#hflag controls wheather we are already in a HIERARCH element
#level controls how deep we are in the element and openTags
#keeps all the open tags in reverse order

                if not hflag:
                    level = 2 * pretty
                    XmlHead.append(level * indent + '<HIERARCH>')
                    hflag = 1
                    openTags = ['HIERARCH']

                hind = ""
                hkeys = key.split()
#                    oinds = range(len(openTags))
                oind = 0

#compare current key elements with openTags and close the ones which don't match

                for ot in openTags:
                    if hkeys.count(ot) == 0:
                        XmlHead.append(level*indent + '</'+ot+'>')
                        level -= 1
                        openTags = openTags[1:]
                    else:
                        dum = hkeys.index(ot)
                        oind = max(oind,dum)


                for hk in hkeys[:oind+1]:
                    hind = hind + "['" + hk + "']"


                for hk in hkeys[oind+1:]:
                    level = (level + 1) * pretty
                    hind = hind + "['" + hk + "']"
                    XmlHead.append(level*indent + '<'+hk+'>')
                    openTags = [hk] + openTags

                    kkeys = eval("self['nodes']"+hind+".keys()")
                    if kkeys.count('Value') > 0:
                        dum = kkeys.index("Value")
                        value = eval("self['nodes']"+hind+"['Value']")
                        comment = eval("self['nodes']"+hind+"['Comment']")
                        XmlHead.append((level+1)*indent*pretty + '<Value>' + \
                                       self.getKeyword(key)[1] + '</Value>')
                        XmlHead.append((level+1)*indent*pretty + '<Comment>' + \
                                       self.getKeyword(key)[2]  + '</Comment>')
                        XmlHead.append(level*indent + '</'+hk+'>')
                        level = (level - 1) * pretty
                        openTags = openTags[1:]

# close current <HEADER> element and continue with next
        level -= 1
        XmlHead.append(level*indent + '</HEADER>')


        return XmlHead



    def VotableSerialize(self, level=0, indent='   ', pretty=1):
        """
        Method serializes HeadDict and creates a list of XML strings.
        If <pretty> is 1 (default) then the
        XML file is nicely indented.

        This version is intended to write VOImage output.

        INPUT:     none mandatory
                   int attribute level, >=0 defines the initial indentation level
                                        default 0, optional
                   string attribute indent, whitespace, defines the amount of indentation
                                            per level. default '   ', optional
        OUTPUT:    string list, XML (VOImage) formatted header.
        """

        if len(self.XmlHead) > 1 and self.XmlHead[0].strip()[:13] == "<RESOURCE id=":
            return self.XmlHead
        XmlHead = []
        hflag = 0
        oind = 0
        openTags = ['']

        xstr = '<RESOURCE id="' + str(self.NUMBER) + '"'
        if 'EXTNAME' in self['nodes']:
           xstr = xstr + ' name="' + self['nodes']['EXTNAME']['Value'][1:-1] +'"'

        xstr = xstr + ' type="meta">'
        XmlHead.append(level*indent + xstr)


        level += 1
        XmlHead.append(level*indent + '<INFO name="position" value="' + \
                       str(self.POS) + '"/>')
        XmlHead.append(level*indent + '<INFO name="datasize" value="' + \
                       str(self.DATASIZE[0]) + '"/>')
        for key in list(self['index'].values()):

# treat all 'normal' keywords

#                if key[0:8] != 'HIERARCH':
             if key[0:8] != '--------':     # for test we treat all keywords the same
                 if hflag:
                     for ot in openTags:
                         level = (level-1) * pretty
                         XmlHead.append(level*indent + '</'+ot+'>')
                     openTags = ['']
                     hflag = 0
                 openTags = ['PARAM']

                 (keyword,val,comm,typ,flag) = self.getKeyword(key)

                 if typ == 'I':
                     voTyp = 'int'
                 elif typ == 'U':
                     voTyp = 'unsignedByte'
                 elif typ == 'S':
                     voTyp = 'short'
                 elif typ == 'L':
                     voTyp = 'long'
                 elif typ == 'F':
                     voTyp = 'float'
                 elif typ == 'D':
                     voTyp = 'double'
                 elif typ == 'C':
                     voTyp = 'char'
                 elif typ == 'B':
                     voTyp = 'boolean'
                 else:
                     voTyp = ''

                 if type(val) == type(''):
                    XmlHead.append(level*indent + '<PARAM name="' + \
                                key + '" value="' + val + '" datatype="' +\
                                voTyp + '">')
                    XmlHead.append((level+1)*indent +'<DESCRIPTION>'+\
                                comm + '</DESCRIPTION>')
                    XmlHead.append(level*indent + '</PARAM>')

                 elif type(val) == type([]):
                    for vv in val:
                        XmlHead.append(level*indent + '<PARAM name="' + \
                                    key + '" value="' + vv + '" datatype="' +\
                                    voTyp + '">')
                        XmlHead.append((level+1)*indent +'<DESCRIPTION>'+\
                                    comm + '</DESCRIPTION>')
                        XmlHead.append(level*indent + '</PARAM>')



# HIERARCH keywords are placed in a real XML hierarchy

#                elif key[0:8] == 'HIERARCH':
             elif key[0:8] == '--------':


#hflag controls whether we are already in a HIERARCH element
#level controls how deep we are in the element and openTags
#keeps all the open tags in reverse order

                if not hflag:
                    XmlHead.append(indent + '<PARAM name="' +\
                                   key + '" value="' + self[key]['Value'] + '" datatype="' +\
                                   self[key]['Type'] + '">')
                    hflag = 1
                openTags = ['HIERARCH']
                level = 2 * pretty

                hind = ""
                hkeys = key.split()
                oind = 0

#compare current key elements with openTags and close the ones which don't match

                for ot in openTags:
                    if hkeys.count(ot) == 0:
                        XmlHead.append(level*indent + '</'+ot+'>')
                        level -= 1
                        openTags = openTags[1:]
                    else:
                        dum = hkeys.index(ot)
                        oind = max(oind,dum)


                    for hk in hkeys[:oind+1]:
                        hind = hind + "['" + hk + "']"


                    for hk in hkeys[oind+1:]:
                        level = (level + 1) * pretty
                        hind = hind + "['" + hk + "']"
                        XmlHead.append(level*indent + '<'+hk+'>')
                        openTags = [hk] + openTags

                        kkeys = eval("self"+hind+".keys()")
                        try:
                            dum = kkeys.index("Value")
                            value = eval("self"+hind+"['Value']")
                            comment = eval("self"+hind+"['Comment']")
                            XmlHead.append((level+1)*indent*pretty + '<Value>' + \
                                           value + '</Value>')
                            XmlHead.append((level+1)*indent*pretty + '<Comment>' + \
                                           comment + '</Comment>')
                            XmlHead.append(level*indent + '</'+hk+'>')
                            level = (level - 1) * pretty
                            openTags = openTags[1:]
                        except:
                            pass

# close current <RESOURCE> element and continue with next

        if self.DATASIZE > 0:
            XmlHead.append(level*indent + '<TABLE name="data">')
            level += 1
            XmlHead.append(level*indent + '<FIELD name="image" type="link" '+\
                           'arraysize="[]" datatype="integer">')
            level += 1
            XmlHead.append(level*indent + '<LINK href="cid:' + str(self.NUMBER) + '"/>')
            level -= 1
            XmlHead.append(level*indent + '</FIELD>')
            level -= 1
            XmlHead.append(level*indent + '</TABLE>')

        level -= 1
        XmlHead.append(level*indent + '</RESOURCE>')


# put the XML into the object

        return XmlHead




####
# All the rest is for the interactive version...
###


def usage():
        """
        Prints out a short help.
        """
        msg = ("Script dumps headers of FITS files to stdout or creates header"
               "files. It supports compressed files (.gz and .Z)"
               ""
               "Synopsis: printhead.py [-s <KEYWORD> -H <number> -M <extnum> -S -x <type> -e -h]"
               " fname1 [fname2]..."
               ""
               "If only a file name is given, the primary header of that file is printed."
               ""
               "--extract|-e:   All the headers of the files found are then"
               "                extracted to directories with the same name as the last"
               "                directory found in the path-names of the files. The"
               "                header files will have the same base name as the file, but"
               "                with the extension '.hdr'."
               "--skey|-s:      if the given KEYWORD is found"
               "                in the header only the matching lines will be printed."
               ""
               "--header|-H:    <number> specifies the number of the header to be printed."
               "                If 99 is given, all headers are printed. If <number> is"
               "                negative only the structure of the file is printed."
               ""
               "--xml|-x:       <type> is either 'vo' or 'xf'. All the headers of the files found"
               "                are then extracted to directories with the same name as the last"
               "                directory found in the path-names of the files. The"
               "                header files will have the same base name as the file, but"
               "                with the extension '.xml'. The files use XFits as a format"
               "                if 'xf' is specified and VOTable format if 'vo' is specified"
               "                for <type>."
               "--Struct|-S     Show the structure of the FITS file"
               ""
               "--tsv|-t        Print keywords as a tab-separated-value list. The list contains"
               "                the items: keyword name, keyword value, comment, keyword type, index"
               "                The index item is the running number of the keyword within the header."
               ""
               "--check|-c      If this flag is set the CRC32 checksum of the data part of the"
               "                extensions is calculated."
               "--mode-m        [1]|0: If set to 0 the program does not try to skip the data part"
               "                between headers. This is useful for interpreting header files."
               "--parse|-p      Switch the full parsing of the header on"
               "                extensions is calculated."
               "--help|-h:      print this help and exit."
               ""
               "Version: " + __version__)
        print(msg)


def run(args,skey='END',header=0, mode=1):
        """
        Implements the loop around several files and opens either a
        pipe (compressed files) or the file directly.
        """
        for name in args:
          try:
            pH = FitsHead(name,skey=skey, show=header, struct=struct,check=check, mode=mode)
            if skey != 'END':
                if header == 99:
                    heads = list(range(len(pH.HEAD)))
                else:
                    heads = [header]
                for h in heads:
                    if pH.Extension[h]['index'].values().count(skey) == 0:
                        print('%s\t%3d\t%s\t*not found*' % (name, h, skey))
                    else:
                       print("%s\t%3d\t%s\t%s" % (name, h, skey, pH.Extension[h].getKeyword(skey)[1]))
            else:
                print(pH.HEAD[header])
          except Exception as e:
            print(e)
#            sys.exit('<ERROR> unable to open file:' +name+' <ERROR>')
        return pH

def tsvFunc(args,skey='END',header=0, mode=1):
        """
        Implements the loop around several files and opens either a
        pipe (compressed files) or the file directly.

        INPUT:     string list, file name to process
                   string attribute skey, keyword to parse, default 'END', optional
                   int attribute header, >=0 number of header to return, default 0, optional
        OUTPUT:    tuple, (<FitsHead instance>, <list of tsv formatted lines>)
        """

        lines = []
        for name in args:
          try:
            pH = FitsHead(name, skey=skey, show=header, struct=1, mode=mode)
            tupleList = pH.parseFitsHead2TupleList(forceString=1)
            if header == 99: hrange = list(range(len(tupleList)))
            else: hrange = [header]
            for hind in hrange:
                if skey != 'END':
                    if pH.Extension[hind].getKeyPos(skey) == -1:
                        lines += ['%s\t%s\t*not found*' % (name,skey)]
                    else:
                        ind = pH.Extension[hind].getKeyPos(skey)
                        # print skey, ind, tupleList[ind]
                        lines += ascii_load_lines([tupleList[hind][ind]],'\t','\n')
                else:
                    lines += ascii_load_lines(tupleList[hind],'\t','\n')
          except Exception as e:
              print(e)
              return
        return (pH, lines)

def ascii_load_lines(res, TABsep, RETsep):
    """
    Helper function takes a list of tuples, [(1,2,3,3,),(4,5,6,7)],
    where each tuple represents a record to be loaded, and
    returns a string formated according to the syntax used by the IQ load command.
    """
#    lines = RETsep.join(map(lambda x:TABsep.join(x),res))
    lines = [TABsep.join(x) + RETsep for x in res]

#    lines = ""
#    for row in res:
#       line =""
#       for column in row[:-1]:
#          # columns + Tab separator
#          line = line + str(column) + TABsep
#
#       # Columns + Last column + Enter Separator
#       line = line  + str(row[-1]) + RETsep
#       lines = lines + line
    return(lines)


def hdrExtract(name, xmlfl='', xtract=0, skey='END', show=0, struct=1, check=0, mode=1):
    """
    Extracts headers of all files found by glob(name) into
    header file <file_id>.hdr or <file_id>.xml. The last directory
    in the path defined by <name> is maintained also for the
    header files.
    """
    file_list = glob(name)
    if xmlfl >= 1:
        oext = '.xml'
    else:
        oext = '.hdr'

    if len(file_list) == 0:
        return -1
    for file in file_list:
        (path,base) = os.path.split(file)
        (fileb,ext) = os.path.splitext(base)
        if path:
            #last directory of orig-files will be used to order the
            #extracted headers

            night = os.path.split(path)[1]
        else:
            night = ''

        pH = FitsHead(file,skey = skey, show=show, struct=struct, \
                      check=check, mode=mode)
        pH.fd.close()

        if ext == '.Z' or ext == '.gz':
            (file_id,ext) = os.path.splitext(fileb)
        else:
            file_id = fileb


        if night:
            if not os.path.isdir(night): os.mkdir(night)
            ofnm = night + '/' + file_id + oext
        else:
            ofnm = file_id + oext
        if xtract == 1:
#            print 'extracting header of file ',file,' to ',ofnm
            o = open(ofnm,'w')
            o.write(pH.HEAD[0])
            o.close()
        elif xmlfl != '':
#            print 'extracting header of file ',file,' to ',ofnm
            pH.parseFitsHead()
            XmlHead = pH.xmlHead(format=xmlfl, head=show)

            # if outfile is specified write the XML to it

            if len(ofnm) > 0:
                try:
                    o = open(ofnm,'w')
                except:
                    print("ERROR: Unable to open ", outfile)
                    return 1

                for xml in XmlHead:
                    if type(xml) == type(''):
                        o.write(xml + "\n")
                    elif type(xml) == type([]):
                        o.write('\n'.join(xml))
                o.close()

        else:
            pH.parseFitsHead()

    fh = pH.Extension[0].Serialize()

    return pH



def mergeExtPrimary(file,extnum=1,outf=1,verb=1):
    """
    Merge Extension <extnum> (default 1) with primary header and attach the
    data of extension <extnum> as the primary data part.

    This is only possible if there is no original primary data part (NAXIS = 0)
    and if the data part of the extension is an image (XTENSION = 'IMAGE')
    """

    pk = {'SIMPLE':0,'XTENSION':0,'BITPIX':1,'NAXIS':2,'NAXIS1':3,\
              'NAXIS2':4,'NAXIS3':5,'NAXIS4':6}


    pH = FitsHead(file, struct=1, show=99)
    pH.parseFitsHead()

    if pH.SIZE[0] != 0:
        print('There is a primary data part already! Size: ', pH.SIZE[0], ' bytes')
        print('Bailing out!')
        return pH

    print(len(pH.Extension))
    if pH.Extension[extnum].getKeyword('XTENSION')[1] != 'IMAGE':
        print('The extension is not an IMAGE but ', pH.Extension[extnum].getKeyword('XTENSION')[1])
        print('Bailing out!')
        return pH

    extHead = pH.Extension[extnum]


    maxind = list(pH.Extension[0]['index'].keys())[-1]
#    del(pH.Extension[0]['index'][maxind])   # get rid of the END keyword

    for k in list(extHead['index'].values())[:-1]:

        keyDict = extHead.getKeyDict(k)
        keyDict['index'] = {}
        if k[0:8] != 'XTENSION' and k[:3] != 'END':

            ind = pH.Extension[0].getKeyIndex(k)
            if ind == -1:
                ind = max(pH.Extension[0]['index'].keys())
                if k in pk:
                    ind = pk[k]
            keyDict['index'].update({ind:k})

            print(keyDict)
            pH.Extension[0].updateKeyword(keyDict,force=1)

#            pH.Extension[0]['nodes'].update({k:extHead['nodes'][k]})
#            print {k:extHead['nodes'][k]}
#        elif k[0:8] == 'HIERARCH':
#
#            hind = "['nodes']"
#            hkeys = k.split()
#
#            fullInd = ''
#            for hk in hkeys:
#                fullInd += "['"+hk+"']"
#
#            fitsLine = k
#            fitsLine = fitsLine + (29 - len(fitsLine)) * ' ' + '= '
#            for hk in hkeys:
#                if eval("pH.Extension[0]"+hind+".has_key('"+hk+"')"):
#                    hind = hind + "['" + hk + "']"
#                    if eval("extHead"+hind+".has_key('Value')"):
#                        vale = eval("extHead"+hind+"['Value']")
#                        valo = eval("pH.Extension[0]"+hind+"['Value']")
#                        if vale != valo:
#                            eval("pH.Extension[0]"+hind+".update({'Value':" + vale +"})")
#                            com = eval("extHead"+hind+"['Comment']")
#                            eval("pH.Extension[0]"+hind+".update({'Comment':'" + com +"'})")
#                        maxind = pH.Extension[0]['index'].keys()[-1]
#                        pH.Extension[0]['index'].update({maxind+1:k})
#                else:
#                    val = eval("extHead"+hind + "['" + hk + "']")
#                    eval("pH.Extension[0]"+hind+".update({hk:val})")
#                    del(val)
#                    hind = hind + "['" + hk + "']"

    maxind = list(pH.Extension[0]['index'].keys())[-1]
    pH.Extension[0]['index'].update({maxind+1:'END'})


    if outf != 0:
        (path,base) = os.path.split(file)
        (fileb,ext) = os.path.splitext(base)
        outf = fileb + ".new" + ext
        outFd = open(outf,'w')


    for dd in range(len(pH.Extension)):
        if dd != extnum:      # extnum header and data are with primary
                              # but keep other extensions.
            if (verb):print("Extracting FITS header for extension number ",dd)
            if dd == 0:
                pH.Extension[dd].sortKeys()
                datapart = extnum
            else:
                datapart = dd
            FitsHd = pH.Extension[dd].Serialize()


# if outfile is specified write the FHead to it

        if outf != 0:
#            print "Header cards:",len(FHead)
            totLen = 0
            for fitsLine in FitsHd:
                totLen += len(fitsLine)
                if type(fitsLine) == type(''):
                    outFd.write(fitsLine)
                elif type(fitsLine) == type([]):
                    for x in fitsLine:
                        outFd.write(x)

#           append the binary part to the header
            if (verb):print("Extracting datapart number ", datapart)
            pH.fd.seek(pH.POS[datapart][0],0)
            dataBlocks = int(ceil(pH.SIZE[datapart]/2880.))
            data = ''
            for d in range(dataBlocks):
                data = pH.fd.read(2880)
                outFd.write(data)
            del(data)

    if outf !=0: outFd.close()


    del(FitsHd)
    return pH


if __name__ == '__main__':

        import getopt

        args = sys.argv[1:]
        opts,args = getopt.getopt(args,"s:H:x:M:m:peSctqh",\
                   ["parse","extract","skey=","header=","xml=","struct","merge=",\
                    "mode=","check","tsv","quiet","help"])

        _VERBOSE_ = 1

        xtract = 0
        parse = 0
        xmlfl = ''
        skeyfl = 0
        skey = 'END'
        show = -1
        struct = 0
        tsv = 0
        check = 0
        mergefl = 0
        hfl = 0
        breakfl = 0
        mode = 1

        while True:
            if len(args) == 0:
                usage()
                break
    #            sys.exit()
            try:

                for o,v in opts:
                    if o in ("-e","--extract"):
                        xtract = 1
                    if o in ("-p","--parse"):
                        parse = 1
                    if o in ("-s","--skey"):
                        skey = v.strip()
                        skeyfl = 1
                    if o in ("-t","--tsv"):
                        if hfl == 0: show = 99
                        struct = 1
                        tsv = 1
                    if o in ("-H","--header"):
                        hfl = 1
                        show = int(v)
                        struct = 1
                    if o in ("-x","--xml"):
                        if v[:2].lower() =='xf':
                            xmlfl = 'xfits'
                        elif v.lower() == 'vo':
                            xmlfl = 'vo'
                    if o in ("-S","--Struct"):
                        show = -99
                        struct = 1
                    if o in ("-m", "--mode"):
                        mode = int(v)
                    if o in ("-M","--merge"):
                        mergefl = 1
                        show = -1
                        struct = 1
                        breakfl = 1
                        print(">>> Careful this does not work correctly!!!!")
                        for f in args:
                            pH = mergeExtPrimary(f,extnum=int(v),verb=1)
                    if o in ("-q","--quiet"):
                        usage()
                        breakfl = 1
                    if o in ("-c","--check"):
                        # makes only sense with showing the structure.
                        show = -99
                        struct = 1
                        check = 1
                    if o in ("-h","--help"):
                        usage()
                        breakfl = 1
            except Exception as e:
                errMsg = "Problem parsing command line options: %s" % str(e)
                print(errMsg)
                break
            try:
                if tsv == 1:
                    head = int(show)
                    if head < 0: head = 0
                    (pH, lines) = tsvFunc(args, skey=skey, header = head, mode=mode)
                    for l in lines:
                        print(l[:-1])  # don't print the \n

                elif xtract == 1:
                    if xmlfl != '':
                        xtract = 0
                    for f in args:
                        pH = hdrExtract(f,xmlfl=xmlfl, show=show, xtract=xtract, mode = mode)
                elif skeyfl == 1:
                    for f in args:
                        head = int(show)
                        if head < 0: head = 0
                        pH = run([f],skey=skey,header=head, mode=mode)
                elif xmlfl != '':
                    struct = 1
                    for f in args:
                        pH = FitsHead(f,skey = skey, show=show, struct=struct, \
                                      check=check, mode=mode)
                        pH.fd.close()
                        pH.parseFitsHead()
                        XmlHead = pH.xmlHead(format=xmlfl, head=show)
                        for xml in XmlHead:
                            if type(xml) == type(''):
                                print(xml + "\n")
                            elif type(xml) == type([]):
                                print('\n'.join(xml))

                elif struct > 0:
                    if mergefl == 0:
                        for f in args:
                            pH = FitsHead(f, struct=struct, check=check, verbose=0, \
                                           show=show,mode=mode)
                            if show == -99:
                                output = '\n'.join(pH.STRUCT)
                            elif show == 99:
                                output = ''.join(pH.HEAD)
                            elif show >= 0 and show <= len(pH.HEAD):
                                output = pH.HEAD[show]
                            else:
                                output = "Invalid header number specified. Should be: [0-%d,99]" % \
                                (len(pH.HEAD)-1)
                            print(output)
                elif breakfl == 1:
                    break
                else:
                   pH = run(args)
                break
            except Exception as e:
               errMsg = "Problem extracting headers: %s" % str(e)
               print(errMsg)
               break
