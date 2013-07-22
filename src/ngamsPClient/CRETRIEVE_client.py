import urllib2
import email
import mimetypes
import sys, os
from optparse import OptionParser
import cStringIO
import errno

parser = OptionParser(usage="""\
Retrieve a container as a directory of files.

At least container_id or container_name must be specified.

Usage: %prog [options]
""")
parser.add_option('-i', '--container_id',
                  type='string', action='store',
                  help="""container_id of the container to retrieve.""")
parser.add_option('-n', '--container_name',
                  type='string', action='store',
                  help="""container_name of the container to retrieve.""")
parser.add_option('-d', '--directory',
                  type='string', action='store',
                  help="""Unpack the MIME message into the named
                  directory (must exist).""")
opts, args = parser.parse_args()
if not (opts.container_id or opts.container_name):
    parser.print_help()
    sys.exit(1)
contId = opts.container_id
contName = opts.container_name
URLstring = 'http://192.168.204.130:7777/CRETRIEVE?'
saveDir = opts.directory


if contId:
    URLstring += 'container_id=' + contId
    if contName:
        URLstring += '&'
if contName:
    contName = contName.replace(" ", '%20')
    URLstring += 'container_name=' + contName


fr = urllib2.urlopen(URLstring + '&reload=1')

#Get deliminater and remove remaining header
fr.readline()
tempStr = fr.readline()
deliminater = tempStr[-29:-2]
EOF = '--' + deliminater
EOC = EOF + '--'
fr.readline()
fr.readline()

#Begin reading the content and store to respective files
dataStr = fr.read(2**16)
newFile = True
while dataStr != '':
    if newFile:
        start = dataStr.find('filename="') + 10
        end = dataStr.find('"\r\n\n', start)
        filename = dataStr[start:end]
        if saveDir[-1] != '/': saveDir += '/'
        if saveDir: filename = saveDir + filename
        directory = filename.rsplit('/', 1)[0]
        try:
            os.mkdir(directory)
        except OSError as e:
            # Ignore directory exists error
            if e.errno != errno.EEXIST:
                raise
        fw = open(filename, 'wb')
        newFile = False
        dataStr = dataStr[end+4:]
    dataList = dataStr.split(EOF, 1)
    lastChars = ''
    if dataList[0] != '':
        lastChars = dataList[0][-30:]
        fw.write(dataList[0][:-30])
    if len(dataList) > 1:
        fw.write(lastChars)
        fw.close()
        dataStr = dataList[1]
        newFile = True
    else:
        dataStr = fr.read(2**16)
        dataStr = lastChars + dataStr
    if dataStr[:2] == '--':
        dataStr = ''
fr.close()
