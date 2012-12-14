#******************************************************************************
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      14/12/2012  Created
#

import urllib
from ngamsMWAAsyncProtocol import *
import cPickle as pickle


svrUrl = 'http://localhost:7778/ASYNCLISTRETRIEVE'
file_id = ['8881_20120914160104_32.fits', '23_20120914165048_71.fits', '110026_20120914130904_12.fits', '25_20120914165052_71.fits']
pushUrl = 'http://localhost:7777/QARCHIVE'

def testStartRetrieve():
    myReq = AsyncListRetrieveRequest(file_id, pushUrl)
    strReq = pickle.dumps(myReq)
    strRes = urllib.urlopen(svrUrl, strReq).read()
    myRes = pickle.loads(strRes)
    print("session_uuid = %s" % myRes.session_uuid)
    print("errorcode = %d" % myRes.errorcode)
    print("file_info length = %d" % len(myRes.file_info))
    for fileinfo in myRes.file_info:
        print("\tfile id: %s" % fileinfo.file_id)
        print("\tfile size: %d" % fileinfo.filesize)
        print("\tfile status: %d" % fileinfo.status)


if __name__ == '__main__':
    testStartRetrieve()