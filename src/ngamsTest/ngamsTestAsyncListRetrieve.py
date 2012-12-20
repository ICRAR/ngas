#******************************************************************************
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      14/12/2012  Created
#

import urllib, time
from ngamsMWAAsyncProtocol import *
import cPickle as pickle


svrUrl = 'http://localhost:7778/ASYNCLISTRETRIEVE'
file_id = ['8881_20120914160104_32.fits', '23_20120914165048_71.fits', '110026_20120914130904_12.fits', '25_20120914165052_71.fits']
cancel_file_id = ['8881_20120914160104_32.fits', '23_20120914165048_71.fits', '110026_20120914130904_12.fits', '25_20120914165052_71.fits', '21_20120914165044_71.fits', \
                  '8880_20120914160102_32.fits', '8875_20120914160053_32.fits', '110023_20120914130857_12.fits', '23_20120914165048_71.fits', '110026_20120914130904_12.fits']
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

def testCancelRetrieval():
    myReq = AsyncListRetrieveRequest(cancel_file_id, pushUrl)
    sessionId = myReq.session_uuid
    
    strReq = pickle.dumps(myReq)
    print("Sending retrievel request")
    strRes = urllib.urlopen(svrUrl, strReq).read()
    myRes = pickle.loads(strRes)
    print("Retrieval response received")
    respSessionId = myRes.session_uuid
    if (sessionId != respSessionId):
        print "session id is corrupted"
        return
    
    print("Sleep for 2 second.")
    time.sleep(2.0)
    
    print("Sending cancel request.")
    cancelUrl = svrUrl + "?uuid=" + sessionId + "&cmd=cancel"
    strRes = urllib.urlopen(cancelUrl).read()
    myRes = pickle.loads(strRes)
    
    print("Cancelled session_uuid = %s" % myRes.session_uuid)
    print("Cancel errorcode = %d" % myRes.errorcode)       

    #now cancel the same session again, it should return invalid session id
    print("Sending the same cancel request again ...")
    strRes = urllib.urlopen(cancelUrl).read()
    myRes = pickle.loads(strRes)
    
    print("Cancel errorcode = %d" % myRes.errorcode)
    
    if (myRes.errorcode == AsyncListRetrieveProtocolError.INVALID_UUID):
        print "Yes, correct."
    else:
        print "Wrong, test failed. Session is still there, not cancelled!"
    #print("Cancelled session_uuid = %s" % myRes.session_uuid)

def testSuspendThenResume():
    myReq = AsyncListRetrieveRequest(cancel_file_id, pushUrl)
    sessionId = myReq.session_uuid
    
    strReq = pickle.dumps(myReq)
    print("Sending retrievel request")
    strRes = urllib.urlopen(svrUrl, strReq).read()
    myRes = pickle.loads(strRes)
    print("Retrieval response received")
    respSessionId = myRes.session_uuid
    if (sessionId != respSessionId):
        print "session id is corrupted"
        return
    
    print("Sleep for 2 second.")
    time.sleep(2.0)
    
    print("Sending suspend request.")
    suspendUrl = svrUrl + "?uuid=" + sessionId + "&cmd=suspend"
    strRes = urllib.urlopen(suspendUrl).read()
    myRes = pickle.loads(strRes)
    
    print("Suspended session_uuid = %s" % myRes.session_uuid)
    print("Suspend errorcode = %d" % myRes.errorcode)
    print("Next file to be delivered = %s" % myRes.current_fileid)
    
    # sleep for a while before resume
    print("Suspend for about 15 seconds ....")
    time.sleep(15.0)
    
    #now resume the same session again
    print("Resuming file delivery for session '%s' " % sessionId)
    resumeUrl = svrUrl + "?uuid=" + sessionId + "&cmd=resume"
    strRes = urllib.urlopen(resumeUrl).read()
    myRes = pickle.loads(strRes)
    
    print("Resumed session_uuid = %s" % myRes.session_uuid)
    print("Resume errorcode = %d" % myRes.errorcode)
    print("Next file to be delivered = %s" % myRes.current_fileid)

def testStatus():
    myReq = AsyncListRetrieveRequest(cancel_file_id, pushUrl)
    sessionId = myReq.session_uuid
    
    strReq = pickle.dumps(myReq)
    print("Sending retrievel request")
    strRes = urllib.urlopen(svrUrl, strReq).read()
    myRes = pickle.loads(strRes)
    print("Retrieval response received")
    respSessionId = myRes.session_uuid
    if (sessionId != respSessionId):
        print "session id is corrupted"
        return
    
    statusUrl = svrUrl + "?uuid=" + sessionId + "&cmd=status"
    for x in range(0, 3):
        print("\nSleep for 2 seconds.")
        time.sleep(2.0)
        print("Sending get status request for the %d time(s)" % (x + 1))        
        strRes = urllib.urlopen(statusUrl).read()
        myRes = pickle.loads(strRes)
        print("\tstatus errorcode      = %d" % myRes.errorcode)
        print("\tfiles delivered       = %d" % myRes.number_files_delivered)
        print("\tbytes delivered       = %d" % myRes.number_bytes_delivered)
        print("\tfiles to be delivered = %d" % myRes.number_files_to_be_delivered)
        print("\tbytes to be delivered = %d" % myRes.number_bytes_to_be_delivered)

def testSuspendThenStatusThenResume():
    myReq = AsyncListRetrieveRequest(cancel_file_id, pushUrl)
    sessionId = myReq.session_uuid
    
    strReq = pickle.dumps(myReq)
    print("Sending retrievel request")
    strRes = urllib.urlopen(svrUrl, strReq).read()
    myRes = pickle.loads(strRes)
    print("Retrieval response received")
    respSessionId = myRes.session_uuid
    if (sessionId != respSessionId):
        print "session id is corrupted"
        return
    
    print("Sleep for 5 seconds.")
    time.sleep(5.0)
    
    print("Sending suspend request.")
    suspendUrl = svrUrl + "?uuid=" + sessionId + "&cmd=suspend"
    strRes = urllib.urlopen(suspendUrl).read()
    myRes = pickle.loads(strRes)
    
    print("Suspended session_uuid = %s" % myRes.session_uuid)
    print("Suspend errorcode = %d" % myRes.errorcode)
    print("Next file to be delivered = %s" % myRes.current_fileid)
    
    statusUrl = svrUrl + "?uuid=" + sessionId + "&cmd=status"
    print("Sending get status request")
    strRes = urllib.urlopen(statusUrl).read()
    myRes = pickle.loads(strRes)
    print("\tstatus errorcode      = %d" % myRes.errorcode)
    print("\tfiles delivered       = %d" % myRes.number_files_delivered)
    print("\tbytes delivered       = %d" % myRes.number_bytes_delivered)
    print("\tfiles to be delivered = %d" % myRes.number_files_to_be_delivered)
    print("\tbytes to be delivered = %d" % myRes.number_bytes_to_be_delivered)
    
     # sleep for a while before resume
    print("Suspend for about 5 seconds ....")
    time.sleep(5.0)
    
    #now resume the same session again
    print("Resuming file delivery for session '%s' " % sessionId)
    resumeUrl = svrUrl + "?uuid=" + sessionId + "&cmd=resume"
    strRes = urllib.urlopen(resumeUrl).read()
    myRes = pickle.loads(strRes)
    
    print("Resumed session_uuid = %s" % myRes.session_uuid)
    print("Resume errorcode = %d" % myRes.errorcode)
    print("Next file to be delivered = %s" % myRes.current_fileid)
    
    

if __name__ == '__main__':
    #testStartRetrieve()
    #testCancelRetrieval()
    #testSuspendThenResume()
    #testStatus()
    testSuspendThenStatusThenResume()