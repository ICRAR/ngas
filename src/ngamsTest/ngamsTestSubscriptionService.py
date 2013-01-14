"""
Should cover several cases

Case 1. ngas-B subscribes with ngas-A. Then multiple clients simultaneously archive files to ngas-A at the same time interval.
Case 2. a client archives several files to ngas-A, then ngas-B subscribes with A. After a while, ngas-C subscribe with A.
Case 3. a client archives several files to ngas-A, then ngas-B subscribes with A from the past. After a while, ngas-C subscribe with A from the past.
"""
import urllib, time, os, commands
import ngamsPClient

ngasA_host = '180.149.251.189'
ngasA_port = 7785

ngasB_host = '180.149.251.189'
ngasB_port = 7786
ngasB_url = 'http://%s:%d' % (ngasB_host, ngasB_port)

ngasC_host = '180.149.251.189'
ngasC_port = 7787
ngasC_url = 'http://%s:%d' % (ngasC_host, ngasC_port)

tmpDir = '/tmp/testNGASSub'
mime_type = 'application/octet-stream'

class WaitTimeout(Exception):
    "Exception due to wait timeout."
    pass

def createTmpFiles(number = 10):    
    if (not os.path.exists(tmpDir)):
        os.makedirs(tmpDir)
    base_name = str(time.mktime(time.gmtime())).split('.')[0]
    for num in range(number):
        fname = base_name + '-' + str(num)
        cmd = 'dd if=/dev/zero of=%s/%s bs=65536 count=100MB' % (tmpDir, fname)
        status, output = commands.getstatusoutput(cmd)
        if (status):
            print output
    return base_name
        
def removeAllTmpFiles():
    if (os.path.exists(tmpDir)):
        cmd = 'rm -rf %s' % tmpDir
        status, output = commands.getstatusoutput(cmd)
        if (status):
            print output

def removeTmpFiles(base_name):
    if (not os.path.exists(tmpDir)):
        return -1
    cmd = 'rm %s/%s-*' % (tmpDir, base_name)
    status, output = commands.getstatusoutput(cmd)
    if (status):
        print output
        
def waitUntilFileDelivered(file_id, pclient, timeout = 0):
    howlong = 0
    wait_interval = 1
    while (1):
        if (timeout > 0 and howlong > timeout):
            raise WaitTimeout
        stat = pclient.sendCmd('STATUS', pars=[['file_id', file_id]])
        msg = stat.getMessage()
        if (msg == 'Successfully handled command STATUS'):
            return howlong
        time.sleep(wait_interval)
        howlong += wait_interval

def TestCase02(num_files):
    # create tmp files to be quick archived
    base_name = createTmpFiles(num_files)
    clientA = ngamsPClient.ngamsPClient(ngasA_host, ngasA_port)
    clientB = ngamsPClient.ngamsPClient(ngasB_host, ngasB_port)
    clientC = ngamsPClient.ngamsPClient(ngasC_host, ngasC_port)

    stat = clientA.unsubscribe(ngasB_url)
    msg = stat.getMessage.split()[0]
    if (msg == 'Successfully'):
        print 'Removed the existing subscriber: \"%s\"' % ngasB_url
    
    stat = clientA.unsubscribe(ngasC_url)
    msg = stat.getMessage.split()[0]
    if (msg == 'Successfully'):
        print 'Removed the existing subscriber: \"%s\"' % ngasC_url
        
    # quick archive each tmp file
    for num in range(num_files):
        fileUri = '%s/%s-%s', (tmpDir, base_name, str(num))
        print 'Archiving file %s' % fileUri 
        stat = clientA.pushFile(fileUri, mime_type, cmd = 'QARCHIVE')
        msg = stat.getMessage.split()[0]
        if (msg != 'Successfully'):
            raise Exception('Fail to archive \"%s\"' % fileUri)
    
    #get the first file's ingestion_date
    fname = '%s-0' % base_name
    stat = clientA.sendCmd('STATUS', pars=[['file_id', fname]]) 
    msg = stat.getMessage()
    
    # make sure the file exists first!
    if (msg != 'Successfully handled command STATUS'):
        raise Exception('The first file \"%\" is not even properly archived!' % fname)
    
    doc = stat.dumpBuf()
    # this is highly fragile, but show work for now
    ingestion_date = doc.split('\n')[36].split('                      ')[1]        
    stat = clientA.sendCmd('SUBSCRIBE', pars=[['url', ngasB_url], ['start_date', ingestion_date], ['concurrent_threads', '2']])
    msg = stat.getMessage.split()[0]
    if (msg != 'Successfully'):
        raise Exception('Fail to subscribe using \"%s\"' % ngasB_url)
    
    try:
        howlong = waitUntilFileDelivered(fname, clientB, timeout = 30)
    except WaitTimeout as e:
        print 'Timeout waiting for the first file \"%s\" to be delivered to \"%s\"' % (fname, ngasB_url)
        raise e
    else:
        print 'After %d seconds, the first file \"%s\" is delivered to \"%s\"' % (howlong, fname, ngasB_url)
    
    stat = clientA.sendCmd('SUBSCRIBE', pars=[['url', ngasC_url], ['start_date', ingestion_date], ['concurrent_threads', '2']])
    msg = stat.getMessage.split()[0]
    if (msg != 'Successfully'):
        raise Exception('Fail to subscribe using \"%s\"' % ngasC_url)
    
    try:
        howlong = waitUntilFileDelivered(fname, clientC, timeout = 30)
    except WaitTimeout as e:
        print 'Timeout waiting for the first file \"%s\" to be delivered to \"%s\"' % (fname, ngasC_url)
        raise e
    else:
        print 'After %d seconds, the first file \"%s\" is delivered to \"%s\"' % (howlong, fname, ngasC_url)
    
    lastfname = '%s-%d' % (base_name, num_files - 1) # this might not be the last file for multi-threaded delivery, but approximately for now
    
    print("Wait for the last file to be delivered to %s" % (ngasB_url))       
    howlong = waitUntilFileDelivered(lastfname, clientB)
    print 'After %d seconds, the last file \"%s\" is delivered to \"%s\"' % (howlong, lastfname, ngasB_url)
    
    print("Wait for the last file to be delivered to %s" % (ngasC_url))       
    howlong = waitUntilFileDelivered(lastfname, clientC)
    print 'After %d seconds, the last file \"%s\" is delivered to \"%s\"' % (howlong, lastfname, ngasC_url)
    
    time.sleep(3)
    
    verifyCase(base_name, num_files, clientB, False)
    verifyCase(base_name, num_files, clientC, True)

def verifyCase(base_name, num_files, pclient, clean_on_complete = True):
    """
    """
    good_list = []
    bad_list = []
    for num in range(num_files):
        file_id = '%s-%d' % (base_name, num)
        stat = pclient.sendCmd('STATUS', pars=[['file_id', file_id]])
        msg = stat.getMessage.split()[0]
        if (msg != 'Successfully'):
            bad_list.append(file_id)
        else:
            doc = stat.dumpBuf()
            # this is highly fragile, but show work for now
            ingestion_date = doc.split('\n')[36].split('                      ')[1]
            li = [file_id, ingestion_date]
            good_list.append(li)
    
    print 'delivered_file\t\t\tingestion_date'
    print '--------------\t\t\t--------------'
    if (len(good_list) < 1):
        print 'None\t\t\t\tNone'
    else:
        for li in good_list:
            print '%s\t\t\t%s' % (li[0], li[1])
    
    if (len(bad_list) > 0):
        print '******  WARNING *******'
        print 'failed to deliver files'
        print '-----------------------'
        for fname in bad_list:
            print fname
    
    if (clean_on_complete):
        #clean the tmp file in the end
        removeTmpFiles(base_name)
