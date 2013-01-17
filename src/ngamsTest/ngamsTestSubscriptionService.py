"""
Should cover several cases

Case 1. ngas-B subscribes with ngas-A. Then multiple clients simultaneously archive files to ngas-A at the same time interval.
Case 2. a client archives several files to ngas-A, then ngas-B subscribes with A from the past. After a while, ngas-C subscribe with A from the past.
"""
import time, os, commands, threading, thread
import ngamsPClient

from pybarrier import *

ngasA_host = '180.149.251.189'
ngasA_port = 7785

ngasB_host = '180.149.251.189'
ngasB_port = 7786
ngasB_url = 'http://%s:%d/QARCHIVE' % (ngasB_host, ngasB_port)

ngasC_host = '180.149.251.189'
ngasC_port = 7787
ngasC_url = 'http://%s:%d/QARCHIVE' % (ngasC_host, ngasC_port)

tmpDir = '/mnt/r5/test/testNGASSub'
mime_type = 'application/octet-stream'

file_ext = '.data'

clientA = ngamsPClient.ngamsPClient(ngasA_host, ngasA_port)
clientB = ngamsPClient.ngamsPClient(ngasB_host, ngasB_port)
clientC = ngamsPClient.ngamsPClient(ngasC_host, ngasC_port)

class WaitTimeout(Exception):
    "Exception due to wait timeout."
    pass

def createTmpFiles(number = 10):    
    if (not os.path.exists(tmpDir)):
        os.makedirs(tmpDir)
    base_name = str(time.mktime(time.gmtime())).split('.')[0]
    for num in range(number):
        fname = base_name + '-' + str(num)
        #cmd = 'dd if=/dev/zero of=%s/%s%s bs=65536 count=1600' % (tmpDir, fname, file_ext) #100MB per file
        cmd = 'dd if=/dev/zero of=%s/%s%s bs=65536 count=320' % (tmpDir, fname, file_ext) #20 MB perfile
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
        
def waitUntilFileDelivered(file_id, pclient, timeout = 0, wait_interval = 1):
    howlong = 0
    while (1):
        if (timeout > 0 and howlong > timeout):
            raise WaitTimeout
        stat = pclient.sendCmd('STATUS', pars=[['file_id', file_id]])
        msg = stat.getMessage()
        if (msg == 'Successfully handled command STATUS'):
            return howlong
        time.sleep(wait_interval)
        howlong += wait_interval

def _archiveThread(pclient, fileUriList, interval, clientIdx, my_bar):
    for fileUri in fileUriList:
        my_bar.enter()
        print 'Archiving file %s' % fileUri 
        stat = pclient.pushFile(fileUri, mime_type, cmd = 'QARCHIVE')
        msg = stat.getMessage().split()[0]
        if (msg != 'Successfully'):
            raise Exception('Fail to archive \"%s\"' % fileUri)
        time.sleep(interval)
    
    print 'Thread [%d] exits' % clientIdx
    thread.exit()

def _waitUntilThreadsExit(threads):
    num = len(threads)
    if num < 1:
        return
    while (num > 0): 
        for thread in threads:
            if (not thread.isAlive()):
                num -= 1
        time.sleep(1)

def _unSubscribe(pclient, url):
    stat = pclient.sendCmd('UNSUBSCRIBE', pars = [["url", url]])
    msg = stat.getMessage()
    if (msg != 'Handled UNSUBSCRIBE command'):
        print 'Error when removing the existing subscriber: \"%s\"' % url

def TestCase01(num_file_per_client, num_clients, interval = 4, base_name = None):
    if (num_file_per_client < 1):
        raise Exception("each client at least archives one file!")
    
    stat = clientA.sendCmd('SUBSCRIBE', pars=[['url', ngasB_url], ['concurrent_threads', '2']])
    msg = stat.getMessage()
    if (msg != 'Handled SUBSCRIBE command'):
        raise Exception('Fail to subscribe using \"%s\", error msg = %s' % (ngasB_url, msg))
    
    stat = clientA.sendCmd('SUBSCRIBE', pars=[['url', ngasC_url], ['concurrent_threads', '2']])
    msg = stat.getMessage()
    if (msg != 'Handled SUBSCRIBE command'):
        raise Exception('Fail to subscribe using \"%s\", error msg = %s' % (ngasC_url, msg))
    
    
    num_files = num_file_per_client * num_clients
    if (base_name == None):
        print 'Creating %d dummy files ...' % num_files
        base_name = createTmpFiles(num_files)
    
    file_list = []
    for client in range(num_clients):
        file_list.append([])
        
    for num in range(num_files):
        fileUri = '%s/%s-%s%s' % (tmpDir, base_name, str(num), file_ext)
        file_list[num % num_clients].append(fileUri)
        
    deliveryThreads = []
    my_barrier = barrier(num_clients)
    for clientIdx in range(num_clients):
        args = (clientA, file_list[clientIdx], interval, clientIdx, my_barrier)
        deliveryThrRef = threading.Thread(None, _archiveThread, 'ArchiveThrd' + str(clientIdx), args)
        deliveryThrRef.setDaemon(0)
        deliveryThrRef.start()
        deliveryThreads.append(deliveryThrRef)
    
    print 'Wait until all archive threads are done'
    _waitUntilThreadsExit(deliveryThreads)
       
    print 'Wait %d seconds so that all files are delivered to all subscribers' % (num_files * 3)
    time.sleep(num_files * 3) # assume each file needs 3 seconds to delivery (in parallel)
    
    verifyCase(base_name, num_files, clientB, False)
    verifyCase(base_name, num_files, clientC, True)
    
    _unSubscribe(clientA, ngasB_url)
    _unSubscribe(clientA, ngasC_url)
    
    

def TestCase02(num_files, base_name = None):
    # create tmp files to be quick archived
    need_archive = False
    if (base_name == None):
        base_name = createTmpFiles(num_files)
        need_archive = True      

    
    
    # quick archive each tmp file
    if (need_archive):
        for num in range(num_files):
            fileUri = '%s/%s-%s%s' % (tmpDir, base_name, str(num), file_ext)
            print 'Archiving file %s' % fileUri 
            stat = clientA.pushFile(fileUri, mime_type, cmd = 'QARCHIVE')
            msg = stat.getMessage().split()[0]
            if (msg != 'Successfully'):
                raise Exception('Fail to archive \"%s\"' % fileUri)
    
    #get the first file's ingestion_date
    fname = '%s-0%s' % (base_name, file_ext)
    stat = clientA.sendCmd('STATUS', pars=[['file_id', fname]]) 
    msg = stat.getMessage()
    
    # make sure the file exists first!
    if (msg != 'Successfully handled command STATUS'):
        raise Exception('The first file \"%s\" is not even properly archived!' % fname)
    
    doc = stat.dumpBuf()
    # this is highly fragile, but show work for now
    ingestion_date = doc.split('\n')[36].split('                      ')[1]        
    stat = clientA.sendCmd('SUBSCRIBE', pars=[['url', ngasB_url], ['start_date', ingestion_date], ['concurrent_threads', '2']])
    msg = stat.getMessage()
    if (msg != 'Handled SUBSCRIBE command'):
        raise Exception('Fail to subscribe using \"%s\", error msg = %s' % (ngasB_url, msg))
    
    try:
        howlong = waitUntilFileDelivered(fname, clientB, timeout = 30)
    except WaitTimeout as e:
        print 'Timeout waiting for the first file \"%s\" to be delivered to \"%s\"' % (fname, ngasB_url)
        raise e
    else:
        print 'After %d seconds, the first file \"%s\" is delivered to \"%s\"' % (howlong, fname, ngasB_url)
    
    stat = clientA.sendCmd('SUBSCRIBE', pars=[['url', ngasC_url], ['start_date', ingestion_date], ['concurrent_threads', '2']])
    msg = stat.getMessage()
    if (msg != 'Handled SUBSCRIBE command'):
        raise Exception('Fail to subscribe using \"%s\"' % ngasC_url)
    
    try:
        howlong = waitUntilFileDelivered(fname, clientC, timeout = 30)
    except WaitTimeout as e:
        print 'Timeout waiting for the first file \"%s\" to be delivered to \"%s\"' % (fname, ngasC_url)
        raise e
    else:
        print 'After %d seconds, the first file \"%s\" is delivered to \"%s\"' % (howlong, fname, ngasC_url)
    
    lastfname = '%s-%d%s' % (base_name, num_files - 1, file_ext) # this might not be the last file for multi-threaded delivery, but approximately for now
    
    print("Wait for the last file to be delivered to %s" % (ngasB_url))       
    howlong = waitUntilFileDelivered(lastfname, clientB)
    print 'After %d seconds, the last file \"%s\" is delivered to \"%s\"' % (howlong, lastfname, ngasB_url)
    
    print("Wait for the last file to be delivered to %s" % (ngasC_url))       
    howlong = waitUntilFileDelivered(lastfname, clientC)
    print 'After %d seconds, the last file \"%s\" is delivered to \"%s\"' % (howlong, lastfname, ngasC_url)
    
    time.sleep(3)
    
    verifyCase(base_name, num_files, clientB, False)
    verifyCase(base_name, num_files, clientC, True)
    
    _unSubscribe(clientA, ngasB_url)
    _unSubscribe(clientA, ngasC_url)

def verifyCase(base_name, num_files, pclient, clean_on_complete = True):
    """
    """
    print ('\n\t\t\t *** Delivery report *** ')
    print '\nSummary on server: %s:%d' % (pclient.getHost(), pclient.getPort())
    good_list = []
    bad_list = []
    for num in range(num_files):
        file_id = '%s-%d%s' % (base_name, num, file_ext)
        stat = pclient.sendCmd('STATUS', pars=[['file_id', file_id]])
        msg = stat.getMessage().split()[0]
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
            print '%s\t\t%s' % (li[0], li[1])
    
    if (len(bad_list) > 0):
        print '\n******  WARNING *******\n'
        print 'failed to deliver files'
        print '-----------------------'
        for fname in bad_list:
            print fname
    
    if (clean_on_complete):
        #clean the tmp file in the end
        removeTmpFiles(base_name)
        
if __name__ == '__main__':
    #TestCase02(16, base_name = '1358203679')
    #TestCase02(16)
    TestCase01(50, 6, interval = 3)
