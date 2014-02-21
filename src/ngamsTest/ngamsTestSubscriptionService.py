"""
Should cover several cases

Case 1. Both ngas-B and ngas-C subscribe with ngas-A. Then multiple clients simultaneously archive files to ngas-A at the same time interval.

Case 2. A client archives several files to ngas-A, then ngas-B subscribes with A from the past. After a while, ngas-C subscribe with A from the past.

Case 3, A client subscribes with concurrent_threads = 2. Restart the NGAS server, the client then archive files to this NGAS server.

Case 4. Running ngas-A in the cache mode. Both ngas-B and ngas-C subscribe with ngas-A. But ngas-C is shut down. 
        Then multiple clients simultaneously archive files to ngas-A at the same time interval. Observe how many files left in A's ngas_files table
        Then start ngas-C but unsubscribe C soon after ngas-C is restarted. Observe how many files left in A's ngas_files table
        
Case 5. Running ngas-A in the cache mode. Both ngas-B and ngas-C subscribe with ngas-A. But ngas-C is shut down. 
        Then multiple clients simultaneously archive files to ngas-A at the same time interval. Observe how many file left in A's ngas_files table
        Then start ngas-C. Wait until all ngas-B got all the files, then unsubscribe C. Observe how many files left in A's ngas_files table
        
Case 6. Running ngas-A in the cache mode. ngas-B subscribes with ngas-A. Then multiple clients simultaneously archive files to ngas-A. During this time, 
        A is shut down manually. Then A is restarted again. After while, verify how many files get delivered to ngas-B, and how many files left in ngas-A (should be zero)
        
Case 7. Running ngas-A in the cache mode. ngas-B subscribes with ngas-A. Then multiple clients simultaneously archive files to ngas-A. During this time,
        B is first shut down manually. Then after while,  ngas-A is shut down manually. Restart ngas-B, and then restart ngas-A, which should resume delivering
        In the end, observe how many files get delivered to ngas-B, and how many files are left in ngas-A

Case 8. ngas-B subscribes with ngas-A. Then multiple clients simultaneously archive files to ngas-A. After delivery starts, suspend the delivery
        Issue usubscribe command to change the url of the subscription so that files will now be delivered to ngas-C.  Resume the delivery, observe how many
        files are delivered to ngas-C.
        
Case 9. ngas-B subscribes with ngas-A. Then multiple clients simultaneously archive files to ngas-A. After delivery starts, shut down ngas-B. When all failed deliveries
        are back logged, issue usubscribe command to change the url of the subscription so that files will now be delivered to ngas-C. now trigger subscription so that
        back logged files should be delivered to ngas-C. Finally, verify how many files get to ngas-C.

Case 10. ngas-B subscribes with ngas-A. Then multiple clients simultaneously archive files to ngas-A. After delivery starts, issue usubscribe command to change the
        concurrent_threads to 6, after a while, change the concurrent_threads to 1. In the end, verify how many files get to ngas-B by the original single thread, and 
        how many by one of the 6 threads
        
Case 11. ngas-B subscribes with ngas-A. Then multiple clients simultaneously archive files to ngas-A. After delivery starts, issue usubscribe command to change the
        priority to 10, after a while, change the priority back to 1. In the end, verify how many files get to ngas-B, and verify throughtput difference under different 
        priorities.

Case 12. Running ngas-A in the standard mode. A will randomly choose B or C to deliver file

Case 13  Testing the proxy archive
"""
import time, os, commands, threading, thread, base64
import ngamsPClient

from pybarrier import *

ngasA_host = '127.0.0.1'
ngasA_port = 7777

#ngasB_host = '127.0.0.1'
ngasB_host = 'eor-14.mit.edu'
#ngasB_port = 7778
ngasB_port = 7777
#ngasB_port = 9000
ngasB_url = 'http://%s:%d/QARCHIVE' % (ngasB_host, ngasB_port)
#ngasB_url = 'houdt://%s:%d/QARCHIVE' % (ngasB_host, ngasB_port)


#ngasC_host = 'macbook46.icrar.org'
#ngasC_host = '127.0.0.1'
ngasC_host = 'eor-04.mit.edu'
#ngasC_port = 7779
ngasC_port = 7777
ngasC_url = 'http://%s:%d/QARCHIVE' % (ngasC_host, ngasC_port)

ngasB_proxy_url = 'http://%s:%d/PARCHIVE%%3Fnexturl%%3D%s' % (ngasB_host, ngasB_port, ngasC_url)

tmpDir = '/tmp/testNGASSub'
mime_type = 'application/octet-stream'

file_ext = '.data'

clientA = ngamsPClient.ngamsPClient(ngasA_host, ngasA_port)
clientA.setAuthorization(base64.encodestring('ngasmgr:ngas$dba'))
clientB = ngamsPClient.ngamsPClient(ngasB_host, ngasB_port)
clientB.setAuthorization(base64.encodestring('ngasmgr:ngas$dba'))
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
        stat = None
        try:
            stat = pclient.pushFile(fileUri, mime_type, cmd = 'QARCHIVE')
        except Exception as e:
            print "Exception '%s' occurred while archiving file %s" % (str(e), fileUri)
            continue
        msg = stat.getMessage().split()[0]
        if (msg != 'Successfully'):
            #raise Exception('Fail to archive \"%s\"' % fileUri)
            print "Exception '%s' occurred while archiving file %s" % (stat.getMessage(), fileUri)
            continue
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

def _unSubscribe(pclient, subscrId):
    stat = pclient.sendCmd('UNSUBSCRIBE', pars = [["subscr_id", subscrId]])
    msg = stat.getMessage()
    if (msg != 'Successfully handled UNSUBSCRIBE command'):
        print 'Error when removing the existing subscriber: \"%s\". Exception: %s' % (subscrId, msg)
        
def TestCase04(num_file_per_client, num_clients, interval = 4, base_name = None):
    TestCase01(num_file_per_client, num_clients, interval, base_name, False, True, False)
    
def TestCase05(num_file_per_client, num_clients, interval = 4, base_name = None):
    TestCase01(num_file_per_client, num_clients, interval, base_name, False, True, True)
    
def TestCase07(num_file_per_client, num_clients, interval = 1, base_name = None):
    TestCase06(num_file_per_client, num_clients, interval, base_name, True)
    
def TestCase11(num_file_per_client, num_clients, interval = 2, base_name = None):
    stat = clientA.sendCmd('SUBSCRIBE', pars=[['url', ngasB_url], ['concurrent_threads', '2'], ['subscr_id', 'A-to-B'], ['priority', 1]])
    msg = stat.getMessage()
    if (msg != 'Handled SUBSCRIBE command'):
        raise Exception('Fail to subscribe using \"%s\", error msg = %s' % (ngasB_url, msg))
    
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
    
    tt = 'N'
    while (tt != 'Y' and tt != 'y'):
        tt = raw_input("\nShall we change the priority from 1 to 10 now?(Y/N)\n")
    stat = clientA.sendCmd('USUBSCRIBE', pars=[['subscr_id', 'A-to-B'], ['priority', 10]])
    msg = stat.getMessage()
    print msg
    
    tt = 'N'
    while (tt != 'Y' and tt != 'y'):
        tt = raw_input("\nShall we change the priority from 10 back to 1 now?(Y/N)\n")
    stat = clientA.sendCmd('USUBSCRIBE', pars=[['subscr_id', 'A-to-B'], ['priority', 1]])
    msg = stat.getMessage()
    print msg
    
    print 'Wait until all archive threads are done'
    _waitUntilThreadsExit(deliveryThreads)
    
    tt = 'N'
    while (tt != 'Y' and tt != 'y'):
        tt = raw_input("\nShall we verify ngas-B now ?(Y/N)")
    verifyCase(base_name, num_files, clientB, True)
    _unSubscribe(clientA, 'A-to-B')
    
def TestCase10(num_file_per_client, num_clients, interval = 2, base_name = None):
    stat = clientA.sendCmd('SUBSCRIBE', pars=[['url', ngasB_url], ['concurrent_threads', '1'], ['subscr_id', 'A-to-B'], ['priority', 1]])
    msg = stat.getMessage()
    if (msg != 'Handled SUBSCRIBE command'):
        raise Exception('Fail to subscribe using \"%s\", error msg = %s' % (ngasB_url, msg))
    
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
    
    tt = 'N'
    while (tt != 'Y' and tt != 'y'):
        tt = raw_input("\nShall we change concurrent_threads to 3 now?(Y/N)\n")
    stat = clientA.sendCmd('USUBSCRIBE', pars=[['subscr_id', 'A-to-B'], ['concurrent_threads', '3']])
    msg = stat.getMessage()
    print msg
    
    tt = 'N'
    while (tt != 'Y' and tt != 'y'):
        tt = raw_input("\nShall we change concurrent_threads back to 1 now?(Y/N)\n")
    stat = clientA.sendCmd('USUBSCRIBE', pars=[['subscr_id', 'A-to-B'], ['concurrent_threads', '1']])
    msg = stat.getMessage()
    print msg
    
    print 'Wait until all archive threads are done'
    _waitUntilThreadsExit(deliveryThreads)
    
    tt = 'N'
    while (tt != 'Y' and tt != 'y'):
        tt = raw_input("\nShall we verify ngas-B now ?(Y/N)")
    verifyCase(base_name, num_files, clientB, True)
    _unSubscribe(clientA, 'A-to-B')
    
def TestCase09(num_file_per_client, num_clients, interval = 2, base_name = None):
    stat = clientA.sendCmd('SUBSCRIBE', pars=[['url', ngasB_url], ['concurrent_threads', '2'], ['subscr_id', 'A-to-B'], ['priority', 1]])
    msg = stat.getMessage()
    if (msg != 'Handled SUBSCRIBE command'):
        raise Exception('Fail to subscribe using \"%s\", error msg = %s' % (ngasB_url, msg))
    
    num_files = num_file_per_client * num_clients
    if (base_name == None):
        print 'Creating %d dummy files ...' % num_files
        base_name = createTmpFiles(num_files)
    
    #first_fname = '%s-%s%s' % (base_name, str(0), file_ext)
    #last_fname = '%s-%s%s' % (base_name, num_files - 1, file_ext)
    
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
       
    tt = 'N'
    while (tt != 'Y' and tt != 'y'):
        tt = raw_input("Have you shut down ngas-B yet? (Y/N)")

    tt = 'N'
    while (tt != 'Y' and tt != 'y'):
        tt = raw_input("Shall we change subscriber url to ngas-C's url now?(Y/N)")
    stat = clientA.sendCmd('USUBSCRIBE', pars=[['subscr_id', 'A-to-B'], ['url', ngasC_url]])
    msg = stat.getMessage()
    print msg
    
    print 'Wait until all archive threads are done'
    _waitUntilThreadsExit(deliveryThreads)
    
    tt = 'N'
    while (tt != 'Y' and tt != 'y'):
        tt = raw_input("Shall we start to verify ngas-B and ngas-C now?(Y/N)")
    verifyCase(base_name, num_files, clientB, False)
    verifyCase(base_name, num_files, clientC, True)
    _unSubscribe(clientA, 'A-to-B')

def TestCase08(num_file_per_client, num_clients, interval = 2, base_name = None, suspendFirst = True):
    stat = clientA.sendCmd('SUBSCRIBE', pars=[['url', ngasB_url], ['concurrent_threads', '2'], ['subscr_id', 'A-to-B'], ['priority', 1]])
    msg = stat.getMessage()
    if (msg != 'Handled SUBSCRIBE command'):
        raise Exception('Fail to subscribe using \"%s\", error msg = %s' % (ngasB_url, msg))
    
    num_files = num_file_per_client * num_clients
    if (base_name == None):
        print 'Creating %d dummy files ...' % num_files
        base_name = createTmpFiles(num_files)
    
    #first_fname = '%s-%s%s' % (base_name, str(0), file_ext)
    #last_fname = '%s-%s%s' % (base_name, num_files - 1, file_ext)
    
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
    
    if (suspendFirst):
        tt = 'N'
        while (tt != 'Y' and tt != 'y'):
            tt = raw_input("Shall we suspend file delivery now? (Y/N)")
    
        stat = clientA.sendCmd('USUBSCRIBE', pars=[['subscr_id', 'A-to-B'], ['suspend', 1]])
        msg = stat.getMessage()
        if (msg != 'Successfully SUSPENDED for the subscriber A-to-B'):
            _unSubscribe(clientA, 'A-to-B')
            raise Exception('Fail to suspend subscriber A-to-B, error msg = %s' % msg)
    
    tt = 'N'
    while (tt != 'Y' and tt != 'y'):
        tt = raw_input("Shall we change subscriber url now?(Y/N)")
    stat = clientA.sendCmd('USUBSCRIBE', pars=[['subscr_id', 'A-to-B'], ['url', ngasC_url]])
    msg = stat.getMessage()
    print msg
    
    if (suspendFirst):
        tt = 'N'
        while (tt != 'Y' and tt != 'y'):
            tt = raw_input("Shall we resume file delivery for subscriber A-to-B now?(Y/N)")
        stat = clientA.sendCmd('USUBSCRIBE', pars=[['subscr_id', 'A-to-B'], ['suspend', 0]])
        msg = stat.getMessage()
        print msg
    
    print 'Wait until all archive threads are done'
    _waitUntilThreadsExit(deliveryThreads)
    
    tt = 'N'
    while (tt != 'Y' and tt != 'y'):
        tt = raw_input("Shall we start to verify ngas-B and ngas-C now?(Y/N)")
    verifyCase(base_name, num_files, clientB, False)
    verifyCase(base_name, num_files, clientC, True)
    _unSubscribe(clientA, 'A-to-B')
    
def TestCase06(num_file_per_client, num_clients, interval = 1, base_name = None, shutdownB = False):    
    fc_old = raw_input("How many files currently in NGAS server A? - i.e. select count(*) from ngas_files;\n")
    stat = clientA.sendCmd('SUBSCRIBE', pars=[['url', ngasB_url], ['concurrent_threads', '2'], ['subscr_id', 'A-to-B'], ['priority', 1]])
    msg = stat.getMessage()
    if (msg != 'Handled SUBSCRIBE command'):
        raise Exception('Fail to subscribe using \"%s\", error msg = %s' % (ngasB_url, msg))
    
    num_files = num_file_per_client * num_clients
    if (base_name == None):
        print 'Creating %d dummy files ...' % num_files
        base_name = createTmpFiles(num_files)
    
    first_fname = '%s-%s%s' % (base_name, str(0), file_ext)
    last_fname = '%s-%s%s' % (base_name, num_files - 1, file_ext)
    
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
    
    if (not shutdownB):
        print "Wait until NGAS-B has received the first file"
        try:
            howlong = waitUntilFileDelivered(first_fname, clientB, timeout = num_files, wait_interval = 1)
        except WaitTimeout as e:
            print 'Timeout waiting for the first file \"%s\" to be delivered to \"%s\"' % (first_fname, ngasB_url)
            _unSubscribe(clientA, 'A-to-B')
            return
        else:
            print 'After %d seconds, the first file \"%s\" is delivered to \"%s\". Please shut down ngas-A right now!!' % (howlong, first_fname, ngasB_url)
    
    if (shutdownB):
        tt = 'N'
        while (tt != 'Y' and tt != 'y'):
            tt = raw_input("Have you shut down NGAS server B? (Y/N)")
    
    tt = 'N'
    while (tt != 'Y' and tt != 'y'):
        tt = raw_input("Have you shut down NGAS server A? (Y/N)")
    
    if (shutdownB):
        tt = 'N'
        while (tt != 'Y' and tt != 'y'):
            tt = raw_input("Have you restarted NGAS server B? (Y/N)")
    
    last_fname = raw_input('What is the last file name?\n')
            
    tt = 'N'
    while (tt != 'Y' and tt != 'y'):
        tt = raw_input("Now. have you restarted NGAS server A? (Y/N)")
    
    print "Wait until NGAS-B has received the last file"
    try:
        howlong = waitUntilFileDelivered(last_fname, clientB, timeout = num_files, wait_interval = 2)
    except WaitTimeout as e:
        print 'Timeout waiting for the last file \"%s\" to be delivered to \"%s\"' % (last_fname, ngasB_url)
        _unSubscribe(clientA, 'A-to-B')
        return
    else:
        print 'After %d seconds, the last file \"%s\" is delivered to \"%s\"' % (howlong, last_fname, ngasB_url)
    
    verifyCase(base_name, num_files, clientB, True)
    
    tt = 'N'
    while (tt != 'Y' and tt != 'y'):
        tt = raw_input("Are you ready to verify the result on NGAS-A? (Y/N)")
        
    fc_new = raw_input("How many files now in NGAS server A? - i.e. select count(*) from ngas_files;")
    
    if (int(fc_old) != int(fc_new)):
        _unSubscribe(clientA, 'A-to-B')
        raise Exception('Not all files are removed from the cache server A')
    else:
        print 'All newly archived files in NGAS-A have all been removed.'
        
    _unSubscribe(clientA, 'A-to-B')
    
def TestCase13(num_file_per_client, num_clients, interval = 4, base_name = None):
    """
    Testing the proxy archive
    """
    if (num_file_per_client < 1):
        raise Exception("each client at least archives one file!")
    
    stat = clientA.sendCmd('SUBSCRIBE', pars=[['url', ngasB_proxy_url], ['concurrent_threads', '2'], ['subscr_id', 'A-to-C-via-B'], ['priority', 1]])
    msg = stat.getMessage()
    if (msg != 'Handled SUBSCRIBE command'):
        raise Exception('Fail to subscribe using \"%s\", error msg = %s' % (ngasB_proxy_url, msg))
    
    num_files = num_file_per_client * num_clients
    if (base_name == None):
        print 'Creating %d dummy files ...' % num_files
        base_name = createTmpFiles(num_files)
    
    #last_fname = '%s-%s%s' % (base_name, num_files - 1, file_ext)
    
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
    
    tt = 'N'
    while (tt != 'Y' and tt != 'y'):
        tt = raw_input("\nAre we ready to verify ngas-C?(Y/N)\n")
        
    verifyCase(base_name, num_files, clientC, True)
    
    _unSubscribe(clientA, 'A-to-C-via-B')
    

def TestCase12(num_file_per_client, num_clients, interval = 4, base_name = None):
    if (num_file_per_client < 1):
        raise Exception("each client at least archives one file!")
    
    stat = clientA.sendCmd('SUBSCRIBE', pars=[['url', ngasB_url + '----' + ngasC_url], ['concurrent_threads', '2'], ['subscr_id', 'A-to-B_OR_C'], ['priority', 1]])
    msg = stat.getMessage()
    if (msg != 'Handled SUBSCRIBE command'):
        raise Exception('Fail to subscribe using \"%s\", error msg = %s' % (ngasB_url, msg))
    
    num_files = num_file_per_client * num_clients
    if (base_name == None):
        print 'Creating %d dummy files ...' % num_files
        base_name = createTmpFiles(num_files)
    
    last_fname = '%s-%s%s' % (base_name, num_files - 1, file_ext)
    
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
    
    tt = 'N'
    while (tt != 'Y' and tt != 'y'):
        tt = raw_input("\nAre we ready to verify both ngas-B and ngas-C?(Y/N)\n")
        
    verifyCase(base_name, num_files, clientB, False)
    verifyCase(base_name, num_files, clientC, True)
    
    _unSubscribe(clientA, 'A-to-B_OR_C')

def TestCase01(num_file_per_client, num_clients, interval = 4, base_name = None, 
               wait_for_C_files = True, 
               wait_for_C_restart = False,
               wait_for_C_files_after_restart = True):
    
    if (not wait_for_C_files):
        tt = 'N'
        while (tt != 'Y' and tt != 'y'):
            tt = raw_input("Have you shut down NGAS server C? (Y/N)")
            
    if (num_file_per_client < 1):
        raise Exception("each client at least archives one file!")
    
    stat = clientA.sendCmd('SUBSCRIBE', pars=[['url', ngasB_url], ['concurrent_threads', '2'], ['subscr_id', 'A-to-B'], ['priority', 1]])
    msg = stat.getMessage()
    if (msg != 'Handled SUBSCRIBE command'):
        raise Exception('Fail to subscribe using \"%s\", error msg = %s' % (ngasB_url, msg))
    
    stat = clientA.sendCmd('SUBSCRIBE', pars=[['url', ngasC_url], ['concurrent_threads', '2'], ['subscr_id', 'A-to-C'], ['priority', 1]])
    msg = stat.getMessage()
    if (msg != 'Handled SUBSCRIBE command'):
        raise Exception('Fail to subscribe using \"%s\", error msg = %s' % (ngasC_url, msg))
    
    
    num_files = num_file_per_client * num_clients
    if (base_name == None):
        print 'Creating %d dummy files ...' % num_files
        base_name = createTmpFiles(num_files)
    
    last_fname = '%s-%s%s' % (base_name, num_files - 1, file_ext)
    
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
       
    #print 'Wait %d seconds so that all files are delivered to all subscribers' % (num_files * 3)
    """
    print "Wait until NGAS-B has received the last file"
    try:
        howlong = waitUntilFileDelivered(last_fname, clientB, timeout = num_files, wait_interval = 2)
    except WaitTimeout as e:
        print 'Timeout waiting for the last file \"%s\" to be delivered to \"%s\"' % (last_fname, ngasB_url)
        _unSubscribe(clientA, 'A-to-B')
        _unSubscribe(clientA, 'A-to-C')
        return
    else:
        print 'After %d seconds, the last file \"%s\" is delivered to \"%s\"' % (howlong, last_fname, ngasB_url)
    
    if (wait_for_C_files):
        print "Wait until NGAS-C has received the last file"
        try:
            howlong = waitUntilFileDelivered(last_fname, clientC, timeout = num_files, wait_interval = 1)
        except WaitTimeout as e:
            print 'Timeout waiting for the last file \"%s\" to be delivered to \"%s\"' % (last_fname, ngasC_url)
            _unSubscribe(clientA, 'A-to-B')
            _unSubscribe(clientA, 'A-to-C')
            return
        else:
            print 'After %d seconds, the last file \"%s\" is delivered to \"%s\"' % (howlong, last_fname, ngasC_url)

    #time.sleep(num_files * 3) # assume each file needs 3 seconds to delivery (in parallel)
    """
    tt = 'N'
    while (tt != 'Y' and tt != 'y'):
        tt = raw_input("\nAre we ready to verify both ngas-B and ngas-C?(Y/N)\n")
        
    verifyCase(base_name, num_files, clientB, False)
    
    if (wait_for_C_files):
        verifyCase(base_name, num_files, clientC, True)
        _unSubscribe(clientA, 'A-to-C')
    
    _unSubscribe(clientA, 'A-to-B')
    
    if (wait_for_C_restart):
        tt = 'N'
        while (tt != 'Y' and tt != 'y'):
            tt = raw_input("Have you started NGAS server C? (Y/N)")
        
        if (wait_for_C_files_after_restart):
            stat = clientA.sendCmd('TRIGGERSUBSCRIPTION')
            msg = stat.getMessage()
            #if (msg != 'Command TRIGGERSUBSCRIPTION executed successfully'):
                #raise Exception('Fail to trigger subscription')
            print msg 
            print "Wait until NGAS-C has received the last file"
            try:
                howlong = waitUntilFileDelivered(last_fname, clientC, timeout = num_files * 2, wait_interval = 1)
            except WaitTimeout as e:
                print 'Timeout waiting for the last file \"%s\" to be delivered to \"%s\"' % (last_fname, ngasC_url)
                _unSubscribe(clientA, 'A-to-B')
                _unSubscribe(clientA, 'A-to-C')
                return
            else:
                print 'After %d seconds, the last file \"%s\" is delivered to \"%s\"' % (howlong, last_fname, ngasC_url)
    
        _unSubscribe(clientA, 'A-to-C')

def _subscribeAwithB(concurrent_threads = 4):
    stat = clientA.sendCmd('SUBSCRIBE', pars=[['url', ngasB_url], ['concurrent_threads', '%d' % concurrent_threads], ['subscr_id', 'A-to-B'], ['priority', 1]])
    msg = stat.getMessage()
    if (msg != 'Handled SUBSCRIBE command'):
        raise Exception('Fail to subscribe using \"%s\", error msg = %s' % (ngasB_url, msg))
    else:
        print msg
    
 
def TestCase03(num_files, base_name = None):
    print 'Subscribing A with B\'s url ...'
    _subscribeAwithB()
    tt = 'N'
    while (tt != 'Y' and tt != 'y'):
        tt = raw_input("Have you restarted NGAS server A? (Y/N)")
    if (base_name == None):
        print 'Creating %d dummy files ...' % num_files
        base_name = createTmpFiles(num_files)
    for num in range(num_files):
        fileUri = '%s/%s-%s%s' % (tmpDir, base_name, str(num), file_ext)
        print 'Archiving file %s' % fileUri 
        stat = clientA.pushFile(fileUri, mime_type, cmd = 'QARCHIVE')
        msg = stat.getMessage().split()[0]
        if (msg != 'Successfully'):
            raise Exception('Fail to archive \"%s\"' % fileUri)
    lastfname = '%s-%d%s' % (base_name, num_files - 1, file_ext) # this might not be the last file for multi-threaded delivery, but approximately for now
    howlong = waitUntilFileDelivered(lastfname, clientB)
    print 'After %d seconds, the last file \"%s\" is delivered to \"%s\"' % (howlong, lastfname, ngasB_url)
    
    time.sleep(3)
    verifyCase(base_name, num_files, clientB, True)
    _unSubscribe(clientA, 'A-to-B')
    
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
    stat = clientA.sendCmd('SUBSCRIBE', pars=[['url', ngasB_url], ['start_date', ingestion_date], ['concurrent_threads', '2'], ['subscr_id', 'A-to-B'], ['priority', 1]])
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
    
    stat = clientA.sendCmd('SUBSCRIBE', pars=[['url', ngasC_url], ['start_date', ingestion_date], ['concurrent_threads', '2'], ['subscr_id', 'A-to-C'], ['priority', 1]])
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
    
    _unSubscribe(clientA, 'A-to-B')
    _unSubscribe(clientA, 'A-to-C')

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
    #TestCase01(10, 2, interval = 3)
    #TestCase04(10, 3, interval = 3)
    #TestCase05(10, 3, interval = 3)
    #TestCase03(16)
    #TestCase06(8, 3)
    #TestCase07(8, 3)
    #TestCase08(8, 3, suspendFirst = False)
    #TestCase09(6, 3)
    #TestCase10(8,3)
    #TestCase11(7,3)
    #TestCase12(10, 2, interval = 3)
    TestCase13(1, 2, interval = 3) # testing proxy archive
