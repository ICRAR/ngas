#******************************************************************************
#
#
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      22/11/2013  Created
#

""" A python wrapper that interacts with DMF using command line """

import pcc
from ngams import * 
import ngamsPlugInApi
import os, time

TAPE_ONLY_STATUS = ['NMG', 'OFL', 'PAR'] # these states tell us there are only complete copies of the file currently on tape (no copies on disks)
ERROR_STATUS = ['INV']
MIGRATED_STATUS = ['DUL', 'OFL', 'PAR'] # steady states after at least one migration
RELEASABLE_STATUS = ['DUL'] # 'PAR' is not releasable further as it will make the while file offline
ON_DISK_STATUS = ['DUL', 'REG']

def getDMFStatus(filename):
    cmd = "dmls -l " + filename
    """
    Lists file names in long format, giving 
    mode, number of links, owner, group, size in bytes, time of last modification, and, 
    in parentheses before the file name, the DMF state
    """
    t = ngamsPlugInApi.execCmd(cmd, -1)
    exitCode = t[0]
    if (exitCode != 0 or len(t) != 2):
        errMsg = "Fail to query the online/offline status for file " + filename
        alert(errMsg)
        raise Exception(errMsg)
    return t[1].split()[7][1:4] # this is ugly hardcoded

def isFileOnDisk(filename):
    """
    To check if the file has a FULL copy on the disk
    
    return 1 - has a FULL copy on disk, 0 - has no FULL copies on disk, -1 - query error
    """
    try:
        status = getDMFStatus(filename) 
        if (status in ON_DISK_STATUS):
            return 1
        else:
            return 0        
    except Exception, e:
        errMsg = "Exception '%s' when querying the status of %s" % (str(e), filename)
        alert(errMsg)
        return -1

def isFileReleasable(filename):
    """
    To check if the file is releasable from the disk
    
    return 1 - releasable, 0 - not releasable, -1 - query error
    """
    try:
        status = getDMFStatus(filename)
        if (status in RELEASABLE_STATUS):
            return 1
        else:
            return 0        
    except Exception, e:
        errMsg = "Exception '%s' when querying the status of %s" % (str(e), filename)
        alert(errMsg)
        return -1
    

def isFileOnTape(filename):
    """
    To check if the file is completely on tape ONLY, thus no copy is on the disk
    
    return 1 - on tape, 0 - not on tape, -1 - query error
    """
    try:
        status = getDMFStatus(filename)
        if (status in TAPE_ONLY_STATUS):
            return 1
        elif (status in ERROR_STATUS):
            return -1
        else:
            return 0        
    except Exception, e:
        errMsg = "Exception '%s' when querying the status of %s" % (str(e), filename)
        alert(errMsg)
        return -1
    
def releaseFiles(filenameList, kpFitsHdrOnline = True):
    """
    Release the disk space of a list of files 
    that already have copies on tape
    
    RETURN   the number of files released. -1, if any errors
    """
    cmd = 'dmput -r'
    num_released = 0
    if (kpFitsHdrOnline):
        cmd += ' -K 0:2880'
    for filename in filenameList:
        if (isFileReleasable(filename) == 1):
            cmd += ' %s' % filename
            num_released += 1
    #print cmd
    t = ngamsPlugInApi.execCmd(cmd, -1)
    exitCode = t[0]
    if (exitCode != 0):
        errMsg = "Releasing problem: " + str(exitCode) + ", cmd: " + cmd
        alert(errMsg)
        #print errMsg
        return -1
    
    return num_released

def getPawseyDBConn():
    import psycopg2
    try:
        l_db_conn = psycopg2.connect(database = 'ngas', user= 'ngas', 
                            password = 'bmdhcyRkYmE=\n'.decode('base64'), 
                            host = 'mwa-pawsey-db01.pawsey.ivec.org')
        return l_db_conn
    except Exception, e:
        errStr = 'Cannot create LTA DB Connection: %s' % str(e)
        raise Exception, errStr

def executeQuery(conn, sqlQuery):
    try:
        cur = conn.cursor()
        cur.execute(sqlQuery)
        return cur.fetchall()
    finally:
        if (cur):
            del cur

def checkFileCRC(filelist):
    import binascii
    wrong_list = []
    
    blockSize = 524288 # 512K block size
    conn = getPawseyDBConn()
    
    for filename in filelist:
        block = "-"
        crc = 0
        
        fdIn = open(filename)
        while (block != ""):
            block = fdIn.read(blockSize)
            crc = binascii.crc32(block, crc)
        fdIn.close()
         
        sqlQuery = "SELECT checksum FROM ngas_files WHERE file_id = '%s' AND file_version = 2" % os.path.basename(filename)
        res = executeQuery(conn, sqlQuery)
        for re in res:
            crc_db = int(re[0])
            if (crc != crc_db):
                wrong_item = (filename, crc_db, crc)
                wrong_list.append(wrong_item)
            break
    
    return wrong_list

def stageFiles(filenameList, checkCRC = False, printWarn = False):
    """
    Stage a list of files. 
    The system will sort files in the order that they are archived on the tape volumes for better performance
    
    RETURN   the number of files staged. -1, if any errors
    """
    cmd1 = "dmget"
    num_staged = 0
    size_staged = 0
    staged_files = []
    for filename in filenameList:
        if (isFileOnTape(filename) == 1):
            cmd1 += ' %s' % filename
            num_staged += 1
            size_staged += os.path.getsize(filename)
            staged_files.append(filename)
    #print cmd1    
    t = ngamsPlugInApi.execCmd(cmd1, -1)
    exitCode = t[0]
    if (exitCode != 0):
        errMsg = "Staging problem: " + str(exitCode) + ", cmd: " + cmd1
        if (printWarn):
            print errMsg
        else:
            alert(errMsg)
        return -1
    
    crc_time = 0
    if (checkCRC):
        if (printWarn):
            print 'Checking CRC'
        else:
            info(3, 'Checking CRC')
        cst = time.time()
        wrongList = checkFileCRC(staged_files)
        cet = time.time()
        if (len(wrongList) > 0):
            warnMsg = 'CRC checking has errors !! - %s' % str(wrongList)
            if (printWarn):
                print warnMsg
            else:
                alert(warnMsg)
        else:
            infoMsg = 'CRC are all correct.'
            if (printWarn):
                print infoMsg
            else:
                info(3, infoMsg)
        crc_time = cet - cst
    
    return (num_staged, size_staged, crc_time)

def sorted_ls(path):
    """
    http://stackoverflow.com/questions/4500564/directory-listing-based-on-time
    
    call stat() on each of the files and sort by one of the timestamps, 
    perhaps by using a key function that returns a file's timestamp
    """
    mtime = lambda f: os.stat(os.path.join(path, f)).st_mtime
    return list(sorted(os.listdir(path), key=mtime))

if __name__ == '__main__':
    from optparse import OptionParser
    
    parser = OptionParser()
    parser.add_option("-p", "--paths",
                  action="store", type="string", dest="filepaths",
                  help="File paths separated by semicolons, e.g. /home/ngas/NGAS/volume1:/home/ngas/NGAS/volume2")
    
    parser.add_option("-f", "--nfpp", type="int", dest="num_per_path", default = 5, 
                      help="Number of files per path")
    
    parser.add_option("-n", "--nocrc",
                  action="store_false", dest="check_crc", default = True,
                  help="Do not check CRC after staging")
    
    parser.add_option("-r", "--random_order",
                  action="store_false", dest="order_by_name", default = True,
                  help="Randomly order the file to be staged. By default, files are alphabetically ordered by names")
    
    parser.add_option("-t", "--nofitshdr",
                  action="store_false", dest="kp_fits_hdr", default = True,
                  help="Do not keep fits header online")
    
    (options, args) = parser.parse_args()
    if (None == options.filepaths):
        parser.print_help()
        exit(1)
    
    filepaths = options.filepaths # separated by comma
    num_per_path = options.num_per_path # number of files per path
    order_by_name = options.order_by_name
    check_crc = options.check_crc
    
    final_file_list = []
    dirs = filepaths.split(':')
    for filepath in dirs:
        if (not os.path.exists(filepath)):
            continue
        
        listfiles = os.listdir(filepath)
        if (not order_by_name):
            import random
            random.shuffle(listfiles)
    
        if (len(listfiles) < num_per_path):
            end = len(listfiles)
        else:
            end = num_per_path
        
        for filename in listfiles[0:end]:
            final_file_list.append(filepath + '/' + filename)
    cc = len(final_file_list)
    if (cc == 0):
        print "Nothing to release"
        exit(1)
    print 'Releasing %d files' % cc        
    releaseFiles(final_file_list)
    
    print 'Staging %d files' % cc            
    st = time.time()
    no_files, total_size, crc_time = stageFiles(final_file_list, checkCRC = check_crc, printWarn = True)
    et = time.time()
    tt = et - st - crc_time
    print("\nStaging summary")
    print("------------------------------------")
    print('# of files:\t\t%d' % no_files)
    print('Total time:\t\t%.2f seconds' % tt) 
    print('Mean time:\t\t%.2f seconds' % (float(tt) / no_files))
    print('Staging rate:\t\t%.2f MB/s' % ((total_size / (1024.** 2)) / tt))
    print('')
    
    
    
    
    
