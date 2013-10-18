import psycopg2
import os, sys
from optparse import OptionParser
import urllib, urllib2, base64
from xml.etree.ElementTree import Element, SubElement, Comment, tostring
from xml.etree import ElementTree


def discardFile(file_id, file_version, disk_id, host_id):
    # Construct URL to discard file. Issue command to host that has the file. 
    data = {}
    data['disk_id'] = disk_id
    data['file_id'] = file_id
    data['file_version'] = file_version
    data['execute'] = 1
    url_values = urllib.urlencode(data)
    # http://NGAS_Server_host:7777/DISCARD?disk_id=XXXX&file_id=XXXX&file_version=1&execute=1
    full_url = "http://" + host_id + "/DISCARD?" + url_values
    
    request = urllib2.Request(full_url)
    base64string = base64.encodestring('%s:%s' % ('', '')).replace('\n', '')
    request.add_header("Authorization", "Basic %s" % base64string)
    
    print "Discarding %s" % (full_url,) 

    sock = None
    try:
        # Issue command
        sock =  urllib2.urlopen(request, timeout=20)
        # Get server response
        buff = sock.read()
        print buff
        ele = ElementTree.XML(buff)
        res = ele.find('Status')
        # check return code
        if res.get('Status') != 'SUCCESS':
            raise Exception("Discard failed for " + file_id)
    # close connection
    finally:
        if sock:
            sock.close()

def main():
    parser = OptionParser(usage="usage: %prog [options]", version="%prog 1.0")
    parser.add_option("-o", type='string', action="store", dest="obs", help="Observation ID")
    parser.add_option("-d", default='1', type='string', action="store", dest="duplicates", help="Delete duplicates only (1: delete duplicates only DEFAULT, 0: remove all)")

    (options, args) = parser.parse_args()
    
    if not options.obs:
        print 'Observation id needs to be supplied'
        sys.exit(-1)
    
    # use ngas_ro user
    conn = psycopg2.connect(database='ngas', user='ngas_ro', host='eor-02.mit.edu', password='ngas$ro')
    cur = conn.cursor()
    
    # find all files with that observation id
    # Each file belongs on a host with a disk id so join to find that info and issue the DISPOSE command to that host
    cur.execute("SELECT file_id, file_version, ngas_files.disk_id, ngas_disks.host_id FROM ngas_files INNER JOIN ngas_disks ON ngas_files.disk_id = ngas_disks.disk_id where ngas_files.file_id LIKE %s", ['%' + str(options.obs) + '%']);
    rows = cur.fetchall()
    if len(rows) <= 0:
        print "No files exist for %s" % (options.obs,)  
        sys.exit(-1)
    
    if options.duplicates == '1':
        print 'Deleting ONLY DUPLICATE files for observation %s' % (options.obs,)
    else:
        print 'Deleting ALL files for observation %s' % (options.obs,)
         
    count = 0
    for r in rows:
        if options.duplicates == '1':
            # only delete files with a version number greater 1 i.e. duplicates
            if r[1] > 1:
                #print r[0], r[1], r[2], r[3]
                count += 1
                # Discard each file for the observation
                discardFile(r[0], r[1], r[2], r[3])
        else:
            #print r[0], r[1], r[2], r[3]
            count += 1
            discardFile(r[0], r[1], r[2], r[3])
    
    print "Files deleted: %s" % (count,)
    
        
if __name__ == "__main__":
    main()