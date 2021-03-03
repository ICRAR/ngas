#
#    (c) University of Western Australia
#    International Centre of Radio Astronomy Research
#    M468/35 Stirling Hwy
#    Perth WA 6009
#    Australia
#
#    Copyright by UWA,
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
import urllib2

def Decimal(svalue):
    """
    Helper function to convert values returned as Decimal() from getHostsList
    INPUTS:    
        svalue:       string, string representation of value
    
    RETURNS:    
        integer
    """
    return int(svalue)


def getHostsList(host):
    """
    Retrieve the list of hosts registered in the DB of an NGAS host [host]
    INPUTS:    
        host:       string, a valid reachable NGAS host name.
    
    RETURNS:    
        list of hosts defined on that server
    """
    conn = urllib2.urlopen(host + '/QUERY?query=hosts_list')
    res = conn.read()
    conn.close()
    
    exec('ares=%s' % res)  # reconstruct the list from the string returned
    
    return ares[0]


def filterOnline(hlist):
    """
    Filter a hosts list for NGAS servers in ONLINE state
    INPUTS:    
        hlist:       list, list as returned by getHostsList
    
    RETURNS:    
        filtered hosts lists
    """
    return filter(lambda x:x[15]=='ONLINE',hlist)


class RemoteNgasLogFile():
    """
    Class representing a remote NGAS log file

    RETURNS:    
        File-like object
    """
    def __init__(self, url):
        """
        description
        INPUTS:    
            url:       string, valid URL pointing to a NGAS log-file
        
        """
        self.url = url
        self.file = None
        self.host = urllib2.urlparse.urlsplit(url).netloc
        if self.host.find(':') >= 0:
            (self.host, self.port) = urllib2.urlparse.urlsplit(url).netloc.split(':')
            self.port = int(self.port)
        else:
            self.port = 80
        try:
            self.file = urllib2.urlopen(self.url)
        except Exception,e:
            print e


def retrieveLogFromNgas(hlist1):
    """
    Retrieve the current log-file from one NGAS server specified by
    a single list entry as returned by getHostsList 
    INPUTS:    
        hlist1:       list, single NGAS server specs
    
    RETURNS:    
        
    """
    nhost = hlist1[0].split(':')[0]   #just the hostname without port
    ndomain = hlist1[1]
    nport = hlist1[10]
    url = 'http://%s.%s:%s/RETRIEVE?ng_log' % (nhost, ndomain, nport)
    logs = ""
    print ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
    print "Opening connection to %s" % url
    lf = RemoteNgasLogFile(url)
    if lf.file: 
        logs = lf.file.read()
        lf.file.close()
        del(lf)
    else:
        print "Unable to open log file"
    
    return logs

if __name__ == '__main__':
    host = 'http://store06.icrar.org:7777'
    log = ''
    hlist = getHostsList(host)
    fhlist = filterOnline(hlist)
    for h in fhlist:
        log += retrieveLogFromNgas(h)
        print "========================"