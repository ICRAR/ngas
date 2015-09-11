"""
This module provides routines to parse out pairs of coordinates from
GAVO Web/VO server logs

It can resolve both SIMBAD and GLEAM objects

created on 1 Sep 2015 chen.wu@icrar.org

"""

from gavo.protocols import simbadinterface
from urllib2 import urlopen, quote

resolve_dict = dict()


def parse_pos_from_logs(fn):
    """
    fn - e.g. /Users/Chen/data/gleam_logs/cutout.log
              /Users/Chen/data/gleam_logs/snapshot.log

    log is obtained by running this:
    ==================================
    ssh mwa-web.icrar.org
    cd /var/gavo/logs
    grep "Processing starts: /gleam_postage/q/form" web.log.* # for cutout.log
    grep "Processing starts: /gleam/q/form" web.log.* # for snapshot.log

    """
    with open(fn) as fin:
        lines = fin.readlines()
        for l in lines:
            try:
                # this is really bad!!
                pos = l.split("'pos': ")[1].split(",],")[0].split("[")[1].replace("'","")
                print pos
            except:
                continue

def remove_empty_lines(fn):
    """
     fn - e.g. /Users/Chen/data/gleam_logs/cutout.log
               /Users/Chen/data/gleam_logs/snapshot.log
    """
    with open(fn) as fin:
        lines = fin.readlines()
        for l in lines:
            if len(l.strip()) < 1:
                continue
            else:
                print l.strip()

def resolve_gleam_obj(pos):
    obj_name = pos.split()[-1]
    url = "http://localhost:7778/resolve_gleam_obj?obj_name=%s" % (quote(obj_name))
    try:
        resp = urlopen(url)
        ret = resp.read()
        if ("None" == ret):
            return None
        else:
            retstr = ret.split(",")
            retdict = {"RA":retstr[0], "dec":retstr[1]}
            return retdict
    except Exception:
        return None

def getRADec(pos, check_gleam_obj=False):
    """
    Return a tuple of floats (ra, dec) in degrees
    """
    if (pos == None):
        return None
    try:
        pos = pos.replace(':', ' ')
        return simbadinterface.base.parseCooPair(pos)
    except ValueError:
        if (resolve_dict.has_key(pos)):
            return resolve_dict[pos]
        data = None
        if (check_gleam_obj):
            data = resolve_gleam_obj(pos)
        if not data:
            try:
                data = simbadinterface.base.caches.getSesame("web").query(pos)
            except Exception:
                data = None
        if not data:
            return None
            #raise Exception("Cannot resolve {0}".format(pos))
        else:
            ret = float(data["RA"]), float(data["dec"])
            resolve_dict[pos] = ret
            return ret

if __name__ == '__main__':
    fn = 'snapshot_pos_01'
    from collections import defaultdict
    counter = defaultdict(int)
    with open(fn) as fin:
        lines = fin.readlines()
        for l in lines:
            pos = l.strip()
            radec = getRADec(pos)
            if (not radec):
                continue
            counter[radec] += 1

    for k, v in counter.items():
        print("{0},{1},{2}".format(k[0], k[1], v))




