#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2012
#    Copyright by UWA (in the framework of the ICRAR)
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

#******************************************************************************
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      17/April/2014  Created
#
# time.gmtime(1067868000 + 315964800)
import os, commands, gc, sys, time, math
from os import walk
from collections import namedtuple, defaultdict
import numpy as np
import datetime as dt
import matplotlib
# see http://stackoverflow.com/questions/4706451/how-to-save-a-figure-remotely-with-pylab/4706614#4706614
#matplotlib.use('Agg')
import pylab as pl
from optparse import OptionParser
import urlparse
import re as regx
import cPickle as pickle
import multiprocessing as mp


# retrieval access (date, observation id, was the file offline?, file_size)
RA = namedtuple('RA', 'date obsId offline size user obsdate')

leng_client = 15 #len('client_address=')
patdict = {"'":"","(":'',")":''}

def execCmd(cmd, failonerror = True, okErr = []):
    re = commands.getstatusoutput(cmd)
    if (re[0] != 0 and not (re[0] in okErr)):
        errMsg = 'Fail to execute command: "%s". Exception: %s' % (cmd, re[1])
        if (failonerror):
            raise Exception(errMsg)
        else:
            print errMsg
    return re

def unzipLogFiles(dir):
    """
    unzip all files whose names end with ".gz" in a given directory
    """
    f = []
    for (dirpath, dirnames, filenames) in walk(dir):
        f.extend(filenames)
        break

    for fn in f:
        if fn.endswith('.nglog.gz.gz'):
            # extract
            cmd = 'gzip -d %s/%s' % (dir, fn)
            re = execCmd(cmd, failonerror = False)
            # then change name
            if (re[0] == 0):
                # remove the ".gz" from the end of the file name
                cmd = 'mv %s/%s %s/%s' % (dir, fn[0:-3], dir, fn[0:-6])
                re = execCmd(cmd, failonerror = False)
        elif fn.endswith('.nglog.gz'):
            # extract only
            cmd = 'gzip -d %s/%s' % (dir, fn)
            re = execCmd(cmd, failonerror = False)

def _raListToVTimeNA(al, session_gap = 3600 * 2):
    """
    Convert a list of RA tuples to the virtual time (VTime) num arrays
    Ideally, the RA tuple should only contains read but not write

    1st    virtual time (increase by one for each access)
    2nd    access obsId


    """
    from collections import Counter

    x = []
    y = [] # reference stream
    yd = [] # reuse distance

    uobsDict = {} #key - obsId, last reference time step
    uobsDict_date = {} # key - obsId, val - last access date

    min_date = None
    max_date = None

    c = 1
    gc.disable()
    for a in al:
        if (a.offline == None): # qarchive
            continue

        if (not min_date):
            min_date = a.date

        max_date = a.date

        ns = 0
        if ((not uobsDict_date.has_key(a.obsId)) or
            (_timeGapInSecs(a.date, uobsDict_date[a.obsId]) > session_gap)):
            ns = 1
        uobsDict_date[a.obsId] = a.date

        if (ns):
            if (not uobsDict.has_key(a.obsId)):
                rud = np.nan # referenced for the first time
            else:
                lastref = uobsDict[a.obsId]
                rud = len(Counter(y[lastref:]).keys()) #excluding last reference itself

            x.append(c)
            y.append(a.obsId)
            yd.append(rud)
            uobsDict[a.obsId] = c
            c += 1

    gc.enable()
    # convert absolute obsid into relative obsId (time sequence)
    import scipy.stats as ss
    ranked_y = ss.rankdata(y)
    return (np.array(x), ranked_y, min_date, max_date, np.array(yd))

def _getObsDateFrmFileId(fileId):
    """
    obsId:    1077283216_20140224132102_gpubox10_01(string)
    """
    if ((fileId is None) or (len(fileId) < 1)):
        return None
    od = fileId.split('_')
    if (len(od) < 2 or od[1].startswith('flags.zip')):
        return None
    try:
        return dt.datetime.strptime(od[1],'%Y%m%d%H%M%S').date()
    except Exception, ex:
        print 'Fail to get date from %s: %s' % (fileId, str(ex))
        return None

def _timeGapInSecs(s1, s0):
    """
    both parameter should be string
    ignore microseconds
    """
    d1 = dt.datetime.strptime(s1,'%Y-%m-%dT%H:%M:%S.%f')
    d0 = dt.datetime.strptime(s0,'%Y-%m-%dT%H:%M:%S.%f')

    return (d1 - d0).seconds

def _raListToReuseDist(al, min_access = 100, session_gap = 3600 * 2):
    """
    al:             A list of RA

    """
    from collections import Counter # this requires Python 2.7

    xd = defaultdict(list) # key - userIp, val - access list X
    yd = defaultdict(list) # key - userIp, val - access list Y
    rd = defaultdict(list) # key - userIp, val - reference list (obsId)

    min_access_date = {} # key - user, value - date (string)
    max_access_date = {} # key - user, value - date (string)

    obsdict = defaultdict(dict) # key - userIp, val - a dict: key - obsId, val - virtual time of last reference
    obsdict_date = defaultdict(dict) # key - userIp, val - a dict: key - obsId, val - actual time of last reference
    ret_dict = {}

    print 'length of al = %d' % len(al)

    gc.disable()

    d1 = 0
    d2 = 0

    for a in al:
        if (a.offline == None): # qarchive
            d1 += 1
            continue
        obsDate = a.obsdate
        if (not obsDate): # no valid observation date
            d2 += 1
            continue

        if (not min_access_date.has_key(a.user)):
            min_access_date[a.user] = a.date

        max_access_date[a.user] = a.date # since al is sorted based on a.date

        ns = 0
        uobsDict = obsdict[a.user]
        uobsDict_date = obsdict_date[a.user]
        if ((not uobsDict_date.has_key(a.obsId)) or
            (_timeGapInSecs(a.date, uobsDict_date[a.obsId]) > session_gap)):
            ns = 1
        uobsDict_date[a.obsId] = a.date
        if (not ns):
            continue
        uas = rd[a.user]
        if (not uobsDict.has_key(a.obsId)):
            rud = np.nan # referenced for the first time
        else:
            lastref = uobsDict[a.obsId]
            rud = len(Counter(uas[lastref:]).keys()) #excluding last reference itself

        yd[a.user].append(rud)
        uxd = xd[a.user]
        thisref = len(uxd) + 1
        uxd.append(thisref)
        uas.append(a.obsId)
        uobsDict[a.obsId] = thisref

    for u, xl in xd.iteritems(): #key - user, val - access list
        if (len(xl) < min_access):
            continue
        x = np.array(xl)
        y = np.array(yd[u])
        ret_dict[u] = (x, y, min_access_date[u], max_access_date[u])

    gc.enable()

    print 'No. of archive = %d, no. of invalid date = %d' % (d1, d2)

    return ret_dict

def _raListToVTimeNAByUser(al, min_access = 100, session_gap = 3600 * 2, y_unit = 'obs'):
    """
    al:             A list of RA
    min_access:     if the number of accesses in a list is less than min_access, this list is disregarded
    session_gap:    time in seconds
                    breaks a session if the last reference of this obsId is session_gap away
                    thus there is a one-one mapping between session -- obsId
                    each session is a point on the X-axis of the final plot
    y_unit:         The unit of the Y-axis, either "day" or "obs"


    Return a dictionary: key - user(ip),
                         val - a tuple
                                 x, y, min_access_date, max_access_date

    """
    xd = defaultdict(list) # key - userIp, val - access list X
    yd = defaultdict(list) # key - userIp, val - access list Y
    cd = defaultdict(int) # key - userIp, val - current counter
    md = {} # key - userIp, val - current min observation date
    ad = {} # key - userIp, val - current max observation date

    min_access_date = {} # key - user, value - date (string)
    max_access_date = {} # key - user, value - date (string)

    obsdict = {} # key - userIp, val - a dict: key - obsId, val - datetime.datetime of last reference
    ret_dict = {}

    print 'length of al = %d' % len(al)

    gc.disable()

    d1 = 0
    d2 = 0

    if ('day' == y_unit):
        yunit = 0
    else:
        yunit = 1


    for a in al:
        if (a.offline == None): # qarchive
            d1 += 1
            continue
        obsDate = a.obsdate
        if (not obsDate): # no valid observation date
            d2 += 1
            continue
        # should we break out a session?
        ns = 0 # new session flag

        if (not min_access_date.has_key(a.user)):
            min_access_date[a.user] = a.date

        """
        if (not max_access_date.has_key(a.user)):
            max_access_date[a.user] = a.date
        elif (a.date > max_access_date[a.user]):
        """
        max_access_date[a.user] = a.date # since al is sorted based on a.date


        if (not obsdict.has_key(a.user)):
            obsdict[a.user] = {}

        uobsDict = obsdict[a.user]

        if ((not uobsDict.has_key(a.obsId)) or
            (_timeGapInSecs(a.date, uobsDict[a.obsId]) > session_gap)):
            ns = 1
        uobsDict[a.obsId] = a.date
        if (ns):
            cd[a.user] += 1
            xd[a.user].append(cd[a.user])
            if (yunit == 0):
                yd[a.user].append(obsDate)
            else:
                yd[a.user].append(a.obsId)
            if (md.has_key(a.user)):
                if (obsDate < md[a.user]):
                    md[a.user] = obsDate
            else:
                md[a.user] = obsDate

            if (ad.has_key(a.user)):
                if (obsDate > ad[a.user]):
                    ad[a.user] = obsDate
            else:
                ad[a.user] = obsDate

    for u, xl in xd.iteritems(): #key - user, val - access list
        if (len(xl) < min_access):
            continue
        x = np.array(xl)
        yl = yd[u]
        if (0 == yunit):
            for i in range(len(yl)):
                yl[i] = (yl[i] - md[u]).days
        y = np.array(yl)
        ret_dict[u] = (x, y, min_access_date[u], max_access_date[u], md[u], ad[u])

    gc.enable()

    print 'No. of archive = %d, no. of invalid date = %d' % (d1, d2)

    return ret_dict

def ra_list_to_csv(al, csv_file):
    """
    convert a list of RA/AA tuples to a csv file
    """
    if (al is None or len(al) == 0):
        return

    """
    RA = namedtuple('RA', 'date obsId offline size user obsdate')
    """
    pattern = '%Y-%m-%dT%H:%M:%S'

    with open(csv_file, "a") as myfile:
        for a in al:
            if (a.offline is None):
                act = -1
            elif (a.offline):
                act = 1
            else:
                act = 0
            tt = a.date.split('.')
            epoch = int(time.mktime(time.strptime(tt[0], pattern)))
            date_int = int("{0}{1}".format(epoch, tt[1]))
            line = "{0},{1},{2},{3},{4},{5}\n".format(date_int,
                                                      a.obsId,
                                                      act,
                                                      a.size,
                                                      a.user,
                                                      a.obsdate)
            myfile.write(line)

    print "Appended {0} items to {1}".format(len(al), csv_file)

def csv_to_ra_list(csv_file):
    """
    read data from csv file, and return the populated RA list
    """
    stt = time.time()
    if (not os.path.exists(csv_file)):
        return None
    import csv
    pattern = '%Y-%m-%dT%H:%M:%S'
    al = []
    gc.disable()
    with open(csv_file, 'rb') as f:
        reader = csv.reader(f)
        al_list = list(reader)
        for a in al_list:
            epoch = int(a[0][:-3])
            time_str = time.strftime(pattern, time.localtime(epoch))
            timestamp = "{0}.{1}".format(time_str, a[0][-3:])
            obsNum = a[1]
            a2 = int(a[2])
            if (-1 == a2):
                isOffline = None
            elif (1 == a2):
                isOffline = True
            else:
                isOffline = False
            fsize = int(a[3])
            userIp = a[4]
            obsDate = a[5]
            re = RA(timestamp, obsNum, isOffline, fsize, userIp, obsDate)
            al.append(re)
    gc.enable()
    print "Loading csv to list took {0} seconds".format(time.time() - stt)
    return al

def fit_csv_for_sqlite(csv_in, csv_out):
    """
    Difference between input/output CSV
    time - from string (input) to epoch integer (output)
    offline - from boolean (T/F/None) to integer (1/0/-1)
    """
    al = csv_to_ra_list(csv_in)
    ra_list_to_csv(al, csv_out)

def sort_obsid_from_sqlite(sqlite_file):
    """
    """
    import sqlite3 as dbdrv
    obs_dict = defaultdict(int)
    query = "SELECT DISTINCT(obs_id) FROM ac WHERE offline > -1 ORDER BY obs_id"
    dbconn = dbdrv.connect(sqlite_file)
    cur = dbconn.cursor()
    cur.execute(query)
    all_obs = cur.fetchall() # not OK if we have millions of obs numbers
    cur.close()

    for c, obsid_row in enumerate(all_obs):
        obs_dict[obsid_row[0]] = c + 1

    return (obs_dict, all_obs[0][0], all_obs[-1][0])

def ralist_to_access_matrix(al, obs_info, num_bins=200, serialise_file=None):
    """
    obs_info:   a tuple of  (1) dictionary (key: obsid, sequence_id)
                            (2) min obs number
                            (3) max obs number
    for each day, we plot the distribution (in the form of heatmap) of
    the number of accesses within a contiguous range of observation numbers.
    """
    # sort the obsId
    obs_dict = obs_info[0]
    min_obs = obs_info[1]
    max_obs = obs_info[2]
    step = int(math.ceil(float(obs_dict[max_obs] - obs_dict[min_obs]) / float(num_bins)))
    obs_range = range(min_obs, max_obs, step)
    dfirst = dt.datetime.strptime(al[0].date,'%Y-%m-%dT%H:%M:%S.%f').date()
    dlast = dt.datetime.strptime(al[-1].date,'%Y-%m-%dT%H:%M:%S.%f').date()
    num_days = int((dlast - dfirst).days) + 1
    data = np.zeros((num_bins, num_days), dtype=np.int)

    for a in al:
        if (a.offline is None): #archive
            continue
        di = dt.datetime.strptime(a.date,'%Y-%m-%dT%H:%M:%S.%f').date()
        # if no retrievals on a particular day, that day will show nothing
        ax = int((di - dfirst).days)
        ay = obs_dict[int(a.obsId)] / step
        data[ay][ax] += 1

    if (serialise_file is not None):
        try:
            np.save(serialise_file, data)
        except Exception, exp:
            print "Fail to serialise matrix to '{0}':{1}".format(serialise_file,
                                                              str(exp))
    return (data, obs_range, num_days)

def plot_access_heatmap(data, obs_range, num_days):
    """
    x_axis = []
    for nd in range(num_days):
        if (nd % 30 == 0):
            x_axis.append(str(nd))
        else:
            x_axis.append(' ')

    y_axis = []
    for y in obs_range:
        if (y % 50 == 0):
            y_axis.append(str(y))
        else:
            y_axis.append(' ')
    """

    fig = pl.figure()
    ax = fig.add_subplot(111)
    heatmap = ax.pcolor(data, cmap=matplotlib.pyplot.cm.Blues)

    # put the major ticks at the middle of each cell
    """
    ax.set_xticks(np.arange(data.shape[0])+0.5, minor=False)
    ax.set_yticks(np.arange(data.shape[1])+0.5, minor=False)

    ax.set_xticklabels(x_axis, minor=False)
    ax.set_yticklabels(y_axis, minor=False)
    """

    pl.show()
    pl.close(fig)


def _raListToNumArray(al):
    """
    Convert a list of RA tuples to the following num arrays:

    1st    date stamp (x1)
    2nd    online access obsId (y1)
    3rd    date stamp (x2)
    4th    offline access obsId (y2)
    5th    date stamp (x3)
    6th    number of offline access (y3)
    7th    date stamp (x4)
    8th    number of online access (y4)
    9th    date stamp (x5)
    10th   total size of each day (y5)
    11th   date stamp (x6)
    12th   ingestion obsId (y6)
    13th   date stamp (x7)
    14th   number of ingestion (y7)
    15th   date stamp (x8)
    16th   ingestion size of each day (y8)
    """
    x1 = []
    y1 = []
    x2 = []
    y2 = []
    x3 = []
    y3 = []
    x4 = []
    y4 = []
    x5 = []
    y5 = []
    x6 = []
    y6 = []
    x7 = []
    y7 = []
    x8 = []
    y8 = []

    x1d = defaultdict(set) # k - date, v - a set of obsNum (set)
    x2d = defaultdict(set)
    x6d = defaultdict(set)

    xy3 = defaultdict(int)
    xy4 = defaultdict(int)
    xy5 = defaultdict(int)
    xy6 = defaultdict(int)
    xy7 = defaultdict(int)

    d0 = dt.datetime.strptime(al[0].date,'%Y-%m-%dT%H:%M:%S.%f').date()
    gc.disable()
    for i in range(len(al)):
        a = al[i]
        di = dt.datetime.strptime(a.date,'%Y-%m-%dT%H:%M:%S.%f').date()
        # if no retrievals on a particular day, that day will show nothing
        ax = int((di - d0).days)
        if (a.offline == None): # qarchive
            #x6.append(ax)
            #y6.append(int(a.obsId))
            x6d[ax].add(a.obsId)
            xy6[ax] += 1
            try:
                #int(a.size)
                xy7[ax] += a.size
            except Exception, exp:
                print "a.size = {1}, exception: {0}".format(str(exp), a.size)
        else:
            if (a.offline):
                #x2.append(ax)
                #y2.append(int(a.obsId)) # miss
                x2d[ax].add(a.obsId)
                xy3[ax] += 1
            else:
                #x1.append(ax)
                #y1.append(int(a.obsId)) # hit
                x1d[ax].add(a.obsId)
                xy4[ax] += 1
            xy5[ax] += a.size

    for k, v in x1d.items():
        for oid in v:
            x1.append(k)
            y1.append(oid)

    for k, v in x2d.items():
        for oid in v:
            x2.append(k)
            y2.append(oid)

    for k, v in x6d.items():
        for oid in v:
            x6.append(k)
            y6.append(oid)

    for k, v in xy3.items():
        x3.append(k)
        y3.append(v)

    for k, v in xy4.items():
        x4.append(k)
        y4.append(v)

    for k, v in xy5.items():
        x5.append(k)
        y5.append(v)

    for k, v in xy6.items():
        x7.append(k)
        y7.append(v)

    for k, v in xy7.items():
        x8.append(k)
        y8.append(v)

    gc.enable()
    return (np.array(x1), np.array(y1), np.array(x2), np.array(y2),
            np.array(x3), np.array(y3), np.array(x4), np.array(y4),
            np.array(x5), np.array(y5), np.array(x6), np.array(y6),
            np.array(x7), np.array(y7), np.array(x8), np.array(y8))

def _getLR(list_of_arr):
    left = sys.maxint
    right = 0

    for arr in list_of_arr:
        if (len(arr)):
            l = min(arr)
            r = max(arr)
            if (l < left):
                left = l
            if (r > right):
                right = r
        else:
            continue

    return (left - 2, right + 2)

def _plotReuseDistance(accessList, archName, fgname):
    """
    Plot per-user based re-use distance

    ax    the figure on which plot should reside, if None, create a new one
    """
    print "Converting to num arrary for _plotReuseDistance"
    stt = time.time()
    ret_dict = _raListToReuseDist(accessList)
    print ("Converting to num array takes %d seconds" % (time.time() - stt))

    c = 0
    for u, na in ret_dict.iteritems():
        if c > 10: # we only produce maximum 10 users
            break
        x = na[0]
        y = na[1]
        min_ad = na[2].split('T')[0]
        max_ad = na[3].split('T')[0]
        c += 1
        uname = 'User%d' % c # we do not want to plot user ip addresses
        print '%s ----> %s' % (u, uname)
        fig = pl.figure()
        ax = fig.add_subplot(111)
        ax.set_xlabel('User access from %s to %s' % (min_ad, max_ad), fontsize = 9)
        ax.set_ylabel('Reuse distance', fontsize = 9)
        ax.set_title("%s archive activity for '%s'" % (archName, uname), fontsize=10)
        ax.tick_params(axis='both', which='major', labelsize=8)
        ax.tick_params(axis='both', which='minor', labelsize=6)

        ax.plot(x, y, color = 'b', marker = 'x', linestyle = '',
                            label = 'access', markersize = 3)

        #legend = ax.legend(loc = 'upper left', shadow=True, prop={'size':7})
        fileName, fileExtension = os.path.splitext(fgname)
        fig.savefig('%s_%s_rud%s' % (fileName, u, fileExtension))
        pl.close(fig)

def _plotVirtualTimePerUser(accessList, archName, fgname, yunit = 'Obs Id'):
    """
    Plot per-user based data access based on relative  time
    """
    print "Converting to num arrary for _plotVirtualTimePerUser"
    stt = time.time()
    if ("Obs Id" == yunit):
        ret_dict = _raListToVTimeNAByUser(accessList, y_unit = 'obs')
    else:
        ret_dict = _raListToVTimeNAByUser(accessList, y_unit = 'day')
    print ("Converting to num array takes %d seconds" % (time.time() - stt))

    c = 0
    for u, na in ret_dict.iteritems():
        if c > 20: # we only produce maximum 20 users
            break
        x = na[0]
        y = na[1]
        min_ad = na[2].split('T')[0]
        max_ad = na[3].split('T')[0]
        min_od = str(na[4])
        max_od = str(na[5])
        c += 1
        uname = 'User%d' % c # we do not want to plot user ip addresses
        print '%s ----> %s' % (u, uname)
        fig = pl.figure()
        ax = fig.add_subplot(111)
        ax.set_xlabel('User access from %s to %s' % (min_ad, max_ad), fontsize = 9)
        ax.set_ylabel('%s from %s to %s' % (yunit, min_od, max_od), fontsize = 9)
        ax.set_title("%s archive activity for '%s'" % (archName, uname), fontsize=10)
        ax.tick_params(axis='both', which='major', labelsize=8)
        ax.tick_params(axis='both', which='minor', labelsize=6)

        ax.plot(x, y, color = 'b', marker = 'x', linestyle = '',
                            label = 'access', markersize = 3)

        #legend = ax.legend(loc = 'upper left', shadow=True, prop={'size':7})
        fileName, fileExtension = os.path.splitext(fgname)
        fig.savefig('%s_%s_pu%s' % (fileName, u, fileExtension))
        pl.close(fig)

def _plotVirtualTime(accessList, archName, fgname, rd_bin_width = 250):
    """
    Plot data access based on virtual time
    """
    print "converting to num arrary for _plotVirtualTime"
    stt = time.time()
    x, y, id, ad, yd = _raListToVTimeNA(accessList)
    print ("Converting to num array takes %d seconds" % (time.time() - stt))
    fig = pl.figure()
    ax = fig.add_subplot(211)
    ax.set_xlabel('Access sequence number (%s to %s)' % (id.split('T')[0], ad.split('T')[0]), fontsize = 9)
    ax.set_ylabel('Observation sequence number', fontsize = 9)
    ax.set_title('%s archive activity ' % (archName), fontsize=10)
    ax.tick_params(axis='both', which='major', labelsize=8)
    ax.tick_params(axis='both', which='minor', labelsize=6)

    ax.plot(x, y, color = 'b', marker = 'x', linestyle = '',
                        label = 'access', markersize = 3)

    #legend = ax.legend(loc = 'upper left', shadow=True, prop={'size':7})

    ax1 = fig.add_subplot(212)
    ax1.set_xlabel('Access sequence number (%s to %s)' % (id.split('T')[0], ad.split('T')[0]), fontsize = 9)
    ax1.set_ylabel('Reuse distance (in-between accesses)', fontsize = 9)

    ax1.tick_params(axis='both', which='major', labelsize=8)
    ax1.tick_params(axis='both', which='minor', labelsize=6)

    ax1.plot(x, yd, color = 'k', marker = '+', linestyle = '',
                        label = 'reuse distance', markersize = 3)

    pl.tight_layout()
    fig.savefig(fgname)
    pl.close(fig)

    y1d = yd[~np.isnan(yd)]
    num_bin = (max(y1d) - min(y1d)) / rd_bin_width
    hist, bins = np.histogram(y1d, bins = num_bin)

    width = 0.7 * (bins[1] - bins[0])
    center = (bins[:-1] + bins[1:]) / 2
    fig1 = pl.figure()
    #fig1.suptitle('Histogram of data transfer rate from Pawsey to MIT', fontsize=14)
    ax2 = fig1.add_subplot(111)
    ax2.set_title('Reuse distance Histogram for %s' % archName, fontsize = 10)
    ax2.set_ylabel('Frequency', fontsize = 9)
    ax2.set_xlabel('Reuse distance (# of observation)', fontsize = 9)

    ax2.tick_params(axis='both', which='major', labelsize=8)
    ax2.tick_params(axis='both', which='minor', labelsize=6)

    pl.bar(center, hist, align='center', width=width)

    fileName, fileExtension = os.path.splitext(fgname)
    fig1.savefig('%s_rud_hist%s' % (fileName, fileExtension))

    pl.close(fig1)

def post_actual_time(acl, series_name, col_name, influx_host, influx_port, influx_db):
    pass

def _plotActualTime(accessList, archName, fgname):
    """
    Plot data access based on actual time
    """
    print "converting to num arrary for _plotActualTime"
    stt = time.time()
    x1, y1, x2, y2, x3, y3, x4, y4, x5, y5, x6, y6, x7, y7, x8, y8 = _raListToNumArray(accessList)
    print ("Converting to num array takes %d seconds" % (time.time() - stt))
    fig = pl.figure()
    if (len(x3) or len(x4)):
        ax = fig.add_subplot(211)
    else:
        ax = fig.add_subplot(111)
    ax.set_xlabel('Time (days)', fontsize = 9)
    ax.set_ylabel('Obs number (GPS time)', fontsize = 9)
    ax.set_title('%s archive activity from %s to %s' % (archName, accessList[0].date.split('T')[0],accessList[-1].date.split('T')[0]), fontsize=10)
    ax.tick_params(axis='both', which='major', labelsize=8)
    ax.tick_params(axis='both', which='minor', labelsize=6)

    ax.plot(x1, y1, color = 'b', marker = 'x', linestyle = '',
                        label = 'online access', markersize = 3)
    if (len(x2)):
        ax.plot(x2, y2, color = 'r', marker = '+', linestyle = '',
                            label = 'offline access', markersize = 3)
    ax.plot(x6, y6, color = 'k', marker = 'o', linestyle = '',
                        label = 'ingestion', markersize = 3, markeredgecolor = 'k', markerfacecolor = 'none')

    left, right = _getLR([x1, x2, x3, x4, x5, x6, x7, x8])

    ax.set_xlim([left, right])
    legend = ax.legend(loc = 'upper left', shadow=True, prop={'size':7})

    if (len(x3) or len(x4) or len(x7)):
        ax1 = fig.add_subplot(212)
        ax1.set_xlabel('Time (days)', fontsize = 9)
        ax1.set_ylabel('Number of files', fontsize = 9)
        ax1.tick_params(axis='both', which='major', labelsize=8)
        ax1.tick_params(axis='both', which='minor', labelsize=6)
        ax1.set_title('Number/Volume of data access and ingestion', fontsize=10)

        if (len(x4)):
            ax1.plot(x4, y4, color = 'b', linestyle = '-', marker = 'x', label = 'online access', markersize = 3)
        if (len(x3)):
            ax1.plot(x3, y3, color = 'r', linestyle = '--', marker = '+', label = 'offline access', markersize = 3)
        if (len(x7)):
            ax1.plot(x7, y7, color = 'k', linestyle = '-.', marker = 'o', label = 'ingestion', markersize = 3, markerfacecolor = 'none')

        ax2 = ax1.twinx()
        ax2.set_ylabel('Data volume (TB)', fontsize = 9)
        ax2.tick_params(axis='both', which='major', labelsize=8)
        ax2.tick_params(axis='both', which='minor', labelsize=6)
        ax2.plot(x5, y5 / 1024.0 ** 4, color = 'g', linestyle = '-.', marker = 's', label = 'access volume',
                 markersize = 3, markeredgecolor = 'g', markerfacecolor = 'none')
        ax2.plot(x8, y8 / 1024.0 ** 4, color = 'm', linestyle = ':', marker = 'd', label = 'ingestion volume',
                 markersize = 3, markeredgecolor = 'm', markerfacecolor = 'none')

        ax1.set_xlim([left, right])

        legend1 = ax1.legend(loc = 'upper left', shadow=True, prop={'size':7})
        legend2 = ax2.legend(loc = 'upper right', shadow=True, prop={'size':7})

    pl.tight_layout()
    fig.savefig(fgname)
    pl.close(fig)

def get_sort_key(ra):
    return ra.date

def processLogs(dirs, fgname, stgline='to stage file:',
                aclobj=None, archName='Pawsey',
                obs_trsh=1.05, vir_time=False,
                per_user=False, reuse_dist=False,
                per_user_y_unit='Obs Id', save_acl_file=None):
    """
    process all logs from a list of directories

    dirs:    a list of directories (list)
    fgname:    the name of the plot figure (including the full path)
    """
    if (aclobj):
        accessList = aclobj
        #accessList = sorted(accessList, key=get_sort_key)
    else:
        accessList = []
        pool = mp.Pool(15)
        for dir in dirs:
            f = []
            for (dirpath, dirnames, filenames) in walk(dir):
                f.extend(filenames)
                break
            if (mp.cpu_count() > 2):
                results = [pool.apply_async(parse_log_thrd, args=('%s/%s' % (dir, fn), stgline, obs_trsh)) for fn in f]
                output = [p.get() for p in results]
                for o in output:
                    accessList += o
            else:
                for fn in f:
                    if fn.endswith('.nglog'):
                        fullfn = '%s/%s' % (dir, fn)
                        parseLogFile(fullfn, accessList, stgline, obs_trsh)

        if (len(accessList) == 0):
            print 'There are no retrieval entries found in the logs'
            return

        print "Sorting......."
        stt = time.time()
        try:
            #accessList.sort() # automatically sort based on date
            accessList = sorted(accessList, key=get_sort_key)
            print ("Sorting takes %d seconds" % (time.time() - stt))
        finally:
            if (save_acl_file and accessList):
                ra_list_to_csv(accessList, save_acl_file)
                #pickleSaveACL(accessList, save_acl_file)

    """
    if (vir_time):
        if (per_user):
            if (not reuse_dist):
                _plotVirtualTimePerUser(accessList, archName, fgname, per_user_y_unit)
            else:
                _plotReuseDistance(accessList, archName, fgname)
        else:
            _plotVirtualTime(accessList, archName, fgname)
    else:
        _plotActualTime(accessList, archName, fgname)
    """
    """
    print "Producing actual time plot......"
    _plotActualTime(accessList, archName, fgname + "_actual_time.pdf")
    print "Producing virtual time plot......"
    _plotVirtualTime(accessList, archName, fgname + "_virtual_time.pdf")
    print "Producing RUD plot......"
    _plotReuseDistance(accessList, archName, fgname + "_rud.pdf")
    """
    return accessList

def _getTidFrmLine(line):
    """
    Obtain the thread id from an NGAS log line

    e.g. '....:httpRedirReply:1783:24459:Thread-225]' --> '225'
    """
    tp = line.rfind(':Thread-')
    if (tp == -1):
        return None
    return line[tp:].split('-')[1][0:-1]

def _getObsNumFrmFileId(fileId, obs_trsh):
    obsNum = fileId.split('_')[0]
    try:
        obsId = int(obsNum)
        if (obsId < 1e9 * obs_trsh):
            obsNum = None
            print ' - - --------- Small obsnum = %d' % obsId
    except ValueError, ve:
        obsNum = fileId.split('.')[0]
        try:
            obsId = int(obsNum)
            if (obsId < 1e9 * obs_trsh):
                obsNum = None
                print ' - - --------- Small obsnum = %d' % obsId
        except ValueError, ve1:
            obsNum = None
    return obsNum

def _replaceAll(rep, text):
    """
    http://stackoverflow.com/questions/6116978/python-replace-multiple-strings

    rep:    a dictionary, key - old, val - new string
    text:   string to be replaced
    return:    a new string
    """
    rep = dict((regx.escape(k), v) for k, v in rep.iteritems())
    pattern = regx.compile("|".join(rep.keys()))
    text = pattern.sub(lambda m: rep[regx.escape(m.group(0))], text)
    return text

def _buildRA(access, isOffline, fsize, obs_trsh):
    """
    Construct the retrieval access tuple from the line
    """
    tokens = access.split(' ')
    timestamp = tokens[0]
    #date = timestamp.split('T')[0]
    #time = timestamp.split('T')[1]

    #clientaddress = tokens[5].split('=')[1].split('\'')[1]
    obsNum = None
    obsDate = None
    atts = regx.split(' - |; ', access)
    is_retrieve = True
    ingSize = 0 # for ingestion only
    userIp = None
    for att in atts:
        atttokens = att.split('=')
        attnm = atttokens[0]
        if (attnm == 'path'):
            #e.g. path=|RETRIEVE?time_out=-1&file_id=1077283216_20140224132102_gpubox10_01.fits|
            ff = att.find('=') # skip the first equal sign
            pathcontent = att[ff + 1:].replace('|', '').split('?')
            verb = pathcontent[0]
            if ('RETRIEVE' == verb):
                path = pathcontent[1]
                tt =  urlparse.parse_qs(path)
                if (not tt.has_key('file_id')):
                    continue
                fileId = tt['file_id'][0] #atttokens[2].split('|')[0]
                obsNum = _getObsNumFrmFileId(fileId, obs_trsh)#fileId.split('_')[0]
                obsDate =  _getObsDateFrmFileId(fileId)
            #elif ('QARCHIVE' == verb):
            #    pass

            #obsDate = fileId.split('_')[1][0:8]
        elif (attnm == 'filename'):
            """
            e.g.
            method=POST - path=|QARCHIVE| - host=146.118.87.251 - content-length=1015030080 - content-type=application/octet-stream
            - authorization=Basic bmdhcy1pbnQ6bmdhcyRkYmE= - content-disposition=attachment;
            filename="1077377712_20140225153458_gpubox05_00.fits"; no_versioning=1 [ngamsServer.py:handleHttpRequest:1537:86486:Thread-208369]
            """
            is_retrieve = False
            fileId = atttokens[1].replace('"', '')
            obsNum = _getObsNumFrmFileId(fileId, obs_trsh)#fileId.split('_')[0]
            obsDate =  _getObsDateFrmFileId(fileId)
        elif (attnm == 'content-length'):
            is_retrieve = False
            try:
                ingSize = int(atttokens[1])
            except:
                pass
        elif (attnm.find("client_address") > -1):
            #cidx = attnm.find("client_address=")
            userIp = _replaceAll(patdict, atttokens[1]).split(',')[0]
            #print ' ---- userIp = %s' % userIp
        #elif (attnm == 'user-agent'):
            #userAgent = atttokens[1].split(' ')[0]
    if (timestamp is None):
        return None
    if (obsNum):
        if (is_retrieve):
            re = RA(timestamp, obsNum, isOffline, fsize, userIp, obsDate)
        else:
            re = RA(timestamp, obsNum, None, ingSize, userIp, obsDate)
    else:
        re = None
    return re

def _buildAA(tid, fnline, fsline, ipline, obs_trsh):
    if (fnline.find('flags.zip') > -1):
        print "Ignore flag file"
        return None

    if (fsline == ''):
        print "None for filesize for {0}".format(tid)
        return None

    if (ipline == ''):
        print "None for client ip for {0}".format(tid)
        return None
    try:
        timestamp = fnline.split(' ')[0]
        if (timestamp is None):
            return None
        fileId = fnline.split()[-2]
        obsNum = _getObsNumFrmFileId(fileId, obs_trsh)
        if (not obsNum):
            return None
        obsDate =  _getObsDateFrmFileId(fileId)
        if (fsline.find('Archive Push/Pull') > -1):
            ingSize = int(fsline.split()[-2])
        else:
            ssp = fsline.split('; Transfer rate:')
            ingSize = int(float(ssp[0].split()[-1][0:-2]) * float(ssp[1].split()[0][0:-4]) * 1024 ** 2)
        if (ipline.find('client_address') > -1):
            userIp = ipline.split('client_address')[1].split("'")[1]
        else:
            userIp = ipline.split('HTTP reply sent to:')[1].split("'")[1]
        return RA(timestamp, obsNum, None, ingSize, userIp, obsDate)
    except Exception, exp:
        print "Error in _buildAA: {0}".format(str(exp))
        return None

def parse_log_thrd(fn, stgline='to stage file:', obs_trsh=1.05):
    if (fn.endswith('.nglog')):
        return parseLogFile(fn, stgline=stgline, obs_trsh=obs_trsh)
    else:
        return []

def parseLogFile(fn, accessList=None, stgline = 'to stage file:', obs_trsh = 1.05):
    """
    parse out a list of RA tuples from a single NGAS log file
    add them to the accessList
    """
    if (accessList is None):
        use_list = []
    else:
        use_list = accessList


    if (not os.path.exists(fn)):
        return

    # need to skip the redirect
    # cmd = 'grep -e RETRIEVE\? -e "Reading data block-wise" -e "to stage file:" -e NGAMS_INFO_REDIRECT %s' % fn
    # cmd = 'grep -e \|RETRIEVE\? -e "%s" -e NGAMS_INFO_REDIRECT -e "Sending data back to requestor" -e \|QARCHIVE\| -e "Successfully handled Archive" %s' % (stgline, fn)
    cmd = 'grep -e \|RETRIEVE\? -e "%s" -e NGAMS_INFO_REDIRECT -e "Sending data back to requestor" -e "Transfer rate" -e "Archive Push/Pull" -e path=\|QARCHIVE\| -e "Successfully handled Archive" -e "HTTP reply sent to:" %s' % (stgline, fn)
    re = execCmd(cmd, failonerror = False, okErr = [256])
    if (re[0] != 0 and re[0] != 256):
        print 'Fail to parse log file %s' % fn
        return

    redrct = []
    #goodarch = []
    stg = {}
    raDict = {}
    arch_size_dict = defaultdict(str)
    arch_ip_dict = defaultdict(str)
    goodarch_dict = {}
    fsize = defaultdict(int) # k - tid, v - size
    lines = re[1].split('\n')

    for li in lines:
        tid = _getTidFrmLine(li)
        if (not tid):
            continue

        if (li.find('|RETRIEVE?') > -1):
            raDict[tid] = li
        elif (li.find('Archive Push/Pull') > -1 or li.find('Transfer rate') > -1):
            arch_size_dict[tid] = li
        # check redirect
        #elif (li.find('|QARCHIVE|') > -1):HTTP reply sent to
        elif (li.find('HTTP reply sent to') > -1 or li.find('|QARCHIVE|') > -1):
            arch_ip_dict[tid] = li
        elif (li.find('NGAMS_INFO_REDIRECT') > -1):
            redrct.append(tid)
        # check staging
        elif (li.find(stgline) > -1):
            stg[tid] = 1
        elif (li.find('Sending data back to requestor') > -1): # get retrieval volume
            try:
                sz = int(li.split('Size: ')[1].split()[0])
                fsize[tid] = sz
            except:
                continue
        elif (li.find('Successfully handled Archive') > -1): # get ingestion volume
            #goodarch.append(tid)
            goodarch_dict[tid] = li

    for tid in redrct:
        if (raDict.has_key(tid)):
            #print "removing %s from %s" % (tid, fn)
            raDict.pop(tid) # remove all redirect requests

    for tid in goodarch_dict.keys():
        aa = _buildAA(tid,
                      goodarch_dict[tid],
                      arch_size_dict[tid],
                      arch_ip_dict[tid],
                      obs_trsh)
        if (aa):
            use_list.append(aa)
        else:
            pass
            #print 'non AA for {0} in {1}'.format(tid, fn)

    for k, v in raDict.items():
        ra = _buildRA(v, stg.has_key(k), fsize[k], obs_trsh)
        if (ra):
            use_list.append(ra)
        else:
            pass
            #print 'none RA for %s in file %s' % (k, fn)

    if (accessList):
        return
    else:
        return use_list

def pickleSaveACL(acl, save_acl_file):
    print 'Serialising FileAccessPattern object to the disk......'
    try:
        output = open(save_acl_file, 'wb')
        stt = time.time()
        pickle.dump(acl, output)
        output.close()
        print 'Time for serialising acl: %.2f' % (time.time() - stt)
    except Exception, e:
        ex = str(e)
        print 'Fail to serialise the acl to file %s: %s' % (save_acl_file, ex)

def pickleLoadACL(options):
    """
    Return the fapDict
    """
    if (os.path.exists(options.load_acl_file)):
        try:
            pkl_file = open(options.load_acl_file, 'rb')
            print 'Loading acl object from file %s' % options.load_acl_file
            acl = pickle.load(pkl_file)
            pkl_file.close()
            return acl
            if (acl == None):
                raise Exception("The acl object is None when reading from the file")
        except Exception, e:
            ex = str(e)
            print 'Fail to load the acl object from file %s' % options.load_acl_file
            raise e
    else:
        print 'Cannot locate the acl object file %s' % options.load_acl_file
        return None

def syncLogFileDir(srcDir, tgtDir, pawsey_stage=False):
    """
    to sync NGAS log files from the source directory to the target directory

    srcDir    source directory, which could be remote (via ssh) (string)
    tgtDir    target directory, which must be local (string)
    """
    valid_suffix = ['.nglog', '.nglog.gz.gz', '.nglog.gz']
    remote_src = False

    if (srcDir.find(":") > -1):
        tmp = srcDir.split(":")
        if (pawsey_stage):
            cmd = 'ssh %s "dmls -l %s/"' % (tmp[0], tmp[1])
            print cmd
        else:
            cmd = 'ssh %s "ls %s"' % (tmp[0], tmp[1])
        remote_src = True
        print 'Login to host: %s' % (tmp[0])
    else:
        cmd = 'ls %s' % srcDir

    srcList = execCmd(cmd)[1].split('\n')

    cmd = 'ls %s' % tgtDir

    tmpTgtlist = execCmd(cmd)[1].split('\n')
    tgtList = []
    for tmpf in tmpTgtlist:
        for sf in valid_suffix:
            if (tmpf.endswith(sf)):
                tgtList.append(tmpf[: -1 * len(sf)]) # e.g. 'abcdc.com' --> 'abcdc'
                break
    newlist = []
    stage_list = []
    #print "   ****" + str(srcList[0:20])
    for srcf in srcList:
        if (pawsey_stage):
            ssrcf = srcf.split()
            srcf = ssrcf[-1]
            offline = ssrcf[-2] != '(DUL)'

        for sf in valid_suffix:
            if (srcf.endswith(sf)):
                basef = srcf[: -1 * len(sf)]
                if (not (basef in tgtList)):
                    newlist.append(srcf)
                    if (pawsey_stage and offline):
                        stage_list.append(srcf)
                break # move to the next 'srcf'

    # print newlist
    # copy all files in the newlist to the target dir
    if len(newlist) < 1:
        print "Nothing to sync!"
        #sys.exit(1)
        return

    if (pawsey_stage and len(stage_list) > 0):
        print "Preparing to stage %d files..." % len(stage_list)
        #cmd = 'ssh %s "cd %s; dmget ' % (tmp[0], tmp[1])
        cmd = 'dmget '
        for sfnm in stage_list:
            cmd += '{0} '.format(sfnm)
        print "cd {0}".format(tmp[1])
        print cmd
        return
        #cmd += '"'
        #re = execCmd(cmd)
        #print "Staging result {0}: {1}".format(re[0], re[1])
        #if (re[0] != 0):
            #sys.exit(1)

    if (remote_src):
        verb = 'scp'
        start_f = '\{'
        end_f = '\}'
    else:
        verb = 'cp'
        start_f = '{'
        end_f = '}'
    cmd = '%s %s/%s%s' % (verb, srcDir, start_f, newlist[0])
    if (len(newlist) > 1):
        for nf in newlist[1:]:
            cmd += ',%s' % nf
    cmd += '%s %s/' % (end_f, tgtDir)

    print "Copying %d files from %s to %s" % (len(newlist), srcDir, tgtDir)
    execCmd(cmd)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-d", "--dir", action="store", type="string", dest="dir", help="directories separated by semicolon")
    parser.add_option("-c", "--srcdir", action="store", type="string", dest="srcdir", help="directories separated by semicolon. If this option is present, the system will sync between srcdir and dir")
    parser.add_option("-o", "--output", action="store", type="string", dest="output", help="output figure name (path)")
    parser.add_option("-s", "--stgline", action="store", type="string", dest="stgline", help="a line representing staging activity")
    parser.add_option("-a", "--saveaclfile", action="store", dest="save_acl_file",
                      type = "string", default = "", help = "Save access list to the CSV file")
    parser.add_option("-l", "--loadaclfile", action="store", dest="load_acl_file",
                      type = "string", default = "", help = "Load access list CSV from the file")
    parser.add_option("-r", "--archname", action="store", type="string", dest="arch_name", help="name of the archive")
    parser.add_option("-e", "--threshold", action="store", type="float", dest="obs_trsh",
                      help = "obs_number threshold, below which accesses will not be counted")
    parser.add_option("-t", "--virtime",
                  action="store_true", dest="vir_time", default = False,
                  help="use virtual time as X-axis")
    parser.add_option("-p", "--peruser", action="store_true", dest="per_user", default = False, help = "plot on a per-user basis")
    parser.add_option("-y", "--yunitisday", action="store_true", dest="yunit_day", default = False, help = "Y unit is day (rather than obs id) for per-user vir time plot")
    parser.add_option("-u", "--reusedist", action="store_true", dest="rud", default = False, help = "plot reuse distance per user")

    (options, args) = parser.parse_args()
    if (None == options.output):
        parser.print_help()
        sys.exit(1)

    if (None == options.dir and (not options.load_acl_file)):
        parser.print_help()
        sys.exit(1)

    if (options.srcdir and (not options.load_acl_file)):
        srcdirs = options.srcdir.split('+')
        dirs = options.dir.split('+')
        if (len(srcdirs) > len(dirs)):
            print 'len(srcdirs) > len(dirs)'
            sys.exit(1)
        # sync all the directory
        tt = raw_input("\nDo you want to sync (remote) ngas log src dir and local ngas dir?(Y/N)\n")
        if (tt == 'Y' or tt == 'y'):
            for i in range(len(srcdirs)):
                print 'Syncing %s and %s' % (srcdirs[i], dirs[i])
                syncLogFileDir(srcdirs[i], dirs[i])
            #sys.exit(1)

    if (not options.load_acl_file):
        print 'Checking directories....'
        dirs = options.dir.split('+')
        for d in dirs:
            unzipLogFiles(d)
    else:
        dirs = None

    if (options.load_acl_file):
        #acl = pickleLoadACL(options)
        acl = csv_to_ra_list(options.load_acl_file)
    else:
        print 'Processing logs...'
        acl = None

    archnm = 'Pawsey'
    if (options.arch_name):
        archnm = options.arch_name

    obs_num_threshold = 1.05
    if (options.obs_trsh):
        obs_num_threshold = options.obs_trsh

    puyunit = "Obs Id"
    if (options.yunit_day):
        puyunit = "Obs Day"

    if (None == options.stgline): #options.stgline = "staging it for"
        acl = processLogs(dirs, options.output, aclobj = acl,
                          archName = archnm, obs_trsh = obs_num_threshold, vir_time = options.vir_time,
                          per_user = options.per_user, reuse_dist = options.rud, per_user_y_unit = puyunit, save_acl_file = options.save_acl_file)
    else:
        acl = processLogs(dirs, options.output, stgline = options.stgline, aclobj = acl,
                          archName = archnm, obs_trsh = obs_num_threshold, vir_time = options.vir_time,
                          per_user = options.per_user, reuse_dist = options.rud, per_user_y_unit = puyunit, save_acl_file = options.save_acl_file)

