#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2014
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
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      17/June/2014  Created

"""
Manages multiple runs of diskTest, collects results,
and products integrated plots
"""
import os, sys, commands, datetime, time
from collections import defaultdict
from optparse import OptionParser


PERF_COMPUTE = 1
PERF_WRITE = 2
PERF_INTERNAL = 3 # compute + write but without file closing
PERF_TOTAL = 4 # including everything
PERF_FILECLOSING = 5

MODE_CRC_ONLY = 1
MODE_WRITE_ONLY = 2
MODE_CRC_WRITE = 3

RUN_TEST_ONLY = 1
RUN_PLOT_ONLY = 2
RUN_TEST_N_PLOT = 3

megabytes = 1024 ** 2
gigabytes = 1024 ** 3

DEFAULT_FNM = 'bspeed.pkl'

perf_key_string = {PERF_COMPUTE:'CRC throughput:', PERF_WRITE:'Pure write throughput:',
              PERF_INTERNAL:'Internal throughput', PERF_TOTAL:'Total throughput', PERF_FILECLOSING:'File closing time:'}

perf_key_label = {PERF_COMPUTE:'Pure compute throughput', PERF_WRITE:'Pure write throughput',
              PERF_INTERNAL:'Internal throughput', PERF_TOTAL:'Total throughput', PERF_FILECLOSING:'File closing time'}

mode_key_string = {MODE_CRC_ONLY:'-u -c b', MODE_WRITE_ONLY:'-w', MODE_CRC_WRITE:'-w -c b'}

mode_key_label = {MODE_CRC_ONLY:'CRC Only', MODE_WRITE_ONLY:'Write Only', MODE_CRC_WRITE:'CRC and Write'}

resultf_key_string = {MODE_CRC_ONLY:'C_only', MODE_WRITE_ONLY:'W_only', MODE_CRC_WRITE:'C_and_W'}

resultf_mode_mapping = {'C_only':MODE_CRC_ONLY, 'W_only':MODE_WRITE_ONLY, 'C_and_W':MODE_CRC_WRITE}

io_mode = {1:'', 2:' -l async ', 3:' -l direct ', 4:'-l sync '}
io_mode_label = {1:'Normal I/O', 2:'Low-level-async I/O', 3:'Low-level-direct I/O', 4:'Low-level-sync I/O'}
io_mode_simlabel = {1:'Normal', 2:'Async', 3:'Direct', 4:'Sync'}

pl_c = ['r', 'b', 'g', 'k', 'm', 'y', 'c']
pl_m = ['o', '+', 'x', 's', '*', '^', 'D']
pl_l = ['-', '--', ':', '-.', '-', '--', ':']

pl_c1 = ['k', 'm', 'y', 'c', 'g']
pl_m1 = ['s', '*', '^', 'D', 'x']
pl_l1 = ['-.', '-', '--', ':', '-.']

plotlib_imported = False

def execCmd(cmd, failonerror = True, okErr = []):
    re = commands.getstatusoutput(cmd)
    if (re[0] != 0 and not (re[0] in okErr)):
        errMsg = 'Fail to execute command: "%s". Error code %s, Exception: %s' % (cmd, str(re[0]), re[1])
        if (failonerror):
            raise Exception(errMsg)
        else:
            print errMsg
    return re

def getResultFn(mode):
    """
    """
    prefix = resultf_key_string[mode]
    timestr = datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S')
    return "%s_%s" % (prefix, timestr)

def _parseDiskTestResult(resultf, perf = PERF_WRITE):
    """
    resultf:    the full path of the result file (string)
    perf:        which performance to measure (integer)
    """
    import numpy as np
    if (not os.path.exists(resultf)):
        raise Exception('File does not exist %s' % resultf)
    if (not perf_key_string.has_key(perf)):
        raise Exception('Invalid mode %s' % str(perf))

    ks = perf_key_string[perf]
    cmd = 'grep "%s" %s' % (ks, resultf)
    re = execCmd(cmd, failonerror = False, okErr = [256])
    nums = []
    if (0 == re[0]):
        for line in re[1].split('\n'):
            num = float(line.split(':')[1].split()[0])
            nums.append(num)

    if (len(nums) == 0):
        nums.append(0)
    return np.array(nums)

def runTest(pybin, dtSrc, testDir, blocksize, mode, numoffiles, perfs, resultdir = None, iomode = 1, iosize = 1, runmode = RUN_TEST_ONLY):
    """
    perfs    a list of performance items (list)

    Return    a dictionary (key: performance item, value: performance numarray
    """
    if (not resultdir):
        resultdir = os.getcwd()
    elif (not os.path.exists(resultdir)):
        raise Exception("Path %s does not exist" % resultdir)

    resultf = "%s/%s.txt" % (resultdir, getResultFn(mode))
    cmd = "%s %s -d %s -b %d -m %s -t %d -i %ld %s > %s" % (pybin, dtSrc, testDir, blocksize, mode_key_string[mode], numoffiles, iosize * gigabytes, io_mode[iomode], resultf)
    print cmd
    execCmd(cmd)
    dict_nums = None
    if (runmode == RUN_TEST_N_PLOT):
        dict_nums = {}
        for perf in perfs:
            dict_nums[perf] = _parseDiskTestResult(resultf, perf)

    # move the pickle file as well
    spdfile = '%s/%s' % (os.getcwd(), DEFAULT_FNM)
    newspdfile = '%s/%s_%dM.pkl' % (resultdir, mode_key_label[mode].replace(' ', '_'), (blocksize / megabytes))
    if (os.path.exists(spdfile)):
        execCmd('mv %s %s' % (spdfile, newspdfile), failonerror = False)

    return dict_nums

class dictOfDictOfList(dict):
    """
    ordering [perf][mode][type][time_step]
    """
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = defaultdict(list)
            return value

def buildDict_result(resultdir):
    import numpy as np
    if (not os.path.exists(resultdir)):
        raise Exception('Result directory %s does not exist' % resultdir)
    dict_result = dictOfDictOfList()
    cmd = 'ls %s' % resultdir
    files = execCmd(cmd)[1].split('\n')
    prefix = resultf_mode_mapping.keys()
    for f in files:
        mode = ''
        for p in prefix:
            if f.startswith(p):
                mode = resultf_mode_mapping[p]
                break
        if (mode):
            dict_nums = {}
            for perf in perf_key_string.keys():
                na = _parseDiskTestResult('%s/%s' % (resultdir, f), perf)
                li = dict_result[perf][mode]
                if (len(li) == 0):
                    li.append([])
                    li.append([])
                li[0].append(np.mean(na))
                li[1].append(np.std(na))
    return dict_result

def importPlotsLib():
    global plotlib_imported
    if (not plotlib_imported):
        import matplotlib
        # Force matplotlib to not use any Xwindows backend.
        matplotlib.use('Agg')
        plotlib_imported = True
    import pylab as pl
    return pl

def plotThruputVsBlocksize(dict_result, resultdir, iomode = 1, iosize = 1):
    """
    Each performance item produces on plot, and each mode is a line
    """
    import numpy as np
    block_list_len = 10
    x = 2 ** (np.array(range(block_list_len)))
    pl = importPlotsLib()
    if (dict_result == None):
        dict_result = buildDict_result(resultdir)

    for perf in dict_result.keys(): # each perf item produces one plot
        fig = pl.figure()
        ax = fig.add_subplot(111)
        ax.set_xlabel('Block size (MB)', fontsize = 9)
        if (perf == PERF_FILECLOSING):
            ylabel = 'Seconds'
        else:
            ylabel = 'Throughput (MB/s)'
        ax.set_ylabel(ylabel, fontsize = 9)
        ax.set_title("Comparison of running modes for performance '%s', %dGB file, %s" % (perf_key_label[perf], iosize, io_mode_label[iomode]), fontsize = 10)
        ax.tick_params(axis='both', which='major', labelsize = 8)
        ax.tick_params(axis='both', which='minor', labelsize = 6)
        ax.set_xscale('log', basex = 2)

        perfitem = dict_result[perf]
        i = 0
        for mode in perfitem.keys(): # each mode produces a line
            y = np.array(perfitem[mode][0])
            y_err = np.array(perfitem[mode][1])
            ax.errorbar(x, y, y_err, ecolor = pl_c[i % len(pl_c)], marker = pl_m[i % len(pl_m)],
                    linestyle = pl_l[i % len(pl_l)], label = mode_key_label[mode], markersize = 5,
                    markeredgecolor = pl_c[i % len(pl_c)], markerfacecolor = 'none', color = pl_c[i % len(pl_c)])
            i += 1

        legend = ax.legend(loc = 'upper left', shadow=True, prop={'size':8})
        fgname = '%s/%s_%s.png' % (resultdir, io_mode_simlabel[iomode], perf_key_label[perf].replace(' ', '_'))
        fig.savefig(fgname)
        pl.close(fig)

def plotThruputBlocksizeReordering(dict_result, resultdir, iomode = 1, iosize = 1):
    """
    reorder the result for ploting
    Each mode produces one plot, and each performance item is a line
    """
    import numpy as np
    block_list_len = 10
    x = 2 ** (np.array(range(block_list_len)))
    pl = importPlotsLib()
    if (dict_result == None):
        dict_result = buildDict_result(resultdir)

    # we need: [mode][perf]
    real_dict_result = dictOfDictOfList()
    for perf, k_mode_v_li in dict_result.iteritems():
        for mode, li in k_mode_v_li.iteritems():
            real_dict_result[mode][perf] = li

    for mode in real_dict_result.keys(): # each mode products on plot
        fig = pl.figure()
        ax = fig.add_subplot(111)
        ax.set_xlabel('Block size (MB)', fontsize = 9)
        ax.set_ylabel('Throughput (MB/s)', fontsize = 9)
        ax.set_title("Comparison of performances for running mode '%s', %dGB file, %s" % (mode_key_label[mode], iosize, io_mode_label[iomode]), fontsize = 10)
        ax.tick_params(axis='both', which='major', labelsize = 8)
        ax.tick_params(axis='both', which='minor', labelsize = 6)
        ax.set_xscale('log', basex = 2)

        modeitem = real_dict_result[mode]
        i = 0
        for perf in modeitem.keys(): # each mode produces a line
            y = np.array(modeitem[perf][0])
            y_err = np.array(modeitem[perf][1])
            ax.errorbar(x, y, y_err, ecolor = pl_c1[i % len(pl_c1)], marker = pl_m1[i % len(pl_m1)],
                    linestyle = pl_l1[i % len(pl_l1)], label = perf_key_label[perf], markersize = 5,
                    markeredgecolor = pl_c1[i % len(pl_c1)], markerfacecolor = 'none', color = pl_c1[i % len(pl_c1)])
            i += 1

        legend = ax.legend(loc = 'upper left', shadow=True, prop={'size':8})
        fgname = '%s/%s_%s.png' % (resultdir, io_mode_simlabel[iomode], mode_key_label[mode].replace(' ', '_'))
        fig.savefig(fgname)
        pl.close(fig)


def testThruoutVsBlocksize(pybin, dtSrc, testdir, resultdir, runmode = RUN_TEST_ONLY, iomode = 1, iosize = 1):
    """
    orchestrate multiple test runs
    """
    dict_result = None
    block_list_len = 10
    #x = 2 ** (np.array(range(block_list_len)))
    blockszlist = []

    for i in range(block_list_len):
        blockszlist.append(2 ** i)

    if (runmode != RUN_PLOT_ONLY):
        sessionId = str(time.time()).split('.')[0]
        resultdir = resultdir + '/' + sessionId
        cmd = 'mkdir -p %s' % resultdir
        execCmd(cmd)

        # ordering [perf][mode][type][time_step], (type - mean or std)
        if (runmode == RUN_TEST_N_PLOT):
            dict_result = dictOfDictOfList()
        """
        defaultdict(dict) # key is perf
        dict_mode = defaultdict(list) # key is mode, value is a list of tuple (mean, std)
        """
        for mode in mode_key_string.keys():
            for blocksize in blockszlist:
                dict_nums = runTest(pybin, dtSrc, testdir + '/%s_f' % sessionId, blocksize * megabytes, mode, 10, perf_key_string.keys(), resultdir, iomode = iomode, iosize = iosize, runmode = runmode)
                # corner turning
                if (runmode == RUN_TEST_N_PLOT):
                    import numpy as np
                    for perf in dict_nums.keys():
                        na = dict_nums[perf]
                        li = dict_result[perf][mode]
                        if (len(li) == 0):
                            li.append([])
                            li.append([])
                        li[0].append(np.mean(na))
                        li[1].append(np.std(na))
    if (runmode != RUN_TEST_ONLY):
        plotThruputVsBlocksize(dict_result, resultdir, iomode = iomode, iosize = iosize)
        plotThruputBlocksizeReordering(dict_result, resultdir, iomode = iomode, iosize = iosize)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-p", "--pybin", action="store", type="string", dest="pybin", help="python executable (e.g. /home/virtualenv/bin/python)")
    parser.add_option("-s", "--source", action="store", type="string", dest="dtsrc", help="diskTest source file (e.g. /home/chen/diskTest.py)")
    parser.add_option("-t", "--testdir", action="store", type="string", dest="testdir", help="Directory being tested")
    parser.add_option("-r", "--resultdir", action="store", type="string", dest="resultdir", help="Directory for results (text and plots)")
    parser.add_option("-i", "--iosize", action="store", type="int", dest="iosize", help = "File size in GB")
    parser.add_option("-m", "--iomode", action="store", type="int", dest="iomode", help = "types of io, 1:normal, 2:lowlevel-direct, 3:lowlevel-async, 4:lowlevel-sync")
    parser.add_option("-u", "--runmode", action="store", type="int", dest="runmode", help = "Running mode: 1 - Test only, 2 - Plot only, 3 - Test and Plot")

    (options, args) = parser.parse_args()

    if (options.runmode):
        run_mode = options.runmode
    else:
        run_mode = RUN_TEST_ONLY

    if ((run_mode != RUN_PLOT_ONLY) and (None == options.pybin or None == options.dtsrc or
        None == options.testdir or None == options.resultdir)):

        parser.print_help()
        sys.exit(1)

    if (run_mode == RUN_PLOT_ONLY and None == options.resultdir):
        parser.print_help()
        sys.exit(1)

    if (options.iosize):
        io_size = options.iosize
    else:
        io_size = 1

    if (options.iomode):
        lio_mode = options.iomode
    else:
        lio_mode = 1



    testThruoutVsBlocksize(options.pybin, options.dtsrc, options.testdir, options.resultdir, runmode = run_mode, iomode = lio_mode, iosize = io_size)







