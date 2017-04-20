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
# "@(#) $Id: ngamsAlmaCacheCtrlPI.py,v 1.1 2010/06/01 13:17:32 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu  10/06/2014  Created

import commands, os, datetime, sys
import logging
from optparse import OptionParser

# root_dir = '/mnt/gleam/processing/movie'
Gleam_NGAS_root = '/mnt/gleam/NGAS'
Gleam_processing_root = '/mnt/gleam/processing'
ngas_client = "/mnt/gleam/software/bin/ngamsCClient"

numToPol = {'1':'I', '2':'Q', '3':'U', '4':'V', '-5':'XX', '-6':'YY'}
ngas_http_prefix = 'http://store02.icrar.org:7777/RETRIEVE?file_id='

logger = logging.getLogger(__name__)


def execCmd(cmd, failonerror = True, okErr = []):
    re = commands.getstatusoutput(cmd)
    if (re[0] != 0 and not (re[0] in okErr)):
        errMsg = 'Fail to execute command: "%s". Exception: %s' % (cmd, re[1])
        if (failonerror):
            raise Exception(errMsg)
        else:
            print errMsg
    return re

def gotoDir(target_dir, createOnNon = True):
    if (os.path.exists(target_dir)):
        os.chdir(target_dir) # change to working directory, i.e. cd $target_dir
    elif (createOnNon):
        execCmd('mkdir -p %s' % target_dir)
        os.chdir(target_dir)
    else:
        raise Exception('Fail to goto dir %s' % target_dir)

def getFileSize(filename):
    """
    filename:    full path + file name (string)
    """
    return os.path.getsize(filename)

def getGleamVOFiles(src_dir, db_dir):
    """
    query the Gleam VO PostgreSQL db
    and export file info to the gleam_vo.csv
    under the directory db_dir

    src_dir   directory where all the source codes are located
    db_dir    directory where the final gleam_movie.sqlite will reside

    """
    srcSQL = src_dir + '/getGleamVOFiles.sql'
    if (not os.path.exists(srcSQL)):
        raise Exception('Cannot locate src file %s' % srcSQL)

    gotoDir(db_dir)

    csvfile = 'gleam_vo.csv'
    if (os.path.exists(csvfile)):
        execCmd('rm %s' % csvfile)

    cmd = 'psql -h mwa-web.icrar.org -f %s gavo zhl' % (srcSQL)
    execCmd(cmd)


    if (not os.path.exists(csvfile)):
        raise Exception('Fail to produce file %s' % csvfile)
    if (getFileSize(csvfile) == 0):
        raise Exception('Zero size for file %s' % csvfile)

def getGleamNGASFiles(src_dir, db_dir):
    """
    query the Gleam NGAS sqlite db
    and export file info the gleam_file.csv
    src_dir   directory where all the source codes are located
    db_dir    directory where the final gleam_movie.sqlite will reside
    """
    if (db_dir == Gleam_NGAS_root):
        raise Exception('db_dir cannot be the same as the Gleam NGAS root!')

    srcSQL = src_dir + '/getGleamNGASFiles.sql'
    if (not os.path.exists(srcSQL)):
        raise Exception('Cannot locate src file %s' % srcSQL)

    gotoDir(db_dir)

    csvfile = 'gleam_file.csv'
    if (os.path.exists(csvfile)):
        execCmd('rm %s' % csvfile)

    if (os.path.exists('ngas.sqlite')):
        execCmd('rm ngas.sqlite')
    execCmd('cp %s/ngas.sqlite ./' % (Gleam_NGAS_root))
    cmd = "sqlite3 ngas.sqlite < %s" % srcSQL
    execCmd(cmd)

    if (not os.path.exists(csvfile)):
        raise Exception('Fail to produce file %s' % csvfile)
    if (getFileSize(csvfile) == 0):
        raise Exception('Zero size for file %s' % csvfile)

def joinNGASVOFiles(src_dir, db_dir):
    """
    """
    srcSQL = src_dir + '/join_ngas_gleamvo.sql'
    if (not os.path.exists(srcSQL)):
        raise Exception('Cannot locate src file %s' % srcSQL)

    gotoDir(db_dir)

    csvfile = 'gleam_vofile_join.csv'
    sqlite_file = 'gleam_movie.sqlite'
    if (os.path.exists(csvfile)):
        execCmd('rm %s' % csvfile)
    if (os.path.exists(sqlite_file)):
        execCmd('rm %s' % sqlite_file) # should back it up, rather than
    cmd = "sqlite3 %s < %s" % (sqlite_file, srcSQL)
    execCmd(cmd)

    if (not os.path.exists(csvfile)):
        raise Exception('Fail to produce file %s' % csvfile)
    if (getFileSize(csvfile) == 0):
        raise Exception('Zero size for file %s' % csvfile)

    if (not os.path.exists(sqlite_file)):
        raise Exception('Fail to produce file %s' % sqlite_file)
    if (getFileSize(sqlite_file) == 0):
        raise Exception('Zero size for file %s' % sqlite_file)

def _makeMovie(filelist, src_dir, work_dir, resultf):
    """
    Return success 0
           failed  errorcode

    write to log if the movie making failed
    """
    date_obs = filelist[0][0]
    cfreq = filelist[0][1]
    pol = filelist[0][2]

    movie_fn = '%s_%d_%d.avi' % (date_obs, cfreq, pol)

    gotoDir(work_dir)
    execCmd('rm *.fits', failonerror = False)

    for fi in filelist:
        re = execCmd('ln -s %s %s' % (fi[4], fi[3]), failonerror = False)
        if (re[0] != 0):
            logger.error(re[1])

    if (os.path.exists('files.txt')):
        execCmd('rm files.txt', failonerror = False)

    execCmd('ls *.fits > files.txt', failonerror = False)
    re = execCmd('./drift_movie.sh files.txt', failonerror = False)
    if (re[0] != 0):
        logger.error('Fail to make movie %s, Exception: %s' % (movie_fn, re[1]))
        return
    else:
        if (os.path.exists('drift_movie.avi')):
            re = execCmd('mv drift_movie.avi %s' % movie_fn, failonerror = False)
            if (re[0] != 0):
                logger.error('Fail to make movie %s, Exception: %s' % (movie_fn, re[1]))
                return
            else:
                logger.info('Movie %s was made successfully.' % (movie_fn))
        else:
            logger.error('Fail to find newly-made movie %s' % (movie_fn))
            return

    # archive the movie to NGAS
    cmd = "%s -host store02.icrar.org -port 7777 -cmd QARCHIVE -mimeType video/x-msvideo -fileUri %s/%s" % (ngas_client, work_dir, movie_fn)
    re = execCmd(cmd, failonerror = False)
    if (re[0] != 0):
        logger.error('Fail to archive movie %s, Exception: %s' % (movie_fn, re[1]))
    else:
        logger.info('Movie %s was archived successfully.' % (movie_fn))

    # put this one into the CSV file
    resultf.write('\n%s,%d,%s,%s' % (date_obs, cfreq, numToPol[str(pol)], ngas_http_prefix + movie_fn))
    resultf.flush()


def doIt(db_dir, src_dir, work_dir, resultf):
    """
    """
    script_shell = src_dir + '/drift_movie.sh'
    script_python = src_dir + '/pct.py'

    if (not os.path.exists(script_shell)):
        raise Exception('File %s does not exist.' % script_shell)
    if (not os.path.exists(script_python)):
        raise Exception('File %s does not exist.' % script_python)

    execCmd('cp %s %s/' % (script_shell, work_dir))
    execCmd('cp %s %s/' % (script_python, work_dir))

    gotoDir(db_dir)
    sqlite_file = 'gleam_movie.sqlite'
    if (not os.path.exists(sqlite_file)):
        raise Exception('Cannot find gleam_movie.sqlite in %s' % db_dir)

    import sqlite3 as dbdrv
    dbconn = dbdrv.connect(sqlite_file)
    query = "select * from gleamvofile;"
    cur = dbconn.cursor()

    cur.execute(query)
    allfiles = cur.fetchall() # this is ok for now, but not after we have millions of files
    cur.close()

    curList = []
    curKey = ''
    for fi in allfiles:
        k = "%s_%d_%d" % (fi[0], fi[1], fi[2])
        if (not curKey):
            curKey = k
            curList.append(fi)
            continue

        if (k != curKey): # new chunk
            # process all files belong to the last chunk
            if (len(curList) > 2):
                _makeMovie(curList, src_dir, work_dir, resultf)
            # setup the new chunk
            del curList
            curList = [fi]
            curKey = k
        else:
            curList.append(fi)

def rotateLogResultFiles(logfile, result_file):
    """
    """
    dt = datetime.datetime.now()
    timestr = dt.strftime('%Y-%m-%dT%H-%M-%S')
    if (os.path.exists(logfile)):
        #move it to another file name with timestamp
        rlognm = Gleam_processing_root + '/gleam_movie_' + timestr + '.log'
        execCmd('mv %s %s' % (logfile, rlognm))


    if (os.path.exists(result_file)):
        #move it to another file name with timestamp
        resultnm = Gleam_processing_root + '/gleam_movie_result_' + timestr + '.csv'
        execCmd('mv %s %s' % (result_file, resultnm))


if __name__ == "__main__":
    """
    """
    # get options correct
    parser = OptionParser()
    parser.add_option("-s", "--src_dir", action="store", type="string", dest="src_dir", help="source directory (required)")
    parser.add_option("-d", "--db_dir", action="store", type="string", dest="db_dir", help="database directory (required)")
    parser.add_option("-w", "--work_dir", action="store", type="string", dest="work_dir", help="working directory")

    parser.add_option("-r", "--rec_db",
                  action="store_true", dest="recreate_db", default = False,
                  help="Whether to recreate database")

    parser.add_option("-o", "--db_only",
                  action="store_true", dest="db_only", default = False,
                  help="Only recreate database, do not launch movie making procedure")

    (options, args) = parser.parse_args()
    if (None == options.src_dir or None == options.db_dir):
        parser.print_help()
        sys.exit(1)

    if ((not options.db_only) and None == options.work_dir):
        print "Must specify work_dir for the non-db_only scenario!"
        parser.print_help()
        sys.exit(1)

    logfile = Gleam_processing_root + '/gleam_movie.log'
    result_file = Gleam_processing_root + '/gleam_movie_result.csv' # this will be uploaded to google docs later
    rotateLogResultFiles(logfile, result_file)

    # setup run-time log file
    FORMAT = "%(asctime)-15s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(filename = logfile, level=logging.DEBUG, format = FORMAT)
    logger.info('gleamMovieMgr Started.......')

    # setup run-time result file
    f = open(result_file, 'wb')
    f.write('obs_date,center_frequency,polarisation,ngas_url\n')

    # run tasks
    if (options.recreate_db):
        getGleamNGASFiles(options.src_dir, options.db_dir)
        getGleamVOFiles(options.src_dir, options.db_dir)
        joinNGASVOFiles(options.src_dir, options.db_dir)

    if (not options.db_only):
        doIt(options.db_dir, options.src_dir, options.work_dir, f)

    # close result file
    if (f):
        f.close()

    rotateLogResultFiles(logfile, result_file)







