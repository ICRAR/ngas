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
"""
Module to create throughput statistics plots.
"""

import pylab, argparse
from pylab import median, mean, where
import sys, datetime
from calendar import monthrange
import getpass

class throughputPlot():
    """
    Class encapsulates the DB query and preparation of the ingest
    statistics of the main ICRAR or the MIT NGAS DB. Depending on
    the input date format the statistics will be done daily for a
    whole month or hourly for one day.
    """
    def __init__(self, args):

        self.DB = {'ICRAR':'192.102.251.250', 'MIT':'eor-02.mit.edu'}
        self.mode = []
        self.y = []
        self.n = []
        self.tvol = 0
        self.fdate = ""
        self.loop = 0
        self.parser(args)

    def parser(self, iargs):
        """
        Main command line parser.

        INPUT:
           iargs:   list of strings, example ['2013-06-12','--db=MIT']

        OUTPUT: None
        """
        myparser = argparse.ArgumentParser(description=throughputPlot.__doc__)

        myparser.add_argument('date', metavar='date', type=str,
                   help='a date to gather the statistics for.' +\
                        'Examples: 2013-06-10 will produce hourly stats for that day.'+
                        '          2013-06 will produce daily stats for June 2013'+
                        '          2013 will produce weekly stats for the whole of 2013')

        myparser.add_argument('--db', dest='db', type=str,
                   default='ICRAR',
                   help='The database to be used [ICRAR] or MIT')


        args = myparser.parse_args(iargs)
        self.date = args.date
        self.db = args.db
        dc = self.date.count('-')
        if dc == 0:
            # Just a year given: switch to weekly mode
            try:
                dt = datetime.datetime.strptime(args.date,'%Y')
                self.start_date = dt
                self.mode = ['Weekly', 'Week']
                mr = monthrange(dt.year, dt.month)
                self.fdate = "'%04-%dT00:00:00.000'"
                self.loop = 53 # cover the whole year
                self.drange =[]
                _construct_drange(self)

            except ValueError:
                raise
                sys.exit()

        if dc == 1:
            # Year and month given: switch to daily mode
            try:
                dt = datetime.datetime.strptime(args.date,'%Y-%m')
                self.mode = ['Daily', 'Day']
                mr = monthrange(dt.year, dt.month)
                self.fdate = "'%s-%02dT24:00:00.000'"
                self.loop = mr[1]
            except ValueError:
                raise
                sys.exit()
        elif dc == 2:
            # Year, month, day given: switch to hourly mode
            try:
                dt = datetime.datetime.strptime(args.date,'%Y-%m-%d')
                self.mode = ['Hourly','Hour']
                self.fdate = "'%sT%02d:00:00.000'"
                self.loop = 24
            except:
                raise
                sys.exit()
        self.dt = dt
        return


def _construct_drange(self):
    """
    Helper method to construct the date range for the Weekly mode.
    """
    dt = self.start_date # initialise the loop
    for ii in range(self.loop):
        delt=datetime.timedelta(days=(7-dt.weekday()))
        de=dt+delt
        if de.year > dt.year:
            de=datetime.datetime.strptime('{0}-12-31'.format(dt.year),'%Y-%m-%d')
        self.drange.append([dt.strftime("'%Y-%m-%dT00:00:00.000'"),de.strftime("'%Y-%m-%dT00:00:00.000'")])
        dt=de


    def queryDb(self):
        """
        Execute the DB queries for a month or 24 hours depending on self.mode.

        INPUT: None
        OUTPUT: None

        SIDE EFFECTS: popultes required arrays for plotting
        """
        if self.db in self.DB.keys():
            import psycopg2 as dbdrv
            hsql="""select count(file_id), sum(uncompressed_file_size)/
            (date_part('epoch', to_timestamp(max(ingestion_date),'YYYY-MM-DD"T"HH24:MI:SS.MS')-
            to_timestamp(min(ingestion_date),'YYYY-MM-DD"T"HH24:MI:SS.MS')) + 10.)/(1024^2) as average,
            max(ingestion_date) as last , min(ingestion_date) as first ,
            sum(uncompressed_file_size)/1024^4 as volume from
            ngas_files where ingestion_date between {0} and {1}"""
            try:
                t=dbpass
            except NameError:
                dbpass = getpass.getpass('%s DB password: ' % self.db)
            dbconn=dbdrv.connect(database="ngas", user="ngas",password=dbpass,host=self.DB[self.db])
        else:
            import sqlite3 as dbdrv
            hsql="""select count(file_id),
            sum(uncompressed_file_size)/(strftime('%s',max(ingestion_date))-
            strftime('%s',min(ingestion_date)))/1024./1024. as average,
            max(ingestion_date) as last , min(ingestion_date) as first ,
            sum(uncompressed_file_size)/1024/1024/1024./1024. as volume
            from ngas_files where ingestion_date between {0} and {1}"""
            dbconn = dbdrv.connect(self.db)
        cur = dbconn.cursor()
        res = []
        for ii in range(1,self.loop+1):
            if self.mode[0] != 'Weekly':
                ssql = hsql.format(self.fdate % (self.date, (ii-1)), self.fdate % (self.date,ii))
            else:
                ssql = hsql.format(self.drange[ii-1][0], self.drange[ii-1][1])
            cur.execute(ssql)
            r = cur.fetchall()
            res.append(r)
            self.ssql = ssql
        res = pylab.array(res)
        y = res[:,:,1].reshape(len(res))
        y[where(y < -1)]=0

        n = res[:,:,0].reshape(len(res))
        n[where(n < -1)]=0

        self.y = pylab.float16(y)
        self.n = pylab.float16(n)
        vol = pylab.float16(res[:,:,4])
        self.tvol = vol[vol>0].sum()
        self.tfils = n[n>0].sum()
        self.res=res
        dbconn.close()
        del(dbconn)

        return


    def plot(self):
        """
        Plot the statistics.

        INPUT: None
        OUTPUT: None

        SIDE EFFECTS: Generates a plot, depending on the output mode of pylab
             this may open a ne window.
        """
        fig = pylab.figure()

        ax1 = fig.add_subplot(111)
        ax1.set_xlabel(self.mode[1])
        ax1.set_ylabel('MB/s')
        ax1.set_xlim([0,self.loop+0.5])
        ax1.bar(where(self.y>=0)[0]+0.1,self.y)
        ax1.xaxis.axes.set_autoscalex_on(False)
        ax1.plot([0,self.loop+0.5],[median(self.y[where(self.y > 0)]),
                                    median(self.y[where(self.y > 0)])])
        ax1.plot([0,self.loop+0.5],[mean(self.y[where(self.y > 0)]),
                                    mean(self.y[where(self.y > 0)])])

        pylab.text(0.02,0.95,'Median: %5.2f MB/s'
                   % median(self.y[where(self.y > 0)]),
                   transform = ax1.transAxes,ha='left', va='bottom', color='b',
                   fontsize=10)
        pylab.text(0.02,0.95,'Mean: %5.2f MB/s'
                   % mean(self.y[where(self.y > 0)]),
                   transform = ax1.transAxes,ha='left', va='top', color='g',
                   fontsize=10)

        ax2 = ax1.twinx()
        ax2.xaxis.axes.set_autoscalex_on(False)
        ax2.plot(where(self.n>=0)[0]+0.5,self.n,'r-', marker='o')

        for tl in ax2.get_yticklabels():
            tl.set_color('r')
        ax2.set_ylabel('Number of files',{'color':'r'})

        if self.mode[1] == 'Day':
            fig.canvas.set_window_title('%s: %s' % (self.db,self.date))
            ax2.set_title('%s %s transfer rate: %s' % (self.db, self.mode[0], self.date))
        else:
            fig.canvas.set_window_title('%s: %s' % (self.db,self.date))
            ax2.set_title('%s %s transfer rate: %s' % (self.db, self.mode[0], self.date))

        pylab.text(0.99,0.95,'Total: %5.2f TB' % self.tvol,transform = ax1.transAxes,ha='right', va='bottom')
        pylab.text(0.99,0.95,'Total # files: %8d' % self.tfils,transform = ax1.transAxes,ha='right', va='top')
        fig.show()



if __name__ == '__main__':
    t = throughputPlot(sys.argv[1:],)
    t.queryDb()
    t.plot()
    raw_input('Press ENTER to continue....')
