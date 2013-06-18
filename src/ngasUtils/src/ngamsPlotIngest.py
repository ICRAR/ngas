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
        self.x = []
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
                        '          2013-06 will produce daily stats for June 2013')

        myparser.add_argument('--db', dest='db', type=str,
                   default='ICRAR',
                   help='The database to be used [ICRAR] or MIT')


        args = myparser.parse_args(iargs)
        self.date = args.date
        self.db = args.db
        try:
            dt = datetime.datetime.strptime(args.date,'%Y-%m')
            self.mode = ['Daily', 'Day']
            mr = monthrange(dt.year, dt.month)
            self.fdate = "'%s-%02dT24:00:00.000'"
            self.loop = mr[1]
        except ValueError:
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
            (date_part('epoch', to_timestamp(max(ingestion_date),'YYYY-MM-DD"T"HH24:MI:SS.MS')- to_timestamp(min(ingestion_date),'YYYY-MM-DD"T"HH24:MI:SS.MS')) + 10.)/(1024^2) as average, 
            max(ingestion_date) as last , min(ingestion_date) as first , sum(uncompressed_file_size)/1024^4 as volume from ngas_files where ingestion_date between {0} and {1}"""
            try:
                t=dbpass
            except NameError:
                dbpass = getpass.getpass('%s DB password: ' % self.db)
            dbconn=dbdrv.connect(database="ngas", user="ngas",password=dbpass,host=self.DB[self.db])
        else:
            import sqlite3 as dbdrv
            hsql="""select count(file_id),
            sum(uncompressed_file_size)/(strftime('%s',max(ingestion_date))-strftime('%s',min(ingestion_date)))/1024./1024. as average, 
            max(ingestion_date) as last , min(ingestion_date) as first , sum(uncompressed_file_size)/1024/1024/1024./1024. as volume from ngas_files where ingestion_date between {0} and {1}"""
            dbconn = dbdrv.connect(self.db)
        cur = dbconn.cursor()
        res = []
        for ii in range(1,self.loop+1):
            ssql = hsql.format(self.fdate % (self.date, (ii-1)), self.fdate % (self.date,ii))
            cur.execute(ssql)
            r = cur.fetchall()
            res.append(r)
            self.ssql = ssql
        res = pylab.array(res)
        x = res[:,:,1].reshape(len(res))
        x[pylab.where(x < -1)]=0

        n = res[:,:,0].reshape(len(res))
        n[pylab.where(n < -1)]=0

        self.x = pylab.float16(x)
        self.n = pylab.float16(n)
        vol = pylab.float16(res[:,:,4])
        self.tvol = vol[vol>0].sum()
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
        ax1.bar(pylab.where(self.x>=0)[0]+1.1,self.x)
        ax1.xaxis.axes.set_autoscalex_on(False)
        ax1.plot([0,self.loop],[pylab.median(self.x),pylab.median(self.x)])
        ax1.plot([0,self.loop],[pylab.mean(self.x),pylab.mean(self.x)])

        ax2 = ax1.twinx()
        ax2.xaxis.axes.set_autoscalex_on(False)
        ax2.plot(pylab.where(self.n>=0)[0]+1.5,self.n,'r-', marker='o')

        for tl in ax2.get_yticklabels():
            tl.set_color('r')
        ax2.set_ylabel('Number of files',{'color':'r'})

        if self.mode[1] == 'Day':
            fig.canvas.set_window_title('%s: %s' % (self.db,self.dt.strftime('%B %Y')))
            ax2.set_title('%s %s transfer rate: %s' % (self.db, self.mode[0], self.dt.strftime('%B %Y')))
        else:
            fig.canvas.set_window_title('%s: %s' % (self.db,self.dt.strftime('%d %B %Y')))
            ax2.set_title('%s %s transfer rate: %s' % (self.db, self.mode[0], self.dt.strftime('%d %B %Y')))

        pylab.text(0.99,0.95,'Total: %5.2f TB' % self.tvol,transform = ax1.transAxes,ha='right')
        fig.show()



if __name__ == '__main__':
    t = throughputPlot(sys.argv[1:],)
    t.queryDb()
    t.plot()
    raw_input('Press ENTER to continue....')
