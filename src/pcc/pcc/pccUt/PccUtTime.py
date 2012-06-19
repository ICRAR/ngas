#******************************************************************************
# ESO/DFS
#
# "@(#) $Id: PccUtTime.py,v 1.1 2008/07/02 21:48:10 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  04/05/2000  Created
# szampier  09/11/2000  added class TimeStamp
# jknudstr  16/11/2000  Added TimeStamp.getSybaseTimeStamp()
#

"""
Module providing utilities for manipulating time stamps.
"""

import time, string, types, math


# Modified julian date for 1970
MJD_1970 = 40587


class TimeStamp:
    """
    Method definitions for class TimeStamp, a class representing a timestamp
    value in either MJD (modified julian day) or ISO-8601 string format
    (YYYY-MM-DDThh:mm:ss[.mss]).
    """

    def __init__(self,
                 val = None):
        """
        Constructor method initializing class variables.

        val:  Timestamp. Can be in MJD-OBS format, ISO8601 format.
              If not set the present time is taken (string|double|None).
        """
        if isinstance(val, types.NoneType) == 1:
            self.initFromNow()
        elif isinstance(val, types.FloatType) == 1:
            self.initFromMjd(val)
        elif isinstance(val, types.StringType) == 1:
            self.initFromTimeStamp(val)
        else:
            print "ERROR> can't convert %s to a datetime" % (str(val))
            self.__status = 1


    def status(self):
        """
        Get status value. If 1 is returned a problem has been encountered.

        Returns:   Status flag (0=OK, 1=FAILURE).
        """
        return self.__status

    
    def initFromNow(self):
        """
        Initialize with the current time.

        Returns:   Reference to object itself (TimeStamp).
        """
        timeNow = time.time()
        secs = math.fmod(timeNow, 60.)
        tm = time.localtime(timeNow)
        self.__status = self.tmToMjd(tm,secs)
        return self

    
    def initFromMjd(self,
                    val):
        """
        Initialize from MJD value (modified julian date).

        val:        MJD time (double).

        Returns:    Reference to object itself (TimeStamp).
        """
        self.__mjd = val
        self.__status = 0
        return self


    def initFromTimeStamp(self,
                          val):
        """
        Initialize from string of form YYYY-MM-DDThh:mm:ss[.mss]
        (or YYYY-MM-DD hh:mm:ss[.mss]) or from string of form YYYY-MM-DD.

        val:         Time stamp (string).

        Returns:     Reference to object itself (TimeStamp).
        """
        val = str(val)
        if len(val) > 10:
            formatString = "%Y-%m-%d %H:%M:%S"
        else:
            formatString = "%Y-%m-%d"

        secs = 0.
        # Ignore characters exceeding the maximum possible length
        # of an ISO-8601 time string (23).
        if len(val) > 23: val = val[:23]

        # Store the seconds (.milliseconds) in a separate variable
        # truncate the string to 19 characters (YYYY-MM-DDThh:mm:ss).
        if len(val) >= 19:
            secs = float(val[17:])
            val = val[:19]

        # Replace the T with a blank (strptime does not like it).
        ix = string.find(val, 'T')
        if (ix > 0): val = val[:ix] + ' ' + val[ix+1:]

        timeTuple = time.strptime(val, formatString)
        self.__status = self.tmToMjd(timeTuple, secs)
        return self

        
    def initFromSybaseTimeStamp(self,
                                sybaseTime):
        """
        Initialize the object with a timestamp given in the Sybase
        time format: 'Dec 29 1997 12:00:00:000AM'.

        sybaseTime:     Timestamp (string).

        Returns:        Reference to object itself (TimeStamp).
        """
        # Convert the time into <24h repr>:MM:SS + variable with seconds
        hour = str(sybaseTime[12:14])
        min  = str(sybaseTime[15:17])
        secs = str(sybaseTime[18:20] + "." + sybaseTime[21:24])
        amPm = sybaseTime[24:26]
        if (amPm == "PM"):
            if (int(hour) != 12):
                hour = str(int(hour) + 12)
        # Now build the final, reformatted timestamp.
        timeStamp = sybaseTime[0:12] + hour + ":" + min + ":" + secs[0:2]
        #formatString = "%b %d %Y %T"
        #timeTuple = time.strptime(timeStamp, formatString)
        timeTuple = time.strptime(timeStamp, "%b %d %Y %H:%M:%S")
        self.__status = self.tmToMjd(timeTuple, float(secs))

        return self


    def initFromSecsSinceEpoch(self,
                               secs):
        """
        Initialize from seconds since epoch.

        secs:     Seconds since epoch (integer).

        Returns:  Reference to object itself (TimeStamp).
        """
        secs2 = math.fmod(secs, 60.)
        tm = time.localtime(secs)
        self.__status = self.tmToMjd(tm,secs2)
        return self
 
        
    def mjdToTm(self,
                mjd):
        """
        Convert the mjd (modified julian day) value to a tuple tm.
        (year, month, day, hour, minute, second, weekday, julian day,
        daylight savings flag).
        
        Since the tuple tm doesn't contain the fractional seconds, a
        more exact value for seconds is returned in secs.

        mjd:             Time (double).

        Returns:         Tuple with time components (tuple).
        """
        secs = (mjd - MJD_1970) * 86400
        #t = int(secs + 0.5)
        t = int(secs + 0.0001)
        tm = time.gmtime(t);
        secs = math.fmod(secs, 60.)
        if ((secs + 0.0001) >= 60): secs = 0
        return (tm, secs)


    def tmToMjd(self,
                tm,
                secs):
        """
        Convert a tuple tm to MJD time and store the result in the object.
        The secs argument may contain more exact seconds than
        the tm struct. Returns 0 if OK.

        tm:         Time tuple (tuple_.
        
        secs:       Fraction of seconds (integer).

        Returns:    Reference to object itself (TimeStamp).
        """
        (year,mon,day,hour,min,sec) = (tm[0],tm[1],tm[2],tm[3],tm[4],tm[5])
        
        j = year
        jm = 0
        j = j - (12 - mon)/10
        jm = jm + (1461*(j+4712))/4 + (306*((mon+9)%12)+5)/10 \
             - (3*((j+4900)/100))/4 + day + 96
        jd = jm
        jm  = (12 + hour) * 3600 + min * 60 + sec
        jd = jd + (jm/86400.)
        
        self.__mjd = jd - 2400000.5
        self.__mjd = (self.__mjd - (sec/86400.)) + (secs/86400.)

        return self


    def getTimeStamp(self):
        """
        Get string datetime value as YYYY-MM-DDThh:mm:ss[.mss].

        Returns:   ISO8601 timestamp (string).
        """
        timeTuple = self.mjdToTm(self.__mjd)
        timeStamp = time.strftime("%Y-%m-%dT%H:%M", timeTuple[0])
        secs = "%.3f" % (timeTuple[1])
        secs = string.split(secs,'.')
        if len(secs[0]) == 1:
            s = '0' + secs[0]
        else:
            s = secs[0]
        ms = secs[1][:3]
        timeStamp = timeStamp + ':' + s + '.' + ms
        return timeStamp


    def getSybaseTimeStamp(self):
        """
        Get 'Sybase style' time stamp: MM-DD-YYYY HH:MM:SS[.sss].

        Returns:   'Sybase' timestamp (string).
        """
        timeTuple = self.mjdToTm(self.__mjd)
        timeStamp = time.strftime("%m-%d-%Y %H:%M", timeTuple[0])
        secs = string.split(str(timeTuple[1]),'.')
        if (len(secs) == 1): secs.append("0")
        if len(secs[0]) == 1:
            s = '0' + secs[0]
        else:
            s = secs[0]
        #ms = secs[1][:3]
        #timeStamp = timeStamp + ':' + s + '.' + ms
        ms = secs[1]
        ms = ("%.3f" % float("." + ms))[2:]
        timeStamp = timeStamp + ":%s.%s" % (s, ms)
        return timeStamp


    def getMjd(self):
        """
        Get MJD value.

        Returns:  MJD (double).
        """
        return self.__mjd


    def getDate(self):
        """
        Get string date value as YYYY-MM-DD.

        Returns:   Date (string).
        """
        timeTuple = self.mjdToTm(self.__mjd)
        date = time.strftime("%Y-%m-%d", timeTuple[0])
        return date


    def getNight(self):
        """
        Get string date value as YYYY-MM-DD for the night (noon to noon).

        Return:   Date for night (string).
        """
        timeTuple = self.mjdToTm(self.__mjd - 0.5)
        night = time.strftime("%Y-%m-%d", timeTuple[0])
        return night


    def getTime(self):
        """
        Get string time value as HH:MM:SS.sss.

        Returns:    Time (string).
        """
        timeTuple = self.mjdToTm(self.__mjd)
        timeString = time.strftime("%H:%M", timeTuple[0])
        secs = string.split(str(timeTuple[1]),'.')
        if len(secs[0]) == 1:
            s = '0' + secs[0]
        else:
            s = secs[0]
        ms = secs[1][:3]
        timeString = timeString + ':' + s + '.' + ms
        return timeString


def test():
    """
    Test the methods of the class TimeStamp, by creating a TimeStamp
    object and getting all the date components after initialization
    with an MJD value, a timestamp string, a null value, an integer value.
    """

    print "**test 1 : initialize with a mjd value (51466.33608981)"
    t = TimeStamp(51466.33608981)
    if t.status() == 0:
        print "getMjd:       %s" % (str(t.getMjd()))
        print "getTimeStamp: %s" % (t.getTimeStamp())
        print "getDate:      %s" % (t.getDate())
        print "getNight:     %s" % (t.getNight())
        print "getTime:      %s" % (t.getTime())
        print ''

    print "**test 2 : initialize with a timestamp " +\
          "string('1999-10-15T08:03:58.159')"
    t = TimeStamp('1999-10-15T08:03:58.159')
    if t.status() == 0:
        print "getMjd:       %s" % (str(t.getMjd()))
        print "getTimeStamp: %s" % (t.getTimeStamp())
        print "getDate:      %s" % (t.getDate())
        print "getNight:     %s" % (t.getNight())
        print "getTime:      %s" % (t.getTime())
        print ''

    print "**test 3 : inititialize without arguments"
    t = TimeStamp()
    if t.status() == 0:
        print "getMjd:       %s" % (str(t.getMjd()))
        print "getTimeStamp: %s" % (t.getTimeStamp())
        print "getDate:      %s" % (t.getDate())
        print "getNight:     %s" % (t.getNight())
        print "getTime:      %s" % (t.getTime())
        print ''

    print "**test 4 : initialize with an integer number (illegal)"
    t = TimeStamp(123)
    if t.status() == 0:
        print "getMjd:       %s" % (str(t.getMjd()))
        print "getTimeStamp: %s" % (t.getTimeStamp())
        print "getDate:      %s" % (t.getDate())
        print "getNight:     %s" % (t.getNight())
        print "getTime:      %s" % (t.getTime())


class Timer:
    """
    Timer class used to measure the time between two events.
    """

    def __init__(self,
                 startTime = None):
        """
        Contructor method. Initializing object and taking the
        time at creation as reference.

        startTime:   Set a start time different than the present time. Must
                     be given as time since epoch as returned by
                     time.time() (integer).
        """
        if (startTime != None):
            self.__startTime = startTime
        else:
            self.__startTime = time.time()
    

    def start(self):
        """
        Start the timer.

        Returns:   Reference to object itself (Timer).
        """
        self.__startTime = time.time()
        return self


    def stop(self):
        """
        Stop the timer and return the time since the timer was started.

        Returns:   Time elapsed since timer was started (s).
        """
        timeNow = time.time()
        return (timeNow - self.__startTime)


    def getLap(self):
        """
        Get a lap time.

        Returns:   Time elapsed since timer was started (s).
        """
        # Actually the same as stop(), however, the name stop() was not
        # well chosen.
        timeNow = time.time()
        return (timeNow - self.__startTime)


if __name__ == '__main__': 
    """
    """
    test()


def getIsoTime(onlyDate  = 0,
               precision = 0):
    """
    Generates the current ISO-8601 time with the precision specified.
    If 0 is given as precision, the generated time stamp will not have
    any decimals.

    onlyDate:     Print only the date, i.e. no time information (integer/0|1).
     
    precision:    Precision of the generated time stamp, i.e., the
                  number of decimals to put after the generated
                  time stamp (integer).

    Returns:      The ISO-8601 time stamp (string).
    """
    timeNow = time.time()
    timeTuple = time.localtime(timeNow)
    if (onlyDate == 0):
        isoTimeNow = time.strftime("%Y-%m-%dT%H:%M:%S", timeTuple)
        # Get the decimals.
        if (precision):
            #secDecs = str(math.fmod(timeNow, .99999999))
            #isoTimeNow = isoTimeNow + secDecs[1:(precision + 2)]
            secDecs = "%1.10f" % math.fmod(timeNow, 1.)
            isoTimeNow = isoTimeNow + secDecs[1:(precision + 2)] 
    else:
        isoTimeNow = time.strftime("%Y-%m-%d", timeTuple) 
                
    return isoTimeNow

#
# ___oOo___
