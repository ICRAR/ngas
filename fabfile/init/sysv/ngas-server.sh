#!/bin/sh
### BEGIN INIT INFO
# Provides:        ngams-cache-server 
# Required-Start:  $all
# Required-Stop:   $local_fs $network
# Default-Start:   2 3 4 5
# Default-Stop:    0 1 6
# Short-Description: NGAS daemon
### END INIT INFO
#
#
# chkconfig: 2345 99 70
# description: Starts and stops the NGAMS server as a daemon

# RH, Centos, Fedora configuration style
if [ -r /etc/sysconfig/ngas ]
then
	. /etc/sysconfig/ngas
# Debian, Ubuntu configuration style
elif [ -r /etc/default/ngas ]
then
	. /etc/default/ngas
else
	echo "Missing configuration file, cannot start NGAS" > /dev/stderr
	exit 1
fi

RETVAL=0

# See how we were called.
case "$1" in
  start)
        su - $USER -c "$DAEMON start"
        RETVAL=$?
        echo "NG/AMS startup"
        ;;
  stop)
        su - $USER -c "$DAEMON stop"
        RETVAL=$?
        echo "NG/AMS shutdown"
        ;;
  status)
        echo "Status of $DAEMON: "
        su - $USER -c "$DAEMON status"
        RETVAL=$?
        ;;
  restart)
        echo -n "Restarting $DAEMON: "
        $0 stop
        $0 start
        RETVAL=$?
        ;;
  *)
        echo "Usage: $0 {start|stop|status|restart}"
        RETVAL=1
esac

exit $RETVAL