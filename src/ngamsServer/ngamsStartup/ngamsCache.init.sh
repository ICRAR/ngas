#!/bin/sh
#
#
# chkconfig: 2345 99 70
# description: Starts and stops the ngamsServer
# processname: ngamsServer
# config: /etc/ngamsServer.conf

# Source function library.
. /etc/rc.d/init.d/functions

IPADD=192.168.90.100
PORT=7798
RETVAL=0
NGAMS_PID_FILE="/home/ngas/ngas_rt/NGAS/.NGAS-"${IPADD}"-$PORT"

# See how we were called.
case "$1" in
  start)
#       echo -n "Starting ngamsServer: "

#       daemon --user ngas "/home/ngas/ngas_rt/bin/ngamsServer -cfg /home/ngas/ngas_rt/cfg/ngamsServer.conf -autoOnline -force"
        su - ngas -c "/home/ngas/ngas_rt/bin/python /home/ngas/ngas_rt/src/ngamsServer/ngamsCacheServer.py -cfg /home/ngas/ngas_rt/cfg/NgamsCfg.SQLite.cache.xml -autoOnline -force -multipleSrvs&" 2>&1>/dev/null

        echo "NG/AMS startup"
        [ $RETVAL -eq 0 ] && touch /var/lock/subsys/ngamsServer
#       RETVAL=$?
        ;;
  stop)
#       echo -n "Stopping ngamsServer: "
        su - ngas -c "/home/ngas/ngas_rt/bin/ngamsPClient -port $PORT -host $IPADD -status -cmd OFFLINE -force" 1>/dev/null 2>&1
        su - ngas -c "/home/ngas/ngas_rt/bin/ngamsPClient -port $PORT -host $IPADD -status -cmd EXIT" 1>/dev/null 2>&1
        if [[ -e ${NGAMS_PID_FILE} ]]
        then
          NGAMS_PID=$(cat ${NGAMS_PID_FILE})
          /bin/kill -9 ${NGAMS_PID}
          echo "NG/AMS PID "${NGAMS_PID}" killed."
          rm -f ${NGAMS_PID_FILE}
          echo "NG/AMS PID FILE "${NGAMS_PID_FILE}" removed."
        fi
        RETVAL=$?
        echo "NG/AMS shutdown"
        ;;
  status)
        echo "Status ngamsServer: "
        su - ngas -c "/home/ngas/ngas_rt/bin/ngamsPClient -port $PORT -host $IPADD -status -cmd STATUS"
        RETVAL=$?
        ;;
  restart)
        echo -n "Restarting ngamsServer: "
        $0 stop
        $0 start
        RETVAL=$?
        ;;
  *)
        echo "Usage: $0 {start|stop|status|restart}"
        exit 1
esac

#exit $RETVAL