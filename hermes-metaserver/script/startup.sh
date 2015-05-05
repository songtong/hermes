#!/bin/sh

set -e
set -u

cd `dirname $0`

mkdir -p /opt/logs/hermes/

ENV_FILE="./env.sh"
. "${ENV_FILE}"

LOG_PATH="/opt/logs/hermes/"
LOG_FILE="sysout-metaserver.log"

SERVER_DRIVER=com.ctrip.hermes.metaserver.MetaRestServer

SERVER_HOME=..

CLASSPATH=${SERVER_HOME}

CLASSPATH="${CLASSPATH}":"${CLASSPATH}/lib/*":"${SERVER_HOME}/*":"${SERVER_HOME}/conf"
for i in "${SERVER_HOME}"/*.jar; do
   CLASSPATH="${CLASSPATH}":"${i}"
done



start() {
    ensure_not_started
	if [ ! -d "${LOG_PATH}" ]; then
        mkdir "${LOG_PATH}"
    fi
    nohup java ${JAVA_OPTS} -classpath ${CLASSPATH} ${SERVER_DRIVER} > "${LOG_PATH}/${LOG_FILE}" 2>&1 &
    echo "MetaServer Started!"
}

stop(){
    serverPID=`jps -lvm | grep com.ctrip.hermes.metaserver.MetaRestServer | awk '{print $1}'`
    if [ "${serverPID}" == "" ]; then
        echo "no MetaServer is running"
    else
        kill -9 ${serverPID}
        echo "MetaServer Stopped"
    fi
}


ensure_not_started() {
	serverPID=`jps -lvm | grep com.ctrip.hermes.metaserver.MetaRestServer | awk '{print $1}'`
    if [ "${serverPID}" != "" ]; then
        echo "MetaServer is already running"
        exit 1
    fi
}


_start() {
    java ${JAVA_OPTS} -classpath ${CLASSPATH} ${SERVER_DRIVER}
}


case "$1" in
    start)
        start
	    ;;
	stop)
	    stop
	    ;;
	check_pid)
	    check_pid
	    ;;
    *)
        echo "Usage: $0 {start|check_pid}"
   	    exit 1;
	    ;;
esac
exit 0
