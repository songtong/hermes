#!/bin/sh

set -e
set -u

cd `dirname $0`

if [ $# -ne 1 ];then
	echo "usage: startup.sh [start|stop]"
	exit 1
fi

LOG_PATH="/opt/logs/hermes/"
mkdir -p $LOG_PATH

ENV_FILE="./env.sh"
. "${ENV_FILE}"

LOG_FILE="sysout.log"

SERVER_HOME=..
JETTY_JAR=$(ls ../jetty/*.jar)
WAR=$(ls ../*.war)

start() {
    ensure_not_started
	if [ ! -d "${LOG_PATH}" ]; then
        mkdir "${LOG_PATH}"
    fi
    nohup java ${JAVA_OPTS} -jar $JETTY_JAR $WAR > "${LOG_PATH}/${LOG_FILE}" 2>&1 &
    echo "Instance Started!"
}

stop(){
    serverPID=$(find_pid)
    if [ "${serverPID}" == "" ]; then
        echo "No Instance Is Running"
    else
        kill -9 ${serverPID}
        echo "Instance Stopped"
    fi
}


ensure_not_started() {
	serverPID=$(find_pid)
    if [ "${serverPID}" != "" ]; then
        echo "Instance Already Running"
        exit 1
    fi
}

find_pid() {
	echo $(ps ax | grep java | awk -v war=$WAR '$NF==war{print $1}')
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
