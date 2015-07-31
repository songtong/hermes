#!/bin/bash

set -e
set -u

cd `dirname $0`

if [ $# -ne 1 ];then
	echo "usage: startup.sh [start|stop]"
	exit 1
fi

LOG_PATH=/opt/logs/hermes/
mkdir -p $LOG_PATH

ENV_FILE="./env.sh"
. "${ENV_FILE}"

SYSOUT_LOG=$LOG_PATH/sysout.log
OP_LOG=$LOG_PATH/op.log

SERVER_HOME=..
JETTY_JAR=$(ls ../jetty/*.jar)
WAR=$(ls ../*.war)

if [ ! -f $JAVA_CMD ];then
	log_op "$JAVA_CMD not found!"
	exit 1
fi

start() {
    ensure_not_started
	if [ ! -d "${LOG_PATH}" ]; then
        mkdir "${LOG_PATH}"
    fi
    log_op $(pwd)
    BUILD_ID=jenkinsDontKillMe nohup $JAVA_CMD ${JAVA_OPTS} -jar $JETTY_JAR $WAR > $SYSOUT_LOG 2>&1 &
    log_op "PID $$"
    log_op "Instance Started!"
}

log_op() {
	timestamp=$(date +"%F %T")
	echo "[$timestamp] $@" >> $OP_LOG
}

stop(){
    serverPID=$(find_pid)
    if [ "${serverPID}" == "" ]; then
        log_op "No Instance Is Running"
    else
        kill -9 ${serverPID}
        log_op "Instance Stopped"
    fi
}


ensure_not_started() {
	serverPID=$(find_pid)
    if [ "${serverPID}" != "" ]; then
        log_op "Instance Already Running"
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
