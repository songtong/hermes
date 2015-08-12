#!/bin/sh

set -e
set -u

cd `dirname $0`

mkdir -p /opt/logs/hermes-broker/

ENV_FILE="./env.sh"
. "${ENV_FILE}"

LOG_PATH="/opt/logs/hermes-broker/"
LOG_FILE="sysout.log"

SERVER_DRIVER=com.ctrip.hermes.broker.BrokerServer

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
    echo "BrokerServer Started!"
}

stop(){
    serverPID=`jps -lvm | grep com.ctrip.hermes.broker.BrokerServer | awk '{print $1}'`
    if [ "${serverPID}" == "" ]; then
        echo "no BrokerServer is running"
    else
        echo "telnet 127.0.0.1 4888 and shutdown"
        { echo "shutdown"; sleep 1;} | { telnet 127.0.0.1 4888;}
        #kill  ${serverPID}
        echo "BrokerServer Stopped"
    fi
}

ensure_not_started() {
	serverPID=`jps -lvm | grep com.ctrip.hermes.broker.BrokerServer | awk '{print $1}'`
    if [ "${serverPID}" != "" ]; then
        echo "BrokerServer is already running"
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
    *)
        echo "Usage: $0 {start|stop}"
   	    exit 1;
	    ;;
esac
exit 0
