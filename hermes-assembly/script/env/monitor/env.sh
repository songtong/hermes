#!/bin/bash

LOG_PATH=/opt/logs/hermes-monitor/
JMX_PORT=8307
STOP_PORT=9307
HTTP_PORT=8090

# set jvm startup argument
JAVA_OPTS="$JAVA_OPTS -Djava.awt.headless=true \
            -Dfile.encoding=utf-8 \
            -Xms2g \
            -Xmx2g \
            -XX:PermSize=128m \
            -XX:MaxPermSize=128m \
            -XX:+PrintGC \
            -XX:+PrintGCDetails \
            -XX:+PrintGCDateStamps \
            -Xloggc:$LOG_PATH/gc.log \
            -XX:-OmitStackTraceInFastThrow \
            -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$LOG_PATH
            "
export JAVA_OPTS=${JAVA_OPTS}