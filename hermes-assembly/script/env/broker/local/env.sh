#!/bin/bash

LOG_PATH=/opt/logs/hermes-broker/
JMX_PORT=8301
STOP_PORT=9301
STOP_TIMEOUT=30
# set jvm startup argument
JAVA_OPTS="$JAVA_OPTS -Djava.awt.headless=true \
            -Dfile.encoding=utf-8 \
            -Xms4g \
            -Xmx4g \
            -XX:PermSize=256m \
            -XX:MaxPermSize=256m \
            -XX:MaxDirectMemorySize=4g \
            -XX:+PrintGC \
            -XX:+PrintGCDetails \
            -XX:+PrintGCDateStamps \
            -Xloggc:$LOG_PATH/gc.log \
            -XX:-OmitStackTraceInFastThrow \
            -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$LOG_PATH
            "
export JAVA_OPTS=${JAVA_OPTS}