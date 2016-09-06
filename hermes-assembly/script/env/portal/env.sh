#!/bin/bash

LOG_PATH=/opt/logs/100003806/
JMX_PORT=8305
STOP_PORT=9305
APP_ID=100003806
# set jvm startup argument
JAVA_OPTS="$JAVA_OPTS -Djava.awt.headless=true \
            -Dfile.encoding=utf-8 \
            -Xms4g \
            -Xmx4g \
            -Dtomcat.log=$LOG_PATH/tomcat \
            -XX:PermSize=256m \
            -XX:MaxPermSize=256m \
            -XX:+PrintGC \
            -XX:+PrintGCDetails \
            -XX:+PrintGCDateStamps \
            -Xloggc:$LOG_PATH/gc.log \
            -XX:-OmitStackTraceInFastThrow \
            -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$LOG_PATH
            "
export JAVA_OPTS=${JAVA_OPTS}