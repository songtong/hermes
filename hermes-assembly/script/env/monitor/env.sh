#!/bin/bash

LOG_PATH=/opt/logs/100003808/
JMX_PORT=8307
STOP_PORT=9307
HTTP_PORT=8090
APP_ID=100003808
# set jvm startup argument
JAVA_OPTS="$JAVA_OPTS -Djava.awt.headless=true \
            -Dfile.encoding=utf-8 \
            -Xms2g \
            -Xmx2g \
            -Dtomcat.log=$LOG_PATH/tomcat \
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