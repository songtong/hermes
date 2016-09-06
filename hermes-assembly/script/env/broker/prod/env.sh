#!/bin/bash

LOG_PATH=/opt/logs/100003804/
JMX_PORT=8301
STOP_PORT=9301
STOP_TIMEOUT=90
APP_ID=100003804
# set jvm startup argument
JAVA_OPTS="$JAVA_OPTS -Djava.awt.headless=true \
            -Dfile.encoding=utf-8 \
            -Xms13g \
            -Xmx13g \
            -Xmn9g \
            -Dtomcat.log=$LOG_PATH/tomcat \
            -XX:PermSize=256m \
            -XX:MaxPermSize=256m \
            -XX:MaxDirectMemorySize=5g \
            -XX:+PrintGC \
            -XX:+PrintGCDetails \
            -XX:+PrintGCDateStamps \
            -Xloggc:$LOG_PATH/gc.log \
            -XX:-OmitStackTraceInFastThrow \
            -XX:SurvivorRatio=7 \
            -XX:+UnlockCommercialFeatures \
            -XX:+FlightRecorder \
            -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$LOG_PATH
            "
export JAVA_OPTS=${JAVA_OPTS}