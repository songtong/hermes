#!/bin/bash

LOG_PATH=/opt/logs/hermes-rest/
JMX_PORT=8303
STOP_PORT=9303

# set jvm startup argument
JAVA_OPTS="$JAVA_OPTS -Djava.awt.headless=true \
            -Dfile.encoding=utf-8 \
            -Dio.netty.allocator.type=pooled \
            -Xms4g \
            -Xmx4g \
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