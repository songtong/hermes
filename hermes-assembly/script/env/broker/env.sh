#!/bin/bash

LOG_PATH=/opt/logs/hermes-broker/
JMX_PORT=8301
STOP_PORT=9301
STOP_TIMEOUT=30
JAVA_OPTS="$JAVA_OPTS -XX:MaxDirectMemorySize=4g " 