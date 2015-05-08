#!/bin/sh
set -e
set -u

APP_PREFIX="broker"
APP_PATH="/opt/ctrip/app/hermes-${APP_PREFIX}/"

# copy tar
mkdir ${APP_PATH}/old-deployment
mv ${APP_PATH}/*.tar ${APP_PATH}/old-deployment
cp hermes-${APP_PREFIX}/target/*.tar ${APP_PATH}


cd ${APP_PATH}
rm -rf `ls | grep -v ".*${APP_PREFIX}.*tar"`
tar -xf  *${APP_PREFIX}*.tar
cd h*/bin
bash ./startup.sh stop
bash ./startup.sh start