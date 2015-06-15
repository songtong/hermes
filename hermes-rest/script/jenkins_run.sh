#!/bin/sh
set -e
set -u

APP_PREFIX="rest"
APP_PATH="/opt/ctrip/app/hermes-${APP_PREFIX}"

# copy tar
mkdir -p ${APP_PATH}/old-deployment
mv ${APP_PATH}/*.tar ${APP_PATH}/old-deployment
cp hermes-${APP_PREFIX}/target/*.tar ${APP_PATH}


cd ${APP_PATH}
echo "Remove [" `ls | grep -v ".*${APP_PREFIX}.*tar\|old-deployment"`"]"
rm -rf `ls | grep -v ".*${APP_PREFIX}.*tar\|old-deployment"`
cd h*/bin
bash ./startup.sh stop
bash ./startup.sh start
