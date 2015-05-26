#!/bin/sh
set -e
set -u

APP_PREFIX="metaserver"
APP_PATH="/opt/ctrip/app/hermes-${APP_PREFIX}"

# copy tar
mkdir ${APP_PATH}/old-deployment
mv ${APP_PATH}/*.jar ${APP_PATH}/old-deployment
mv ${APP_PATH}/*.sh ${APP_PATH}/old-deployment
cp hermes-${APP_PREFIX}/target/*.jar ${APP_PATH}
cp hermes-${APP_PREFIX}/script/*.sh ${APP_PATH}


cd ${APP_PATH}
bash ./startup.sh stop
bash ./startup.sh start