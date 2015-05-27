#!/bin/sh
set -e
set -u

APP_PREFIX="metaserver"
APP_PATH="/opt/ctrip/app/hermes-${APP_PREFIX}"

# copy jar
now=$(date +"%F_%T")
mkdir -p ${APP_PATH}/${now}
mv ${APP_PATH}/*.jar ${APP_PATH}/$now/
mv ${APP_PATH}/*.sh ${APP_PATH}/$now/
cp hermes-${APP_PREFIX}/target/*.jar ${APP_PATH}
cp hermes-${APP_PREFIX}/script/*.sh ${APP_PATH}


cd ${APP_PATH}
bash ./startup.sh stop
bash ./startup.sh start
