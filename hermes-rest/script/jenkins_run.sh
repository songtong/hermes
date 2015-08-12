#!/bin/sh
set -e
set -u

APP_PREFIX="rest"
APP_PATH="/opt/ctrip/app/hermes-${APP_PREFIX}"

# copy tar
OLD_DEPLOY="${APP_PATH}/old-deployment/"

echo "Move ["${APP_PATH}/*.tar"] into "${OLD_DEPLOY}
mkdir -p ${OLD_DEPLOY}
mv ${APP_PATH}/*.tar ${OLD_DEPLOY}

echo "Copy ["hermes-${APP_PREFIX}/target/*.tar"] to " ${APP_PATH}
cp hermes-${APP_PREFIX}/target/*.tar ${APP_PATH}


cd ${APP_PATH}

echo "Remove [" `ls | grep -v ".*${APP_PREFIX}.*tar\|old-deployment"`"]"
rm -rf `ls | grep -v ".*${APP_PREFIX}.*tar\|old-deployment"`
tar -xf  *${APP_PREFIX}*.tar
cd h*/bin
bash ./startup.sh stop
bash ./startup.sh start