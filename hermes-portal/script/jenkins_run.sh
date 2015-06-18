#!/bin/sh
set -e
set -u

APP_PREFIX="portal"
APP_PATH="/opt/ctrip/app/hermes-${APP_PREFIX}"
OLD_DEPLOYMENT="${APP_PATH}/old-deployment"
TOMCAT_PATH="/opt/ctrip/app/${APP_PREFIX}-tomcat/webapps/ROOT"
TOMCAT_BIN="/opt/ctrip/app/${APP_PREFIX}-tomcat/bin"

mkdir -p ${OLD_DEPLOYMENT}

if [ -f ${TOMCAT_PATH}/hermes-${APP_PREFIX}*.war ]
then
    echo "Old-Deployment: Moving ["`ls ${TOMCAT_PATH} | grep hermes-${APP_PREFIX}*.war`"] to "${OLD_DEPLOYMENT}
    mv ${TOMCAT_PATH}/hermes-${APP_PREFIX}*.war ${OLD_DEPLOYMENT}
fi

echo "New-Deployment: Moving ["`ls hermes-${APP_PREFIX}/target/hermes-${APP_PREFIX}*.war`"] to "${TOMCAT_PATH}
mv hermes-${APP_PREFIX}/target/hermes-${APP_PREFIX}*.war  ${TOMCAT_PATH}
cd ${TOMCAT_PATH}

echo "Remove all under "${TOMCAT_PATH} " except *.war."
echo "Removing: ["`ls | grep -v ".*${APP_PREFIX}.*war"`]
rm -rf `ls | grep -v ".*${APP_PREFIX}.*war"`
jar -xf hermes-${APP_PREFIX}*.war
sudo ${TOMCAT_BIN}/shutdown.sh
sleep 10
sudo ${TOMCAT_BIN}/startup.sh
