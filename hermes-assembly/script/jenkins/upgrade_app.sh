#!/bin/bash
set -e
set -u

cd `dirname $0`

if [ $# -ne 1 ];then
	echo "usage: `basename $0` metaserver|broker"
	exit 1
fi

valid_app=false
for supported_app in metaserver broker;do
	if [ $1 == $supported_app ];then
		valid_app=true
		break;
	fi
done
if [ $valid_app == false ];then	
	echo "$1 is not a supported app name"
	exit 1
else
	echo "Upgrading $1"
fi

APP_NAME=$1
APP_DIR=/opt/ctrip/app/$APP_NAME
APP_STARTUP_SCRIPT=$APP_DIR/bin/startup.sh

mkdir -p $APP_DIR

if [ -e $APP_STARTUP_SCRIPT ];then
	$APP_STARTUP_SCRIPT stop
fi

rm -rf $APP_DIR/*
tar xf *${APP_NAME}*.tar -C $APP_DIR
chmod +x $APP_STARTUP_SCRIPT
$APP_STARTUP_SCRIPT start
wait