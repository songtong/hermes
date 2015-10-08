#!/bin/bash
set -e
set -u

cd `dirname $0`

if [ $# -lt 1 ];then
	echo "usage: `basename $0` metaserver|broker|portal|rest [port]"
	exit 1
fi

port=""
if [ $# -eq 2 ];then
	port=$2
fi

valid_app=false
for supported_app in metaserver broker portal rest;do
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

APP_BASE_DIR=/opt/ctrip/app
APP_NAME=$1
APP_DIR=$APP_BASE_DIR/hermes-$APP_NAME
APP_RELEASE_DIR=$APP_BASE_DIR/hermes-$APP_NAME.releases/`date "+%Y-%m-%d.%H.%M.%S"`
APP_STARTUP_SCRIPT=$APP_DIR/bin/startup.sh


if [ -e $APP_STARTUP_SCRIPT ];then
	$APP_STARTUP_SCRIPT stop $port
fi

mkdir -p $APP_RELEASE_DIR
tar xf *${APP_NAME}*.tar -C $APP_RELEASE_DIR

if [ -d $APP_DIR ];then 
	rm -rf $APP_DIR
fi
ln -s $APP_RELEASE_DIR $APP_DIR

chmod +x $APP_STARTUP_SCRIPT
$APP_STARTUP_SCRIPT start $port
wait