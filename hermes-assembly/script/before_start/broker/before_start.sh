#!/bin/bash

# Define script absolute path.
SCRIPT_PATH=$(readlink -f $0)

# Define script base dir.
SCRIPT_DIR=$(dirname $SCRIPT_PATH)

# Define app dir.
APP_DIR=$(realpath $SCRIPT_DIR/../../..)

# Define app id.
APP_ID=100003804

# Define hermes config dir.
CONFIG_DIR=/opt/data/hermes

# Define repo dir.
SOURCE_DIR=$CONFIG_DIR/hermes-config/${APP_ID}

# Solid server settings position.
SERVER_SETTINGS=/opt/settings/server.properties

# Get ip of current machine.
IP=$(ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1')

# Create config directory if not exists.
if [[ ! -e $CONFIG_DIR ]]; then
	mkdir -p $CONFIG_DIR
fi

# Clone config project if it's first time running.
if [[ ! -e $CONFIG_DIR/hermes-config ]]; then
	cd ${CONFIG_DIR} && git clone http://git.dev.sh.ctripcorp.com/hermes/hermes-config.git ${CONFIG_DIR}/hermes-config
fi

cd ${CONFIG_DIR}/hermes-config && git pull

# Get env param required for config distribution.
ENV=$(cat $SERVER_SETTINGS | grep env | cut -d'=' -f2 | tr '[:upper:]' '[:lower:]')

# Correct env keyword 'pro' to 'prod'.
if [[ $ENV = 'pro' ]]; then
	ENV='prod'
fi

# Test whether current machine in the list of tools machines.
if [[ $ENV = 'prod' && -n `grep $IP $SOURCE_DIR/tools/servers` ]]; then
	ENV='tools'
fi

# Distribute list of config files.
cp $SOURCE_DIR/$ENV/datasources.xml $CONFIG_DIR/datasources.xml

# Find the path of the war file.
WAR=$(ls $SCRIPT_DIR/../*.war)

# Unzip the war file if can NOT find web.xml.
if [ ! -f $SCRIPT_DIR/../context/WEB-INF/web.xml ];then
	unzip -q -d $SCRIPT_DIR/../context $WAR
fi

cp $SOURCE_DIR/$ENV/hermes.properties $SCRIPT_DIR/../context/WEB-INF/classes/
cp $SOURCE_DIR/$ENV/env.sh $SCRIPT_DIR/

# Test whether logagent is copied to the target dir.
if [[ ! -e $APP_DIR/logagent/logagent-linux-x64-105ebb9 ]]; then
	cp $CONFIG_DIR/hermes-config/utils/logagent-linux-x64-105ebb9 $APP_DIR/logagent/
	chmod +x $APP_DIR/logagent/logagent-linux-x64-105ebb9
fi

if [[ ! -e $APP_DIR/logagent/forwarder.json ]]; then
	cp $CONFIG_DIR/hermes-config/utils/$ENV/forwarder.json $APP_DIR/logagent/
elif [[ -n `diff $APP_DIR/logagent/forwarder.json $CONFIG_DIR/hermes-config/utils/$ENV/forwarder.json` ]]; then
	PID=$(ps ax | grep logagent | awk '$(NF) ~ /logagent\/forwarder.json$/{print $1}' | head -n1) 
	if [[ -n $PID ]]; then
		kill -9 $PID
	fi
	cp $CONFIG_DIR/hermes-config/utils/$ENV/forwarder.json $APP_DIR/logagent/
fi

PID=$(ps ax | grep logagent | awk '$(NF) ~ /logagent\/forwarder.json$/{print $1}' | head -n1) 
if [[ -z $PID ]]; then
	nohup $APP_DIR/logagent/logagent-linux-x64-105ebb9 -config $APP_DIR/logagent/forwarder.json >/dev/null 2>&1 &
fi

