#!/bin/bash

# Define script absolute path.
SCRIPT_PATH=$(readlink -f $0)

# Define script base dir.
SCRIPT_DIR=$(dirname $SCRIPT_PATH)

# Define app id.
APP_ID=100003807

# Define app dir.
APP_DIR=$(realpath $SCRIPT_DIR/../../../..)

# Define hermes dir.
CONFIG_DIR=/opt/data/hermes

# Define source dir.
SOURCE_DIR=$CONFIG_DIR/hermes-config/${APP_ID}

# Solid server settings position.
SERVER_SETTINGS=/opt/settings/server.properties

# Get ip address of current machine.
IP=$(ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1')

# Clone or pull git config project.
if [[ ! -e $CONFIG_DIR ]]; then
	mkdir -p $CONFIG_DIR
fi

if [[ ! -e $CONFIG_DIR/hermes-config ]]; then
	cd $CONFIG_DIR && git clone http://git.dev.sh.ctripcorp.com/hermes/hermes-config.git $CONFIG_DIR/hermes-config
fi

cd $CONFIG_DIR/hermes-config && git pull

# Get env param required for config distribution.
ENV=$(cat $SERVER_SETTINGS | grep env | cut -d'=' -f2 | tr '[:upper:]' '[:lower:]')

# Correct env keyword 'pro' to 'prod'.
if [[ $ENV = 'pro' ]]; then
	ENV='prod'
fi

if [[ $ENV = 'prod' && -n `grep $IP $SOURCE_DIR/tools/servers` ]]; then
	ENV='tools'
fi

# Replace hermes.properties file.
cp $SOURCE_DIR/$ENV/hermes.properties $SCRIPT_DIR/../classes/

# Add extraenv.sh
cp $SOURCE_DIR/env.sh $SCRIPT_DIR/extraenv.sh

# Distribute list of config files.
cp $SOURCE_DIR/$ENV/datasources.xml $CONFIG_DIR/datasources.xml

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

