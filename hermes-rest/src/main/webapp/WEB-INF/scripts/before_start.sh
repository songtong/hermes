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

# Stop logagent if existed
LOGAGENT_PID=$(ps ax | grep logagent | awk '$(NF) ~ /logagent\/forwarder.json$/{print $1}' | head -n1)
if [[ -n $LOGAGENT_PID ]]; then
		kill -9 $LOGAGENT_PID
fi

# Test whether filebeat is copied to the target dir.
if [[ ! -e $APP_DIR/filebeat/filebeat ]]; then
	tar -zxvf $CONFIG_DIR/hermes-config/utils/filebeat.tar.gz -C $APP_DIR/filebeat/ --strip 1
	chmod +x $APP_DIR/filebeat/filebeat
fi

if [[ ! -e $APP_DIR/filebeat/filebeat.yml ]]; then
	cp $CONFIG_DIR/hermes-config/utils/$ENV/filebeat.yml $APP_DIR/filebeat/
elif [[ -n `diff $APP_DIR/filebeat/filebeat.yml $CONFIG_DIR/hermes-config/utils/$ENV/filebeat.yml` ]]; then
	PID=$(ps ax | grep filebeat | awk '$(NF) ~ /filebeat\/filebeat.yml$/{print $1}' | head -n1) 
	if [[ -n $PID ]]; then
		kill -9 $PID
	fi
	cp $CONFIG_DIR/hermes-config/utils/$ENV/filebeat.yml $APP_DIR/filebeat/
fi

PID=$(ps ax | grep filebeat | awk '$(NF) ~ /filebeat\/filebeat.yml$/{print $1}' | head -n1) 
if [[ -z $PID ]]; then
	nohup $APP_DIR/filebeat/filebeat -e -c $APP_DIR/filebeat/filebeat.yml >/dev/null 2>&1 &
fi

