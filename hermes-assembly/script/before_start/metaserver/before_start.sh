#!/bin/bash

# Define script absolute path.
SCRIPT_PATH=$(readlink -f $0)

# Define script base dir.
SCRIPT_DIR=$(dirname $SCRIPT_PATH)

# Define app name.
APP_ID=100003805

# Define hermes config dir.
CONFIG_DIR=/opt/data/hermes

# Define repo dir.
SOURCE_DIR=$CONFIG_DIR/hermes-config/${APP_ID}

# Solid server settings position.
SERVER_SETTINGS=/opt/settings/server.properties

# Get ip of current machine.
IP=$(ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1')

# Clone or pull git config project.
if [[ ! -e $CONFIG_DIR ]]; then
	mkdir -p $CONFIG_DIR
fi

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

# Test whether this machine in the list of tools machines.
if [[ $ENV = 'prod' && -n `grep $IP $SOURCE_DIR/tools/servers` ]]; then
	ENV='tools'
fi

# Distribute list of config files.
cp $SOURCE_DIR/$ENV/datasources.xml $CONFIG_DIR/datasources.xml

WAR=$(ls $SCRIPT_DIR/../*.war)

if [ ! -f $SCRIPT_DIR/../context/WEB-INF/web.xml ];then
	unzip -q -d $SCRIPT_DIR/../context $WAR
fi

cp $SOURCE_DIR/$ENV/hermes.properties $SCRIPT_DIR/../context/WEB-INF/classes/
cp $SOURCE_DIR/$ENV/env.sh $SCRIPT_DIR/




