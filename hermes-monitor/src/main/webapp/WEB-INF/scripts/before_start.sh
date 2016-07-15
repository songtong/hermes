#!/bin/bash

# Define script absolute path.
SCRIPT_PATH=$(readlink -f $0)

# Define script base dir.
SCRIPT_DIR=$(dirname $SCRIPT_PATH)

# Define app id.
APP_ID=100003808

# Define app name.
APP_NAME=monitor

# Define data dir for app.
DATA_DIR=/opt/data/${APP_ID}

# Define source dir.
SOURCE_DIR=${DATA_DIR}/hermes-config/${APP_NAME}

# Define hermes dir.
CONFIG_DIR=/opt/data/hermes

# Solid server settings position.
SERVER_SETTINGS=/opt/settings/server.properties

# Clone or pull git config project.
if [[ ! -e $DATA_DIR ]]; then
	mkdir -p $DATA_DIR
fi

if [[ ! -e $DATA_DIR/hermes-config ]]; then
	git clone http://git.dev.sh.ctripcorp.com/hermes/hermes-config.git ${DATA_DIR}/hermes-config
fi

cd ${DATA_DIR}/hermes-config && git pull

# Get env param required for config distribution.
ENV=$(cat $SERVER_SETTINGS | grep env | cut -d'=' -f2 | tr '[:upper:]' '[:lower:]')

# Correct env keyword 'pro' to 'prod'.
if [[ $ENV = 'pro' ]]; then
	ENV='prod'
fi

# Create dir if not exists.
if [[ ! -e $CONFIG_DIR ]]; then
	mkdir -p $CONFIG_DIR
fi

# Replace hermes.properties file.
cp $SOURCE_DIR/$ENV/hermes.properties $SCRIPT_DIR/../classes/

# Distribute list of config files.
cp $SOURCE_DIR/$ENV/datasources.xml $CONFIG_DIR/datasources.xml
cp $SOURCE_DIR/mail.properties $CONFIG_DIR/mail.properties
cp $SOURCE_DIR/hermes-es.token $CONFIG_DIR/hermes-es.token
cp $SOURCE_DIR/TTS.TOKEN $CONFIG_DIR/TTS.TOKEN




