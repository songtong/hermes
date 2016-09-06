#!/bin/bash

# Define script absolute path.
SCRIPT_PATH=$(readlink -f $0)

# Define script base dir.
SCRIPT_DIR=$(dirname $SCRIPT_PATH)

# Define app id.
APP_ID=100003808

# Define hermes dir.
CONFIG_DIR=/opt/data/hermes

# Define source dir.
SOURCE_DIR=$CONFIG_DIR/hermes-config/${APP_ID}

# Solid server settings position.
SERVER_SETTINGS=/opt/settings/server.properties

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

# Replace hermes.properties file.
cp $SOURCE_DIR/$ENV/hermes.properties $SCRIPT_DIR/../classes/

# Distribute list of config files.
cp $SOURCE_DIR/$ENV/datasources.xml $CONFIG_DIR/datasources.xml
cp $SOURCE_DIR/mail.properties $CONFIG_DIR/mail.properties
cp $SOURCE_DIR/hermes-es.token $CONFIG_DIR/hermes-es.token
cp $SOURCE_DIR/TTS.TOKEN $CONFIG_DIR/TTS.TOKEN
