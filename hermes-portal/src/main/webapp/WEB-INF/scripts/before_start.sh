#!/bin/bash

# Define script absolute path.
SCRIPT_PATH=$(readlink -f $0)

# Define script base dir.
SCRIPT_DIR=$(dirname $SCRIPT_PATH)

# Define app id.
APP_ID=100003806

# Define data dir for app.
DATA_DIR=/opt/data/${APP_ID}

# Define hermes dir.
CONFIG_DIR=/opt/data/hermes

# Solid server settings position.
SERVER_SETTINGS=/opt/settings/server.properties

# Get env param required for config distribution.
ENV=$(cat $SERVER_SETTINGS | grep env | cut -d'=' -f2 | tr '[:upper:]' '[:lower:]')

# Correct env keyword 'pro' to 'prod'.
if [[ $ENV = 'pro' ]]; then
	ENV='prod'
fi

# Create dir if not exists.
if [[ -e $DATA_DIR ]]; then
	mkdir -p $DATA_DIR
fi

# Create dir if not exists.
if [[ ! -e $CONFIG_DIR ]]; then
	mkdir -p $CONFIG_DIR
fi

# Replace web.xml
cp $SCRIPT_DIR/../tars/$ENV/web.xml $SCRIPT_DIR/../web.xml

# Replace hermes.properties file.
cp $SCRIPT_DIR/../tars/$ENV/hermes.properties $SCRIPT_DIR/../classes/

# Distribute list of config files.
cp $SCRIPT_DIR/../tars/$ENV/datasources.xml $CONFIG_DIR/datasources.xml
cp $SCRIPT_DIR/../tars/mail.properties $CONFIG_DIR/mail.properties
cp $SCRIPT_DIR/../tars/hermes-es.token $CONFIG_DIR/hermes-es.token




