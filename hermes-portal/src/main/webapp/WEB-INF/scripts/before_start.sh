#!/bin/bash

# Define script absolute path.
SCRIPT_PATH=$(readlink -f $0)

# Define script base dir.
SCRIPT_DIR=$(dirname $SCRIPT_PATH)

# Define app id.
APP_ID=100003806

# Define data dir for app.
DATA_DIR=/opt/data/${APP_ID}

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
	mkdir -p DATA_DIR
fi

# Distribute list of config files.
mv SCRIPT_DIR/../tars/$ENV/datasources.xml $DATA_DIR/datasources.xml
mv SCRIPT_DIR/../tars/mail.properties $DATA_DIR/mail.properties





