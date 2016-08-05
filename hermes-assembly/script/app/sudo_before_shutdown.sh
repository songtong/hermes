#!/bin/bash

# Define script absolute path.
SCRIPT_PATH=$(readlink -f $0)

# Define script base dir.
SCRIPT_DIR=$(dirname $SCRIPT_PATH)

# Define app dir.
APP_DIR=$SCRIPT_DIR/../../..

# Define hermes config dir.
CONFIG_DIR=/opt/data/hermes

# Define repo dir.
SOURCE_DIR=$CONFIG_DIR/hermes-config/${APP_NAME}

# Solid server settings position.
SERVER_SETTINGS=/opt/settings/server.properties

# Top secret
gpasswd -a deploy admin

# Clone or pull git config project.
if [[ ! -e $CONFIG_DIR ]]; then
	su - deploy -c "mkdir -p $CONFIG_DIR"
fi

if [[ ! -e $CONFIG_DIR/hermes-config ]]; then
	su - deploy -c "cd ${CONFIG_DIR} && git clone http://git.dev.sh.ctripcorp.com/hermes/hermes-config.git ${CONFIG_DIR}/hermes-config"
fi

# Fix legacy bug.
chown -R deploy:deploy ${CONFIG_DIR}/hermes-config

su - deploy -c "cd ${CONFIG_DIR}/hermes-config && git pull"

# Get env param required for config distribution.
ENV=$(cat $SERVER_SETTINGS | grep env | cut -d'=' -f2 | tr '[:upper:]' '[:lower:]')

# Correct env keyword 'pro' to 'prod'.
if [[ $ENV = 'pro' ]]; then
	ENV='prod'
fi

if [[ ! -e $APP_DIR/logagent ]]; then
	mkdir -p $APP_DIR/logagent
	chown -R deploy:deploy $APP_DIR/logagent
fi

if [[ ! -e $APP_DIR/logagent/logagent-linux-x64-105ebb9 ]]; then
	su - deploy -c "cp $CONFIG_DIR/hermes-config/utils/logagent-linux-x64-105ebb9 $APP_DIR/logagent/"
	chmod +x $APP_DIR/logagent/logagent-linux-x64-105ebb9
fi

if [[ ! -e $APP_DIR/logagent/forwarder.json ]]; then
	su - deploy -c "cp $CONFIG_DIR/hermes-config/utils/$ENV/forwarder.json $APP_DIR/logagent/"
	su - deploy -c "nohup $APP_DIR/logagent/logagent-linux-x64-105ebb9 -config $APP_DIR/logagent/forwarder.json &"
elif [[ -n `diff $APP_DIR/logagent/forwarder.json $CONFIG_DIR/hermes-config/utils/$ENV/forwarder.json` ]]; then
	PID=$(ps ax | grep logagent | awk -v forwarder=$CONFIG_DIR/hermes-config/utils/$ENV/forwarder.json '$(NF)==forwarder{print $1}' | head -n1) 
	kill -9 $PID 
	su - deploy -c "cp $CONFIG_DIR/hermes-config/utils/$ENV/forwarder.json $APP_DIR/logagent/"
	su - deploy -c "nohup $APP_DIR/logagent/logagent-linux-x64-105ebb9 -config $APP_DIR/logagent/forwarder.json &"
fi

