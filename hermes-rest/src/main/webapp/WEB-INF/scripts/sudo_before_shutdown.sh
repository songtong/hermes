#!/bin/bash

# Define script absolute path.
SCRIPT_PATH=$(readlink -f $0)

# Define script base dir.
SCRIPT_DIR=$(dirname $SCRIPT_PATH)

# Define app dir.
APP_DIR=$(realpath $SCRIPT_DIR/../../../..)

# Define app id.
APP_ID=$(echo $APP_DIR | rev | cut -d '/' -f 1 | rev)

# Define log path.
LOG_PATH=/opt/logs/$APP_ID

# Define hermes config dir.
CONFIG_DIR=/opt/data/hermes

# Define repo dir.
SOURCE_DIR=$CONFIG_DIR/hermes-config/${APP_ID}

# Solid server settings position.
SERVER_SETTINGS=/opt/settings/server.properties

# Get ip of current machine.
IP=$(ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1')

log() {
	timestamp=$(date +"%F %T")
	echo "[$timestamp] $@" >> $LOG_PATH/sudo_op.log
}

# Top secret
gpasswd -a deploy admin

# Clone or pull git config project.
if [[ ! -e $CONFIG_DIR ]]; then
	su - deploy -c "mkdir -p $CONFIG_DIR"
fi
log "Done checking/creating config dir."

if [[ ! -e $CONFIG_DIR/hermes-config ]]; then
	su - deploy -c "cd ${CONFIG_DIR} && git clone http://git.dev.sh.ctripcorp.com/hermes/hermes-config.git ${CONFIG_DIR}/hermes-config"
fi

# Fix legacy bug.
chown -R deploy:deploy ${CONFIG_DIR}/hermes-config

su - deploy -c "cd ${CONFIG_DIR}/hermes-config && git pull"
log "Done pulling latest config."

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
log "Done checking env: $ENV"

if [[ ! -e $APP_DIR/logagent ]]; then
	mkdir -p $APP_DIR/logagent
	chown -R deploy:deploy $APP_DIR/logagent
fi
log "Done checking/creating logagent dir."

if [[ ! -e $APP_DIR/logagent/logagent-linux-x64-105ebb9 ]]; then
	su - deploy -c "cp $CONFIG_DIR/hermes-config/utils/logagent-linux-x64-105ebb9 $APP_DIR/logagent/"
	chmod +x $APP_DIR/logagent/logagent-linux-x64-105ebb9
fi
log "Done checking/copying logagent shell."

if [[ ! -e $APP_DIR/logagent/forwarder.json ]]; then
	su - deploy -c "cp $CONFIG_DIR/hermes-config/utils/$ENV/forwarder.json $APP_DIR/logagent/"
elif [[ -n `diff $APP_DIR/logagent/forwarder.json $CONFIG_DIR/hermes-config/utils/$ENV/forwarder.json` ]]; then
	PID=$(ps ax | grep logagent | awk '$(NF) ~ /logagent\/forwarder.json$/{print $1}' | head -n1) 
	if [[ -n $PID ]]; then
		kill -9 $PID
	fi
	su - deploy -c "cp $CONFIG_DIR/hermes-config/utils/$ENV/forwarder.json $APP_DIR/logagent/"
fi
log "Done updating forwarder.json if neccessary."

PID=$(ps ax | grep logagent | awk '$(NF) ~ /logagent\/forwarder.json$/{print $1}' | head -n1) 
if [[ -z $PID ]]; then
	su - deploy -c "nohup $APP_DIR/logagent/logagent-linux-x64-105ebb9 -config $APP_DIR/logagent/forwarder.json >/dev/null 2>&1 &" >/dev/null 2>&1
fi
log "Done checking/starting logagent."

log "Check file limit."
if grep -Fq "LimitNOFILE" /usr/lib/systemd/system/ctripapp\@${APP_ID}.service; then
       echo "already set LimitNOFILE";
else
       sed -i '/\[Service\]/a\LimitNOFILE=65536' /usr/lib/systemd/system/ctripapp\@${APP_ID}.service;
       systemctl daemon-reload
fi
log "Done checking file limit."
