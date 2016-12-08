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

log() {
	timestamp=$(date +"%F %T")
	echo "[$timestamp] $@" >> $LOG_PATH/sudo_op.log
}

# Top secret
gpasswd -a deploy admin

# Ensure config dir exist.
if [[ ! -e $CONFIG_DIR ]]; then
	mkdir -p $CONFIG_DIR && chown -R deploy:deploy ${CONFIG_DIR}/hermes-config
fi
log "Done checking/creating config dir."

if [[ ! -e $APP_DIR/logagent ]]; then
	mkdir -p $APP_DIR/logagent && chown -R deploy:deploy $APP_DIR/logagent
fi
log "Done checking/creating logagent dir."

log "Check file limit."
if grep -Fq "LimitNOFILE" /usr/lib/systemd/system/ctripapp\@${APP_ID}.service; then
       echo "already set LimitNOFILE";
else
       sed -i '/\[Service\]/a\LimitNOFILE=65536' /usr/lib/systemd/system/ctripapp\@${APP_ID}.service;
       systemctl daemon-reload
fi
log "Done checking file limit."
