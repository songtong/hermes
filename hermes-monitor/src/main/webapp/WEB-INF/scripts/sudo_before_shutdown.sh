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

# Top secret
gpasswd -a deploy admin

# Clone or pull git config project.
if [[ ! -e $CONFIG_DIR ]]; then
	su - deploy -c "mkdir -p $CONFIG_DIR"
fi

if [[ ! -e $CONFIG_DIR/hermes-config ]]; then
	su - deploy -c "cd $CONFIG_DIR && git clone http://git.dev.sh.ctripcorp.com/hermes/hermes-config.git $CONFIG_DIR/hermes-config"
fi

su - deploy -c "cd $CONFIG_DIR/hermes-config && git pull"

# Copy watchdog shell.
su - deploy -c "cp $SOURCE_DIR/monitor_watchdog.sh $CONFIG_DIR/monitor_watchdog.sh"

# Install crontab.
crontab $SOURCE_DIR/crontab.def
