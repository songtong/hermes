#!/bin/bash

# Top secret
gpasswd -a deploy admin

# Define app id.
APP_ID=100003806

if grep -Fq "LimitNOFILE" /usr/lib/systemd/system/ctripapp\@${APP_ID}.service; then
       echo "already set LimitNOFILE";
else
       sed -i '/\[Service\]/a\LimitNOFILE=65536' /usr/lib/systemd/system/ctripapp\@${APP_ID}.service;
       systemctl daemon-reload
fi
