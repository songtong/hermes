#!/bin/bash
cd `dirname $0`

function restart_trifecta {
        pid=$(ps ax | grep java | grep -E 'trifecta.*\.jar' | awk '{print $1}')
        if [ x$pid != x ];then
                kill -9 $pid
        fi
        nohup java -Xmx1G -jar trifecta_0.18.19.bin.jar --http-start >/dev/null 2>/opt/logs/trifecta/nohup.log &
}

function log {
        now=$(date +"%F %T ")
        echo $now $@ >> keepalive.log
}

#trifecta_jar=`ls trifecta*.jar`
#jps -lvm | awk -vtrifecta_jar=$trifecta_jar '$2==trifecta_jar{print $1}' >> keepalive.log

curl --connect-timeout 1 --max-time 3 http://127.0.0.1:8888 >/dev/null 2>&1
if [ $? != 0 ];then
        log "found trifecta down, restart it"
        restart_trifecta
else
        log "trifecta ok"
fi
