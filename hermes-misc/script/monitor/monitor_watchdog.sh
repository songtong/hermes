LOG=~/monitor_watchdog.log

diff=`grep "Checker suite start.." /opt/logs/hermes-monitor/hermes-monitor.log | tail -n1 | awk -F"," '{print $1}' | sed 's/[-:]/ /g' | awk 'function abs(v) {return v < 0 ? -v : v} {print abs(mktime($0) - systime())}'`


if [ $diff -gt 900 ];then
   echo "[`date`] ERROR" >> $LOG
   /opt/ctrip/app/hermes-monitor/bin/startup.sh stop
   /opt/ctrip/app/hermes-monitor/bin/startup.sh start
else
   echo "[`date`] OK" >> $LOG
fi