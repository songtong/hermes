#! /bin/sh

proc_name="trifecta_0.18.19.bin.jar"

proc_num()
{
    num=`ps -ef | grep $proc_name | grep -v grep | wc -l`
    return $num
}

proc_num
number=$?
if [ $number -eq 0 ]
then
    cd /opt/ctrip/app/trifecta; sudo nohup java -jar trifecta_0.18.19.bin.jar --http-start >/dev/null 2>log &
fi
