#!/bin/sh
set -e
set -u

mkdir -p /opt/ctrip/app/hermes-portal/old-deployment
mv /opt/ctrip/app/tomcat/webapps/ROOT/hermes-portal*.war /opt/ctrip/app/hermes-portal/old-deployment

mv hermes-portal/target/hermes-portal*.war  /opt/ctrip/app/tomcat/webapps/ROOT
cd /opt/ctrip/app/tomcat/webapps/ROOT
jar -xf /opt/ctrip/app/tomcat/webapps/ROOT/hermes-portal*.war
sudo /opt/ctrip/app/tomcat/bin/shutdown.sh
sleep 10
sudo /opt/ctrip/app/tomcat/bin/startup.sh
