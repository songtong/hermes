#!/bin/sh
set -e
set -u

mkdir -p /opt/ctrip/app/hermes/metaserver/old-deployment
mv /opt/ctrip/app/tomcat/webapps/ROOT/hermes-metaserver*.war /opt/ctrip/app/hermes/metaserver/old-deployment

mv hermes-metaserver/target/hermes-metaserver*.war  /opt/ctrip/app/tomcat/webapps/ROOT
cd /opt/ctrip/app/tomcat/webapps/ROOT
jar -xf /opt/ctrip/app/tomcat/webapps/ROOT/hermes-metaserver*.war
sudo /opt/ctrip/app/tomcat/bin/shutdown.sh
sleep 10
sudo /opt/ctrip/app/tomcat/bin/startup.sh
