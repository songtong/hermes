#!/usr/bin/env bash
DATASOURCE_FILE="/data/appdatas/hermes/datasources.xml"
CAT_FILE="/opt/ctrip/data/cat/client.xml"
MAVEN_PATH="/opt/ctrip/maven/apache-maven-3.2.5"
MAVEN_SETTING=${MAVEN_PATH}/conf/settings.xml

LOG_FOLDER="/opt/logs/hermes"

GIT_REPO="git@git.dev.sh.ctripcorp.com:ske/hermes.git"



yesOrNo() {
	read var
	if [ $var == "n" ] 
	then
		echo "Bye" 
		exit
	elif [ $var == "y" ] 
	then
		return 1
	else
		echo "Input 'y' or 'n'"
		yesOrNo
	fi
}

checkDatasource() {
	if [ -f $DATASOURCE_FILE ]
	then	
		echo "$DATASOURCE_FILE is ok."
	else
		echo "$DATASOURCE_FILE NOT found, Continue?['y' or 'n']"
		yesOrNo
	fi
}

checkCat() {
	if [ -f $CAT_FILE ]
	then	
		echo "$CAT_FILE is ok."
	else
		echo "$CAT_FILE NOT found, Continue?['y' or 'n']"
		yesOrNo
	fi
}

checkMaven() {
	if [ -d $MAVEN_PATH ]
	then
		echo "$MAVEN_PATH is ok."
	else 
		
		echo "$MAVEN_PATH NOT found, Continue?['y' or 'n']"
		yesOrNo
	fi
}

checkMavenSetting() {
	if [ -f $MAVEN_SETTING ]
	then
		echo "Login Use: "`whoami`
		echo "Maven settings about repo home is: "
		sed -n '55p' $MAVEN_SETTING
		echo "Continue?['y' or 'n']"
		yesOrNo
	else
		echo "$MAVEN_SETTING NOT found, Continue?['y' or 'n']"
		yesOrNo
	fi

}

mkLogFolder() {
	echo "Going to do: sudo mkdir -p $LOG_FOLDER" 
	sudo mkdir -p $LOG_FOLDER
	echo "Going to do: sudo chown "`whoami` " $LOG_FOLDER"
	sudo chown `whoami` $LOG_FOLDER

	echo "Continue?['y' or 'n']"
	yesOrNo
}

doClone() {
	echo "Going to do: git clong $GIT_REPO in home folder"
	git clone $GIT_REPO /home/`whoami`/repo
}

echo "Step1: Check datasource.xml"
checkDatasource
echo "Step2: Check CAT:client.xml"
checkCat
echo "Step3: Check Maven Folder"
checkMaven
echo "Step4: Check Maven Setting"
checkMavenSetting
echo "Step5: mkdir Log Folder"
mkLogFolder
echo "Step6: Do Clone Mannually"
doClone

echo "All Done!"
