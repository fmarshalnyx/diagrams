#!/bin/bash

getBoolean() {
    if [[ "${1,,}" == t* ]]; then
        echo true
    else
        echo false
    fi
}

executeRemote() {
    local HOST=$1
    local FUNC=$2
    echo "executing $FUNC on remote host $HOST with params ${@:3}"
    ssh $HOST "$(typeset -f); $FUNC ${@:3}" > /dev/null  2> >(grep -v --line-buffered "#" | cat -s) # filter out the annoying ssh warning message without filtering out all stderr
}

executeRemoteWithReturn() {
    local HOST=$1
    local FUNC=$2
    echo $(ssh $HOST "$(typeset -f); $FUNC ${@:3}" 2>/dev/null)
}

getCurrentDir() {
	echo "$(cd $(dirname $0) && pwd)"
}

getHostOS() {
	echo `uname`
}

getJavaHome() {
	# Test host to detemine the jdk location
	if [ getHostOS = "SunOS" ]
	then
		# set location for Solaris host
		echo "/var/opt/icetools/java/jdk1.7"
	else
		# set location for Linux host
		
		if [ -d "/var/opt/icetools/java/openjdk11.0.1" ]; then
			JAVA_HOME="/var/opt/icetools/java/openjdk11.0.1"
		elif [ -d "/var/opt/icetools/java/jdk1.8" ]; then
			JAVA_HOME="/var/opt/icetools/java/jdk1.8"
		elif [ -f "/usr/bin/java" ]; then
			JAVA_HOME="/usr"
		fi
		
		echo "${JAVA_HOME}"
	fi
}

setJavaEnv() {
	if [[ -z $JAVA_HOME ]]; then
		export JAVA_HOME=$(getJavaHome)
	else
		echo "Not setting JAVA_HOME, current val : ${JAVA_HOME}"
	fi	
	
	if [[ -z $JAVA ]]; then
		export JAVA=${JAVA_HOME}/bin/java
	else
		echo "Not setting JAVA, current val : ${JAVA}"
	fi
	
	echo "$(javaVersion)"
	
}

import() {
    if [[ -e ${1} ]]; then
        source ${1}
    fi
}

javaVersion() {
  echo $(${JAVA} -version 2>&1 | sed 's/foo//; 1q')
}

makeDir() {
    mkdir -p $1
}

deleteDir() {
    rm -rf $1
}

giveWorldWriteAccess() {
    chmod -R a+rw $1
}

pushFiles() {
    local TARGET_SVR=$1
    local FILTER=$2
    local TARGET_DIR=$3
    executeRemote $TARGET_SVR makeDir $TARGET_DIR
    echo "pushing files with filter ${FILTER}"
    scp ${FILTER} ${TARGET_SVR}:${TARGET_DIR} > /dev/null 2>&1
}

getFiles() {
    local FILTER=$1
    local TARGET_DIR=$2
    echo "getting files with filter ${FILTER}"
    mkdir -p $TARGET_DIR
    scp ${FILTER} ${TARGET_DIR} > /dev/null 2>&1
}

checkForUnzip() {
    local target=$1
    local unzipOption=$2
    if [[ $(getBoolean $unzipOption) == true ]]; then
        echo "unzipping all files in $target"
        gunzip -r $target > /dev/null 2>&1
    fi
}

## param 1 is the user name to run as
## remaining params contain the commands to run and their arguments
sudoRun() {
    echo "sudo su - $1 > /dev/null 2>&1 << BLOCK" > /tmp/.tmp.cmd
    echo ${@:2} >> /tmp/.tmp.cmd
    echo BLOCK >> /tmp/.tmp.cmd
    chmod +x /tmp/.tmp.cmd
    /tmp/.tmp.cmd >> /tmp/.sudoRun.log
    rm -f /tmp/.tmp.cmd
}

## param 1 is the user name to run as
## param 2 is directory to run from
## remaining params contain the commands to run and their arguments
sudoRunFromDir() {
    echo "sudo su - $1 > /dev/null 2>&1 << BLOCK" > /tmp/.tmp.cmd
    echo cd $2 >> /tmp/.tmp.cmd
    echo ${@:3} >> /tmp/.tmp.cmd
    echo BLOCK >> /tmp/.tmp.cmd
    chmod +x /tmp/.tmp.cmd
    /tmp/.tmp.cmd >> /tmp/.sudoRun.log
    rm -f /tmp/.tmp.cmd
}

## param 1 is a substring to search file names for (case insensitive)
## find the most recently modified file that contains the given character string in the file name (case insensitive)
newest() {
    ls -tra $(dirname $1) | grep -i $(basename $1) | tail -1
}
export -f newest

# simple logging function
log() {
        local prefix="[$(date +%Y/%m/%d\ %H:%M:%S)]: "
        echo "${prefix} $@" >&2
}
export -f log

files_in() {
    local base_dir="${1}"
    >&2 echo find ${base_dir} -name "*instrusummary*.gz" -type f 
    find ${base_dir} -name "*instrusummary*.gz" -type f
}

dirs_in() {
    local base_dir="${1}"
    >&2 echo find ${base_dir} -maxdepth 1 -mindepth 1 -type d
    find ${base_dir}  -maxdepth 1 -mindepth 1 -type d
}

getYesterday(){
	#date +"%Y-%m-%d" -d "yesterday"
	echo `TZ=EST5EDT+24 date +"%Y-%m-%d"`
}

takeThreadDump(){
    PID=${1}
    THREADDUMP="${JAVA_HOME}/bin/jstack "
    DATE=`date +%Y%m%d%H%M`
    $THREADDUMP ${PID} >> ${LOGS_DIR}/threaddump-${PID}-${DATE}.jvm.log
    echo "done with threaddump for process with PID ${PID}"
}

takeHisto(){
   PID=${1}
   HISTO="${JAVA_HOME}/bin/jmap -histo:live"
   DATE=`date +%Y%m%d%H%M`
   $HISTO ${PID} >> ${LOGS_DIR}/histo-${PID}-${DATE}.jvm.log
   echo "done with histogram for process with PID ${PID}"
}

takeHeapDump(){
   PID=${1}
   DATE=`date +%Y%m%d%H%M`
   HEAPDUMP="${JAVA_HOME}/bin/jmap -dump:live,file=${LOGS_DIR}/heapdump-${PID}-${DATE}.jvm.log"
   $HEAPDUMP ${PID}
   echo "done with heapdump"
}

takePermStat(){
   PID=${1}
   DATE=`date +%Y%m%d%H%M`	
   PERMSTAT="${JAVA_HOME}/bin/jmap -permstat"
   $PERMSTAT ${PID} >> ${LOGS_DIR}/permstat-${PID}-${DATE}.jvm.log
   echo "done with PermStat"
}
