#!/bin/bash

#set -x

HOSTOS=`uname`

getScriptDir() {
	if [[ -z $SCRIPTDIR ]]; then
	     echo "$(cd $(dirname $0) && pwd)"
	else
	     echo "${SCRIPTDIR}"
    fi
}
SCRIPT_DIR=$(getScriptDir)
echo "SCRIPT_DIR ${SCRIPT_DIR}"

#getAppDir(){
#	if [[ -z $APP_DIR ]]; then
#	     echo "$(cd ${SCRIPT_DIR}/.. && pwd)"
#	else
#	     echo "${APP_DIR}"
#	fi
#}
#
#APP_DIR=$(getAppDir)

APP_DIR=${SCRIPT_DIR}
echo "APP_DIR ${APP_DIR}"

import() {
    if [[ -e ${1} ]]; then
        source ${1}
    fi
}

importCommonTools() {
    echo "Importing util.sh"
    import "${SCRIPT_DIR}/util.sh"
}

# ****************************************************************************
# ***** BEGIN EXECUTION *****
# ****************************************************************************
### import common utilities
importCommonTools

setJavaEnv

JAVA_VER_NUM=$(javaVersion | head -1 | cut -d'"' -f2 | sed '/^1\./s///' | cut -d'.' -f1)
echo "JAVA_VER_NUM ${JAVA_VER_NUM}"

DATE=`date +%Y%m%d%H%M`
TODAYS_DATE=$(date +"%Y-%m-%d")
CURRENT_TIME=`date +%Y-%m-%d_%H-%M-%S`

CANRUN=0;

if [ ! -d $JAVA_HOME ];
then
    CANRUN=1;
    echo "Need to setup JAVA_HOME for the client to start"
    exit 1
fi
	
setCommonVariables(){
	echo "Setting common variables...."
	LOGS_DIR=${APP_DIR}/logs
	
	if [ ! -d ${LOGS_DIR} ]
	then
	     echo "Creating logs dir ${LOGS_DIR}." 
	     mkdir -p ${LOGS_DIR}
	fi
	
	
	GC_FLAGS="-server -XX:CompressedClassSpaceSize=250m -XX:MetaspaceSize=100M"
	GC_LOG_FILE="${LOGS_DIR}/${TODAYS_DATE}-cfapi_gc.log"

	if [ ${JAVA_VER_NUM} -gt 10 ]
	then
		GC_FLAGS="${GC_FLAGS} -Xss512k -XX:+UseG1GC -XX:G1ReservePercent=25 -XX:+UseCompressedOops -Xlog:gc:file=${GC_LOG_FILE}"
	else
		GC_FLAGS="${GC_FLAGS} -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:${GC_LOG_FILE}"
		GC_FLAGS="${GC_FLAGS} -XX:-CMSConcurrentMTEnabled -XX:CMSInitiatingOccupancyFraction=70 -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:ParallelGCThreads=6 -XX:ConcGCThreads=4 -XX:MaxTenuringThreshold=4"  
	fi

	GC_FLAGS="${GC_FLAGS} -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${LOGS_DIR}"
	GC_FLAGS="${GC_FLAGS} -Xms4G -Xmx4G"
		
	OOM_JVM_PARAM="-XX:OnOutOfMemoryError=\"kill -9 %p\""

	##turn this flag to run the client in debug mode.
	#DEBUG="-Xdebug -Xrunjdwp:transport=dt_socket,address=5005,server=y,suspend=n"
	#DEBUG="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"

	APP_NAME="cfapi-sample"

	##To change the connection timeout add the below JVM property.
	##JVM_PROPS="${GC_FLAGS} ${DEBUG} -Dconnection.timeout=60 -Dapp.name=${APP_NAME}"
	JVM_PROPS="${GC_FLAGS} ${DEBUG} -Dapp.name=${APP_NAME}"

	CLASSPATH="$CLASSPATH:${APP_DIR}:${APP_DIR}/lib/*"
	export LD_LIBRARY_PATH="${APP_DIR}/bin:${LD_LIBRARY_PATH}"	
}

start() {
    #echo `/usr/java/latest/bin/java $JVM_PROPS -XX:OnOutOfMemoryError="kill -9 %p" -cp .:yj-loadrunner.jar com.yj.loadrunner.YJLoadTest2 $APP_PROPS`
    echo "${JAVA} ${JVM_PROPS} "${OOM_JVM_PARAM}" -cp $CLASSPATH $MAIN_CLASS ${@}"
    #"${JAVA}" ${JVM_PROPS} -cp $CLASSPATH $MAIN_CLASS ${@} >>${APP_DIR}/logs/$CURRENT_TIME/${APP_NAME}.out 2>&1 &
    ${JAVA} ${JVM_PROPS} "${OOM_JVM_PARAM}" -cp "$CLASSPATH" $MAIN_CLASS ${@}
    #echo "logging to ${APP_DIR}/logs/$CURRENT_TIME/${APP_NAME}.out"
}

getAppPid(){
    pid=$(ps -aef | grep -v grep | grep "${JAVA}" | grep ${APP_NAME} | awk '{print $2}')
    echo ${pid}
}

stop() {
    echo "Stopping the cfapi java process $JAVA ${APP_NAME}"
    ##  pid=$(pgrep -u yjapp java)
    pid=$(getAppPid)
    echo "Stopping the process with pid ${pid}"
    if [ "${pid}" ]; then kill -9 ${pid}; fi;
    echo "Stopped process with pid ${pid}"
}

if [ ${CANRUN} == 0 ]; then
   echo "CANRUN=${CANRUN} starting the cfapi-java client."
   setCommonVariables
#  echo "Calling createAndSetServerLogDir SERVER_LOG_DIR_ARG $SERVER_LOG_DIR_ARG"
#  createAndSetServerLogDir $SERVER_LOG_DIR_ARG	
fi

echo "LOGS_DIR ${LOGS_DIR}"
echo "JAVA_HOME ${JAVA_HOME}"
echo "JAVA ${JAVA}"
echo "LOG_PREFIX ${LOG_PREFIX}"
echo "JVM_PROPS ${JVM_PROPS}"
echo "APP_NAME ${APP_NAME}"
echo "OOM_JVM_PARAM ${OOM_JVM_PARAM}"
echo "DEBUG ${DEBUG}"
echo "CLASSPATH ${CLASSPATH}"
echo "LD_LIBRARY_PATH ${LD_LIBRARY_PATH}"
