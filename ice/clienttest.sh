#!/bin/bash

[[ "${BASH_SOURCE[0]}" != "${0}" ]] && {
    echo "Don't source clienttest.sh"
    return 1
}

LOG_PREFIX="clienttest"

getScriptDir() {
	if [[ -z $SCRIPTDIR ]]; then
		echo "$(cd $(dirname $0) && pwd)"
	else
		echo "${SCRIPTDIR}"
	fi
}

SCRIPT_DIR=$(getScriptDir)
echo "SCRIPT_DIR ${SCRIPT_DIR} loading ${SCRIPT_DIR}/commonenv.sh"
source ${SCRIPT_DIR}/commonenv.sh ${@}
log $LOG_PREFIX "Start for: $DATE"

if [ ! -d $JAVA_HOME ];
then
    echo "Need to setup JAVA_HOME for the client to start"
    exit 1
fi

MAIN_CLASS=com.theice.cfapi.client.example.ClientSample
start ${@}

#-M -U 2 -C 4 -D
#${JAVA} ${DEBUG} ${GC_FLAGS} "${OOM_JVM_PARAM}" -classpath "$CLASS_PATH" -Djava.library.path=${APP_DIR}/bin -Dconnection.timeout=60 ${MAIN_CLASS} -u <username> -p <password> -r 10 -o logs/cfapiclientsample.log <csp_ip> <csp_port> ./client.input >>$APP_DIR/logs/clientApi-${DATE}.out 2>&1 &

###${JAVA} ${DEBUG} ${GC_FLAGS} "${OOM_JVM_PARAM}" -classpath "$CLASS_PATH" -Djava.library.path=${APP_DIR}/bin -Dconnection.timeout=60 ${MAIN_CLASS} ${@}