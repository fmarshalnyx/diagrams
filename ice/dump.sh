#!/bin/bash

[[ "${BASH_SOURCE[0]}" != "${0}" ]] && {
    echo "Don't source dump.sh"
    return 1
}

LOG_PREFIX="dump.sh"

getScriptDir() {
	if [[ -z $SCRIPTDIR ]]; then
		echo "$(cd $(dirname $0) && pwd)"
	else
		echo "${SCRIPTDIR}"
	fi
}

SCRIPT_DIR=$(getScriptDir)
echo "SCRIPT_DIR ${SCRIPT_DIR}"

source ${SCRIPT_DIR}/commonenv.sh ${@}
log $LOG_PREFIX "Start for: $DATE"

pid=$(getAppPid)

echo "Taking dumps for process with PID: ${pid}"
takeThreadDump ${pid}
takeHisto ${pid}
takeHeapDump ${pid}
#takePermStat ${pid}
takeThreadDump ${pid}
