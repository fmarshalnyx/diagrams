#!/bin/bash

[[ "${BASH_SOURCE[0]}" != "${0}" ]] && {
    echo "Don't source stop-cfapi.sh"
    return 1
}

LOG_PREFIX="stop-cfapi"

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


stop ${@}
