#!/bin/bash
# Wait until `tiup playground` command runs success

TIUP_BIN_DIR=$HOME/.tiup/bin/tiup
INTERVAL=$1
MAX_TIMES=$2

if ([ -z "${INTERVAL}" ] || [ -z "${MAX_TIMES}" ]); then
    echo "Usage: command <interval> <max_times>"
    exit 1
fi

for ((i=0; i<${MAX_TIMES}; i++)); do
    sleep ${INTERVAL}
    $TIUP_BIN_DIR playground display --tag pd_real_cluster_test
    if [ $? -eq 0 ]; then
        exit 0
    fi
    cat ./playground.log
done

exit 1