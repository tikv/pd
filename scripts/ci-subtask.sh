#!/usr/bin/env bash

# ./ci-subtask.sh <TOTAL_TASK_N> <TASK_INDEX>

ROOT_PATH=../../

if [[ $2 -gt 1 ]]; then
    # run tools tests in task 2
    if [[ $2 -eq 2 ]]; then
        cd ./tools && make ci-test-job && cd .. && cat ./covprofile >> covprofile || exit 1
        exit
    fi

    # Currently, we only have 3 integration tests, so we can hardcode the task index.
    integrations_dir=./tests/integrations
    integrations_tasks=($(find "$integrations_dir" -mindepth 1 -maxdepth 1 -type d))
    for t in "${integrations_tasks[@]}"; do
        if [[ "$t" = "$integrations_dir/client" && $2 -eq 3 ]]; then
            cd ./client && make ci-test-job && cat covprofile >> ../covprofile && cd .. || exit 1
            cd $integrations_dir && make ci-test-job test_name=client && cat ./client/covprofile >> "$ROOT_PATH/covprofile" || exit 1
        elif [[ "$t" = "$integrations_dir/tso" && $2 -eq 4 ]]; then
            cd $integrations_dir && make ci-test-job test_name=tso && cat ./tso/covprofile >> "$ROOT_PATH/covprofile" || exit 1
        elif [[ "$t" = "$integrations_dir/mcs" && $2 -eq 5 ]]; then
            cd $integrations_dir && make ci-test-job test_name=mcs && cat ./mcs/covprofile >> "$ROOT_PATH/covprofile" || exit 1
        fi
    done
else

    ./bin/pd-ut run --race --coverprofile covprofile
fi
