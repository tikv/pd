#!/usr/bin/env bash

# ./ci-subtask.sh <TOTAL_TASK_N> <TASK_INDEX>

ROOT_PATH=../../

if [[ $2 -gt 9 ]]; then
    # run tools tests
    if [[ $2 -eq 10 ]]; then
        cd ./tools && make ci-test-job && cd .. && cat ./covprofile >> covprofile || exit 1
        exit
    fi

    # Currently, we only have 3 integration tests, so we can hardcode the task index.
    integrations_dir=./tests/integrations
    integrations_tasks=($(find "$integrations_dir" -mindepth 1 -maxdepth 1 -type d))
    for t in "${integrations_tasks[@]}"; do
        if [[ "$t" = "$integrations_dir/client" && $2 -eq 11 ]]; then
            cd ./client && make ci-test-job && cd .. && cat ./covprofile >> covprofile || exit 1
            cd $integrations_dir && make ci-test-job test_name=client && cat ./client/covprofile >> "$ROOT_PATH/covprofile" || exit 1
        elif [[ "$t" = "$integrations_dir/tso" && $2 -eq 12 ]]; then
            cd $integrations_dir && make ci-test-job test_name=tso && cat ./tso/covprofile >> "$ROOT_PATH/covprofile" || exit 1
        elif [[ "$t" = "$integrations_dir/mcs" && $2 -eq 13 ]]; then
            cd $integrations_dir && make ci-test-job test_name=mcs && cat ./mcs/covprofile >> "$ROOT_PATH/covprofile" || exit 1
        fi
    done
else
    # Get package test list.
    packages=($(go list ./...))
    dirs=($(find . -iname "*_test.go" -exec dirname {} \; | sort -u | sed -e "s/^\./github.com\/tikv\/pd/"))
    tasks=($(comm -12 <(printf "%s\n" "${packages[@]}") <(printf "%s\n" "${dirs[@]}")))

    weight() {
        [[ $1 == "github.com/tikv/pd/server/api" ]] && return 30
        [[ $1 == "github.com/tikv/pd/pkg/schedule" ]] && return 30
        [[ $1 == "github.com/tikv/pd/pkg/core" ]] && return 30
        [[ $1 == "github.com/tikv/pd/tests/server/api" ]] && return 30
        [[ $1 =~ "pd/tests" ]] && return 5
        return 1
    }

    # Create an associative array to store the weight of each task.
    declare -A task_weights
    for t in ${tasks[@]}; do
        weight $t
        task_weights[$t]=$?
    done

    # Sort tasks by weight in descending order.
    tasks=($(printf "%s\n" "${tasks[@]}" | sort -rn))

    scores=($(seq "$1" | xargs -I{} echo 0))

    res=()
    for t in ${tasks[@]}; do
        min_i=0
        for i in ${!scores[@]}; do
            [[ ${scores[i]} -lt ${scores[$min_i]} ]] && min_i=$i
        done
        scores[$min_i]=$((${scores[$min_i]} + ${task_weights[$t]}))
        [[ $(($min_i + 1)) -eq $2 ]] && res+=($t)
    done

    CGO_ENABLED=1 go test -timeout=15m -tags deadlock -race -covermode=atomic -coverprofile=covprofile -coverpkg=./... ${res[@]}
fi
