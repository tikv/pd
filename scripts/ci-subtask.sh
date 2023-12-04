#!/bin/bash

# ./ci-subtask.sh <TOTAL_TASK_N> <TASK_INDEX>

if [[ $2 -gt 10 ]]; then
    # Get integration test list.
    makefile_dirs=($(find . -iname "Makefile" -exec dirname {} \; | sort -u))
    submod_dirs=($(find . -iname "go.mod" -exec dirname {} \; | sort -u))
    integration_tasks=$(comm -12 <(printf "%s\n" "${makefile_dirs[@]}") <(printf "%s\n" "${submod_dirs[@]}") | grep "./tests/integrations/*")
    # Currently, we only have 3 integration tests, so we can hardcode the task index.
    for t in ${integration_tasks[@]}; do
        if [[ "$t" = "./tests/integrations/client" && "$2" = 11 ]]; then
            res=("./client")
            res+=($t)
            printf "%s " "${res[@]}"
            break
        elif [[ "$t" = "./tests/integrations/tso" && "$2" = 12 ]]; then
            printf "%s " "$t"
            break
        elif [[ "$t" = "./tests/integrations/mcs" && "$2" = 13 ]]; then
            printf "%s " "$t"
            break
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
    printf "%s " "${res[@]}"
fi
