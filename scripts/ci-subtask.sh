#!/bin/bash

# ./ci-subtask.sh <TOTAL_TASK_N> <TASK_INDEX> <INTEGRATION_TEST_ONLY>

# Get package test list.
packages=(`go list ./...`)
dirs=(`find . -iname "*_test.go" -exec dirname {} \; | sort -u | sed -e "s/^\./github.com\/tikv\/pd/"`)
tasks=($(comm -12 <(printf "%s\n" "${packages[@]}") <(printf "%s\n" "${dirs[@]}")))
# Get integration test list.
makefile_dirs=(`find . -iname "Makefile" -exec dirname {} \; | sort -u`)
submod_dirs=(`find . -iname "go.mod" -exec dirname {} \; | sort -u`)
integration_tasks=$(comm -12 <(printf "%s\n" "${makefile_dirs[@]}") <(printf "%s\n" "${submod_dirs[@]}") | grep "./tests/integrations/*")

all_tasks=("${tasks[@]}" "${integration_tasks[@]}")

weight () {
    [[ $1 =~ "tests/integrations" ]] && return 50
    [[ $1 == "github.com/tikv/pd/server/api" ]] && return 30
    [[ $1 == "github.com/tikv/pd/pkg/schedule" ]] && return 30
    [[ $1 =~ "pd/tests" ]] && return 5
    return 1
}

scores=(`seq "$1" | xargs -I{} echo 0`)

res=()
for t in ${all_tasks[@]}; do
    min_i=0
    for i in ${!scores[@]}; do
        [[ ${scores[i]} -lt ${scores[$min_i]} ]] && min_i=$i
    done
    weight $t
    scores[$min_i]=$((${scores[$min_i]} + $?))
    if [[ $(($min_i+1)) -eq $2 ]]; then
        # Integration test only.
        if [[ $3 && "$t" =~ "tests/integrations" ]]; then
            res+=($t)
        elif [[ ! $3 && "$t" =~ "github.com/tikv/pd" ]]; then
            res+=($t)
        fi
    fi
done

printf "%s " "${res[@]}"
