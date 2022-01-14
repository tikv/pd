#!/bin/bash

# ./ci-subtask.sh <TOTAL_TASK_N> <TASK_INDEX>

packages=(`go list ./...`)
dirs=(`find . -iname "*_test.go" -exec dirname {} \; | sort -u | sed -e "s/^\./github.com\/tikv\/pd/"`)
tasks=($(comm -12 <(printf "%s\n" "${packages[@]}") <(printf "%s\n" "${dirs[@]}")))

weight () {
    if [[ $1 == "github.com/tikv/pd/server/api" ]]; then return 30; fi
    if [[ $1 == "github.com/tikv/pd/server/tso" ]]; then return 30; fi
    if [[ $1 == "github.com/tikv/pd/server/schedule" ]]; then return 30; fi
    if [[ $1 == "github.com/tikv/pd/tests/client" ]]; then return 30; fi
    if [[ $1 =~ "pd/tests" ]]; then return 5; fi
    return 1
}

scores=(`seq "$1" | xargs -I{} echo 0`)

res=()
for t in ${tasks[@]}; do
    min_i=0
    for i in ${!scores[@]}; do
        if [[ ${scores[i]} -lt ${scores[$min_i]} ]]; then min_i=$i; fi
    done
    weight $t
    scores[$min_i]=$((${scores[$min_i]} + $?))
    if [[ $(($min_i+1)) -eq $2 ]]; then res+=($t); fi
done

printf "%s " "${res[@]}"
