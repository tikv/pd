#!/bin/bash

# ./ci-subtask.sh <TOTAL_TASK_N> <TASK_INDEX>

packages=(`go list ./...`)
dirs=(`find . -iname "*_test.go" -exec dirname {} \; | sort -u | sed -e "s/^\./github.com\/tikv\/pd/"`)
tasks=($(comm -12 <(printf "%s\n" "${packages[@]}") <(printf "%s\n" "${dirs[@]}")))

weight () {
    [[ $1 == "github.com/tikv/pd/server/api" ]] && return 30
    [[ $1 == "github.com/tikv/pd/server/schedule" ]] && return 30
    [[ $1 == "github.com/tikv/pd/tests/client" ]] && return 30
    [[ $1 =~ "pd/tests" ]] && return 5
    return 1
}

scores=(`seq "$1" | xargs -I{} echo 0`)

res=()
for t in ${tasks[@]}; do
    min_i=0
    for i in ${!scores[@]}; do
        [[ ${scores[i]} -lt ${scores[$min_i]} ]] && min_i=$i
    done
    weight $t
    scores[$min_i]=$((${scores[$min_i]} + $?))
    [[ $(($min_i+1)) -eq $2 ]] && res+=($t)
done

printf "%s " "${res[@]}"
