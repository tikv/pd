#!/usr/bin/env bash

# ./ci-subtask.sh <TOTAL_TASK_N> <TASK_INDEX>

ROOT_PATH_COV=$(pwd)/covprofile
# Currently, we only have 3 integration tests, so we can hardcode the task index.
integrations_dir=$(pwd)/tests/integrations

case $1 in
    1)
        # unit tests ignore `tests`
        ./bin/pd-ut run --race --ignore tests --coverprofile $ROOT_PATH_COV || exit 1
        ;;
    2)
        # unit tests only in `tests`
        ./bin/pd-ut run tests --race --coverprofile $ROOT_PATH_COV || exit 1
        ;;
    3)
        # tools tests
        cd ./tools && make ci-test-job && cat covprofile >> $ROOT_PATH_COV || exit 1
        ;;
    4)
        # integration test client
        ./bin/pd-ut it run client --race --coverprofile $ROOT_PATH_COV || exit 1
        # client tests
        cd ./client && make ci-test-job && cat covprofile >> $ROOT_PATH_COV || exit 1
        ;;
    5)
        # integration test tso
        ./bin/pd-ut it run tso --race --coverprofile $ROOT_PATH_COV || exit 1
        ;;
    6)
        # integration test mcs
        ./bin/pd-ut it run mcs --race --coverprofile $ROOT_PATH_COV || exit 1
        ;;
esac
