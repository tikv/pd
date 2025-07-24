#!/usr/bin/env bash

# ./ci-subtask.sh <TASK_INDEX>

ROOT_PATH_COV=$(pwd)/covprofile
ROOT_PATH_JUNITFILE=$(pwd)/junitfile
# Currently, we only have 3 integration tests, so we can hardcode the task index.
integrations_dir=$(pwd)/tests/integrations

case $1 in
    1)
        # unit tests ignore `tests`, `server`, `schedule`,`utils` and `encryption`
        ./bin/pd-ut run --ignore tests,server,pkg/schedule,pkg/utils,pkg/encryption --race --coverprofile $ROOT_PATH_COV --junitfile $ROOT_PATH_JUNITFILE || exit 1
        ;;
    2)
        # unit tests only in `schedule` and `encryption` without `tests`
        ./bin/pd-ut run schedule,encryption --ignore tests --race --coverprofile $ROOT_PATH_COV --junitfile $ROOT_PATH_JUNITFILE || exit 1
        ;;
    3)
        # unit tests only in `server` and `utils` without `tests`
        ./bin/pd-ut run server,utils --ignore tests --race --coverprofile $ROOT_PATH_COV --junitfile $ROOT_PATH_JUNITFILE || exit 1
        ;;
    4)
        # tests only in `tests` without `server/api, server/cluster and `server/config`
        ./bin/pd-ut run tests --ignore tests/server/api,tests/server/cluster,tests/server/config --race --coverprofile $ROOT_PATH_COV --junitfile $ROOT_PATH_JUNITFILE || exit 1
        ;;
    5)
        # tests only in `server/api, server/cluster and `server/config` in `tests`
        ./bin/pd-ut run tests/server/api,tests/server/cluster,tests/server/config --race --coverprofile $ROOT_PATH_COV --junitfile $ROOT_PATH_JUNITFILE || exit 1
        ;;
    6)
        # tools tests
        cd ./tools && make ci-test-job && cat covprofile >> $ROOT_PATH_COV || exit 1
        ;;
    7)
        # integration test client
        ./bin/pd-ut it run client --race --coverprofile $ROOT_PATH_COV --junitfile $ROOT_PATH_JUNITFILE || exit 1
        # client tests
        cd ./client && make ci-test-job && cat covprofile >> $ROOT_PATH_COV || exit 1
        ;;
    8)
        # integration test tso
        ./bin/pd-ut it run tso --race --ignore mcs/tso --coverprofile $ROOT_PATH_COV --junitfile $ROOT_PATH_JUNITFILE || exit 1
        ;;
    9)
        # integration test mcs without mcs/tso
        ./bin/pd-ut it run mcs --race --ignore mcs/tso --coverprofile $ROOT_PATH_COV --junitfile $ROOT_PATH_JUNITFILE || exit 1
        ;;
    10)
        # integration test mcs/tso
        ./bin/pd-ut it run mcs/tso --race --coverprofile $ROOT_PATH_COV --junitfile $ROOT_PATH_JUNITFILE || exit 1
        ;;
esac
