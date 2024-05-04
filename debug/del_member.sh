#!/usr/bin/env bash

curl http://127.0.0.1:2379/pd/api/v1/members/name/pd-2 -XDELETE
curl http://127.0.0.1:2382/pd/api/v1/members/name/pd-2 -XDELETE
curl http://127.0.0.1:2384/pd/api/v1/members/name/pd-2 -XDELETE

for pid in `ps -ef | grep pd-server | grep -v grep | grep 2384 | awk '{print $2}'`
do
    kill -9 $pid
done
echo "pd-2 $1 stopped successfully!"



