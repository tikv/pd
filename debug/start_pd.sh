#!/usr/bin/env bash
nohup ../bin/pd-server --name=pd-0 --config=./pd-0/pd.toml --data-dir=./pd-0/data --peer-urls=http://0.0.0.0:2380 --advertise-peer-urls=http://172.26.99.251:2380 --client-urls=http://0.0.0.0:2379 --advertise-client-urls=http://172.26.99.251:2379 --log-file=./pd-0/pd.log  --initial-cluster=pd-0=http://172.26.99.251:2380,pd-1=http://172.26.99.251:2381,pd-2=http://172.26.99.251:2383  >pd-0.nohup 2>&1 &

nohup ../bin/pd-server --name=pd-1 --config=./pd-1/pd.toml --data-dir=./pd-1/data --peer-urls=http://0.0.0.0:2381 --advertise-peer-urls=http://172.26.99.251:2381 --client-urls=http://0.0.0.0:2382 --advertise-client-urls=http://172.26.99.251:2382 --log-file=./pd-1/pd.log  --initial-cluster=pd-0=http://172.26.99.251:2380,pd-1=http://172.26.99.251:2381,pd-2=http://172.26.99.251:2383  >pd-1.nohup 2>&1 &

nohup ../bin/pd-server --name=pd-2 --config=./pd-2/pd.toml --data-dir=./pd-2/data --peer-urls=http://0.0.0.0:2383 --advertise-peer-urls=http://172.26.99.251:2383 --client-urls=http://0.0.0.0:2384 --advertise-client-urls=http://172.26.99.251:2384 --log-file=./pd-2/pd.log  --initial-cluster=pd-0=http://172.26.99.251:2380,pd-1=http://172.26.99.251:2381,pd-2=http://172.26.99.251:2383  >pd-2.nohup 2>&1 &

#../bin/pd-server --name=pd-3 --config=./pd-3/pd.toml --data-dir=./pd-3/data --peer-urls=http://0.0.0.0:2385 --advertise-peer-urls=http://172.26.99.251:2385 --client-urls=http://0.0.0.0:2386 --advertise-client-urls=http://172.26.99.251:2386 --log-file=./pd-3/pd.log  --initial-cluster=pd-0=http://172.26.99.251:2380,pd-1=http://172.26.99.251:2381,pd-2=http://172.26.99.251:2383,pd-3=http://172.26.99.251:2385
#
sleep 5
../bin/pd-ctl -u 127.0.0.1:2379 member list
../bin/pd-ctl -u 127.0.0.1:2379 health