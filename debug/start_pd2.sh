#!/usr/bin/env bash
nohup ../bin/pd-server --name=pd-10 --config=./pd-10/pd.toml --data-dir=./pd-10/data --peer-urls=http://0.0.0.0:12380 --advertise-peer-urls=http://172.26.99.251:12380 --client-urls=http://0.0.0.0:12379 --advertise-client-urls=http://172.26.99.251:12379 --log-file=./pd-10/pd.log  --initial-cluster=pd-10=http://172.26.99.251:12380,pd-11=http://172.26.99.251:12381,pd-12=http://172.26.99.251:2383  >pd-10.nohup 2>&1 &

nohup ../bin/pd-server --name=pd-11 --config=./pd-11/pd.toml --data-dir=./pd-11/data --peer-urls=http://0.0.0.0:12381 --advertise-peer-urls=http://172.26.99.251:12381 --client-urls=http://0.0.0.0:12382 --advertise-client-urls=http://172.26.99.251:12382 --log-file=./pd-11/pd.log  --initial-cluster=pd-10=http://172.26.99.251:12380,pd-11=http://172.26.99.251:12381,pd-12=http://172.26.99.251:2383  >pd-11.nohup 2>&1 &

nohup ../bin/pd-server --name=pd-12 --config=./pd-12/pd.toml --data-dir=./pd-12/data --peer-urls=http://0.0.0.0:2383 --advertise-peer-urls=http://172.26.99.251:2383 --client-urls=http://0.0.0.0:2384 --advertise-client-urls=http://172.26.99.251:2384 --log-file=./pd-12/pd.log  --initial-cluster=pd-10=http://172.26.99.251:12380,pd-11=http://172.26.99.251:12381,pd-12=http://172.26.99.251:2383  >pd-12.nohup 2>&1 &

#../bin/pd-server --name=pd-3 --config=./pd-3/pd.toml --data-dir=./pd-3/data --peer-urls=http://0.0.0.0:2385 --advertise-peer-urls=http://172.26.99.251:2385 --client-urls=http://0.0.0.0:2386 --advertise-client-urls=http://172.26.99.251:2386 --log-file=./pd-3/pd.log  --initial-cluster=pd-0=http://172.26.99.251:2380,pd-1=http://172.26.99.251:2381,pd-2=http://172.26.99.251:2383,pd-3=http://172.26.99.251:2385
#
sleep 5
../bin/pd-ctl -u 127.0.0.1:12379 member list
../bin/pd-ctl -u 127.0.0.1:12379 health