#!/bin/bash

# TODO: use docker compose
# advertise host
host="127.0.0.1"

# We should use a user defined network.
# See https://docs.docker.com/engine/userguide/networking/dockernetworks/
net="isolated_nw"

function init {
    if ! docker network inspect ${net} > /dev/null 2>&1; then
        echo "crate docker network ${net}"
        docker network create --driver bridge ${net} 
    fi

    docker run --net=${net} -d -p 12379:2379 -p 12380:2380 -p 14001:4001 --name etcd1 \
        --link etcd1:etcd1 --link etcd2:etcd2 --link etcd3:etcd3 \
        pingcap/etcd \
        --name=etcd1 \
        --advertise-client-urls="http://${host}:12379,http://${host}:14001" \
        --initial-advertise-peer-urls="http://etcd1:2380" \
        --initial-cluster-token="etcd-cluster" \
        --initial-cluster="etcd1=http://etcd1:2380,etcd2=http://etcd2:2380,etcd3=http://etcd3:2380" \
        --listen-peer-urls="http://0.0.0.0:2380,http://0.0.0.0:7001" \
        --listen-client-urls="http://0.0.0.0:2379,http://0.0.0.0:4001" \
        --initial-cluster-state="new"

    docker run --net=${net} -d -p 22379:2379 -p 22380:2380 -p 24001:4001 --name etcd2 \
        pingcap/etcd \
        --name=etcd2 \
        --advertise-client-urls="http://${host}:22379,http://${host}:24001" \
        --initial-advertise-peer-urls="http://etcd2:2380" \
        --initial-cluster-token="etcd-cluster" \
        --initial-cluster="etcd1=http://etcd1:2380,etcd2=http://etcd2:2380,etcd3=http://etcd3:2380" \
        --listen-peer-urls="http://0.0.0.0:2380,http://0.0.0.0:7001" \
        --listen-client-urls="http://0.0.0.0:2379,http://0.0.0.0:4001" \
        --initial-cluster-state="new"

    docker run --net=${net} -d -p 32379:2379 -p 32380:2380 -p 34001:4001 --name etcd3 \
        --link etcd1:etcd1 --link etcd2:etcd2 --link etcd3:etcd3 \
        pingcap/etcd \
        --name=etcd3 \
        --advertise-client-urls="http://${host}:32379,http://${host}:34001" \
        --initial-advertise-peer-urls="http://etcd3:2380" \
        --initial-cluster-token="etcd-cluster" \
        --initial-cluster="etcd1=http://etcd1:2380,etcd2=http://etcd2:2380,etcd3=http://etcd3:2380" \
        --listen-peer-urls="http://0.0.0.0:2380,http://0.0.0.0:7001" \
        --listen-client-urls="http://0.0.0.0:2379,http://0.0.0.0:4001" \
        --initial-cluster-state="new"

    sleep 1

    docker run --net=${net} -d -p 11234:1234 --name pd1 \
        --link etcd1:etcd1 --link etcd2:etcd2 --link etcd3:etcd3 \
        pingcap/pd  \
        --etcd=etcd1:2379,etcd2:2379,etcd3:2379 \
        --addr="0.0.0.0:1234" --advertise-addr=${host}:11234 

    docker run --net=${net} -d -p 21234:1234 --name pd2 \
        --link etcd1:etcd1 --link etcd2:etcd2 --link etcd3:etcd3 \
        pingcap/pd  \
        --etcd=etcd1:2379,etcd2:2379,etcd3:2379 \
        --addr="0.0.0.0:1234" --advertise-addr=${host}:21234 

    docker run --net=${net} -d -p 31234:1234 --name pd3 \
        --link etcd1:etcd1 --link etcd2:etcd2 --link etcd3:etcd3 \
        pingcap/pd  \
        --etcd=etcd1:2379,etcd2:2379,etcd3:2379 \
        --addr="0.0.0.0:1234" --advertise-addr=${host}:31234 
}

function start {
    docker start etcd1 etcd2 etcd3 pd1 pd2 pd3
}

function stop {
    docker stop pd1 pd2 pd3 etcd1 etcd2 etcd3 
}

function remove {
    docker rm -f pd1 pd2 pd3 etcd1 etcd2 etcd3
}

i=$1
case $1 in
    -h=*|--host=*)
        host="${i#*=}"
        ;; 
    -n=*|--net=*)
        net="${i#*=}"
        ;;
    *)
    ;;
esac

for i in "$@"
do
    case $i in
        "init")
            init
        ;;
        "start")
            start
        ;;
        "stop")
            stop
        ;;
        "remove")
            remove
        ;;
        -h|--help)
            echo "[-h|--host=host] [-n|--net=network] [init|start|stop]"
            exit 0
            ;;
        *)     
        ;;
    esac
done