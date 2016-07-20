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

    docker run --net=${net} -d -p 11234:1234 -p 19090:9090 -p 12379:2379 -p 12380:2380 --name pd1 \
        --link pd1:pd1 --link pd2:pd2 --link pd3:pd3 \
        pingcap/pd  \
        --addr="0.0.0.0:1234" --advertise-addr=${host}:11234 \
        --etcd-name=pd1 \
        --etcd-advertise-client-url="http://${host}:12379" \
        --etcd-advertise-peer-url="http://pd1:2380" \
        --etcd-initial-cluster-token="etcd-cluster" \
        --etcd-initial-cluster="pd1=http://pd1:2380,pd2=http://pd2:2380,pd3=http://pd3:2380" \
        --etcd-listen-peer-url="http://0.0.0.0:2380" \
        --etcd-listen-client-url="http://0.0.0.0:2379" \
       
    docker run --net=${net} -d -p 21234:1234 -p 29090:9090 -p 22379:2379 -p 22380:2380 --name pd2 \
        --link pd1:pd1 --link pd2:pd2 --link pd3:pd3 \
        pingcap/pd  \
        --addr="0.0.0.0:1234" --advertise-addr=${host}:21234 \
        --etcd-name=pd2 \
        --etcd-advertise-client-url="http://${host}:22379" \
        --etcd-advertise-peer-url="http://pd2:2380" \
        --etcd-initial-cluster-token="etcd-cluster" \
        --etcd-initial-cluster="pd1=http://pd1:2380,pd2=http://pd2:2380,pd3=http://pd3:2380" \
        --etcd-listen-peer-url="http://0.0.0.0:2380" \
        --etcd-listen-client-url="http://0.0.0.0:2379" \
        
    docker run --net=${net} -d -p 31234:1234 -p 39090:9090 -p 32379:2379 -p 32380:2380 --name pd3 \
        --link pd1:pd1 --link pd2:pd2 --link pd3:pd3 \
        pingcap/pd  \
        --addr="0.0.0.0:1234" --advertise-addr=${host}:31234 \
        --etcd-name=pd3 \
        --etcd-advertise-client-url="http://${host}:32379" \
        --etcd-advertise-peer-url="http://pd3:2380" \
        --etcd-initial-cluster-token="etcd-cluster" \
        --etcd-initial-cluster="pd1=http://pd1:2380,pd2=http://pd2:2380,pd3=http://pd3:2380" \
        --etcd-listen-peer-url="http://0.0.0.0:2380" \
        --etcd-listen-client-url="http://0.0.0.0:2379" \
       
}

function start {
    docker start pd1 pd2 pd3
}

function stop {
    docker stop pd1 pd2 pd3
}

function remove {
    docker rm -f pd1 pd2 pd3
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