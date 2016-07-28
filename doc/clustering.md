# Clustering Guide

## A 3-node local cluster

**NOTICE**: Before using join, please make sure there is no data folder in current directory.

```bash
# Set correct HostIP here. 
export HostIP="192.168.199.105"

# Start pd1
pd-server --cluster-id=1 \
          --host=${HostIP} \
          --name=pd1 \
          --port=11234 \
          --http-port=19090 \
          --client-port=12379 \
          --peer-port=12380 \

# Start pd2
pd-server --cluster-id=1 \
          --host=${HostIP} \
          --name=pd2 \
          --port=21234 \
          --http-port=29090 \
          --client-port=22379 \
          --peer-port=22380 \
          --join="http://${HostIP}:12379" # pd1

# Start pd3
pd-server --cluster-id=1 \
          --host=${HostIP} \
          --name=pd3 \
          --port=31234 \
          --http-port=39090 \
          --client-port=32379 \
          --peer-port=32380 \
          --join="http://${HostIP}:22379" # pd2
# Or
# pd-server --cluster-id=1 \
#           --host=${HostIP} \
#           --name=pd3 \
#           --port=31234 \
#           --http-port=39090 \
#           --client-port=32379 \
#           --peer-port=32380 \
#           --join="http://${HostIP}:12379,http://${HostIP}:22379" # pd1, pd2
```

Use `http` to see the cluster members:

```bash
http :12379/v2/members

HTTP/1.1 200 OK
Content-Length: 400
Content-Type: application/json
Date: Thu, 21 Jul 2016 09:41:26 GMT
X-Etcd-Cluster-Id: 2d51087373879c4a

{
    "members": [
        {
            "clientURLs": [
                "http://192.168.199.105:32379"
            ], 
            "id": "53d7e5cda976b1c", 
            "name": "pd3", 
            "peerURLs": [
                "http://192.168.199.105:32380"
            ]
        }, 
        {
            "clientURLs": [
                "http://192.168.199.105:12379"
            ], 
            "id": "4c97c22075384b7c", 
            "name": "pd1", 
            "peerURLs": [
                "http://192.168.199.105:12380"
            ]
        }, 
        {
            "clientURLs": [
                "http://192.168.199.105:22379"
            ], 
            "id": "ce80f2badea42715", 
            "name": "pd2", 
            "peerURLs": [
                "http://192.168.199.105:22380"
            ]
        }
    ]
}
```

## A 3-node local cluster with Docker

Notice that we use docker port mapping, so we should set `advertise` port for outer communication.

```bash

# Set correct HostIP here. 
export HostIP="192.168.199.105"

# Start pd1
docker run -d -p 11234:1234 -p 19090:9090 -p 12379:2379 -p 12380:2380 --name pd1 \
        pingcap/pd  \
        --host=${HostIP} \
        --cluster-id=1 \
        --name=pd1 \
        --advertise-port=11234 \
        --advertise-client-port=12379 \
        --advertise-peer-port=12380 \


# Start pd2
docker run -d -p 21234:1234 -p 29090:9090 -p 22379:2379 -p 22380:2380 --name pd2 \
        pingcap/pd  \
        --host=${HostIP} \
        --cluster-id=1 \
        --name=pd2 \
        --advertise-port=21234 \
        --advertise-client-port=22379 \
        --advertise-peer-port=22380 \
        --join="http://${HostIP}:12379" # pd1

# Start pd3
docker run -d -p 31234:1234 -p 39090:9090 -p 32379:2379 -p 32380:2380 --name pd3 \
        pingcap/pd  \
        --host=${HostIP} \
        --cluster-id=1 \
        --name=pd3 \
        --advertise-port=31234 \
        --advertise-client-port=32379 \
        --advertise-peer-port=32380 \
        --join="http://${HostIP}:22379" # pd2
```

Use `http` to see cluster the members:

```bash
http :12379/v2/members 
HTTP/1.1 200 OK
Content-Length: 400
Content-Type: application/json
Date: Thu, 21 Jul 2016 09:25:54 GMT
X-Etcd-Cluster-Id: 2d51087373879c4a

{
    "members": [
        {
            "clientURLs": [
                "http://192.168.199.105:32379"
            ], 
            "id": "53d7e5cda976b1c", 
            "name": "pd3", 
            "peerURLs": [
                "http://192.168.199.105:32380"
            ]
        }, 
        {
            "clientURLs": [
                "http://192.168.199.105:12379"
            ], 
            "id": "4c97c22075384b7c", 
            "name": "pd1", 
            "peerURLs": [
                "http://192.168.199.105:12380"
            ]
        }, 
        {
            "clientURLs": [
                "http://192.168.199.105:22379"
            ], 
            "id": "ce80f2badea42715", 
            "name": "pd2", 
            "peerURLs": [
                "http://192.168.199.105:22380"
            ]
        }
    ]
}
```

## A 3 nodes multi-machine cluster

TODO...

## Remove PD node dynamically

TODO...

## Using PD with [kubernetes](http://kubernetes.io/)

TODO...