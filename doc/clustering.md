# Clustering Guide

## A 3 nodes local cluster

```bash
# Set correct HostIP here. 
export HostIP="192.168.199.105"

# Start pd1
pd-server --cluster-id=1 \
          --addr=0.0.0.0:11234 \
          --advertise-addr="${HostIP}:11234" \
          --http-addr="0.0.0.0:19090" \
          --cluster-id=1 \
          --etcd-name=pd1 \
          --etcd-data-dir="default.pd1" \
          --etcd-advertise-client-url="http://${HostIP}:12379" \
          --etcd-advertise-peer-url="http://${HostIP}:12380" \
          --etcd-initial-cluster="pd1=http://${HostIP}:12380,pd2=http://${HostIP}:22380,pd3=http://${HostIP}:32380" \
          --etcd-listen-peer-url="http://0.0.0.0:12380" \
          --etcd-listen-client-url="http://0.0.0.0:12379"  
          
# Start pd2 in one shell
pd-server --cluster-id=1 \
          --addr=0.0.0.0:21234 \
          --advertise-addr="${HostIP}:21234" \
          --http-addr="0.0.0.0:29090" \
          --cluster-id=1 \
          --etcd-name=pd2 \
          --etcd-data-dir="default.pd2" \
          --etcd-advertise-client-url="http://${HostIP}:22379" \
          --etcd-advertise-peer-url="http://${HostIP}:22380" \
          --etcd-initial-cluster="pd1=http://${HostIP}:12380,pd2=http://${HostIP}:22380,pd3=http://${HostIP}:32380" \
          --etcd-listen-peer-url="http://0.0.0.0:22380" \
          --etcd-listen-client-url="http://0.0.0.0:22379"  

# Start pd3
pd-server --cluster-id=1 \
          --addr=0.0.0.0:31234 \
          --advertise-addr="${HostIP}:31234" \
          --http-addr="0.0.0.0:39090" \
          --cluster-id=1 \
          --etcd-name=pd3 \
          --etcd-data-dir="default.pd3" \
          --etcd-advertise-client-url="http://${HostIP}:32379" \
          --etcd-advertise-peer-url="http://${HostIP}:32380" \
          --etcd-initial-cluster="pd1=http://${HostIP}:12380,pd2=http://${HostIP}:22380,pd3=http://${HostIP}:32380" \
          --etcd-listen-peer-url="http://0.0.0.0:32380" \
          --etcd-listen-client-url="http://0.0.0.0:32379"  
          
```

Use `http` to see cluster members:

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

## A 3 nodes local cluster with docker

```bash

# Set correct HostIP here. 
export HostIP="192.168.199.105"

# Start pd1
docker run -d -p 11234:1234 -p 19090:9090 -p 12379:2379 -p 12380:2380 --name pd1 \
        pingcap/pd  \
        --addr="0.0.0.0:1234" --advertise-addr=${HostIP}:11234 \
        --http-addr="0.0.0.0:9090" \
        --cluster-id=1 \
        --etcd-name=pd1 \
        --etcd-advertise-client-url="http://${HostIP}:12379" \
        --etcd-advertise-peer-url="http://${HostIP}:12380" \
        --etcd-initial-cluster="pd1=http://${HostIP}:12380,pd2=http://${HostIP}:22380,pd3=http://${HostIP}:32380" \
        --etcd-listen-peer-url="http://0.0.0.0:2380" \
        --etcd-listen-client-url="http://0.0.0.0:2379" 
       
# Start pd2
docker run -d -p 21234:1234 -p 29090:9090 -p 22379:2379 -p 22380:2380 --name pd2 \
        pingcap/pd  \
        --addr="0.0.0.0:1234" --advertise-addr=${HostIP}:21234 \
        --http-addr="0.0.0.0:9090" \
        --cluster-id=1 \
        --etcd-name=pd2 \
        --etcd-advertise-client-url="http://${HostIP}:22379" \
        --etcd-advertise-peer-url="http://${HostIP}:22380" \
        --etcd-initial-cluster="pd1=http://${HostIP}:12380,pd2=http://${HostIP}:22380,pd3=http://${HostIP}:32380" \
        --etcd-listen-peer-url="http://0.0.0.0:2380" \
        --etcd-listen-client-url="http://0.0.0.0:2379" 
        
# Start pd3
docker run -d -p 31234:1234 -p 39090:9090 -p 32379:2379 -p 32380:2380 --name pd3 \
        pingcap/pd  \
        --addr="0.0.0.0:1234" --advertise-addr=${HostIP}:31234 \
        --http-addr="0.0.0.0:9090" \
        --cluster-id=1 \
        --etcd-name=pd3 \
        --etcd-advertise-client-url="http://${HostIP}:32379" \
        --etcd-advertise-peer-url="http://${HostIP}:32380" \
        --etcd-initial-cluster="pd1=http://${HostIP}:12380,pd2=http://${HostIP}:22380,pd3=http://${HostIP}:32380" \
        --etcd-listen-peer-url="http://0.0.0.0:2380" \
        --etcd-listen-client-url="http://0.0.0.0:2379" 
```

Using `docker ps` to see cluster starts ok:

```bash
docker ps
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                                                                                                NAMES
4ca7aac99f2d        pingcap/pd          "pd-server --addr=0.0"   8 seconds ago       Up 7 seconds        0.0.0.0:31234->1234/tcp, 0.0.0.0:32379->2379/tcp, 0.0.0.0:32380->2380/tcp, 0.0.0.0:39090->9090/tcp   pd3
2ff56c6fa7a4        pingcap/pd          "pd-server --addr=0.0"   9 seconds ago       Up 8 seconds        0.0.0.0:21234->1234/tcp, 0.0.0.0:22379->2379/tcp, 0.0.0.0:22380->2380/tcp, 0.0.0.0:29090->9090/tcp   pd2
b8a3d7f815b2        pingcap/pd          "pd-server --addr=0.0"   9 seconds ago       Up 9 seconds        0.0.0.0:11234->1234/tcp, 0.0.0.0:12379->2379/tcp, 0.0.0.0:12380->2380/tcp, 0.0.0.0:19090->9090/tcp   pd1
```

Use `http` to see cluster members:

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

## A 3 nodes multi machine cluster

TODO...

## Add/Remove pd node dynamically

TODO...

## Using pd with [kubernetes](http://kubernetes.io/)

TODO...