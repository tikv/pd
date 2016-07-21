# PD 

[![Build Status](https://travis-ci.org/pingcap/pd.svg?branch=master)](https://travis-ci.org/pingcap/pd)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/pd)](https://goreportcard.com/report/github.com/pingcap/pd)

PD is the abbreviation for Placement Driver. It is used to manage and schedule the [TiKV](https://github.com/pingcap/tikv) cluster. 

PD supports distribution and fault-tolerance by embedding [etcd](https://github.com/coreos/etcd). 

## Build

1. Make sure [​*Go*​](https://golang.org/) (version 1.5+) is installed.
2. Use `make build` to install PD. PD is installed in the `bin` directory. 

## Usage

### PD ports

You can use the following default ports in PD:

+ **1234**: for client requests with customized protocol.
+ **9090**: for client requests with HTTP.
+ **2379**: for embedded etcd client requests 
+ **2380**: for embedded etcd peer communication.

You can change these ports when starting PD.

### Single Node

```bash
# Set correct HostIP here. 
export HostIP="192.168.199.105"

pd-server --cluster-id=1 \
          --addr=0.0.0.0:1234 \
          --advertise-addr="${HostIP}:1234" \
          --http-addr="0.0.0.0:9090" \
          --etcd-name="default" \
          --etcd-data-dir="default.pd" \
          --etcd-listen-peer-url="http://0.0.0.0:2380" \
          --etcd-advertise-peer-url="http://${HostIP}:2380" \
          --etcd-listen-client-url="http://0.0.0.0:2379" \
          --etcd-advertise-client-url="http://${HostIP}:2379" \
          --etcd-initial-cluster="default=http://${HostIP}:2380" \
          --etcd-initial-cluster-state="new"  
```

Command flag explanation:

+ `cluster-id`: Unique ID to distinguish different PD clusters. It can't be changed after bootstrapping.  
+ `addr`: Listening address for client traffic. The default official address is `0.0.0.0:1234`.
+ `advertise-addr`: Advertise address for external client communication. It must be accessible to the PD node.
+ `http-addr`: HTTP listening address for client requests. 
+ `etcd-name`: etcd human readable name for this node. 
+ `etcd-data-dir`: etcd path to the data directory.
+ `etcd-listen-peer-url`: etcd listening address for peer traffic.
+ `etcd-advertise-peer-url`: etcd advertise peer url to the rest of the cluster.
+ `etcd-listen-client-url`: etcd listening address for client traffic.
+ `etcd-advertise-client-url`: etcd advertise url to the public, it must be accessible to PD machine.
+ `etcd-initial-cluster-state`: etcd initial cluster state. The value is either`new` or `existing`.
+ `etcd-initial-cluster`: etcd initail cluster configuration for bootstrapping. 

Using `curl` to see PD member:

```bash
curl :2379/v2/members

{"members":[{"id":"f62e88a6e81c149","name":"default","peerURLs":["http://192.168.199.105:2380"],"clientURLs":["http://192.168.199.105:2379"]}]}
```

A better tool [httpie](https://github.com/jkbrzt/httpie) is recommended:

```bash
http :2379/v2/members
HTTP/1.1 200 OK
Content-Length: 144
Content-Type: application/json
Date: Thu, 21 Jul 2016 09:37:12 GMT
X-Etcd-Cluster-Id: 33dc747581249309

{
    "members": [
        {
            "clientURLs": [
                "http://192.168.199.105:2379"
            ], 
            "id": "f62e88a6e81c149", 
            "name": "default", 
            "peerURLs": [
                "http://192.168.199.105:2380"
            ]
        }
    ]
}
```

### Docker

You can use the following command to build a PD image directly:

```
docker build -t pingcap/pd .
```

Or you can also use following command to get PD from Docker hub:

```
docker pull pingcap/pd
```

Run a single node with Docker: 

```bash
# Set correct HostIP here. 
export HostIP="192.168.199.105"

docker run -d -p 1234:1234 -p 9090:9090 -p 2379:2379 -p 2380:2380 --name pd pingcap/pd \
          --cluster-id=1 \
          --addr=0.0.0.0:1234 \
          --advertise-addr="${HostIP}:1234" \
          --http-addr="0.0.0.0:9090" \
          --etcd-name="default" \
          --etcd-data-dir="default.pd" \
          --etcd-listen-peer-url="http://0.0.0.0:2380" \
          --etcd-advertise-peer-url="http://${HostIP}:2380" \
          --etcd-listen-client-url="http://0.0.0.0:2379" \
          --etcd-advertise-client-url="http://${HostIP}:2379" \
          --etcd-initial-cluster="default=http://${HostIP}:2380" \
          --etcd-initial-cluster-state="new" 
```

### Cluster

For how to set up and use PD cluster, see [clustering](./doc/clustering.md).