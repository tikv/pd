# PD 

[![Build Status](https://travis-ci.org/pingcap/pd.svg?branch=master)](https://travis-ci.org/pingcap/pd)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/pd)](https://goreportcard.com/report/github.com/pingcap/pd)

PD is the abbreviation for Placement Driver. It is used to manage and schedule the [TiKV](https://github.com/pingcap/tikv) cluster. 

PD supports distribution and fault-tolerance by embedding [etcd](https://github.com/coreos/etcd). 

## Build

1. Make sure [​*Go*​](https://golang.org/) (version 1.5+) is installed.
2. Use `make` to install PD. PD is installed in the `bin` directory. 

## Usage

### PD ports

You can use the following default ports in PD:

+ **2379**: for client requests 
+ **2380**: for peer communication.
+ **1234**: for client requests with customized protocol (deprecated later).
+ **9090**: for client requests with HTTP (deprecated later).

You can change these ports when starting PD.

### Single Node with default ports

You can run `pd-server` directly on your local machine, if you want to connect to PD from outside, 
you can let PD listen on the host IP.

```bash
# Set correct HostIP here. 
export HostIP="192.168.199.105"

pd-server --cluster-id=1 \
          --name="pd" \
          --client-urls="http://${HostIP}:2379" \
          --peer-urls="http://${HostIP}:2380"
```

The command flag explanation:

+ `cluster-id`: The unique ID to distinguish different PD clusters. It can't be changed after bootstrapping.  
+ `name`: The human readable name for this node. 
+ `client-urls`: The listening URL list for client traffic.
+ `peer-urls`: The listening URL list for peer traffic.

Using `curl` to see PD member:

```bash
curl ${HostIP}:2379/v2/members

{"members":[{"id":"f62e88a6e81c149","name":"default","peerURLs":["http://192.168.199.105:2380"],"clientURLs":["http://192.168.199.105:2379"]}]}
```

A better tool [httpie](https://github.com/jkbrzt/httpie) is recommended:

```bash
http ${HostIP}:2379/v2/members
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
          --name="pd" \
          --client-urls="http://0.0.0.0:2379" \
          --advertise-client-urls="http://${HostIP}:2379" \
          --peer-urls="http://0.0.0.0:2380" \
          --advertise-peer-urls="http://${HostIP}:2380" \
          --addr="0.0.0.0:1234" \
          --advertise-addr="${HostIP}:1234"
```


The command flag explanation:

+ `advertise-client-urls`: The advertise URL list for client traffic from outside.
+ `advertise-peer-urls`: The advertise URL list for peer traffic from outside.
