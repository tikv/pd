# pd [![Build Status](https://travis-ci.org/pingcap/pd.svg?branch=master)](https://travis-ci.org/pingcap/pd)

Pd is the abbreviate for placement driver.

## Docker

### Build

```
docker build -t pingcap/pd .
```

### Usage

### Single Node

```
// Start etcd
export HostIP="0.0.0.0"
docker run -d -p 2379:2379 -p 2380:2380 -p 4001:4001 --name etcd pingcap/etcd \
    --listen-client-urls="http://0.0.0.0:2379" \
    --advertise-client-urls="http://${HostIP}:2379"

// Start pd
docker run -d -p 1234:1234 --name pd --link etcd:etcd pingcap/pd --etcd=etcd:2379
```

### Cluster

A simple script [run_cluster.sh](./run_cluster.sh) can help you run these in local.
