# pd 

[![Build Status](https://travis-ci.org/pingcap/pd.svg?branch=master)](https://travis-ci.org/pingcap/pd)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/pd)](https://goreportcard.com/report/github.com/pingcap/pd)

Pd is the abbreviate for placement driver.

## Usage

+ Install [*Go*](https://golang.org/), (version 1.5+ is required).
+ `make build`
+ `./bin/pd-server`

## Docker

### Build

```
docker build -t pingcap/pd .
```

### Usage

### Single Node

```
export HostIP="0.0.0.0"

# Start pd
docker run -d -p 1234:1234 -p 9090:9090 -p 2379:2379 -p 2380:2380 --name pd pingcap/pd \
    --etcd-listen-client-url="http://0.0.0.0:2379" \
    --etcd-advertise-client-url="http://${HostIP}:2379"
```

### Cluster

A simple script [run_cluster.sh](./run_cluster.sh) can help you run these in local.