# PD

[![Check Status](https://github.com/tikv/pd/actions/workflows/check.yaml/badge.svg)](https://github.com/tikv/pd/actions/workflows/check.yaml)
[![Build & Test Status](https://github.com/tikv/pd/actions/workflows/pd-tests.yaml/badge.svg?branch=master)](https://github.com/tikv/pd/actions/workflows/pd-tests.yaml)
[![TSO Consistency Test Status](https://github.com/tikv/pd/actions/workflows/tso-consistency-test.yaml/badge.svg)](https://github.com/tikv/pd/actions/workflows/tso-consistency-test.yaml)
[![GitHub release](https://img.shields.io/github/release/tikv/pd.svg)](https://github.com/tikv/pd/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/tikv/pd)](https://goreportcard.com/report/github.com/tikv/pd)
[![codecov](https://codecov.io/gh/tikv/pd/branch/master/graph/badge.svg)](https://codecov.io/gh/tikv/pd)

PD is the abbreviation for Placement Driver. It is used to manage and schedule the [TiKV](https://github.com/tikv/tikv) cluster.

PD supports distribution and fault-tolerance by embedding [etcd](https://github.com/etcd-io/etcd).

[<img src="docs/contribution-map.png" alt="contribution-map" width="180">](https://github.com/pingcap/tidb-map/blob/master/maps/contribution-map.md#pd-placement-driver-for-tikv)

If you're interested in contributing to PD, see [CONTRIBUTING.md](./CONTRIBUTING.md). For more contributing information, please click on the contributor icon above.

## Build

1. Make sure [​*Go*​](https://golang.org/) (version 1.16) is installed.
2. Use `make` to install PD. PD is installed in the `bin` directory.

## Usage

### Command flags

See [PD Configuration Flags](https://pingcap.com/docs/dev/reference/configuration/pd-server/configuration/#pd-configuration-flags).

### Single Node with default ports

You can run `pd-server` directly on your local machine, if you want to connect to PD from outside,
you can let PD listen on the host IP.

```bash
# Set correct HostIP here.
export HostIP="192.168.199.105"

pd-server --name="pd" \
          --data-dir="pd" \
          --client-urls="http://${HostIP}:2379" \
          --peer-urls="http://${HostIP}:2380" \
          --log-file=pd.log
```

Using `curl` to see PD member:

```bash
curl http://${HostIP}:2379/pd/api/v1/members

{
    "members": [
        {
            "name":"pd",
            "member_id":"f62e88a6e81c149",
            "peer_urls": [
                "http://192.168.199.105:2380"
            ],
            "client_urls": [
                "http://192.168.199.105:2379"
            ]
        }
    ]
}
```

A better tool [httpie](https://github.com/jkbrzt/httpie) is recommended:

```bash
http http://${HostIP}:2379/pd/api/v1/members
Access-Control-Allow-Headers: accept, content-type, authorization
Access-Control-Allow-Methods: POST, GET, OPTIONS, PUT, DELETE
Access-Control-Allow-Origin: *
Content-Length: 673
Content-Type: application/json; charset=UTF-8
Date: Thu, 20 Feb 2020 09:49:42 GMT

{
    "members": [
        {
            "client_urls": [
                "http://192.168.199.105:2379"
            ],
            "member_id": "f62e88a6e81c149",
            "name": "pd",
            "peer_urls": [
                "http://192.168.199.105:2380"
            ]
        }
    ]
}
```

### Docker

You can use the following command to build a PD image directly:

```bash
docker build -t tikv/pd .
```

Or you can also use following command to get PD from Docker hub:

```bash
docker pull pingcap/pd
```

Run a single node with Docker:

```bash
# Set correct HostIP here.
export HostIP="192.168.199.105"

docker run -d -p 2379:2379 -p 2380:2380 --name pd tikv/pd \
          --name="pd" \
          --data-dir="pd" \
          --client-urls="http://0.0.0.0:2379" \
          --advertise-client-urls="http://${HostIP}:2379" \
          --peer-urls="http://0.0.0.0:2380" \
          --advertise-peer-urls="http://${HostIP}:2380" \
          --log-file=pd.log
```

### Cluster

As a component of TiKV project, PD needs to run with TiKV to work. The cluster can also include TiDB to provide SQL services. You can refer [Deploy a TiDB Cluster Using TiUP](https://docs.pingcap.com/tidb/stable/production-deployment-using-tiup) or [TiDB in Kubernetes Documentation](https://docs.pingcap.com/tidb-in-kubernetes/stable) for detailed instructions to deploy a cluster.
