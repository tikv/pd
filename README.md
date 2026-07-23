# PD

[![Check Status](https://github.com/tikv/pd/actions/workflows/check.yaml/badge.svg)](https://github.com/tikv/pd/actions/workflows/check.yaml)
[![Build & Test Status](https://github.com/tikv/pd/actions/workflows/pd-tests.yaml/badge.svg?branch=master)](https://github.com/tikv/pd/actions/workflows/pd-tests.yaml)
[![GitHub release](https://img.shields.io/github/release/tikv/pd.svg)](https://github.com/tikv/pd/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/tikv/pd)](https://goreportcard.com/report/github.com/tikv/pd)
[![codecov](https://codecov.io/gh/tikv/pd/branch/master/graph/badge.svg)](https://codecov.io/gh/tikv/pd)

PD is the abbreviation for Placement Driver. It manages and schedules [TiKV](https://github.com/tikv/tikv) clusters.

PD supports fault-tolerance by embedding [etcd](https://github.com/etcd-io/etcd).

[<img src="docs/contribution-map.png" alt="contribution-map" width="180"/>](https://github.com/pingcap/tidb-map/blob/master/maps/contribution-map.md#pd-placement-driver-for-tikv)

If you're interested in contributing to PD, see [CONTRIBUTING.md](./CONTRIBUTING.md). For more contributing information about where to start, click on the contributor icon above.

## Build

1. Make sure [*Go*](https://golang.org/) (version 1.25 or later) is installed.
2. Run `make` (equivalent to `make build`) to build PD. `pd-server`, `pd-ctl`, and `pd-recover` will be produced in the `bin` directory.

## Usage

PD can be configured using command-line flags. For more information, see [PD Configuration Flags](https://docs.pingcap.com/tidb/stable/command-line-flags-for-pd-configuration).

### Single node with default ports

You can run `pd-server` directly on your local machine. If you want to connect to PD from outside, you can let PD listen on the host IP.

```bash
# Set HOST_IP to the address you want to listen on
export HOST_IP="192.168.199.105"

pd-server --name="pd" \
          --data-dir="pd" \
          --client-urls="http://${HOST_IP}:2379" \
          --peer-urls="http://${HOST_IP}:2380" \
          --log-file=pd.log
```

Using `curl` to view PD members:

```bash
curl http://${HOST_IP}:2379/pd/api/v1/members

{
  "header": {
    "cluster_id": 7637715636087433014
  },
  "members": [
    {
      "name": "pd",
      "member_id": 15980934438217023866,
      "peer_urls": [
        "http://192.168.199.105:2380"
      ],
      "client_urls": [
        "http://192.168.199.105:2379"
      ],
      "deploy_path": "/",
      "binary_version": "v8.5.6",
      "git_hash": "21bd0a379e5f9d2be11a92d17645a6ada19f02c1"
    }
  ],
  "leader": {
    "name": "pd",
    "member_id": 15980934438217023866,
    "peer_urls": [
      "http://192.168.199.105:2380"
    ],
    "client_urls": [
      "http://192.168.199.105:2379"
    ],
    "deploy_path": "/",
    "binary_version": "v8.5.6",
    "git_hash": "21bd0a379e5f9d2be11a92d17645a6ada19f02c1"
  },
  "etcd_leader": {
    "name": "pd",
    "member_id": 15980934438217023866,
    "peer_urls": [
      "http://192.168.199.105:2380"
    ],
    "client_urls": [
      "http://192.168.199.105:2379"
    ],
    "deploy_path": "/",
    "binary_version": "v8.5.6",
    "git_hash": "21bd0a379e5f9d2be11a92d17645a6ada19f02c1"
  }
}
```

You can also use [httpie](https://github.com/httpie/cli) to call the API:

```bash
http http://${HOST_IP}:2379/pd/api/v1/members

Access-Control-Allow-Headers: accept, content-type, authorization
Access-Control-Allow-Methods: POST, GET, OPTIONS, PUT, DELETE
Access-Control-Allow-Origin: *
Content-Length: <length>
Content-Type: application/json; charset=UTF-8
Date: <response-date>

{
  "header": {
    "cluster_id": 7637715636087433014
  },
  "members": [
    {
      "name": "pd",
      "member_id": 15980934438217023866,
      "peer_urls": [
        "http://192.168.199.105:2380"
      ],
      "client_urls": [
        "http://192.168.199.105:2379"
      ],
      "deploy_path": "/",
      "binary_version": "v8.5.6",
      "git_hash": "21bd0a379e5f9d2be11a92d17645a6ada19f02c1"
    }
  ],
  "leader": {
    "name": "pd",
    "member_id": 15980934438217023866,
    "peer_urls": [
      "http://192.168.199.105:2380"
    ],
    "client_urls": [
      "http://192.168.199.105:2379"
    ],
    "deploy_path": "/",
    "binary_version": "v8.5.6",
    "git_hash": "21bd0a379e5f9d2be11a92d17645a6ada19f02c1"
  },
  "etcd_leader": {
    "name": "pd",
    "member_id": 15980934438217023866,
    "peer_urls": [
      "http://192.168.199.105:2380"
    ],
    "client_urls": [
      "http://192.168.199.105:2379"
    ],
    "deploy_path": "/",
    "binary_version": "v8.5.6",
    "git_hash": "21bd0a379e5f9d2be11a92d17645a6ada19f02c1"
  }
}
```

### Docker

You can choose one of the following methods to get a PD image:

- Build locally:

    ```bash
    docker build -t pingcap/pd .
    ```

- Pull from Docker Hub:

    ```bash
    docker pull pingcap/pd
    ```

Then you can run a single node using the following command:

```bash
# Set HOST_IP to the address you want to listen on
export HOST_IP="192.168.199.105"

docker run -d -p 2379:2379 -p 2380:2380 --name pd pingcap/pd \
          --name="pd" \
          --data-dir="pd" \
          --client-urls="http://0.0.0.0:2379" \
          --advertise-client-urls="http://${HOST_IP}:2379" \
          --peer-urls="http://0.0.0.0:2380" \
          --advertise-peer-urls="http://${HOST_IP}:2380" \
          --log-file=pd.log
```

### Cluster

As a component of the TiKV project, PD needs to run with TiKV to work. The cluster can also include TiDB to provide SQL services. For detailed instructions to deploy a cluster, refer to [Deploy a TiDB Cluster Using TiUP](https://docs.pingcap.com/tidb/stable/production-deployment-using-tiup) or [TiDB on Kubernetes Documentation](https://docs.pingcap.com/tidb-in-kubernetes/stable).


a debug line
