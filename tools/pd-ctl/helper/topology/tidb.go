// Copyright 2025 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package topology

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	tidbTopologyKeyPrefix = "/topology/tidb/"
	fetchTimeout          = 5 * time.Second
	tidbAliveTTL          = 45 * time.Second
)

// TiDBInfo describes TiDB topology information needed for HTTP discovery.
type TiDBInfo struct {
	IP         string
	Port       int
	StatusPort uint
}

// DiscoverTiDBStatusAddrs returns alive TiDB status addresses discovered from PD's etcd topology keys.
// Returned addresses are formatted as http(s)://host:status_port.
func DiscoverTiDBStatusAddrs(ctx context.Context, endpoints []string, tlsConfig *tls.Config) ([]string, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("pd endpoints are required for TiDB discovery")
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		TLS:         tlsConfig,
		DialTimeout: fetchTimeout,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer cli.Close()

	nodes, err := fetchAliveTiDBTopology(ctx, cli)
	if err != nil {
		return nil, err
	}

	scheme := "http"
	if tlsConfig != nil {
		scheme = "https"
	}

	addrs := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if n.StatusPort == 0 {
			continue
		}
		host := n.IP
		if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
			host = "[" + host + "]"
		}
		addrs = append(addrs, fmt.Sprintf("%s://%s:%d", scheme, host, n.StatusPort))
	}
	if len(addrs) == 0 {
		return nil, errors.New("no alive TiDB instances found in topology")
	}
	return addrs, nil
}

func fetchAliveTiDBTopology(ctx context.Context, cli *clientv3.Client) ([]TiDBInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()

	resp, err := cli.Get(ctx, tidbTopologyKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.WithStack(err)
	}

	nodesAlive := make(map[string]struct{})
	nodesInfo := make(map[string]*TiDBInfo)

	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if !strings.HasPrefix(key, tidbTopologyKeyPrefix) {
			continue
		}
		remainingKey := key[len(tidbTopologyKeyPrefix):]
		keyParts := strings.Split(remainingKey, "/")
		if len(keyParts) != 2 {
			continue
		}

		switch keyParts[1] {
		case "info":
			info, err := parseTiDBInfo(keyParts[0], kv.Value)
			if err == nil {
				nodesInfo[keyParts[0]] = info
			}
		case "ttl":
			alive, err := parseTiDBAliveness(kv.Value)
			if err == nil && alive {
				nodesAlive[keyParts[0]] = struct{}{}
			}
		}
	}

	nodes := make([]TiDBInfo, 0, len(nodesInfo))
	for addr, info := range nodesInfo {
		if _, ok := nodesAlive[addr]; !ok {
			continue
		}
		nodes = append(nodes, *info)
	}

	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].IP < nodes[j].IP {
			return true
		}
		if nodes[i].IP > nodes[j].IP {
			return false
		}
		return nodes[i].Port < nodes[j].Port
	})

	if len(nodes) == 0 {
		return nil, errors.New("no alive TiDB topology entries in PD")
	}
	return nodes, nil
}

func parseTiDBInfo(address string, value []byte) (*TiDBInfo, error) {
	data := struct {
		StatusPort uint `json:"status_port"`
	}{}
	if err := json.Unmarshal(value, &data); err != nil {
		return nil, errors.WithStack(err)
	}

	host, port, err := splitAddress(address)
	if err != nil {
		return nil, err
	}

	return &TiDBInfo{
		IP:         host,
		Port:       port,
		StatusPort: data.StatusPort,
	}, nil
}

func parseTiDBAliveness(value []byte) (bool, error) {
	unixTimestampNano, err := strconv.ParseUint(string(value), 10, 64)
	if err != nil {
		return false, errors.WithStack(err)
	}
	t := time.Unix(0, int64(unixTimestampNano))
	return time.Since(t) <= tidbAliveTTL, nil
}

func splitAddress(address string) (string, int, error) {
	idx := strings.LastIndex(address, ":")
	if idx <= 0 || idx == len(address)-1 {
		return "", 0, errors.Errorf("invalid TiDB address %s", address)
	}
	host := address[:idx]
	portStr := address[idx+1:]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, errors.WithStack(err)
	}
	return host, port, nil
}
