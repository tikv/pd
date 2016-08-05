// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"fmt"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/wal"
	"github.com/juju/errors"
	"golang.org/x/net/context"
)

// the maximum amount of time a dial will wait for a connection to setup.
// 30s is long enough for most of the network conditions.
const defaultDialTimeout = 30 * time.Second

// TODO: support HTTPS
func genClientV3Config(cfg *Config) clientv3.Config {
	endpoints := strings.Split(cfg.Join, ",")
	return clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: defaultDialTimeout,
	}
}

func memberAdd(client *clientv3.Client, urls []string) (*clientv3.MemberAddResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultDialTimeout)
	defer cancel()

	return client.MemberAdd(ctx, urls)
}

func memberList(client *clientv3.Client) (*clientv3.MemberListResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultDialTimeout)
	defer cancel()

	return client.MemberList(ctx)
}

// prepareJoinCluster send MemberAdd command to pd cluster,
// returns pd initial cluster configuration.
//
// Etcd automatically re-join cluster if there is data dir.
// Join cases:
//  1. join cluster
//    1.1. with data-dir: return ""
//    1.2. without data-dir: MemberAdd, MemberList, gen initial-cluster
//  2. join self
//    2.1. with data-dir: return ""
//    2.2. without data-dir: return ""
//  3. re-join after fail
//    3.1. with data-dir: return ""
//    3.2. without data-dir: return error(etcd reports: raft log corrupted, truncated, or lost?)
//  4. join after delete
//    4.1. with data-dir: return "" (cluster will reject it, however itself will keep runing.)
//    4.2. without data-dir: treat as case 1.2
func (cfg *Config) prepareJoinCluster() (string, string, error) {
	initialCluster := ""
	if wal.Exist(cfg.DataDir) {
		// case 1.1, 2.1, 3.1, 4.1
		return initialCluster, embed.ClusterStateFlagExisting, nil
	}

	// case 2.2, join self
	if cfg.Join == cfg.AdvertiseClientUrls {
		initialCluster = fmt.Sprintf("%s=%s", cfg.Name, cfg.AdvertisePeerUrls)
		return initialCluster, embed.ClusterStateFlagNew, nil
	}

	client, err := clientv3.New(genClientV3Config(cfg))
	if err != nil {
		return "", "", errors.Trace(err)
	}
	defer client.Close()

	listResp, err := memberList(client)
	if err != nil {
		return "", "", errors.Trace(err)
	}

	in := false
	for _, m := range listResp.Members {
		if m.Name == cfg.Name {
			in = true
		}
	}
	if in {
		// case 3.2
		return "", "", errors.New("missing raft log")
	}

	// case 1.2 and 4.2
	addResp, err := memberAdd(client, []string{cfg.AdvertisePeerUrls})
	if err != nil {
		return "", "", errors.Trace(err)
	}

	listResp, err = memberList(client)
	if err != nil {
		return "", "", errors.Trace(err)
	}

	pds := []string{}
	for _, memb := range listResp.Members {
		for _, m := range memb.PeerURLs {
			n := memb.Name
			if memb.ID == addResp.Member.ID {
				n = cfg.Name
			}
			pds = append(pds, fmt.Sprintf("%s=%s", n, m))
		}
	}
	initialCluster = strings.Join(pds, ",")

	return initialCluster, embed.ClusterStateFlagExisting, nil
}
