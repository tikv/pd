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
// TL;TR: The join functionality is safe. With data, join does nothing, w/o data
//        and it is not a member of cluster, join does MemberAdd, otherwise
//        return an error.
//
// Etcd automatically re-join cluster if there is a data dir, so first we check
// if there is data dir or not. With data dir, it returns an empty string(etcd
// will get correct configurations from data dir.)
//
// Without data dir, we may have following cases:
//
//  1. a new pd joins to an existing cluster.
//      join does: MemberAdd, MemberList, then generate initial-cluster.
//
//  2. a new pd joins itself
//      join does: nothing.
//
//  3. an failed pd re-joins to previous cluster.
//      join does: return an error(etcd reports: raft log corrupted, truncated,
//                 or lost?)
//
//  4. a join self pd failed and it restarted with join while other peers try
//     to connect to it.
//      join does: nothing. (join can not detect whether it is in a cluster or
//                 not, however, etcd will handle it safey, if there is no data
//                 in cluster the restarted pd will join to cluster, otherwise
//                 pd will shutdown as soon as other peers connect to it. etcd
//                 reports: raft log corrupted, truncated, or lost?)
//
//  5. a deleted pd joins to previous cluster.
//      join does: same as case1. (it is not in member list and there is no
//                 data, so we can treat it as a new pd.)
//
// With data dir, special case:
//
//  6. a failed pd tries to join to previous cluster but it has been deleted
//     during it's downtime.
//      join does: return "" (etcd will connect to other peers and will find
//                 itself has been removed)
//
//  7. a deleted pd joins to previous cluster.
//      join does: return "" (as etcd will read data dir and find itself has
//                 been removed, so an empty string is fine.)
func (cfg *Config) prepareJoinCluster() (string, string, error) {
	initialCluster := ""
	// cases with data
	if wal.Exist(cfg.DataDir) {
		return initialCluster, embed.ClusterStateFlagExisting, nil
	}

	// case 2, case 4
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

	existed := false
	for _, m := range listResp.Members {
		if m.Name == cfg.Name {
			existed = true
		}
	}

	// case 3
	if existed {
		return "", "", errors.New("missing data or join a duplicated pd")
	}

	// case 1, case 5
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
