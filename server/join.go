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

var (
	errExistingData = errors.New("existing previous raft log")
	errMissingData  = errors.New("missing raft log")
)

// TODO: support HTTPS
func (cfg *Config) genClientV3Config() clientv3.Config {
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

func (cfg *Config) hasPreviousData() bool {
	return wal.Exist(cfg.DataDir)
}

func (cfg *Config) isInCluster(client *clientv3.Client) (bool, *clientv3.MemberListResponse, error) {
	list, err := memberList(client)
	if err != nil {
		return false, nil, errors.Trace(err)
	}

	for _, m := range list.Members {
		if m.Name == cfg.Name {
			return true, list, nil
		}
	}
	return false, list, nil
}

func genInitalClusterStr(list *clientv3.MemberListResponse) string {
	pds := []string{}
	for _, memb := range list.Members {
		for _, m := range memb.PeerURLs {
			pds = append(pds, fmt.Sprintf("%s=%s", memb.Name, m))
		}
	}

	return strings.Join(pds, ",")
}

// prepareJoinCluster send MemberAdd command to pd cluster,
// returns pd initial cluster configuration.
//
// join cases:
//  1. join cluster
//  2. join self, new cluster
//  3. re-join after fail
//  4. join after delete (treat as case 1)
//
// requires:
//  1. no data-dir, MemberAdd
//  2. no data-dir
//  3. need data-dir(etcd reports: raft log corrupted, truncated, or lost?)
//  4. no datat-dir, MemberAdd
func (cfg *Config) prepareJoinCluster() (string, string, error) {
	// case 2, join self
	if cfg.Join == cfg.AdvertiseClientUrls {
		if cfg.hasPreviousData() {
			return "", "", errors.Trace(errExistingData)
		}
		initialCluster := fmt.Sprintf("%s=%s", cfg.Name, cfg.AdvertisePeerUrls)
		return initialCluster, embed.ClusterStateFlagNew, nil
	}

	client, err := clientv3.New(cfg.genClientV3Config())
	if err != nil {
		return "", "", errors.Trace(err)
	}
	defer client.Close()

	ok, listResp, err := cfg.isInCluster(client)
	if err != nil {
		return "", "", errors.Trace(err)
	}

	if ok {
		// case 3, re-join after fail
		if cfg.hasPreviousData() {
			return genInitalClusterStr(listResp), embed.ClusterStateFlagExisting, nil
		}
		return "", "", errors.Trace(errMissingData)
	}

	// case 1 and 4
	if cfg.hasPreviousData() {
		return "", "", errors.Trace(errExistingData)
	}

	addResp, err := memberAdd(client, []string{cfg.AdvertisePeerUrls})
	if err != nil {
		return "", "", errors.Trace(err)
	}

	listResp, err = memberList(client)
	if err != nil {
		return "", "", errors.Trace(err)
	}

	for i := range listResp.Members {
		if listResp.Members[i].ID == addResp.Member.ID {
			listResp.Members[i].Name = cfg.Name
			break
		}
	}

	return genInitalClusterStr(listResp), embed.ClusterStateFlagExisting, nil
}
