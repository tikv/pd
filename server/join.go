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

// prepareJoinCluster sends MemberAdd command to PD cluster,
// and returns the initial configuration of the PD cluster.
//
// TL;TR: The join functionality is safe. With data, join does nothing, w/o data
//        and it is not a member of cluster, join does MemberAdd, it returns an
//        error if PD tries to join itself, missing data or join a duplicated PD.
//
// Etcd automatically re-joins the cluster if there is a data directory. So
// first it checks if there is a data directory or not. If there is, it returns
// an empty string (etcd will get the correct configurations from the data
// directory.)
//
// If there is no data directory, there are following cases:
//
//  - A new PD joins an existing cluster.
//      What join does: MemberAdd, MemberList, then generate initial-cluster.
//
//  - A failed PD re-joins the previous cluster.
//      What join does: return an error. (etcd reports: raft log corrupted,
//                      truncated, or lost?)
//
//  - A deleted PD joins to previous cluster.
//      What join does: MemberAdd, MemberList, then generate initial-cluster.
//                      (it is not in the member list and there is no data, so
//                       we can treat it as a new PD.)
//
// If there is a data directory, there are following special cases:
//
//  - A failed PD tries to join the previous cluster but it has been deleted
//    during its downtime.
//      What join does: return "" (etcd will connect to other peers and find
//                      that the PD itself has been removed.)
//
//  - A deleted PD joins the previous cluster.
//      What join does: return "" (as etcd will read data directory and find
//                      that the PD itself has been removed, so an empty string
//                      is fine.)
func prepareJoinCluster(cfg *Config) (string, string, error) {
	// - A PD tries to join itself.
	if cfg.Join == cfg.AdvertiseClientUrls {
		return "", "", errors.New("join self is forbidden")
	}

	// Cases with data directory.

	initialCluster := ""
	if wal.Exist(cfg.DataDir) {
		return initialCluster, embed.ClusterStateFlagExisting, nil
	}

	// Below are cases without data directory.

	client, err := clientv3.New(genClientV3Config(cfg))
	if err != nil {
		return "", "", errors.Trace(err)
	}
	defer client.Close()

	listResp, err := listEtcdMembers(client)
	if err != nil {
		return "", "", errors.Trace(err)
	}

	existed := false
	for _, m := range listResp.Members {
		if m.Name == cfg.Name {
			existed = true
		}
	}

	// - A failed PD re-joins the previous cluster.
	if existed {
		return "", "", errors.New("missing data or join a duplicated pd")
	}

	// - A new PD joins an existing cluster.
	// - A deleted PD joins to previous cluster.
	addResp, err := addEtcdMember(client, []string{cfg.AdvertisePeerUrls})
	if err != nil {
		return "", "", errors.Trace(err)
	}

	listResp, err = listEtcdMembers(client)
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
