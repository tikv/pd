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

// prepareJoinCluster sends MemberAdd command to PD cluster,
// and returns the initial configuration of the PD cluster.
//
// TL;TR: The join functionality is safe. With data, join does nothing, w/o data
//        and it is not a member of cluster, join does MemberAdd, otherwise
//        return an error.
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
//  - A new PD joins itself.
//      What join does: nothing.
//
//  - A failed PD re-joins the previous cluster.
//      What join does: return an error. (etcd reports: raft log corrupted,
//                      truncated, or lost?)
//
//  - A PD starts with join itself and fails, it is restarted with the same
//    arguments while other peers try to connect to it.
//      What join does: nothing. (join cannot detect whether it is in a cluster
//                      or not, however, etcd will handle it safey, if there is
//                      no data in the cluster the restarted PD will join the
//                      cluster, otherwise, PD will shutdown as soon as other
//                      peers connect to it. etcd reports: raft log corrupted,
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
func (cfg *Config) prepareJoinCluster() (string, string, error) {
	initialCluster := ""
	// Cases with data directory.
	if wal.Exist(cfg.DataDir) {
		return initialCluster, embed.ClusterStateFlagExisting, nil
	}

	// Below are cases without data directory.

	// - A new PD joins itself.
	// - A PD starts with join itself and fails, it is restarted with the same
	//   arguments while other peers try to connect to it.
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

	// - A failed PD re-joins the previous cluster.
	if existed {
		return "", "", errors.New("missing data or join a duplicated pd")
	}

	// - A new PD joins an existing cluster.
	// - A deleted PD joins to previous cluster.
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
