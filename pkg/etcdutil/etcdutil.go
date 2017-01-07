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

package etcdutil

import (
	"net/url"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver"
	"github.com/coreos/etcd/pkg/types"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/pd/pkg/apiutil"
	"golang.org/x/net/context"
)

const (
	// DefaultDialTimeout is the maximum amount of time a dial will wait for a
	// connection to setup. 30s is long enough for most of the network conditions.
	DefaultDialTimeout = 30 * time.Second

	// DefaultRequestTimeout 10s is long enough for most of etcd clusters.
	DefaultRequestTimeout = 10 * time.Second

	// DefaultSlowRequestTime 1s for the threshold for normal request, for those
	// longer then 1s, they are considered as slow requests.
	DefaultSlowRequestTime = 1 * time.Second

	maxCheckEtcdRunningCount = 60 * 10
	checkEtcdRunningDelay    = 1 * time.Second
)

// unixToHTTP replace unix scheme with http.
var unixToHTTP = strings.NewReplacer("unix://", "http://", "unixs://", "http://")

// CheckClusterID checks Etcd's cluster ID, returns an error if mismatch.
// This function will never block even quorum is not satisfied.
func CheckClusterID(localClusterID types.ID, um types.URLsMap) error {
	if len(um) == 0 {
		return nil
	}

	var peerURLs []string
	for _, urls := range um {
		peerURLs = append(peerURLs, urls.StringSlice()...)
	}

	for i, u := range peerURLs {
		u, gerr := url.Parse(u)
		if gerr != nil {
			return errors.Trace(gerr)
		}
		trp := apiutil.NewHTTPTransport(u.Scheme)

		// For tests, change scheme to http.
		// etcdserver/api/v3rpc does not recognize unix protocol.
		if u.Scheme == "unix" || u.Scheme == "unixs" {
			peerURLs[i] = unixToHTTP.Replace(peerURLs[i])
		}

		remoteCluster, gerr := etcdserver.GetClusterFromRemotePeers([]string{peerURLs[i]}, trp)
		trp.CloseIdleConnections()
		if gerr != nil {
			// Do not return error, because other members may be not ready.
			log.Error(gerr)
			continue
		}

		remoteClusterID := remoteCluster.ID()
		if remoteClusterID != localClusterID {
			return errors.Errorf("Etcd cluster ID mismatch, expect %d, got %d", localClusterID, remoteClusterID)
		}
	}
	return nil
}

// AddEtcdMember adds an etcd members.
func AddEtcdMember(client *clientv3.Client, urls []string) (*clientv3.MemberAddResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultDialTimeout)
	defer cancel()

	return client.MemberAdd(ctx, urls)
}

// ListEtcdMembers returns a list of internal etcd members.
func ListEtcdMembers(etcdClient *clientv3.Client) (*clientv3.MemberListResponse, error) {
	ctx := etcdClient.Ctx()

	listResp, err := etcdClient.MemberList(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return listResp, nil
}

// WaitEtcdStart checks etcd starts ok or not
func WaitEtcdStart(c *clientv3.Client, endpoint string) error {
	var err error
	for i := 0; i < maxCheckEtcdRunningCount; i++ {
		// etcd may not start ok, we should wait and check again
		_, err = endpointStatus(c, endpoint)
		if err == nil {
			return nil
		}

		time.Sleep(checkEtcdRunningDelay)
		continue
	}

	return errors.Trace(err)
}

// endpointStatus checks whether current etcd is running.
func endpointStatus(c *clientv3.Client, endpoint string) (*clientv3.StatusResponse, error) {
	m := clientv3.NewMaintenance(c)

	start := time.Now()
	ctx, cancel := context.WithTimeout(c.Ctx(), DefaultRequestTimeout)
	resp, err := m.Status(ctx, endpoint)
	cancel()

	if cost := time.Now().Sub(start); cost > DefaultSlowRequestTime {
		log.Warnf("check etcd %s status, resp: %v, err: %v, cost: %s", endpoint, resp, err, cost)
	}

	return resp, errors.Trace(err)
}
