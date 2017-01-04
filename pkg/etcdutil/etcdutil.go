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

	"github.com/coreos/etcd/etcdserver"
	"github.com/coreos/etcd/pkg/types"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/pd/pkg/apiutil"
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
		if gerr != nil {
			// Do not return error, because other members may be not ready.
			log.Error(gerr)
			trp.CloseIdleConnections()
			continue
		}

		remoteClusterID := remoteCluster.ID()
		if remoteClusterID != localClusterID {
			trp.CloseIdleConnections()
			return errors.Errorf("Etcd cluster ID mismatch, expect %d, got %d", localClusterID, remoteClusterID)
		}

		trp.CloseIdleConnections()
	}
	return nil
}
