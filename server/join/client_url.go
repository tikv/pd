// Copyright 2026 TiKV Project Authors.
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

package join

import (
	"encoding/hex"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/storage/kv"
)

// clientURLRegistryPath returns the etcd key that records which member owns a
// given advertised client URL. The URL is hex-encoded so it forms a single,
// slash-free key segment.
func clientURLRegistryPath(clusterID uint64, clientURL string) string {
	return fmt.Sprintf("/pd/%d/member/client-urls/%s", clusterID, hex.EncodeToString([]byte(clientURL)))
}

// checkAndClaimClientURLs makes sure none of advertiseClientURLs is already
// owned by another member, then atomically claims each URL in an etcd registry.
//
// It is intentionally a standalone function (not folded into the join member
// loop) so the same logic can be reused on any startup path. It has two layers:
//
//  1. Reject if any *other* member in the current member list already advertises
//     one of these client URLs. This catches the common case where an existing,
//     live member owns the URL — e.g. a joining member reusing a Service URL
//     while pointing --join elsewhere (issue #10999).
//  2. Atomically claim each URL via an etcd transaction (create-if-absent). This
//     guards against concurrent joiners that have not published their client
//     URLs yet: the first claimer wins; a later joiner finds the key already
//     owned by another member and is rejected.
//
// The registry entry is keyed by URL and valued by member name; a restart or a
// previous failed-start incarnation keeps the same name and therefore re-claims
// its own URL without error.
//
// Known limitation: the claim is persistent, so if a member is removed and a
// *different* member later wants to reuse its client URL, the stale entry must
// be cleaned up first. Cleanup on member removal is tracked as follow-up work.
func checkAndClaimClientURLs(
	client *clientv3.Client,
	clusterID uint64,
	name string,
	advertiseClientURLs []string,
	members []*etcdserverpb.Member,
) error {
	// Layer 1: reject against the current member list.
	for _, url := range advertiseClientURLs {
		for _, m := range members {
			// Skip our own entry (a normal restart or a previous failed-start
			// incarnation keeps the same name).
			if m.Name == name {
				continue
			}
			for _, owned := range m.ClientURLs {
				if owned == url {
					return errors.Errorf(
						"advertise-client-urls %q is already used by member %q (id %d)",
						url, m.Name, m.ID)
				}
			}
		}
	}

	// Layer 2: atomically claim each URL.
	for _, url := range advertiseClientURLs {
		key := clientURLRegistryPath(clusterID, url)
		resp, err := kv.NewSlowLogTxn(client).
			If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, name)).
			Else(clientv3.OpGet(key)).
			Commit()
		if err != nil {
			return errors.WithStack(err)
		}
		if resp.Succeeded {
			continue // freshly claimed
		}
		// The key already exists; make sure we are the owner.
		kvs := resp.Responses[0].GetResponseRange().Kvs
		if len(kvs) == 0 {
			return errors.Errorf("failed to claim advertise-client-urls %q, please retry", url)
		}
		if owner := string(kvs[0].Value); owner != name {
			return errors.Errorf(
				"advertise-client-urls %q is already claimed by member %q", url, owner)
		}
		log.Info("re-claimed an advertised client URL owned by this member",
			zap.String("name", name), zap.String("client-url", url))
	}
	return nil
}
