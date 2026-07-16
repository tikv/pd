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
	"encoding/json"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/storage/kv"
)

// clientURLOwner identifies the member that has claimed a client URL. Peer URLs
// are globally unique per member (etcd enforces peer-URL uniqueness) and stable
// across restarts, so they — not the name — decide whether a registry entry
// belongs to "me". This closes the gap where two nodes sharing a name would
// otherwise both accept a claim keyed only by name.
type clientURLOwner struct {
	Name     string   `json:"name"`
	PeerURLs []string `json:"peer_urls"`
}

// clientURLRegistryPath returns the etcd key that records which member owns a
// given advertised client URL. The URL is hex-encoded so it forms a single,
// slash-free key segment.
func clientURLRegistryPath(clusterID uint64, clientURL string) string {
	return fmt.Sprintf("/pd/%d/member/client-urls/%s", clusterID, hex.EncodeToString([]byte(clientURL)))
}

// checkClientURLConflict rejects the member if any *other* member in the current
// member list already advertises one of advertiseClientURLs. "Other" is decided
// by peer URLs (unique per member), not name, so a different member that happens
// to share our name is still checked. This catches the common case where an
// existing, live member owns the URL — e.g. issue #10999.
//
// It is read-only (no etcd write, no quorum requirement), so it runs on every
// startup — fresh join and a restart that changed its advertise-client-urls
// alike — without ever blocking a member that needs to restart to *restore*
// quorum.
func checkClientURLConflict(
	advertiseClientURLs []string,
	advertisePeerURLs []string,
	members []*etcdserverpb.Member,
) error {
	for _, url := range advertiseClientURLs {
		for _, m := range members {
			// Skip only our own entry, matched by peer URLs so a different
			// member sharing our name is still checked.
			if slice.EqualWithoutOrder(m.PeerURLs, advertisePeerURLs) {
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
	return nil
}

// claimClientURLs atomically claims each advertised client URL in an etcd
// registry via a create-if-absent transaction. Because the transaction is
// serialized through raft, two concurrent joiners cannot both take the same
// client URL: the first wins and a later joiner — even one sharing the same name
// — finds a different peer URL in the owner record and is rejected.
//
// The registry entry is keyed by URL and valued by the owner's identity (name +
// peer URLs). This writes to etcd (needs quorum) and is therefore only used on
// the fresh-join path, where quorum is required anyway for MemberAdd; a restart
// relies on the read-only checkClientURLConflict instead.
//
// Known limitation: the claim is persistent, so if a member is removed and a
// *different* member later wants to reuse its client URL, the stale entry must
// be cleaned up first. Cleanup on member removal is tracked as follow-up work.
func claimClientURLs(
	client *clientv3.Client,
	clusterID uint64,
	name string,
	advertiseClientURLs []string,
	advertisePeerURLs []string,
) error {
	self := clientURLOwner{Name: name, PeerURLs: advertisePeerURLs}
	value, err := json.Marshal(self)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, url := range advertiseClientURLs {
		key := clientURLRegistryPath(clusterID, url)
		resp, err := kv.NewSlowLogTxn(client).
			If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, string(value))).
			Else(clientv3.OpGet(key)).
			Commit()
		if err != nil {
			return errors.WithStack(err)
		}
		if resp.Succeeded {
			continue // freshly claimed
		}
		// The key already exists; make sure the owner is this member (matched by
		// peer URLs), otherwise reject.
		kvs := resp.Responses[0].GetResponseRange().Kvs
		if len(kvs) == 0 {
			return errors.Errorf("failed to claim advertise-client-urls %q, please retry", url)
		}
		var owner clientURLOwner
		if err := json.Unmarshal(kvs[0].Value, &owner); err != nil {
			return errors.Errorf("advertise-client-urls %q is claimed by an unrecognized owner", url)
		}
		if !slice.EqualWithoutOrder(owner.PeerURLs, advertisePeerURLs) {
			return errors.Errorf(
				"advertise-client-urls %q is already claimed by member %q", url, owner.Name)
		}
		log.Info("re-claimed an advertised client URL owned by this member",
			zap.String("name", name), zap.String("client-url", url))
	}
	return nil
}
