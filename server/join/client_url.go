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
	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/slice"
)

// checkClientURLConflict rejects the member if any *other* member in the current
// member list already advertises one of advertiseClientURLs. "Other" is decided
// by peer URLs (unique per member), not name, so a different member that happens
// to share our name is still checked. This catches the case where an existing,
// published member already owns the URL — e.g. a joining or restarting member
// reusing another member's client URL (issue #10999).
//
// It is read-only (no etcd write, no quorum requirement), so it can run on the
// join and restart-with-data paths without ever blocking a member that needs to
// restart to *restore* quorum.
//
// Scope / follow-ups: this only detects a conflict against members that have
// already published their client URLs, so it does not by itself close the
// window between two concurrent joiners/restarts before either publishes, nor
// does it cover a member bootstrapped with --initial-cluster (which never
// reaches this path). Making membership uniqueness authoritative (an atomic
// reservation bound to the member lifecycle) and verifying responder identity in
// health checks are tracked separately.
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
