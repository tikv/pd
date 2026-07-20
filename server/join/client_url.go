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
	"net"
	"net/url"
	"strings"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/slice"
)

// canonicalizeURL normalizes a URL so that semantically equivalent endpoints
// compare equal: the scheme and hostname are lower-cased (DNS is
// case-insensitive), a trailing dot on the hostname is dropped, and IP literals
// (notably IPv6) are reduced to their canonical form. If the input cannot be
// parsed it is returned trimmed but otherwise unchanged, so comparison degrades
// to the raw string rather than silently treating a bad URL as matching.
func canonicalizeURL(raw string) string {
	s := strings.TrimSpace(raw)
	u, err := url.Parse(s)
	if err != nil || u.Host == "" {
		return s
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if ip := net.ParseIP(host); ip != nil {
		host = ip.String()
	}
	hostport := host
	if port := u.Port(); port != "" {
		hostport = net.JoinHostPort(host, port)
	}
	return strings.ToLower(u.Scheme) + "://" + hostport
}

// canonicalizeURLs canonicalizes each URL in the slice.
func canonicalizeURLs(raws []string) []string {
	out := make([]string, len(raws))
	for i, raw := range raws {
		out[i] = canonicalizeURL(raw)
	}
	return out
}

// checkClientURLConflict rejects the member if any *other* member in the current
// member list already advertises one of advertiseClientURLs. "Other" is decided
// by peer URLs (unique per member), not name, so a different member that happens
// to share our name is still checked. This catches the case where an existing,
// published member already owns the URL — e.g. a joining or restarting member
// reusing another member's client URL (issue #10999).
//
// URLs are compared after canonicalization, so case-only or format-only
// differences (e.g. http://pd.example:2379 vs http://PD.EXAMPLE:2379) are still
// detected as the same endpoint.
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
	selfPeers := canonicalizeURLs(advertisePeerURLs)
	// canonical client URL -> the original string, for a readable error.
	self := make(map[string]string, len(advertiseClientURLs))
	for _, raw := range advertiseClientURLs {
		self[canonicalizeURL(raw)] = raw
	}
	for _, m := range members {
		// Skip only our own entry, matched by peer URLs so a different member
		// sharing our name is still checked.
		if slice.EqualWithoutOrder(canonicalizeURLs(m.PeerURLs), selfPeers) {
			continue
		}
		for _, owned := range m.ClientURLs {
			if orig, ok := self[canonicalizeURL(owned)]; ok {
				return errors.Errorf(
					"advertise-client-urls %q is already used by member %q (id %d)",
					orig, m.Name, m.ID)
			}
		}
	}
	return nil
}
