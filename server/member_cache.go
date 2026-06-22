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

package server

import (
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/pingcap/kvproto/pkg/pdpb"
)

const membersCacheTTL = time.Second
const (
	membersCacheLoadKey      = "members"
	membersCacheForceLoadKey = "members-force"
)

type membersCache struct {
	mu         sync.RWMutex
	ttl        time.Duration
	members    []*pdpb.Member
	expireAt   time.Time
	refreshSeq uint64
	flight     singleflight.Group
}

func newMembersCache(ttl time.Duration) *membersCache {
	return &membersCache{ttl: ttl}
}

func (c *membersCache) get(forceRefresh bool, load func() ([]*pdpb.Member, error)) ([]*pdpb.Member, error) {
	if !forceRefresh {
		if members, ok := c.getFresh(); ok {
			return members, nil
		}
	}

	key := membersCacheLoadKey
	if forceRefresh {
		key = membersCacheForceLoadKey
	}
	value, err, _ := c.flight.Do(key, func() (any, error) {
		if !forceRefresh {
			if members, ok := c.getFresh(); ok {
				return members, nil
			}
		}

		return c.loadAndStore(load)
	})
	if err != nil {
		return nil, err
	}
	return cloneMembers(value.([]*pdpb.Member)), nil
}

func (c *membersCache) loadAndStore(load func() ([]*pdpb.Member, error)) ([]*pdpb.Member, error) {
	seq := c.beginRefresh()
	members, err := load()
	if err != nil {
		return nil, err
	}
	cachedMembers := cloneMembers(members)
	c.storeIfLatest(seq, cachedMembers)
	return cachedMembers, nil
}

func (c *membersCache) beginRefresh() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.refreshSeq++
	return c.refreshSeq
}

func (c *membersCache) storeIfLatest(seq uint64, members []*pdpb.Member) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if seq != c.refreshSeq {
		return
	}
	c.members = members
	c.expireAt = time.Now().Add(c.ttl)
}

func (c *membersCache) getFresh() ([]*pdpb.Member, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.members == nil || !time.Now().Before(c.expireAt) {
		return nil, false
	}
	return cloneMembers(c.members), true
}

func cloneMembers(members []*pdpb.Member) []*pdpb.Member {
	if members == nil {
		return nil
	}
	cloned := make([]*pdpb.Member, 0, len(members))
	for _, member := range members {
		cloned = append(cloned, cloneMember(member))
	}
	return cloned
}

func cloneMember(member *pdpb.Member) *pdpb.Member {
	if member == nil {
		return nil
	}
	cloned := *member
	cloned.PeerUrls = append([]string(nil), member.GetPeerUrls()...)
	cloned.ClientUrls = append([]string(nil), member.GetClientUrls()...)
	return &cloned
}
