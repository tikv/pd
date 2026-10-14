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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/pdpb"
)

func TestMembersCacheReturnsClonedMembers(t *testing.T) {
	re := require.New(t)
	cache := newMembersCache(time.Hour)
	var loadCount atomic.Int32

	members, err := cache.get(false, func() ([]*pdpb.Member, error) {
		loadCount.Add(1)
		return []*pdpb.Member{{
			Name:       "pd-1",
			MemberId:   1,
			ClientUrls: []string{"http://127.0.0.1:2379"},
			PeerUrls:   []string{"http://127.0.0.1:2380"},
		}}, nil
	})
	re.NoError(err)
	members[0].Name = "mutated"
	members[0].ClientUrls[0] = "mutated"
	members[0].PeerUrls[0] = "mutated"

	members, err = cache.get(false, func() ([]*pdpb.Member, error) {
		re.FailNow("cache should serve fresh members without loading")
		return nil, nil
	})
	re.NoError(err)
	re.Equal(int32(1), loadCount.Load())
	re.Equal("pd-1", members[0].GetName())
	re.Equal([]string{"http://127.0.0.1:2379"}, members[0].GetClientUrls())
	re.Equal([]string{"http://127.0.0.1:2380"}, members[0].GetPeerUrls())
}

func TestMembersCacheForceRefresh(t *testing.T) {
	re := require.New(t)
	cache := newMembersCache(time.Hour)
	var loadCount atomic.Int32
	load := func() ([]*pdpb.Member, error) {
		id := uint64(loadCount.Add(1))
		return []*pdpb.Member{{MemberId: id}}, nil
	}

	members, err := cache.get(false, load)
	re.NoError(err)
	re.Equal(uint64(1), members[0].GetMemberId())

	members, err = cache.get(false, load)
	re.NoError(err)
	re.Equal(uint64(1), members[0].GetMemberId())

	members, err = cache.get(true, load)
	re.NoError(err)
	re.Equal(uint64(2), members[0].GetMemberId())
	re.Equal(int32(2), loadCount.Load())
}

func TestMembersCacheForceRefreshDoesNotShareNormalRefresh(t *testing.T) {
	re := require.New(t)
	cache := newMembersCache(time.Hour)
	var loadCount atomic.Int32
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	load := func() ([]*pdpb.Member, error) {
		id := loadCount.Add(1)
		switch id {
		case 1:
			close(firstStarted)
			<-releaseFirst
		case 2:
		default:
			return nil, errors.New("unexpected member load")
		}
		return []*pdpb.Member{{MemberId: uint64(id)}}, nil
	}

	normalCh := make(chan struct {
		members []*pdpb.Member
		err     error
	}, 1)
	go func() {
		members, err := cache.get(false, load)
		normalCh <- struct {
			members []*pdpb.Member
			err     error
		}{members: members, err: err}
	}()
	<-firstStarted

	forceMembers, err := cache.get(true, load)
	re.NoError(err)
	re.Equal(uint64(2), forceMembers[0].GetMemberId())
	re.Equal(int32(2), loadCount.Load())

	close(releaseFirst)
	normalResult := <-normalCh
	re.NoError(normalResult.err)
	re.Equal(uint64(1), normalResult.members[0].GetMemberId())

	cachedMembers, err := cache.get(false, func() ([]*pdpb.Member, error) {
		return nil, errors.New("cache should keep the forced refresh result")
	})
	re.NoError(err)
	re.Equal(uint64(2), cachedMembers[0].GetMemberId())
	re.Equal(int32(2), loadCount.Load())
}

func TestMembersCacheSingleflight(t *testing.T) {
	re := require.New(t)
	cache := newMembersCache(time.Hour)
	var loadCount atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	load := func() ([]*pdpb.Member, error) {
		if loadCount.Add(1) > 1 {
			return nil, errors.New("duplicate member load")
		}
		once.Do(func() { close(started) })
		<-release
		return []*pdpb.Member{{MemberId: 1}}, nil
	}

	const concurrency = 16
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := cache.get(false, load)
			errCh <- err
		}()
	}
	<-started
	close(release)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		re.NoError(err)
	}
	re.Equal(int32(1), loadCount.Load())
}
