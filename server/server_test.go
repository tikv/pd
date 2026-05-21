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
	"context"
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/server/config"
)

var errTestFollowerRegionStorage = stderrors.New("test follower region storage error")

func TestResetFollowerRegionCacheRequiresRegionStorage(t *testing.T) {
	re := require.New(t)
	cfg := config.NewConfig()
	cfg.PDServerCfg.UseRegionStorage = false
	s := &Server{persistOptions: config.NewPersistOptions(cfg)}

	re.ErrorContains(s.ResetFollowerRegionCache(), "region storage is disabled")

	cfg.PDServerCfg.UseRegionStorage = true
	s = newTestFollowerRegionResetServer(context.Background())
	s.persistOptions = config.NewPersistOptions(cfg)
	s.member = member.NewMember(nil, nil, 1)
	re.Error(s.ResetFollowerRegionCache())
}

func TestDeleteFollowerRegion(t *testing.T) {
	re := require.New(t)
	s := newTestFollowerRegionResetServer(context.Background())

	cachedRegion := newTestFollowerRegionMeta(1)
	re.NoError(s.storage.SaveRegion(cachedRegion))
	s.basicCluster.PutRegion(core.NewRegionInfo(cachedRegion, nil, core.SetSource(core.Storage)))
	re.NoError(s.deleteFollowerRegion(cachedRegion.GetId()))
	assertTestFollowerRegionDeleted(re, s, cachedRegion.GetId())

	storageOnlyRegion := newTestFollowerRegionMeta(2)
	re.NoError(s.storage.SaveRegion(storageOnlyRegion))
	re.NoError(s.deleteFollowerRegion(storageOnlyRegion.GetId()))
	assertTestFollowerRegionDeleted(re, s, storageOnlyRegion.GetId())

	re.NoError(s.deleteFollowerRegion(3))
}

func TestDeleteFollowerRegionReturnsStorageErrors(t *testing.T) {
	re := require.New(t)
	s := newTestFollowerRegionResetServer(context.Background())

	s.storage = &testFollowerRegionStorage{
		Storage:       s.storage,
		loadRegionErr: errTestFollowerRegionStorage,
	}
	re.ErrorContains(s.deleteFollowerRegion(1), "load follower region from local storage")

	s = newTestFollowerRegionResetServer(context.Background())
	region := newTestFollowerRegionMeta(2)
	s.basicCluster.PutRegion(core.NewRegionInfo(region, nil, core.SetSource(core.Storage)))
	s.storage = &testFollowerRegionStorage{
		Storage:         s.storage,
		deleteRegionErr: errTestFollowerRegionStorage,
	}
	re.ErrorContains(s.deleteFollowerRegion(region.GetId()), "delete follower region from local storage")
}

func TestDeleteFollowerRegionStorage(t *testing.T) {
	re := require.New(t)
	s := newTestFollowerRegionResetServer(context.Background())

	regions := []*metapb.Region{
		newTestFollowerRegionMeta(10),
		newTestFollowerRegionMeta(11),
	}
	for _, region := range regions {
		re.NoError(s.storage.SaveRegion(region))
	}
	re.NoError(s.deleteFollowerRegionStorage())
	for _, region := range regions {
		assertTestFollowerRegionDeleted(re, s, region.GetId())
	}

	s = newTestFollowerRegionResetServer(context.Background())
	region := newTestFollowerRegionMeta(12)
	re.NoError(s.storage.SaveRegion(region))
	regionStorage := s.storage
	s.storage = &testFollowerRegionStorage{
		Storage:       regionStorage,
		loadRegionErr: errTestFollowerRegionStorage,
	}
	re.NoError(s.deleteFollowerRegionStorage())
	s.storage = regionStorage
	assertTestFollowerRegionDeleted(re, s, region.GetId())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s = newTestFollowerRegionResetServer(ctx)
	re.ErrorIs(s.deleteFollowerRegionStorage(), context.Canceled)
}

func TestDeleteFollowerRegionStorageReturnsStorageErrors(t *testing.T) {
	re := require.New(t)
	s := newTestFollowerRegionResetServer(context.Background())
	s.storage = &testFollowerRegionStorage{
		Storage:      s.storage,
		loadRangeErr: errTestFollowerRegionStorage,
	}
	re.ErrorContains(s.deleteFollowerRegionStorage(), "load follower regions from local storage")

	s = newTestFollowerRegionResetServer(context.Background())
	region := newTestFollowerRegionMeta(21)
	re.NoError(s.storage.SaveRegion(region))
	s.storage = &testFollowerRegionStorage{
		Storage:         s.storage,
		deleteRegionErr: errTestFollowerRegionStorage,
	}
	re.ErrorContains(s.deleteFollowerRegionStorage(), "delete follower region from local storage")

	s = newTestFollowerRegionResetServer(context.Background())
	s.storage = &testFollowerRegionStorage{
		Storage:       s.storage,
		loadRangeKeys: []string{"invalid-region-key"},
	}
	re.ErrorContains(s.deleteFollowerRegionStorage(), "invalid region storage key")
}

func TestParseRegionIDFromStorageKey(t *testing.T) {
	re := require.New(t)

	regionID, err := parseRegionIDFromStorageKey(keypath.RegionPath(123))
	re.NoError(err)
	re.Equal(uint64(123), regionID)

	_, err = parseRegionIDFromStorageKey("invalid-region-key")
	re.ErrorContains(err, "invalid region storage key")

	_, err = parseRegionIDFromStorageKey("/pd/0/raft/r/not-a-number")
	re.ErrorContains(err, "parse region storage key")
}

func newTestFollowerRegionResetServer(ctx context.Context) *Server {
	cfg := config.NewConfig()
	return &Server{
		ctx:          ctx,
		cfg:          cfg,
		storage:      storage.NewStorageWithMemoryBackend(),
		basicCluster: core.NewBasicCluster(),
	}
}

func newTestFollowerRegionMeta(regionID uint64) *metapb.Region {
	return &metapb.Region{
		Id:          regionID,
		StartKey:    []byte{byte(regionID)},
		EndKey:      []byte{byte(regionID + 1)},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers: []*metapb.Peer{
			{Id: regionID*10 + 1, StoreId: 1},
		},
	}
}

func assertTestFollowerRegionDeleted(re *require.Assertions, s *Server, regionID uint64) {
	region := &metapb.Region{}
	ok, err := s.storage.LoadRegion(regionID, region)
	re.NoError(err)
	re.False(ok)
	re.Nil(s.basicCluster.GetRegion(regionID))
}

type testFollowerRegionStorage struct {
	storage.Storage
	loadRegionErr   error
	deleteRegionErr error
	loadRangeErr    error
	loadRangeKeys   []string
}

func (s *testFollowerRegionStorage) LoadRange(key, endKey string, limit int) (keys []string, values []string, err error) {
	if s.loadRangeErr != nil {
		return nil, nil, s.loadRangeErr
	}
	if s.loadRangeKeys != nil {
		return s.loadRangeKeys, nil, nil
	}
	return s.Storage.LoadRange(key, endKey, limit)
}

func (s *testFollowerRegionStorage) LoadRegion(regionID uint64, region *metapb.Region) (bool, error) {
	if s.loadRegionErr != nil {
		return false, s.loadRegionErr
	}
	return s.Storage.LoadRegion(regionID, region)
}

func (s *testFollowerRegionStorage) DeleteRegion(region *metapb.Region) error {
	if s.deleteRegionErr != nil {
		return s.deleteRegionErr
	}
	return s.Storage.DeleteRegion(region)
}
