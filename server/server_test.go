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
	"github.com/tikv/pd/pkg/storage/kv"
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
	tests := []struct {
		name        string
		setup       func(*require.Assertions, *Server) uint64
		errContains string
		check       func(*require.Assertions, *Server, uint64)
	}{
		{
			name: "cached region",
			setup: func(re *require.Assertions, s *Server) uint64 {
				region := newTestFollowerRegionMeta(1)
				re.NoError(s.storage.SaveRegion(region))
				s.basicCluster.PutRegion(core.NewRegionInfo(region, nil, core.SetSource(core.Storage)))
				return region.GetId()
			},
			check: assertTestFollowerRegionDeleted,
		},
		{
			name: "storage-only region",
			setup: func(re *require.Assertions, s *Server) uint64 {
				region := newTestFollowerRegionMeta(2)
				re.NoError(s.storage.SaveRegion(region))
				return region.GetId()
			},
			check: assertTestFollowerRegionDeleted,
		},
		{
			name: "missing region",
			setup: func(*require.Assertions, *Server) uint64 {
				return 3
			},
		},
		{
			name: "load storage error",
			setup: func(_ *require.Assertions, s *Server) uint64 {
				s.storage = &testFollowerRegionStorage{
					Storage:       s.storage,
					loadRegionErr: errTestFollowerRegionStorage,
				}
				return 4
			},
			errContains: "load follower region from local storage",
		},
		{
			name: "delete storage error",
			setup: func(_ *require.Assertions, s *Server) uint64 {
				region := newTestFollowerRegionMeta(5)
				s.basicCluster.PutRegion(core.NewRegionInfo(region, nil, core.SetSource(core.Storage)))
				s.storage = &testFollowerRegionStorage{
					Storage:         s.storage,
					deleteRegionErr: errTestFollowerRegionStorage,
				}
				return region.GetId()
			},
			errContains: "delete follower region from local storage",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			re := require.New(t)
			s := newTestFollowerRegionResetServer(context.Background())
			regionID := test.setup(re, s)

			err := s.deleteFollowerRegion(regionID)
			if test.errContains != "" {
				re.ErrorContains(err, test.errContains)
				return
			}
			re.NoError(err)
			if test.check != nil {
				test.check(re, s, regionID)
			}
		})
	}
}

func TestDeleteFollowerRegionStorage(t *testing.T) {
	tests := []struct {
		name        string
		ctx         context.Context
		setup       func(*require.Assertions, *Server) func(*require.Assertions, *Server)
		errIs       error
		errContains string
	}{
		{
			name: "deletes all region storage keys",
			setup: func(re *require.Assertions, s *Server) func(*require.Assertions, *Server) {
				regions := []*metapb.Region{
					newTestFollowerRegionMeta(10),
					newTestFollowerRegionMeta(11),
				}
				for _, region := range regions {
					re.NoError(s.storage.SaveRegion(region))
				}
				return func(re *require.Assertions, s *Server) {
					for _, region := range regions {
						assertTestFollowerRegionDeleted(re, s, region.GetId())
					}
				}
			},
		},
		{
			name: "deletes by key without loading region meta",
			setup: func(re *require.Assertions, s *Server) func(*require.Assertions, *Server) {
				region := newTestFollowerRegionMeta(12)
				re.NoError(s.storage.SaveRegion(region))
				regionStorage := s.storage
				s.storage = &testFollowerRegionStorage{
					Storage:       regionStorage,
					loadRegionErr: errTestFollowerRegionStorage,
				}
				return func(re *require.Assertions, s *Server) {
					s.storage = regionStorage
					assertTestFollowerRegionDeleted(re, s, region.GetId())
				}
			},
		},
		{
			name: "context canceled",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			}(),
			errIs: context.Canceled,
		},
		{
			name: "load range error",
			setup: func(_ *require.Assertions, s *Server) func(*require.Assertions, *Server) {
				s.storage = &testFollowerRegionStorage{
					Storage:      s.storage,
					loadRangeErr: errTestFollowerRegionStorage,
				}
				return nil
			},
			errContains: "load follower regions from local storage",
		},
		{
			name: "delete transaction error",
			setup: func(re *require.Assertions, s *Server) func(*require.Assertions, *Server) {
				region := newTestFollowerRegionMeta(21)
				re.NoError(s.storage.SaveRegion(region))
				s.storage = &testFollowerRegionStorage{
					Storage:     s.storage,
					runInTxnErr: errTestFollowerRegionStorage,
				}
				return nil
			},
			errContains: "delete follower regions from local storage",
		},
		{
			name: "invalid region storage key",
			setup: func(_ *require.Assertions, s *Server) func(*require.Assertions, *Server) {
				s.storage = &testFollowerRegionStorage{
					Storage:       s.storage,
					loadRangeKeys: []string{"invalid-region-key"},
				}
				return nil
			},
			errContains: "invalid region storage key",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			re := require.New(t)
			ctx := test.ctx
			if ctx == nil {
				ctx = context.Background()
			}
			s := newTestFollowerRegionResetServer(ctx)
			var check func(*require.Assertions, *Server)
			if test.setup != nil {
				check = test.setup(re, s)
			}

			err := s.deleteFollowerRegionStorage()
			switch {
			case test.errIs != nil:
				re.ErrorIs(err, test.errIs)
				return
			case test.errContains != "":
				re.ErrorContains(err, test.errContains)
				return
			default:
				re.NoError(err)
			}
			if check != nil {
				check(re, s)
			}
		})
	}
}

func TestParseRegionIDFromStorageKey(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		regionID    uint64
		errContains string
	}{
		{
			name:     "valid region key",
			key:      keypath.RegionPath(123),
			regionID: 123,
		},
		{
			name:        "missing region id",
			key:         "invalid-region-key",
			errContains: "invalid region storage key",
		},
		{
			name:        "invalid region id",
			key:         "/pd/0/raft/r/not-a-number",
			errContains: "parse region storage key",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			re := require.New(t)
			regionID, err := parseRegionIDFromStorageKey(test.key)
			if test.errContains != "" {
				re.ErrorContains(err, test.errContains)
				return
			}
			re.NoError(err)
			re.Equal(test.regionID, regionID)
		})
	}
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
	runInTxnErr     error
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

func (s *testFollowerRegionStorage) RunInTxn(ctx context.Context, f func(txn kv.Txn) error) error {
	if s.runInTxnErr != nil {
		return s.runInTxnErr
	}
	return s.Storage.RunInTxn(ctx, f)
}
