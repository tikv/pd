// Copyright 2024 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"

	"github.com/gogo/protobuf/proto"

	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// RegionStorage is a storage for the PD region meta information based on LevelDB,
// which will override the default implementation of the `endpoint.RegionStorage`.
type RegionStorage struct {
	kv.Base
	backend *levelDBBackend
}

var _ endpoint.RegionStorage = (*RegionStorage)(nil)

func newRegionStorage(backend *levelDBBackend) *RegionStorage {
	return &RegionStorage{Base: backend.Base, backend: backend}
}

// LoadRegion implements the `endpoint.RegionStorage` interface.
func (s *RegionStorage) LoadRegion(regionID uint64, region *metapb.Region) (bool, error) {
	return s.backend.LoadRegion(regionID, region)
}

// LoadRegions implements the `endpoint.RegionStorage` interface.
func (s *RegionStorage) LoadRegions(ctx context.Context, f func(region *core.RegionInfo) []*core.RegionInfo) error {
	return s.backend.LoadRegions(ctx, f)
}

// SaveRegion implements the `endpoint.RegionStorage` interface.
// Instead of saving the region directly, it will encrypt the region and then save it in batch.
func (s *RegionStorage) SaveRegion(region *metapb.Region) error {
	encryptedRegion, err := encryption.EncryptRegion(region, s.backend.ekm)
	if err != nil {
		return err
	}
	value, err := proto.Marshal(encryptedRegion)
	if err != nil {
		return errs.ErrProtoMarshal.Wrap(err).GenWithStackByCause()
	}
	return s.backend.SaveIntoBatch(keypath.RegionPath(region.GetId()), value)
}

// DeleteRegion implements the `endpoint.RegionStorage` interface.
func (s *RegionStorage) DeleteRegion(region *metapb.Region) error {
	return s.backend.Remove((keypath.RegionPath(region.GetId())))
}

// Flush implements the `endpoint.RegionStorage` interface.
func (s *RegionStorage) Flush() error {
	return s.backend.Flush()
}

// Close implements the `endpoint.RegionStorage` interface.
func (s *RegionStorage) Close() error {
	return s.backend.Close()
}
