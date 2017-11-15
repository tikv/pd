// Copyright 2017 PingCAP, Inc.
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

package faketikv

// Client is a PD (Placement Driver) client.
// It should not be used after calling Close().
type Client interface {
	GetClusterID(ctx context.Context) uint64
	AllocID(ctx context.Context) (uint64, error)
	Bootstrap(ctx context.Context, store *metapb.Store, region *metapb.Region) error
	PutStore(ctx context.Context, store *metapb.Store) error
	StoreHeartbeat(ctx context.Context, stats *pdpb.StoreStats) error
	RegionHeartbeat(ctx context.Context, region *core.RegionInfo) error
	Close()
}

// NewClient creates a PD client.
func NewClient(pdAddr string) (Client, <-chan *pdpb.RegionHeartbeatResponse, error) {}
