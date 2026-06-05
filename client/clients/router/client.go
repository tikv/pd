// Copyright 2024 TiKV Project Authors.
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

package router

import (
	"context"
	"encoding/hex"
	"net/url"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/client/opt"
)

// Region contains information of a region's meta and its peers.
type Region struct {
	Meta         *metapb.Region
	Leader       *metapb.Peer
	DownPeers    []*metapb.Peer
	PendingPeers []*metapb.Peer
	Buckets      *metapb.Buckets
}

// KeyRange defines a range of keys in bytes.
type KeyRange struct {
	StartKey []byte
	EndKey   []byte
}

// NewKeyRange creates a new key range structure with the given start key and end key bytes.
// Notice: the actual encoding of the key range is not specified here. It should be either UTF-8 or hex.
//   - UTF-8 means the key has already been encoded into a string with UTF-8 encoding, like:
//     []byte{52 56 54 53 54 99 54 99 54 102 50 48 53 55 54 102 55 50 54 99 54 52}, which will later be converted to "48656c6c6f20576f726c64"
//     by using `string()` method.
//   - Hex means the key is just a raw hex bytes without encoding to a UTF-8 string, like:
//     []byte{72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100}, which will later be converted to "48656c6c6f20576f726c64"
//     by using `hex.EncodeToString()` method.
func NewKeyRange(startKey, endKey []byte) *KeyRange {
	return &KeyRange{startKey, endKey}
}

// EscapeAsUTF8Str returns the URL escaped key strings as they are UTF-8 encoded.
func (r *KeyRange) EscapeAsUTF8Str() (startKeyStr, endKeyStr string) {
	startKeyStr = url.QueryEscape(string(r.StartKey))
	endKeyStr = url.QueryEscape(string(r.EndKey))
	return
}

// EscapeAsHexStr returns the URL escaped key strings as they are hex encoded.
func (r *KeyRange) EscapeAsHexStr() (startKeyStr, endKeyStr string) {
	startKeyStr = url.QueryEscape(hex.EncodeToString(r.StartKey))
	endKeyStr = url.QueryEscape(hex.EncodeToString(r.EndKey))
	return
}

// Client defines the interface of a router client, which includes the methods for obtaining the routing information.
type Client interface {
	// GetRegion gets a region and its leader Peer from PD by key.
	// The region may expire after split. Caller is responsible for caching and
	// taking care of region change.
	// Also, it may return nil if PD finds no Region for the key temporarily,
	// client should retry later.
	GetRegion(ctx context.Context, key []byte, opts ...opt.GetRegionOption) (*Region, error)
	// GetRegionFromMember gets a region from certain members.
	GetRegionFromMember(ctx context.Context, key []byte, memberURLs []string, opts ...opt.GetRegionOption) (*Region, error)
	// GetPrevRegion gets the previous region and its leader Peer of the region where the key is located.
	GetPrevRegion(ctx context.Context, key []byte, opts ...opt.GetRegionOption) (*Region, error)
	// GetRegionByID gets a region and its leader Peer from PD by id.
	GetRegionByID(ctx context.Context, regionID uint64, opts ...opt.GetRegionOption) (*Region, error)
	// Deprecated: use BatchScanRegions instead.
	// ScanRegions gets a list of regions, starts from the region that contains key.
	// Limit limits the maximum number of regions returned. It returns all the regions in the given range if limit <= 0.
	// If a region has no leader, corresponding leader will be placed by a peer
	// with empty value (PeerID is 0).
	ScanRegions(ctx context.Context, key, endKey []byte, limit int, opts ...opt.GetRegionOption) ([]*Region, error)
	// BatchScanRegions gets a list of regions, starts from the region that contains key.
	// Limit limits the maximum number of regions returned. It returns all the regions in the given ranges if limit <= 0.
	// If a region has no leader, corresponding leader will be placed by a peer
	// with empty value (PeerID is 0).
	// The returned regions are flattened, even there are key ranges located in the same region, only one region will be returned.
	BatchScanRegions(ctx context.Context, keyRanges []KeyRange, limit int, opts ...opt.GetRegionOption) ([]*Region, error)
}
