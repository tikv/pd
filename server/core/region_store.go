// Copyright 2023 TiKV Project Authors.
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

package core

import (
	"bytes"
	"errors"

	"github.com/pingcap/kvproto/pkg/pdpb"
)

// Add implements the informer interface.
func (t *RegionTree) Add(item interface{}) error {
	return t.Update(item)
}

// Update implements the informer interface.
func (t *RegionTree) Update(item interface{}) error { //revive:disable
	var r *regionItem
	regionStat := item.(*pdpb.Item).GetRegionStat()
	if regionStat != nil {
		region := NewRegionInfo(regionStat.GetRegion(), regionStat.GetLeader(),
			SetWrittenBytes(regionStat.GetRegionStats().GetBytesWritten()),
			SetWrittenKeys(regionStat.GetRegionStats().GetKeysWritten()),
			SetReadBytes(regionStat.GetRegionStats().GetBytesRead()),
			SetReadKeys(regionStat.GetRegionStats().GetKeysRead()),
			// TODO: make bucket work as expected
			SetBuckets(regionStat.GetBuckets()),
			SetFromHeartbeat(false))
		r = &regionItem{region}
		t.update(r, false)
		return nil
	}

	// TODO: support region without statistics

	return errors.New("unexpected item type")
}

// Delete implements the informer interface.
func (t *RegionTree) Delete(item interface{}) error {
	regionStat := item.(*pdpb.Item).GetRegionStat()
	if regionStat != nil {
		region := NewRegionInfo(regionStat.GetRegion(), regionStat.GetLeader())
		t.remove(region)
		return nil
	}
	return errors.New("unexpected item type")
}

// List implements the informer interface.
func (t *RegionTree) List(start, end string, limit int) ([]interface{}, error) {
	var res []interface{}
	t.scanRange([]byte(start), func(region *RegionInfo) bool {
		if len([]byte(end)) > 0 && bytes.Compare(region.GetStartKey(), []byte(end)) >= 0 {
			return false
		}
		if limit > 0 && len(res) >= limit {
			return false
		}
		res = append(res, region)
		return true
	})
	return res, nil
}

// GetByKey implements the informer interface.
func (t *RegionTree) GetByKey(key string) (item interface{}, err error) {
	item = t.search([]byte(key))
	return
}
