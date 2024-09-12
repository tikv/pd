// Copyright 2022 TiKV Project Authors.
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

package buckets

import (
	"context"

	"github.com/pingcap/kvproto/pkg/metapb"
)

type flowItemTaskKind uint32

const (
	checkBucketsTaskType flowItemTaskKind = iota
	collectBucketStatsTaskType
)

func (kind flowItemTaskKind) String() string {
	switch kind {
	case checkBucketsTaskType:
		return "check_buckets"
	case collectBucketStatsTaskType:
		return "collect_bucket_stats"
	}
	return "unknown"
}

// flowBucketsItemTask indicates the task in flowItem queue
type flowBucketsItemTask interface {
	taskType() flowItemTaskKind
	runTask(cache *HotBucketCache)
}

// checkBucketsTask indicates the task in checkBuckets queue
type checkBucketsTask struct {
	Buckets *metapb.Buckets
}

// NewCheckPeerTask creates task to update peerInfo
func NewCheckPeerTask(buckets *metapb.Buckets) flowBucketsItemTask {
	return &checkBucketsTask{
		Buckets: buckets,
	}
}

func (*checkBucketsTask) taskType() flowItemTaskKind {
	return checkBucketsTaskType
}

func (t *checkBucketsTask) runTask(cache *HotBucketCache) {
	newItems, overlaps := cache.checkBucketsFlow(t.Buckets)
	cache.putItem(newItems, overlaps)
}

type collectBucketStatsTask struct {
	minDegree int
	regionIDs []uint64
	ret       chan map[uint64][]*BucketStat // RegionID ==>Buckets
}

// NewCollectBucketStatsTask creates task to collect bucket stats.
func NewCollectBucketStatsTask(minDegree int, regionIDs ...uint64) *collectBucketStatsTask {
	return &collectBucketStatsTask{
		minDegree: minDegree,
		regionIDs: regionIDs,
		ret:       make(chan map[uint64][]*BucketStat, 1),
	}
}

func (*collectBucketStatsTask) taskType() flowItemTaskKind {
	return collectBucketStatsTaskType
}

func (t *collectBucketStatsTask) runTask(cache *HotBucketCache) {
	t.ret <- cache.GetHotBucketStats(t.minDegree, t.regionIDs)
}

// WaitRet returns the result of the task.
func (t *collectBucketStatsTask) WaitRet(ctx context.Context) map[uint64][]*BucketStat {
	select {
	case <-ctx.Done():
		return nil
	case ret := <-t.ret:
		return ret
	}
}
