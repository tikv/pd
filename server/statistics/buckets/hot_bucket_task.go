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
	"github.com/pingcap/kvproto/pkg/metapb"
)

type flowItemTaskKind uint32

const (
	checkBucketsTaskType flowItemTaskKind = iota
)

func (kind flowItemTaskKind) String() string {
	if kind == checkBucketsTaskType {
		return "check_buckets"
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

func (t *checkBucketsTask) taskType() flowItemTaskKind {
	return checkBucketsTaskType
}

func (t *checkBucketsTask) runTask(cache *HotBucketCache) {
	newItems, overlaps := cache.checkBucketsFlow(t.Buckets)
	cache.putItem(newItems, overlaps)
}
