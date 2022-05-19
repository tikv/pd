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

// BucketStatInformer is used to get the bucket statistics.
type BucketStatInformer interface {
	BucketsStats(degree int) map[uint64][]*BucketStat
}

// BucketStat is the record the bucket statistics.
type BucketStat struct {
	RegionID  uint64
	StartKey  []byte
	EndKey    []byte
	HotDegree int
	Interval  uint64
	// see statistics.RegionStatKind
	Loads []uint64
}
