// Copyright 2024 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package keypath

import "sync/atomic"

// clusterID is the unique ID for the cluster. We put it in this package is
// because it is always used with key path.
var clusterID atomic.Value

// ClusterID returns the cluster ID.
func ClusterID() uint64 {
	id := clusterID.Load()
	if id == nil {
		return 0
	}
	return id.(uint64)
}

// SetClusterID sets the cluster ID.
func SetClusterID(id uint64) {
	clusterID.Store(id)
}

// ResetClusterID resets the cluster ID to 0. It's only used in tests.
func ResetClusterID() {
	clusterID.Store(uint64(0))
}
