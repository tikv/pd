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

package election

import (
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func newTestLease(purpose string, id int64) *Lease {
	l := &Lease{purpose: purpose}
	l.setID(clientv3.LeaseID(id))
	return l
}

func TestLocalTTLRemainingCollector(t *testing.T) {
	re := require.New(t)

	collector := newLocalTTLRemainingCollector()
	re.NotNil(collector)
	re.Empty(collector.leases)

	// Register a new lease.
	lease1 := newTestLease("test1", 1)
	collector.register(lease1)
	re.Len(collector.leases, 1)
	// Register a duplicate lease.
	collector.register(lease1)
	re.Len(collector.leases, 1)
	// Register a new lease with a different purpose.
	lease2 := newTestLease("test2", 2)
	collector.register(lease2)
	re.Len(collector.leases, 2)
	// Unregister a non-existent lease.
	lease3 := newTestLease("test3", 3)
	collector.unregister(lease3)
	re.Len(collector.leases, 2)
	// Unregister a different lease with the same purpose as the existing one.
	collector.unregister(newTestLease("test1", 1))
	re.Len(collector.leases, 2)
	// Unregister exact the same lease as the existing one.
	collector.unregister(lease1)
	re.Len(collector.leases, 1)
	collector.unregister(lease2)
	re.Empty(collector.leases)
}
