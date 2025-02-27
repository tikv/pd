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

package keypath

import (
	"fmt"
)

// GCStateRevisionPath returns the key path for storing the revision of GC state data.
func GCStateRevisionPath() string {
	return fmt.Sprintf(gcStateRevisionPathFormat, ClusterID())
}

// GCSafePointPath returns the key path of the global (NullKeyspace) GC safe point.
func GCSafePointPath() string {
	return fmt.Sprintf(gcSafePointPathFormat, ClusterID())
}

// KeyspaceGCSafePointPath returns the keyspace-level GC safe point.
func KeyspaceGCSafePointPath(keyspaceID uint32) string {
	return fmt.Sprintf(keyspaceGCSafePointPathFormat, ClusterID(), keyspaceID)
}

// TxnSafePointPath returns the key path of the global (NullKeyspace) txn safe point.
func TxnSafePointPath() string {
	return txnSafePointPath
}

// KeyspaceTxnSafePointPath returns the keyspace-level txn safe point.
func KeyspaceTxnSafePointPath(keyspaceID uint32) string {
	return fmt.Sprintf(keyspaceTxnSafePointPath, keyspaceID)
}

// GCBarrierPrefix returns the prefix of the paths of GC barriers of the global GC (NullKeyspace).
func GCBarrierPrefix() string {
	return GCBarrierPath("")
}

// GCBarrierPath returns the key path of the GC barriers of the global GC (NullKeyspace).
func GCBarrierPath(barrierID string) string {
	return fmt.Sprintf(gcBarrierPathFormat, ClusterID(), barrierID)
}

// KeyspaceGCBarrierPrefix returns the prefix of the paths of the GC barriers of keyspace-level GC for the given
// keyspaceID.
func KeyspaceGCBarrierPrefix(keyspaceID uint32) string {
	return KeyspaceGCBarrierPath(keyspaceID, "")
}

// KeyspaceGCBarrierPath returns the key path of the GC barriers of keyspace-level GC for the given keyspaceID.
func KeyspaceGCBarrierPath(keyspaceID uint32, barrierID string) string {
	return fmt.Sprintf(keyspaceGCBarrierPathFormat, ClusterID(), keyspaceID, barrierID)
}

// ServiceGCSafePointPrefix returns the prefix of the paths of service safe points. It internally shares the same data
// with GC barriers.
func ServiceGCSafePointPrefix() string {
	// The service safe points (which is deprecated and replaced by GC barriers) shares the same data with GC barriers.
	return GCBarrierPrefix()
}

// ServiceGCSafePointPath returns the key path of the service safe point with the given service ID.
func ServiceGCSafePointPath(serviceID string) string {
	return GCBarrierPath(serviceID)
}

// CompatibleTiDBMinStartTSPrefix returns the prefix of the paths where TiDB reports its min start ts for NullKeyspace
// (no keyspace is specified).
func CompatibleTiDBMinStartTSPrefix() string {
	return tidbMinStartTSPrefix
}

// CompatibleKeyspaceTiDBMinStartTSPrefix returns the prefix of the paths where TiDB reports its min start ts for the
// given keyspace.
func CompatibleKeyspaceTiDBMinStartTSPrefix(keyspaceID uint32) string {
	return fmt.Sprintf(keyspaceTiDBMinStartTSPrefix, keyspaceID)
}

// GCSafePointV2Path is the storage path of gc safe point v2.
func GCSafePointV2Path(keyspaceID uint32) string {
	return fmt.Sprintf(gcSafePointV2PathFormat, ClusterID(), keyspaceID)
}

// ServiceSafePointV2Path is the storage path of service safe point v2.
func ServiceSafePointV2Path(keyspaceID uint32, serviceID string) string {
	return fmt.Sprintf(serviceSafePointV2PathFormat, ClusterID(), keyspaceID, serviceID)
}

// ServiceSafePointV2Prefix is the path prefix of all service safe point that belongs to a specific keyspace.
// Can be used to retrieve keyspace's service safe point at once.
func ServiceSafePointV2Prefix(keyspaceID uint32) string {
	return ServiceSafePointV2Path(keyspaceID, "")
}

// GCSafePointV2Prefix is the path prefix to all gc safe point v2.
func GCSafePointV2Prefix() string {
	return Prefix(GCSafePointV2Path(0))
}
