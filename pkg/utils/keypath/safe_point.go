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

	"github.com/tikv/pd/pkg/mcs/utils/constant"
)

// GCStateRevisionPath returns the key path for storing the revision of GC state data.
func GCStateRevisionPath() string {
	return fmt.Sprintf(gcStateRevisionPathFormat, ClusterID())
}

// GCSafePointPath returns the key path of the GC safe point.
// Please note that the value format differs between the NullKeyspace and other keyspaces.
func GCSafePointPath(keyspaceID uint32) string {
	if keyspaceID == constant.NullKeyspaceID {
		return fmt.Sprintf(unifiedGCSafePointPathFormat, ClusterID())
	}
	return fmt.Sprintf(keyspaceLevelGCSafePointPathFormat, ClusterID(), keyspaceID)
}

// TxnSafePointPath returns the key path of the  txn safe point.
func TxnSafePointPath(keyspaceID uint32) string {
	if keyspaceID == constant.NullKeyspaceID {
		return unifiedTxnSafePointPath
	}
	return fmt.Sprintf(keyspaceLevelTxnSafePointPath, keyspaceID)
}

// GCBarrierPrefix returns the prefix of the paths of GC barriers.
func GCBarrierPrefix(keyspaceID uint32) string {
	return GCBarrierPath(keyspaceID, "")
}

// GCBarrierPath returns the key path of the GC barrier with given barrierID.
func GCBarrierPath(keyspaceID uint32, barrierID string) string {
	if keyspaceID == constant.NullKeyspaceID {
		return fmt.Sprintf(unifiedGCBarrierPathFormat, ClusterID(), barrierID)
	}
	return fmt.Sprintf(keyspaceLevelGCBarrierPathFormat, ClusterID(), keyspaceID, barrierID)
}

// ServiceGCSafePointPrefix returns the prefix of the paths of service safe points. It internally shares the same data
// with GC barriers and only works for NullKeyspace.
func ServiceGCSafePointPrefix() string {
	// The service safe points (which is deprecated and replaced by GC barriers) shares the same data with GC barriers.
	return GCBarrierPrefix(constant.NullKeyspaceID)
}

// ServiceGCSafePointPath returns the key path of the service safe point with the given service ID.
func ServiceGCSafePointPath(serviceID string) string {
	return GCBarrierPath(constant.NullKeyspaceID, serviceID)
}

// CompatibleTiDBMinStartTSPrefix returns the prefix of the paths where TiDB reports its min start ts.
func CompatibleTiDBMinStartTSPrefix(keyspaceID uint32) string {
	if keyspaceID == constant.NullKeyspaceID {
		return unifiedTiDBMinStartTSPrefix
	}
	return fmt.Sprintf(keyspaceLevelTiDBMinStartTSPrefix, keyspaceID)
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
