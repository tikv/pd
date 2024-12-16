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

// GCSafePointPath returns the GC safe point key path.
func GCSafePointPath() string {
	return fmt.Sprintf(gcSafePointPathFormat, ClusterID())
}

// GCSafePointServicePrefixPath returns the GC safe point service key path prefix.
func GCSafePointServicePrefixPath() string {
	return Prefix(GCSafePointServicePath(""))
}

// GCSafePointServicePath returns the GC safe point service key path with the given service ID.
func GCSafePointServicePath(serviceID string) string {
	return fmt.Sprintf(gcSafePointServicePathFormat, ClusterID(), serviceID)
}

// GCSafePointV2Path is the storage path of gc safe point v2.
// Path: keyspaces/gc_safe_point/{keyspaceID}
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
	return Prefix(ServiceSafePointV2Path(keyspaceID, ""))
}

// GCSafePointV2Prefix is the path prefix to all gc safe point v2.
func GCSafePointV2Prefix() string {
	return Prefix(GCSafePointV2Path(0))
}
