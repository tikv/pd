// Copyright 2023 TiKV Project Authors.
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

package discovery

import (
	"strconv"
	"strings"
)

const (
	registryPrefix = "/ms"
	registryKey    = "registry"
)

// RegistryPath returns the full path to store microservice addresses.
func RegistryPath(clusterID, serviceName, serviceAddr string) string {
	return strings.Join([]string{registryPrefix, clusterID, serviceName, registryKey, serviceAddr}, "/")
}

// ServicePath returns the path to store microservice addresses.
func ServicePath(clusterID, serviceName string) string {
	return strings.Join([]string{registryPrefix, clusterID, serviceName, registryKey}, "/")
}

// TSOPath returns the path to store TSO addresses.
func TSOPath(clusterID uint64) string {
	return ServicePath(strconv.FormatUint(clusterID, 10), "tso") + "/"
}
