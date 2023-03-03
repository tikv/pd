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

import "path"

const (
	registryPrefix = "/pd/microservice"
	registryKey    = "registry"
)

func registryPath(serviceName, serviceAddr string) string {
	return path.Join(registryPrefix, serviceName, registryKey, serviceAddr)
}

func discoveryPath(serviceName string) string {
	return path.Join(registryPrefix, serviceName, registryKey)
}
