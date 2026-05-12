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

import "fmt"

// ConfigPath returns the path to save the PD config.
func ConfigPath() string {
	return fmt.Sprintf(configPathFormat, ClusterID())
}

// SchedulerConfigPathPrefix returns the path prefix to save the scheduler config.
func SchedulerConfigPathPrefix() string {
	return SchedulerConfigPath("")
}

// SchedulerConfigPath returns the path to save the scheduler config.
func SchedulerConfigPath(schedulerName string) string {
	return fmt.Sprintf(schedulerConfigPathFormat, ClusterID(), schedulerName)
}
