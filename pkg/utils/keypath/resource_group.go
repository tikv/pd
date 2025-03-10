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

// ControllerConfigPath returns the path to save the controller config.
func ControllerConfigPath() string {
	return controllerConfigPath
}

// ResourceGroupSettingPath returns the path to save the resource group settings.
func ResourceGroupSettingPath(groupName string) string {
	return fmt.Sprintf(resourceGroupSettingsPathFormat, groupName)
}

// ResourceGroupStatePath returns the path to save the resource group states.
func ResourceGroupStatePath(groupName string) string {
	return fmt.Sprintf(resourceGroupStatesPathFormat, groupName)
}

// ResourceGroupSettingPrefix returns the prefix of the resource group settings.
func ResourceGroupSettingPrefix() string {
	return ResourceGroupSettingPath("")
}

// ResourceGroupStatePrefix returns the prefix of the resource group states.
func ResourceGroupStatePrefix() string {
	return ResourceGroupStatePath("")
}
