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
	"strconv"
	"strings"

	"github.com/tikv/pd/pkg/mcs/utils/constant"
)

// ControllerConfigPath returns the path to save the controller config.
func ControllerConfigPath() string {
	return controllerConfigPath
}

// resourceGroupSettingPath returns the path to save the legacy resource group settings.
func resourceGroupSettingPath(groupName string) string {
	return fmt.Sprintf(resourceGroupSettingsPathFormat, groupName)
}

// resourceGroupStatePath returns the path to save the legacy resource group states.
func resourceGroupStatePath(groupName string) string {
	return fmt.Sprintf(resourceGroupStatesPathFormat, groupName)
}

// ResourceGroupSettingPrefix returns the prefix of the legacy resource group settings.
func ResourceGroupSettingPrefix() string {
	return resourceGroupSettingPath("")
}

// ResourceGroupStatePrefix returns the prefix of the legacy resource group states.
func ResourceGroupStatePrefix() string {
	return resourceGroupStatePath("")
}

// KeyspaceResourceGroupSettingPath returns the path to save the keyspace resource group settings.
func KeyspaceResourceGroupSettingPath(keyspaceID uint32, groupName string) string {
	if keyspaceID == constant.NullKeyspaceID {
		return resourceGroupSettingPath(groupName)
	}
	return fmt.Sprintf(keyspaceResourceGroupSettingsPathFormat, keyspaceID, groupName)
}

// KeyspaceResourceGroupStatePath returns the path to save the keyspace resource group states.
func KeyspaceResourceGroupStatePath(keyspaceID uint32, groupName string) string {
	if keyspaceID == constant.NullKeyspaceID {
		return resourceGroupStatePath(groupName)
	}
	return fmt.Sprintf(keyspaceResourceGroupStatesPathFormat, keyspaceID, groupName)
}

// KeyspaceResourceGroupSettingPrefix returns the prefix of the keyspace resource group settings.
func KeyspaceResourceGroupSettingPrefix() string {
	return keyspaceResourceGroupSettingsPathPrefixFormat
}

// KeyspaceResourceGroupStatePrefix returns the prefix of the keyspace resource group states.
func KeyspaceResourceGroupStatePrefix() string {
	return keyspaceResourceGroupStatesPathPrefixFormat
}

// KeyspaceServiceLimitPath returns the path to save the keyspace service limit.
func KeyspaceServiceLimitPath(keyspaceID uint32) string {
	return fmt.Sprintf(keyspaceServiceLimitsPathFormat, keyspaceID)
}

// KeyspaceServiceLimitPrefix returns the prefix of the keyspace service limits.
func KeyspaceServiceLimitPrefix() string {
	return keyspaceServiceLimitsPathPrefixFormat
}

// ParseKeyspaceResourceGroupPath parses the keyspace ID and resource group name from the keyspace resource group path.
func ParseKeyspaceResourceGroupPath(path string) (uint32, string, error) {
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		return 0, "", fmt.Errorf("invalid keyspace resource group setting path: %s", path)
	}
	keyspaceIDStr := parts[0]
	keyspaceID, err := strconv.ParseUint(keyspaceIDStr, 10, 32)
	if err != nil {
		return 0, "", fmt.Errorf("invalid keyspace ID str: %s", keyspaceIDStr)
	}
	return uint32(keyspaceID), strings.TrimPrefix(path, keyspaceIDStr+"/"), nil
}
