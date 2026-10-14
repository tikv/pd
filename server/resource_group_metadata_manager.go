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

package server

import rm_server "github.com/tikv/pd/pkg/mcs/resourcemanager/server"

type pdMetadataManagerProvider struct {
	*Server
}

// GetResourceGroupWriteRole returns the manager write role for PD local resource group metadata APIs.
func (*pdMetadataManagerProvider) GetResourceGroupWriteRole() rm_server.ResourceGroupWriteRole {
	return rm_server.ResourceGroupWriteRolePDMetaOnly
}

// GetResourceGroupMetadataManager returns the singleton local manager used by PD to handle RM metadata writes.
func (s *Server) GetResourceGroupMetadataManager() *rm_server.Manager {
	s.resourceGroupMetadataManagerOnce.Do(func() {
		s.resourceGroupMetadataManager = rm_server.NewManager[*pdMetadataManagerProvider](
			&pdMetadataManagerProvider{Server: s},
		)
	})
	return s.resourceGroupMetadataManager
}
