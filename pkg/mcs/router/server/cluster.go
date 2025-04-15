// Copyright 2025 TiKV Project Authors.
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

import (
	"context"

	"github.com/tikv/pd/pkg/core"
)

// Cluster is used to manage all information for router purpose.
type Cluster struct {
	ctx    context.Context
	cancel context.CancelFunc
	*core.BasicCluster
}

// NewCluster creates a new cluster.
func NewCluster(
	parentCtx context.Context,
	basicCluster *core.BasicCluster,
) (*Cluster, error) {
	ctx, cancel := context.WithCancel(parentCtx)
	c := &Cluster{
		ctx:          ctx,
		cancel:       cancel,
		BasicCluster: basicCluster,
	}
	return c, nil
}

// GetBasicCluster returns the basic cluster.
func (c *Cluster) GetBasicCluster() *core.BasicCluster {
	return c.BasicCluster
}
