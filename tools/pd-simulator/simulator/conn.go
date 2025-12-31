// Copyright 2018 TiKV Project Authors.
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

package simulator

import (
	"github.com/pingcap/kvproto/pkg/metapb"

	"github.com/tikv/pd/tools/pd-simulator/simulator/cases"
	"github.com/tikv/pd/tools/pd-simulator/simulator/config"
)

// Connection records the information of connection among nodes.
type Connection struct {
	Nodes map[uint64]*Node
}

// NewConnection creates nodes according to the configuration and returns the connection among nodes.
func NewConnection(simCase *cases.Case, storeConfig *config.SimConfig) (*Connection, error) {
	conn := &Connection{
		Nodes: make(map[uint64]*Node),
	}

	for _, store := range simCase.Stores {
		node, err := NewNode(store, storeConfig)
		if err != nil {
			return nil, err
		}
		conn.Nodes[store.ID] = node
	}

	return conn, nil
}

func (c *Connection) nodeHealth(storeID uint64) bool {
	n, ok := c.Nodes[storeID]
	if !ok {
		return false
	}

	return n.GetNodeState() == metapb.NodeState_Preparing || n.GetNodeState() == metapb.NodeState_Serving
}

func (c *Connection) getNodes() []*Node {
	var nodes []*Node
	for _, n := range c.Nodes {
		if n.GetNodeState() != metapb.NodeState_Removed {
			nodes = append(nodes, n)
		}
	}
	return nodes
}
