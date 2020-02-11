// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package simulator

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/v4/tools/pd-simulator/simulator/cases"
)

// Connection records the informations of connection among nodes.
type Connection struct {
	pdAddr string
	Nodes  map[uint64]*Node
}

// NewConnection creates nodes according to the configuration and returns the connection among nodes.
func NewConnection(simCase *cases.Case, pdAddr string, storeConfig *SimConfig) (*Connection, error) {
	conn := &Connection{
		pdAddr: pdAddr,
		Nodes:  make(map[uint64]*Node),
	}

	for _, store := range simCase.Stores {
		node, err := NewNode(store, pdAddr, storeConfig.StoreIOMBPerSecond)
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

	return n.GetState() == metapb.StoreState_Up
}
