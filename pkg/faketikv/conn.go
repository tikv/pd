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

package faketikv

// Conn records the informations of connection among nodes.
type Conn struct {
	Nodes map[uint64]*Node
}

// NewConn returns a conn.
func NewConn(nodes map[uint64]*Node) (*Conn, error) {
	conn := &Conn{
		Nodes: nodes,
	}
	return conn, nil
}

func (c *Conn) nodeHealth(storeID uint64) bool {
	n, ok := c.Nodes[storeID]
	if !ok {
		return false
	}

	return n.GetState() == Up
}
