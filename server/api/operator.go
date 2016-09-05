// Copyright 2016 PingCAP, Inc.
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

package api

import (
	"encoding/json"

	"github.com/juju/errors"
	"github.com/pingcap/pd/server"
)

type changePeerOperator struct {
	Name     string `json:"name"`
	RegionID uint64 `json:"region_id"`
	StoreID  uint64 `json:"store_id"`
	PeerID   uint64 `json:"peer_id"`
}

func newOperator(cluster *server.RaftCluster, m json.RawMessage) (uint64, server.Operator, error) {
	op := &changePeerOperator{}
	if err := json.Unmarshal(m, op); err != nil {
		return 0, nil, errors.Trace(err)
	}

	var (
		operator server.Operator
		err      error
	)
	switch op.Name {
	case "add_peer":
		operator, err = cluster.NewAddPeerOperator(op.RegionID, op.StoreID)
	case "remove_peer":
		operator, err = cluster.NewRemovePeerOperator(op.RegionID, op.PeerID)
	default:
		return 0, nil, errors.Errorf("invalid operator %v", op)
	}
	if err != nil {
		return 0, nil, errors.Trace(err)
	}

	return op.RegionID, operator, nil
}
