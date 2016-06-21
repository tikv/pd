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
	"github.com/pingcap/pd/server"
)

type balancerController struct {
	baseController
}

type balancerInfo struct {
	ID       uint64          `json:"id"`
	Balancer server.Operator `json:"balancer"`
}

type balancersInfo struct {
	Count     int             `json:"count"`
	Balancers []*balancerInfo `json:"balancers"`
}

func (bc *balancerController) GetBalancers() {
	cluster, err := server.PdServer.GetRaftCluster()
	if err != nil {
		bc.serveError(500, err)
		return
	}
	if cluster == nil {
		bc.ServeJSON()
		return
	}

	balancers := cluster.GetBalanceOperators()
	balancersInfo := &balancersInfo{
		Count:     len(balancers),
		Balancers: make([]*balancerInfo, 0, len(balancers)),
	}

	for key, value := range balancers {
		balancer := &balancerInfo{
			ID:       key,
			Balancer: value,
		}

		balancersInfo.Balancers = append(balancersInfo.Balancers, balancer)
	}

	bc.Data["json"] = balancersInfo
	bc.ServeJSON()
}
