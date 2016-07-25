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

package server

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	raftpb "github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

func (c *conn) handleTso(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetTso()
	if request == nil {
		return nil, errors.Errorf("invalid tso command, but %v", req)
	}

	count := request.GetCount()
	ts := c.s.getRespTS(count)
	if ts == nil {
		return nil, errors.New("can not get timestamp")
	}

	return &pdpb.Response{
		Tso: &pdpb.TsoResponse{Timestamp: ts, Count: &count},
	}, nil
}

func (c *conn) handleAllocID(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetAllocId()
	if request == nil {
		return nil, errors.Errorf("invalid alloc id command, but %v", req)
	}

	// We can use an allocator for all types ID allocation.
	id, err := c.s.idAlloc.Alloc()
	if err != nil {
		return nil, errors.Trace(err)
	}

	idResp := &pdpb.AllocIdResponse{
		Id: proto.Uint64(id),
	}

	return &pdpb.Response{
		AllocId: idResp,
	}, nil
}

func (c *conn) handleIsBootstrapped(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetIsBootstrapped()
	if request == nil {
		return nil, errors.Errorf("invalid is bootstrapped command, but %v", req)
	}

	cluster, err := c.s.GetRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}

	resp := &pdpb.IsBootstrappedResponse{
		Bootstrapped: proto.Bool(cluster != nil),
	}

	return &pdpb.Response{
		IsBootstrapped: resp,
	}, nil
}

func (c *conn) handleBootstrap(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetBootstrap()
	if request == nil {
		return nil, errors.Errorf("invalid bootstrap command, but %v", req)
	}

	cluster, err := c.s.GetRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if cluster != nil {
		return newBootstrappedError(), nil
	}

	return c.s.bootstrapCluster(request)
}

func (c *conn) getRaftCluster() (*RaftCluster, error) {
	cluster, err := c.s.GetRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if cluster == nil {
		return nil, errors.Trace(errClusterNotBootstrapped)
	}
	return cluster, nil
}

func (c *conn) handleGetStore(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetGetStore()
	if request == nil {
		return nil, errors.Errorf("invalid get store command, but %v", req)
	}

	cluster, err := c.getRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}

	storeID := request.GetStoreId()
	store, _, err := cluster.GetStore(storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &pdpb.Response{
		GetStore: &pdpb.GetStoreResponse{
			Store: store,
		},
	}, nil
}

func (c *conn) handleGetRegion(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetGetRegion()
	if request == nil {
		return nil, errors.Errorf("invalid get region command, but %v", req)
	}

	cluster, err := c.getRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}

	key := request.GetRegionKey()
	region, leader := cluster.getRegion(key)
	return &pdpb.Response{
		GetRegion: &pdpb.GetRegionResponse{
			Region: region,
			Leader: leader,
		},
	}, nil
}

func (c *conn) handleRegionHeartbeat(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetRegionHeartbeat()
	if request == nil {
		return nil, errors.Errorf("invalid region heartbeat command, but %v", request)
	}

	cluster, err := c.getRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}

	leader := request.GetLeader()
	if leader == nil {
		return nil, errors.Errorf("invalid request leader, %v", request)
	}

	region := request.GetRegion()
	if region.GetId() == 0 {
		return nil, errors.Errorf("invalid request region, %v", request)
	}

	resp, changePeer, err := cluster.cachedCluster.regions.heartbeat(region, leader)
	if err != nil {
		return nil, errors.Trace(err)
	}

	res, err := cluster.handleRegionHeartbeat(region, leader)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var ops []clientv3.Op
	if resp.putRegion != nil {
		regionValue, err := proto.Marshal(resp.putRegion)
		if err != nil {
			return nil, errors.Trace(err)
		}
		regionPath := makeRegionKey(cluster.clusterRoot, resp.putRegion.GetId())
		ops = append(ops, clientv3.OpPut(regionPath, string(regionValue)))
	}

	if resp.removeRegion != nil && resp.removeRegion.GetId() != resp.putRegion.GetId() {
		// Well, we meet overlap and remove and then put the same region id,
		// so here we ignore the remove operation here.
		// The heartbeat will guarantee that if RemoveRegion exists, PutRegion can't
		// be nil, if not, we will panic.
		regionPath := makeRegionKey(cluster.clusterRoot, resp.removeRegion.GetId())
		ops = append(ops, clientv3.OpDelete(regionPath))
	}

	// TODO: we can update in etcd asynchronously later.
	if len(ops) > 0 {
		resp, err := c.s.LeaderTxn().Then(ops...).Commit()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !resp.Succeeded {
			return nil, errors.New("handle region heartbeat failed")
		}
	}

	if changePeer != nil {
		var op Operator
		if changePeer.GetChangeType() == raftpb.ConfChangeType_AddNode {
			op = newAddPeerOperator(region.GetId(), changePeer.GetPeer())
		} else {
			op = newRemovePeerOperator(region.GetId(), changePeer.GetPeer())
		}

		cluster.balancerWorker.postEvent(op, evtEnd)
	}

	return &pdpb.Response{
		RegionHeartbeat: res,
	}, nil
}

func (c *conn) handleStoreHeartbeat(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetStoreHeartbeat()
	stats := request.GetStats()
	if stats == nil {
		return nil, errors.Errorf("invalid store heartbeat command, but %v", request)
	}

	cluster, err := c.getRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}

	ok := cluster.cachedCluster.updateStoreStatus(stats)
	if !ok {
		return nil, errors.Errorf("cannot find store to update stats, stats %v", stats)
	}

	return &pdpb.Response{
		StoreHeartbeat: &pdpb.StoreHeartbeatResponse{},
	}, nil
}

func (c *conn) handleGetClusterConfig(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetGetClusterConfig()
	if request == nil {
		return nil, errors.Errorf("invalid get cluster config command, but %v", req)
	}

	cluster, err := c.getRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}

	conf := cluster.GetConfig()
	return &pdpb.Response{
		GetClusterConfig: &pdpb.GetClusterConfigResponse{
			Cluster: conf,
		},
	}, nil
}

func (c *conn) handlePutClusterConfig(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetPutClusterConfig()
	if request == nil {
		return nil, errors.Errorf("invalid put cluster config command, but %v", req)
	}

	cluster, err := c.getRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}

	conf := request.GetCluster()
	if err = cluster.putConfig(conf); err != nil {
		return nil, errors.Trace(err)
	}

	log.Infof("put cluster config ok - %v", conf)

	return &pdpb.Response{
		PutClusterConfig: &pdpb.PutClusterConfigResponse{},
	}, nil
}

func (c *conn) handlePutStore(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetPutStore()
	if request == nil {
		return nil, errors.Errorf("invalid put store command, but %v", req)
	}
	store := request.GetStore()

	cluster, err := c.getRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err = cluster.putStore(store); err != nil {
		return nil, errors.Trace(err)
	}

	log.Infof("put store ok - %v", store)

	return &pdpb.Response{
		PutStore: &pdpb.PutStoreResponse{},
	}, nil
}

func (c *conn) handleAskSplit(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetAskSplit()
	if request.GetRegion().GetStartKey() == nil {
		return nil, errors.New("missing region start key for split")
	}

	cluster, err := c.getRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}

	split, err := cluster.handleAskSplit(request)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &pdpb.Response{
		AskSplit: split,
	}, nil
}

func (c *conn) handleReportSplit(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetReportSplit()
	if request == nil {
		return nil, errors.Errorf("invalid report split command, but %v", req)
	}

	cluster, err := c.getRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}

	split, err := cluster.handleReportSplit(request)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &pdpb.Response{
		ReportSplit: split,
	}, nil
}
