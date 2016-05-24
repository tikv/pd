package server

import (
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"golang.org/x/net/context"
)

func (c *conn) handleTso(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetTso()
	if request == nil {
		return nil, errors.Errorf("invalid tso command, but %v", req)
	}

	tso := &pdpb.TsoResponse{}

	count := request.GetCount()
	for i := uint32(0); i < count; i++ {
		ts := c.s.getRespTS()
		if ts == nil {
			return nil, errors.New("can not get timestamp")
		}

		tso.Timestamps = append(tso.Timestamps, ts)
	}

	return &pdpb.Response{
		Tso: tso,
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

	cluster, err := c.s.getRaftCluster()
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

	cluster, err := c.s.getRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if cluster != nil {
		return NewBootstrappedError(), nil
	}

	return c.s.bootstrapCluster(request)
}

func (c *conn) getRaftCluster() (*raftCluster, error) {
	cluster, err := c.s.getRaftCluster()
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
	store, err := cluster.GetStore(storeID)
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
	region, err := cluster.GetRegion(key)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &pdpb.Response{
		GetRegion: &pdpb.GetRegionResponse{
			Region: region,
		},
	}, nil
}

func (c *conn) handleRegionHeartbeat(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetRegionHeartbeat()
	if request == nil {
		return nil, errors.Errorf("invalid region heartbeat command, but %v", request)
	}

	// Handle split/merge region.
	// For split, we should handle heartbeat carefully.
	// E.g, for region 1 [a, c) -> 1 [a, b) + 2 [b, c).
	// after split, region 1 and 2 will do heartbeat independently.
	cluster, err := c.getRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}

	reqRegion := request.GetRegion()
	reqRegionStartKey := reqRegion.GetStartKey()
	searchRegion, err := cluster.GetRegion(reqRegionStartKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	splitOps, err := cluster.maybeSplit(request, reqRegion, searchRegion)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Handle change peer for region.
	changePeerOps, changePeer, err := cluster.maybeChangePeer(request, reqRegion, searchRegion)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Check whether need to update etcd meta info.
	ops := append(splitOps, changePeerOps...)
	if len(ops) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		resp, err := c.s.client.Txn(ctx).
			If(c.s.leaderCmp()).
			Then(ops...).
			Commit()
		cancel()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !resp.Succeeded {
			return nil, errors.New("handle region heartbeat failed")
		}
	}

	return &pdpb.Response{
		RegionHeartbeat: &pdpb.RegionHeartbeatResponse{
			ChangePeer: changePeer,
		},
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

	conf, err := cluster.GetConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}

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
	if err = cluster.PutConfig(conf); err != nil {
		return nil, errors.Trace(err)
	}

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

	if err = cluster.PutStore(store); err != nil {
		return nil, errors.Trace(err)
	}
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

	split, err := cluster.HandleAskSplit(request)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &pdpb.Response{
		AskSplit: split,
	}, nil
}
