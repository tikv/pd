package server

import (
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

func (c *conn) handleTso(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetTso()
	if request == nil {
		return nil, errors.Errorf("invalid tso command, but %v", req)
	}

	tso := &pdpb.TsoResponse{}

	count := request.GetNumber()
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
	// Store may be nil, should we return an error instead of none result?
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

func (c *conn) handleAskChangePeer(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetAskChangePeer()
	if request.GetRegion() == nil {
		return nil, errors.New("missing region for changing peer")
	}
	if request.GetLeaderStoreId() == 0 {
		return nil, errors.New("missing leader for changing peer")
	}

	cluster, err := c.getRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err = cluster.HandleAskChangePeer(request); err != nil {
		return nil, errors.Trace(err)
	}

	return &pdpb.Response{
		AskChangePeer: &pdpb.AskChangePeerResponse{},
	}, nil
}

func (c *conn) handleAskSplit(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetAskSplit()
	if request.GetRegion() == nil {
		return nil, errors.New("missing region for split")
	}
	if request.GetLeaderStoreId() == 0 {
		return nil, errors.New("missing leader for split")
	}
	if request.GetSplitKey() == nil {
		return nil, errors.New("missing split key for split")
	}

	cluster, err := c.getRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err = cluster.HandleAskSplit(request); err != nil {
		return nil, errors.Trace(err)
	}

	return &pdpb.Response{
		AskSplit: &pdpb.AskSplitResponse{},
	}, nil
}
