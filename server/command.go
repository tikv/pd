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

func (c *conn) getRaftCluster(req *pdpb.Request) (*raftCluster, error) {
	cluster, err := c.s.getRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if cluster == nil {
		return nil, errors.Trace(errClusterNotBootstrapped)
	}
	return cluster, nil
}

func (c *conn) handleGetMeta(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetGetMeta()
	if request == nil {
		return nil, errors.Errorf("invalid get meta command, but %v", req)
	}

	cluster, err := c.getRaftCluster(req)
	if err != nil {
		return nil, errors.Trace(err)
	}

	resp := &pdpb.GetMetaResponse{
		MetaType: request.MetaType,
	}

	switch request.GetMetaType() {
	case pdpb.MetaType_StoreType:
		storeID := request.GetStoreId()
		store, err := cluster.GetStore(storeID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Store may be nil, should we return an error instead of none result?
		resp.Store = store
	case pdpb.MetaType_RegionType:
		key := request.GetRegionKey()
		region, err := cluster.GetRegion(key)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.Region = region
	case pdpb.MetaType_ClusterType:
		meta, err := cluster.GetMeta()
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.Cluster = meta
	default:
		return nil, errors.Errorf("invalid meta type %v", request.GetMetaType())
	}

	return &pdpb.Response{
		GetMeta: resp,
	}, nil
}

func (c *conn) handlePutMeta(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetPutMeta()
	if request == nil {
		return nil, errors.Errorf("invalid put meta command, but %v", req)
	}

	cluster, err := c.getRaftCluster(req)
	if err != nil {
		return nil, errors.Trace(err)
	}

	switch request.GetMetaType() {
	case pdpb.MetaType_StoreType:
		store := request.GetStore()
		if err = cluster.PutStore(store); err != nil {
			return nil, errors.Trace(err)
		}
	case pdpb.MetaType_ClusterType:
		meta := request.GetCluster()
		if err = cluster.PutMeta(meta); err != nil {
			return nil, errors.Trace(err)
		}
	default:
		return nil, errors.Errorf("invalid meta type %v", request.GetMetaType())
	}

	resp := &pdpb.PutMetaResponse{
		MetaType: request.MetaType,
	}

	return &pdpb.Response{
		PutMeta: resp,
	}, nil
}

func (c *conn) handleAskChangePeer(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetAskChangePeer()
	if request == nil {
		return nil, errors.Errorf("invalid ask change peer command, but %v", req)
	}
	if request.Region == nil {
		return nil, errors.New("missing region for changing peer")
	}
	if request.Leader == nil {
		return nil, errors.New("missing leader for changing peer")
	}

	cluster, err := c.getRaftCluster(req)
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
	if request == nil {
		return nil, errors.Errorf("invalid ask split command, but %v", req)
	}
	if request.Region == nil {
		return nil, errors.New("missing region for split")
	}
	if request.Leader == nil {
		return nil, errors.New("missing leader for split")
	}
	if request.SplitKey == nil {
		return nil, errors.New("missing split key for split")
	}

	cluster, err := c.getRaftCluster(req)
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
