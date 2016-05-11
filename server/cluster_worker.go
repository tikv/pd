package server

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/msgpb"
	"github.com/pingcap/kvproto/pkg/pd_jobpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/pingcap/kvproto/pkg/raftpb"
	"github.com/pingcap/kvproto/pkg/util"
	"github.com/twinj/uuid"
	"golang.org/x/net/context"
)

const (
	checkJobInterval = 10 * time.Second

	readTimeout  = 3 * time.Second
	writeTimeout = 3 * time.Second

	maxSendRetry = 10
)

func (c *raftCluster) onJobWorker() {
	defer c.wg.Done()

	ticker := time.NewTicker(checkJobInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.quitCh:
			return
		case <-c.askJobCh:
			if !c.s.IsLeader() {
				log.Warn("we are not leader, no need to handle job")
				continue
			}

			job, err := c.getJob()
			if err != nil {
				log.Errorf("get first job err %v", err)
			}
			if job == nil {
				// no job now, wait
				continue
			}

			if err = c.handleJob(job); err != nil {
				log.Errorf("handle job %v err %v, retry", job, err)
				// wait and force retry
				time.Sleep(c.s.cfg.nextRetryDelay)
				asyncNotify(c.askJobCh)
				continue
			}

			if err = c.popJob(job); err != nil {
				log.Errorf("pop job %v err %v", job, err)
			}

			// Notify to job again.
			asyncNotify(c.askJobCh)
		case <-ticker.C:
			// Try to check job regularly.
			asyncNotify(c.askJobCh)
		}
	}
}

func asyncNotify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (c *raftCluster) postJob(job *pd_jobpd.Job) error {
	jobID, err := c.s.idAlloc.Alloc()
	if err != nil {
		return errors.Trace(err)
	}

	job.Id = proto.Uint64(jobID)
	job.Request.Header.Uuid = uuid.NewV4().Bytes()
	job.Status = pd_jobpd.JobStatus_Pending.Enum()

	jobValue, err := proto.Marshal(job)
	if err != nil {
		return errors.Trace(err)
	}

	jobPath := makeJobKey(c.clusterRoot, jobID)

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	resp, err := c.s.client.Txn(ctx).
		If(c.s.leaderCmp()).
		Then(clientv3.OpPut(jobPath, string(jobValue))).
		Commit()
	cancel()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.Errorf("post job %v fail", job)
	}

	// Tell job worker to handle the job
	asyncNotify(c.askJobCh)

	return nil
}

func (c *raftCluster) getJob() (*pd_jobpd.Job, error) {
	job := &pd_jobpd.Job{}

	jobKey := makeJobKey(c.clusterRoot, 0)
	maxJobKey := makeJobKey(c.clusterRoot, math.MaxUint64)

	sortOpt := clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)
	ok, err := getProtoMsg(c.s.client, jobKey, job, clientv3.WithRange(maxJobKey), clientv3.WithLimit(1), sortOpt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !ok {
		return nil, nil
	}

	return job, nil
}

func (c *raftCluster) popJob(job *pd_jobpd.Job) error {
	jobKey := makeJobKey(c.clusterRoot, job.GetId())
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	resp, err := c.s.client.Txn(ctx).
		If(c.s.leaderCmp()).
		Then(clientv3.OpDelete(jobKey)).
		Commit()
	cancel()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.New("pop first job failed")
	}
	return nil
}

func (c *raftCluster) updateJobStatus(job *pd_jobpd.Job, status pd_jobpd.JobStatus) error {
	jobKey := makeJobKey(c.clusterRoot, job.GetId())
	job.Status = status.Enum()
	jobValue, err := proto.Marshal(job)
	if err != nil {
		return errors.Trace(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	resp, err := c.s.client.Txn(ctx).
		If(c.s.leaderCmp()).
		Then(clientv3.OpPut(jobKey, string(jobValue))).
		Commit()
	cancel()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.New("update job status failed")
	}
	return nil
}

type checkOKFunc func(*raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.AdminResponse, error)

func (c *raftCluster) handleJob(job *pd_jobpd.Job) error {
	log.Debugf("begin to handle job %v", job)

	var (
		request = job.Request
		// must be administrator request, check later.
		adminRequest = request.AdminRequest

		checkOK checkOKFunc

		resp *raft_cmdpb.AdminResponse
		err  error
	)

	switch adminRequest.GetCmdType() {
	case raft_cmdpb.AdminCmdType_ChangePeer:
		checkOK = c.checkChangePeerOK
	case raft_cmdpb.AdminCmdType_Split:
		checkOK = c.checkSplitOK
	default:
		log.Errorf("unsupported request %v, ignore", request)
		return nil
	}

	if job.GetStatus() == pd_jobpd.JobStatus_Pending {
		if err = c.updateJobStatus(job, pd_jobpd.JobStatus_Running); err != nil {
			return errors.Trace(err)
		}
		// If the job is first running, no need to check whether the job
		// is finished OK.
		checkOK = nil
	} else {
		// Here means the job is not first running, we can first check whether
		// the job is finished OK.
		// The got response != nil means we have already executed the job and
		// the region version/conf version is changed, so we don't need to process
		// the job again.
		if resp, err = checkOK(request); err != nil {
			return errors.Trace(err)
		}
	}

	if resp == nil {
		resp, err = c.processJob(job, checkOK)
		if err != nil {
			return errors.Trace(err)
		}
		if resp == nil {
			return nil
		}
	}

	switch resp.GetCmdType() {
	case raft_cmdpb.AdminCmdType_ChangePeer:
		return c.handleChangePeerOK(resp.ChangePeer)
	case raft_cmdpb.AdminCmdType_Split:
		return c.handleSplitOK(resp.Split)
	default:
		log.Errorf("invalid admin response %v, ignore", resp)
		return nil
	}
}

func (c *raftCluster) processJob(job *pd_jobpd.Job, checkOK checkOKFunc) (*raft_cmdpb.AdminResponse, error) {
	request := job.Request

	response, err := c.sendRaftCommand(request, job.Region)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if response.Header != nil && response.Header.Error != nil {
		// If we don't supply check ok function, it means that the job is
		// first running, not retried, we can safely cancel it.
		if checkOK == nil {
			log.Warnf("handle %v but failed with response %v, cancel it", job.Request, response.Header.Error)
			return nil, nil
		}

		log.Errorf("handle %v but failed with response %v, check in raft server", job.Request, response.Header.Error)

		adminResponse, err := checkOK(job.Request)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if adminResponse == nil {
			log.Warnf("raft server doesn't execute %v, cancel it", job.Request)
			return nil, nil
		}
		return adminResponse, nil
	}

	// must administrator response, check later.
	return response.AdminResponse, nil
}

func (c *raftCluster) handleAddPeerReq(region *metapb.Region) (*metapb.Peer, error) {
	peerID, err := c.s.idAlloc.Alloc()
	if err != nil {
		return nil, errors.Trace(err)
	}

	mu := &c.mu
	mu.RLock()
	defer mu.RUnlock()

	// Find a proper store which the region has not in.
	// Now we just choose the first store. Later we will do
	// a better choice.
	//
	// TODO:
	// 1, The stores may be in same machine, so we should choose the best
	// store which the region's current stores are not in. We can use IP to
	// check this.
	// 2, We can check the store statistics and find a low load store.
	// 3, more algorithms...
	var matchStore *metapb.Store
	for _, store := range mu.stores {
		storeID := store.GetId()

		existStore := false
		for _, peer := range region.Peers {
			if peer.GetStoreId() == storeID {
				// we can't add peer in the same store.
				existStore = true
				break
			}
		}

		if existStore {
			continue
		}

		matchStore = &store

		break
	}

	if matchStore == nil {
		return nil, errors.Errorf("find no store to add peer for region %v", region)
	}

	peer := &metapb.Peer{
		StoreId: proto.Uint64(matchStore.GetId()),
		Id:      proto.Uint64(peerID),
	}

	return peer, nil
}

// If leader is nil, we will return an error, or else we can remove none leader peer.
func (c *raftCluster) handleRemovePeerReq(region *metapb.Region, leader *metapb.Peer) (*metapb.Peer, error) {
	if len(region.Peers) <= 1 {
		return nil, errors.Errorf("can not remove peer for region %v", region)
	}
	if leader == nil {
		return nil, errors.Errorf("invalid leader for region %v", region)
	}

	for _, peer := range region.Peers {
		if peer.GetId() != leader.GetId() {
			return peer, nil
		}
	}

	// Maybe we can't enter here.
	return nil, errors.Errorf("find no proper peer to remove for region %v", region)
}

func (c *raftCluster) HandleAskChangePeer(request *pdpb.AskChangePeerRequest) error {
	clusterMeta, err := c.GetConfig()
	if err != nil {
		return errors.Trace(err)
	}

	var (
		maxPeerNumber = int(clusterMeta.GetMaxPeerNumber())
		region        = request.GetRegion()
		regionID      = region.GetId()
		peerNumber    = len(region.GetPeers())
		changeType    raftpb.ConfChangeType
		peer          *metapb.Peer
	)

	if peerNumber == maxPeerNumber {
		log.Infof("region %d peer number equals %d, no need to change peer", regionID, maxPeerNumber)
		return nil
	} else if peerNumber < maxPeerNumber {
		log.Infof("region %d peer number %d < %d, need to add peer", regionID, peerNumber, maxPeerNumber)
		changeType = raftpb.ConfChangeType_AddNode
		if peer, err = c.handleAddPeerReq(region); err != nil {
			return errors.Trace(err)
		}
	} else {
		log.Infof("region %d peer number %d > %d, need to remove peer", regionID, peerNumber, maxPeerNumber)
		changeType = raftpb.ConfChangeType_RemoveNode
		if peer, err = c.handleRemovePeerReq(region, request.Leader); err != nil {
			return errors.Trace(err)
		}
	}

	changePeer := &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_ChangePeer.Enum(),
		ChangePeer: &raft_cmdpb.ChangePeerRequest{
			ChangeType: changeType.Enum(),
			Peer:       peer,
		},
	}

	req := &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId:    proto.Uint64(regionID),
			RegionEpoch: region.RegionEpoch,
			Peer:        request.Leader,
		},
		AdminRequest: changePeer,
	}

	job := &pd_jobpd.Job{
		Request: req,
		Region:  request.Region,
	}

	return c.postJob(job)
}

func (c *raftCluster) handleChangePeerOK(changePeer *raft_cmdpb.ChangePeerResponse) error {
	region := changePeer.Region

	// Update region
	regionSearchPath := makeRegionSearchKey(c.clusterRoot, region.GetEndKey())
	regionValue, err := proto.Marshal(region)
	if err != nil {
		return errors.Trace(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	resp, err := c.s.client.Txn(ctx).
		If(c.s.leaderCmp()).
		Then(clientv3.OpPut(regionSearchPath, string(regionValue))).
		Commit()
	cancel()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.New("update change peer region failed")
	}

	return nil
}

func (c *raftCluster) checkChangePeerOK(request *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.AdminResponse, error) {
	regionID := request.Header.GetRegionId()
	leader := request.Header.Peer

	detail, err := c.getRegionDetail(regionID, leader)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// If leader's conf version has changed, we can think ChangePeerOK,
	// else we can think the raft server doesn't execute this change peer command.
	if detail.Region.RegionEpoch.GetConfVer() > request.Header.RegionEpoch.GetConfVer() {
		return &raft_cmdpb.AdminResponse{
			CmdType: raft_cmdpb.AdminCmdType_ChangePeer.Enum(),
			ChangePeer: &raft_cmdpb.ChangePeerResponse{
				Region: detail.Region,
			},
		}, nil
	}

	return nil, nil
}

func (c *raftCluster) HandleAskSplit(request *pdpb.AskSplitRequest) error {
	newRegionID, err := c.s.idAlloc.Alloc()
	if err != nil {
		return errors.Trace(err)
	}

	peerIDs := make([]uint64, len(request.Region.Peers))
	for i := 0; i < len(peerIDs); i++ {
		if peerIDs[i], err = c.s.idAlloc.Alloc(); err != nil {
			return errors.Trace(err)
		}
	}

	split := &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_Split.Enum(),
		Split: &raft_cmdpb.SplitRequest{
			NewRegionId: proto.Uint64(newRegionID),
			NewPeerIds:  peerIDs,
			SplitKey:    request.SplitKey,
		},
	}

	req := &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId:    request.Region.Id,
			RegionEpoch: request.Region.RegionEpoch,
			Peer:        request.Leader,
		},
		AdminRequest: split,
	}

	job := &pd_jobpd.Job{
		Request: req,
		Region:  request.Region,
	}

	return c.postJob(job)
}

func (c *raftCluster) handleSplitOK(split *raft_cmdpb.SplitResponse) error {
	left := split.Left
	right := split.Right

	// Update region
	leftSearchPath := makeRegionSearchKey(c.clusterRoot, left.GetEndKey())
	rightSearchPath := makeRegionSearchKey(c.clusterRoot, right.GetEndKey())

	leftValue, err := proto.Marshal(left)
	if err != nil {
		return errors.Trace(err)
	}

	rightValue, err := proto.Marshal(right)
	if err != nil {
		return errors.Trace(err)
	}

	var ops []clientv3.Op

	leftPath := makeRegionKey(c.clusterRoot, left.GetId())
	rightPath := makeRegionKey(c.clusterRoot, right.GetId())

	ops = append(ops, clientv3.OpPut(leftPath, encodeRegionSearchKey(left.GetEndKey())))
	ops = append(ops, clientv3.OpPut(rightPath, encodeRegionSearchKey(right.GetEndKey())))
	ops = append(ops, clientv3.OpPut(leftSearchPath, string(leftValue)))
	ops = append(ops, clientv3.OpPut(rightSearchPath, string(rightValue)))

	var cmps []clientv3.Cmp
	cmps = append(cmps, c.s.leaderCmp())
	// new left search path must not exist
	cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(leftSearchPath), "=", 0))
	// new right search path must exist, because it is the same as origin split path.
	cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(rightSearchPath), ">", 0))
	cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(rightPath), "=", 0))

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	resp, err := c.s.client.Txn(ctx).
		If(cmps...).
		Then(ops...).
		Commit()
	cancel()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		// The transaction may be retried, so certainly can't be OK, we should
		// check whether split regions in Etcd or not here.
		// Now we only use whether leftSearchPath exists to check, maybe we should
		// do more check later.
		v, err := getValue(c.s.client, leftSearchPath)
		if err != nil {
			return errors.Trace(err)
		}
		if v != nil {
			// We can find the left region with the new end key, so we can
			// think we have already executed this transaction successfully.
			return nil
		}

		return errors.New("update split region failed")
	}

	return nil
}

func (c *raftCluster) checkSplitOK(request *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.AdminResponse, error) {
	split := request.AdminRequest.Split
	leftRegionID := request.Header.GetRegionId()
	rightRegionID := split.GetNewRegionId()
	leader := request.Header.Peer

	leftDetail, err := c.getRegionDetail(leftRegionID, leader)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// If leader's version has changed, we can think SplitOK,
	// else we can think the raft server doesn't execute this split command.
	if leftDetail.Region.RegionEpoch.GetVersion() <= request.Header.RegionEpoch.GetVersion() {
		return nil, nil
	}

	rightDetail, err := c.getRegionDetail(rightRegionID, leader)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &raft_cmdpb.AdminResponse{
		CmdType: raft_cmdpb.AdminCmdType_Split.Enum(),
		Split: &raft_cmdpb.SplitResponse{
			Left:  leftDetail.Region,
			Right: rightDetail.Region,
		},
	}, nil
}

func (c *raftCluster) sendRaftCommand(request *raft_cmdpb.RaftCmdRequest, region *metapb.Region) (*raft_cmdpb.RaftCmdResponse, error) {
	originPeer := request.Header.Peer

RETRY:
	for i := 0; i < maxSendRetry; i++ {
		resp, err := c.callCommand(request)
		if err != nil {
			// We may meet some error, maybe network broken, node down, etc.
			// We can check later next time.
			return nil, errors.Trace(err)
		}

		if resp.Header.Error != nil && resp.Header.Error.NotLeader != nil {
			log.Warnf("peer %v is not leader, we got %v", request.Header.Peer, resp.Header.Error)

			leader := resp.Header.Error.NotLeader.Leader
			if leader != nil {
				// The origin peer is not leader and we get the new leader,
				// send this message to the new leader again.
				request.Header.Peer = leader
				continue
			}

			regionID := region.GetId()
			// The origin peer is not leader, but we can't get the leader now,
			// so we try to get the leader in other region peers.
			for _, peer := range region.Peers {
				if peer.GetId() == originPeer.GetId() {
					continue
				}

				leader, err := c.getRegionLeader(regionID, peer)
				if err != nil {
					log.Errorf("get region %d leader err %v", regionID, err)
					continue
				}
				if leader == nil {
					log.Infof("can not get leader for region %d in peer %v", regionID, peer)
					continue
				}

				// We get leader here.
				request.Header.Peer = leader
				continue RETRY
			}
		}

		return resp, nil
	}

	return nil, errors.Errorf("send raft command %v failed", request)
}

func (c *raftCluster) callCommand(request *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.RaftCmdResponse, error) {
	storeID := request.Header.Peer.GetStoreId()
	store, err := c.GetStore(storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	nc, err := c.storeConns.GetConn(store.GetAddress())
	if err != nil {
		return nil, errors.Trace(err)
	}

	msg := &msgpb.Message{
		MsgType: msgpb.MessageType_Cmd.Enum(),
		CmdReq:  request,
	}

	msgID := atomic.AddUint64(&c.s.msgID, 1)
	if err = util.WriteMessage(nc.conn, msgID, msg); err != nil {
		c.storeConns.RemoveConn(store.GetAddress())
		return nil, errors.Trace(err)
	}

	msg.Reset()
	if _, err = util.ReadMessage(nc.conn, msg); err != nil {
		c.storeConns.RemoveConn(store.GetAddress())
		return nil, errors.Trace(err)
	}

	if msg.CmdResp == nil {
		// This is a very serious bug, should we panic here?
		return nil, errors.Errorf("invalid command response message but %v", msg)
	}

	return msg.CmdResp, nil
}

func (c *raftCluster) getRegionLeader(regionID uint64, peer *metapb.Peer) (*metapb.Peer, error) {
	request := &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			Uuid:     uuid.NewV4().Bytes(),
			RegionId: proto.Uint64(regionID),
			Peer:     peer,
		},
		StatusRequest: &raft_cmdpb.StatusRequest{
			CmdType:      raft_cmdpb.StatusCmdType_RegionLeader.Enum(),
			RegionLeader: &raft_cmdpb.RegionLeaderRequest{},
		},
	}

	resp, err := c.callCommand(request)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if resp.StatusResponse != nil && resp.StatusResponse.RegionLeader != nil {
		return resp.StatusResponse.RegionLeader.Leader, nil
	}

	return nil, errors.Errorf("get region %d leader failed, got resp %v", regionID, resp)
}

func (c *raftCluster) getRegionDetail(regionID uint64, peer *metapb.Peer) (*raft_cmdpb.RegionDetailResponse, error) {
	request := &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			Uuid:     uuid.NewV4().Bytes(),
			RegionId: proto.Uint64(regionID),
			Peer:     peer,
		},
		StatusRequest: &raft_cmdpb.StatusRequest{
			CmdType:      raft_cmdpb.StatusCmdType_RegionDetail.Enum(),
			RegionDetail: &raft_cmdpb.RegionDetailRequest{},
		},
	}

	resp, err := c.callCommand(request)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if resp.StatusResponse != nil && resp.StatusResponse.RegionDetail != nil {
		return resp.StatusResponse.RegionDetail, nil
	}

	return nil, errors.Errorf("get region %d detail failed, got resp %v", regionID, resp)
}
