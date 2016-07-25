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
	"os"
	"path"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"golang.org/x/net/context"
)

const checkEtcdLeaderInterval = time.Second

// isLeader returns whether server is leader or not.
func (s *Server) isLeader() bool {
	return atomic.LoadInt64(&s.isLeaderValue) == 1
}

func (s *Server) enableLeader(b bool) {
	value := int64(0)
	if b {
		value = 1
	}

	atomic.StoreInt64(&s.isLeaderValue, value)

	if !b {
		// if we lost leader, we may:
		//	1, close all client connections
		//	2, close all running raft clusters
		s.closeAllConnections()

		s.cluster.stop()
	}
}

func (s *Server) getLeaderPath() string {
	return path.Join(s.rootPath, "leader")
}

func (s *Server) leaderLoop() {
	defer s.wg.Done()

	for {
		if s.isClosed() {
			log.Infof("server is closed, return leader loop")
			return
		}

		leader, err := s.getLeader()
		if err != nil {
			log.Errorf("get leader err %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if leader != nil {
			if s.isSameLeader(leader) {
				// oh, we are already leader, we may meet something wrong
				// in previous campaignLeader. we can resign and campaign again.
				log.Warnf("leader is still %s, resign and campaign again", leader)
				if err = s.resignLeader(); err != nil {
					log.Errorf("resign leader err %s", err)
					time.Sleep(200 * time.Millisecond)
					continue
				}
			} else {
				log.Infof("leader is %s, watch it", leader)
				s.watchLeader()
				log.Info("leader changed, try to campaign leader")
			}
		}

		if !s.isEtcdLeader() {
			// we should put pd leader and etcd leader together
			log.Infof("pd's etcd %s is not leader, leader is %s", s.etcd.Server.ID(), s.etcd.Server.Leader())
			time.Sleep(checkEtcdLeaderInterval)
			continue
		}

		if err = s.campaignLeader(); err != nil {
			log.Errorf("campaign leader err %s", errors.ErrorStack(err))
		}
	}
}

// getLeader gets server leader from etcd.
func getLeader(c *clientv3.Client, leaderPath string) (*pdpb.Leader, error) {
	leader := &pdpb.Leader{}
	ok, err := getProtoMsg(c, leaderPath, leader)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !ok {
		return nil, nil
	}

	return leader, nil
}

func (s *Server) getLeader() (*pdpb.Leader, error) {
	return getLeader(s.client, s.getLeaderPath())
}

func (s *Server) isSameLeader(leader *pdpb.Leader) bool {
	return leader.GetAddr() == s.cfg.AdvertiseAddr && leader.GetPid() == int64(os.Getpid())
}

func (s *Server) marshalLeader() string {
	leader := &pdpb.Leader{
		Addr: proto.String(s.cfg.AdvertiseAddr),
		Pid:  proto.Int64(int64(os.Getpid())),
	}

	data, err := proto.Marshal(leader)
	if err != nil {
		// can't fail, so panic here.
		log.Fatalf("marshal leader %s err %v", leader, err)
	}

	return string(data)
}

func (s *Server) isEtcdLeader() bool {
	return s.etcd.Server.ID() == s.etcd.Server.Leader()
}

func (s *Server) campaignLeader() error {
	log.Debugf("begin to campaign leader %s", s.cfg.AdvertiseAddr)

	lessor := clientv3.NewLease(s.client)
	defer lessor.Close()

	start := time.Now()
	ctx, cancel := context.WithTimeout(s.client.Ctx(), requestTimeout)
	leaseResp, err := lessor.Grant(ctx, s.cfg.LeaderLease)
	cancel()

	if cost := time.Now().Sub(start); cost > slowRequestTime {
		log.Warnf("lessor grants too slow, cost %s", cost)
	}

	if err != nil {
		return errors.Trace(err)
	}

	leaderKey := s.getLeaderPath()
	// The leader key must not exist, so the CreateRevision is 0.
	resp, err := s.txn().
		If(clientv3.Compare(clientv3.CreateRevision(leaderKey), "=", 0)).
		Then(clientv3.OpPut(leaderKey, s.leaderValue, clientv3.WithLease(clientv3.LeaseID(leaseResp.ID)))).
		Commit()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.New("campaign leader failed, other server may campaign ok")
	}

	log.Debugf("campaign leader ok %s", s.cfg.AdvertiseAddr)
	s.enableLeader(true)
	defer s.enableLeader(false)

	// Try to create raft cluster.
	err = s.createRaftCluster()
	if err != nil {
		return errors.Trace(err)
	}

	// Make the leader keepalived.
	ch, err := lessor.KeepAlive(s.client.Ctx(), clientv3.LeaseID(leaseResp.ID))
	if err != nil {
		return errors.Trace(err)
	}

	log.Debug("sync timestamp for tso")
	if err = s.syncTimestamp(); err != nil {
		return errors.Trace(err)
	}

	tsTicker := time.NewTicker(time.Duration(updateTimestampStep) * time.Millisecond)
	defer tsTicker.Stop()

	leaderTicker := time.NewTicker(checkEtcdLeaderInterval)
	defer leaderTicker.Stop()

	for {
		select {
		case _, ok := <-ch:
			if !ok {
				log.Info("keep alive channel is closed")
				return nil
			}
		case <-tsTicker.C:
			if err = s.updateTimestamp(); err != nil {
				return errors.Trace(err)
			}
		case <-leaderTicker.C:
			if !s.isEtcdLeader() {
				return errors.New("current etcd member is not leader")
			}
		case <-s.client.Ctx().Done():
			return errors.New("server closed")
		}
	}
}

func (s *Server) watchLeader() {
	watcher := clientv3.NewWatcher(s.client)
	defer watcher.Close()

	ctx := s.client.Ctx()
	for {
		rch := watcher.Watch(ctx, s.getLeaderPath())
		for wresp := range rch {
			if wresp.Canceled {
				return
			}

			for _, ev := range wresp.Events {
				if ev.Type == mvccpb.DELETE {
					log.Info("leader is deleted")
					return
				}
			}
		}

		select {
		case <-ctx.Done():
			// server closed, return
			return
		default:
		}
	}
}

func (s *Server) resignLeader() error {
	// delete leader itself and let others start a new election again.
	leaderKey := s.getLeaderPath()
	resp, err := s.leaderTxn().Then(clientv3.OpDelete(leaderKey)).Commit()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.New("resign leader failed, we are not leader already")
	}

	return nil
}

func (s *Server) leaderCmp() clientv3.Cmp {
	return clientv3.Compare(clientv3.Value(s.getLeaderPath()), "=", s.leaderValue)
}
