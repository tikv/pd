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
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

const (
	campaignInterval = time.Millisecond * 100
)

// IsLeader returns whether this server is the leader or not.
func (s *Server) IsLeader() bool {
	return s.getLessor() != nil
}

// GetLeader returns the leader for the current election.
func (s *Server) GetLeader() (*pdpb.Leader, error) {
	return GetLeader(s.client, s.leaderPath())
}

func (s *Server) leaderPath() string {
	return path.Join(s.rootPath, "leader")
}

func (s *Server) leaderLoop() {
	defer s.wg.Done()

	leader := &pdpb.Leader{
		Addr: s.GetAddr(),
		Pid:  int64(os.Getpid()),
	}

	for !s.IsClosed() {
		select {
		case <-s.client.Ctx().Done():
			return
		case <-time.After(campaignInterval):
		}

		lessor, err := NewLessor(s.client, int(s.cfg.LeaderLease), s.leaderPath())
		if err != nil {
			log.Errorf("failed to create lessor: %v", err)
			continue
		}

		err = lessor.Campaign(leader)
		if err != nil {
			log.Errorf("failed to campaign leader: %v", err)
			continue
		}

		log.Infof("campaign leader ok: %v", s.Name())
		s.leaderRound(lessor)
	}
}

func (s *Server) leaderRound(lessor *Lessor) {
	s.becomeLeader(lessor)
	defer s.resignLeader()

	if err := s.createRaftCluster(); err != nil {
		log.Error(err)
		return
	}

	if err := s.syncTimestamp(); err != nil {
		log.Error(err)
		return
	}

	ticker := time.NewTicker(updateTimestampStep)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.updateTimestamp(); err != nil {
				log.Error(err)
				return
			}
		case <-lessor.Done():
			return
		}
	}
}

func (s *Server) becomeLeader(lessor *Lessor) {
	s.lessor.Store(lessor)
	s.closeAllConnections()
}

func (s *Server) resignLeader() {
	lessor := s.getLessor()
	if lessor != nil {
		lessor.Close()
	}
	s.lessor.Store((*Lessor)(nil))
	s.closeAllConnections()
	s.cluster.stop()
}

func (s *Server) getLessor() *Lessor {
	lessor, _ := s.lessor.Load().(*Lessor)
	return lessor
}

// txn returns an etcd transaction wrapper. It guarantees that the
// transaction will be executed only when this server is the leader.
func (s *Server) txn() clientv3.Txn {
	lessor := s.getLessor()
	if lessor != nil {
		return lessor.Txn()
	}
	return newNotLeaderTxn()
}
