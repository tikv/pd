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
	"net"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/juju/errors"
	"github.com/ngaut/log"
)

const (
	etcdTimeout = time.Second * 3
)

// Server is the pd server.
type Server struct {
	cfgLock sync.RWMutex
	cfg     *Config

	etcd *embed.Etcd

	listener net.Listener

	client *clientv3.Client

	rootPath string

	isLeaderValue int64
	// leader value saved in etcd leader key.
	// Every write will use this to check leader validation.
	leaderValue string

	wg sync.WaitGroup

	connsLock sync.Mutex
	conns     map[*conn]struct{}

	closed int64

	// for tso
	ts            atomic.Value
	lastSavedTime time.Time

	// for id allocator, we can use one allocator for
	// store, region and peer, because we just need
	// a unique ID.
	idAlloc *idAllocator

	// for raft cluster
	clusterLock sync.RWMutex
	cluster     *RaftCluster

	msgID uint64

	id uint64
}

// NewServer creates the pd server with given configuration.
func NewServer(cfg *Config) (*Server, error) {
	cfg.adjust()

	log.Info("start etcd...")
	etcdCfg, err := cfg.genEmbedEtcdConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}

	etcd, err := embed.StartEtcd(etcdCfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	endpoints := []string{etcd.Clients[0].Addr().String()}

	log.Infof("create etcd v3 client with endpoints %v", endpoints)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: etcdTimeout,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err = waitEtcdStart(client, endpoints[0]); err != nil {
		return nil, errors.Trace(err)
	}

	log.Infof("listening address %s", cfg.Addr)
	l, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		client.Close()
		return nil, errors.Trace(err)
	}

	// If advertise addr not set, using default listening address.
	if len(cfg.AdvertiseAddr) == 0 {
		cfg.AdvertiseAddr = l.Addr().String()
	}

	s := &Server{
		cfg:           cfg,
		etcd:          etcd,
		listener:      l,
		client:        client,
		isLeaderValue: 0,
		conns:         make(map[*conn]struct{}),
		closed:        0,
		rootPath:      path.Join(cfg.RootPath, strconv.FormatUint(cfg.ClusterID, 10)),
		id:            uint64(etcd.Server.ID()),
	}

	s.idAlloc = &idAllocator{s: s}
	s.cluster = &RaftCluster{
		s:           s,
		running:     false,
		clusterID:   cfg.ClusterID,
		clusterRoot: s.getClusterRootPath(),
	}

	return s, nil
}

// Close closes the server.
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt64(&s.closed, 0, 1) {
		// server is already closed
		return
	}

	log.Info("closing server")

	s.enableLeader(false)

	if s.listener != nil {
		s.listener.Close()
	}

	if s.client != nil {
		s.client.Close()
	}

	if s.etcd != nil {
		s.etcd.Close()
	}

	s.wg.Wait()

	log.Info("close server")
}

// isClosed checks whether server is closed or not.
func (s *Server) isClosed() bool {
	return atomic.LoadInt64(&s.closed) == 1
}

// Run runs the pd server.
func (s *Server) Run() error {
	// We use "127.0.0.1:0" for test and will set correct listening
	// address before run, so we set leader value here.
	s.leaderValue = s.marshalLeader()

	s.wg.Add(1)
	go s.leaderLoop()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Errorf("accept err %s", err)
			break
		}

		c, err := newConn(s, conn)
		if err != nil {
			log.Warn(err)
			conn.Close()
			continue
		}

		s.wg.Add(1)
		go c.run()
	}

	return nil
}

// GetEndpoints returns the etcd endpoints for outer use.
func (s *Server) GetEndpoints() []string {
	return s.client.Endpoints()
}

// ID returns the unique etcd ID for this server in etcd cluster.
func (s *Server) ID() uint64 {
	return s.id
}

func (s *Server) closeAllConnections() {
	s.connsLock.Lock()
	defer s.connsLock.Unlock()

	if len(s.conns) == 0 {
		return
	}

	for conn := range s.conns {
		err := conn.close()
		if err != nil {
			log.Warnf("close conn failed - %v", err)
		}
	}

	s.conns = make(map[*conn]struct{})
}

// txn returns an etcd client transaction wrapper.
// The wrapper will set a request timeout to the context and log slow transactions.
func (s *Server) txn() clientv3.Txn {
	return newSlowLogTxn(s.client)
}

// leaderTxn returns txn() with a leader comparison to guarantee that
// the transaction can be executed only if the server is leader.
func (s *Server) leaderTxn(cs ...clientv3.Cmp) clientv3.Txn {
	return s.txn().If(append(cs, s.leaderCmp())...)
}
