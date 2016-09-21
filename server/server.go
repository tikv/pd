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
	"net/http"
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
	// pdRootPath for all pd servers.
	pdRootPath  = "/pd"
	pdAPIPrefix = "/pd/"
	pdRPCPrefix = "/pd/rpc"
)

// Server is the pd server.
type Server struct {
	cfg *Config

	etcd *embed.Etcd

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
	lastSavedTime atomic.Value

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
	s, err := CreateServer(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return s, s.StartEtcd(nil)
}

// CreateServer creates the UNINITIALIZED pd server with given configuration.
func CreateServer(cfg *Config) (*Server, error) {
	if err := cfg.adjust(); err != nil {
		return nil, errors.Trace(err)
	}

	log.Infof("PD config - %v", cfg)

	s := &Server{
		cfg:           cfg,
		isLeaderValue: 0,
		conns:         make(map[*conn]struct{}),
		closed:        1,
		rootPath:      path.Join(pdRootPath, strconv.FormatUint(cfg.ClusterID, 10)),
	}

	s.idAlloc = &idAllocator{s: s}
	s.cluster = newRaftCluster(s, cfg.ClusterID)

	return s, nil
}

// StartEtcd starts an embed etcd server with an user handler.
func (s *Server) StartEtcd(apiHandler http.Handler) error {
	etcdCfg, err := s.cfg.genEmbedEtcdConfig()
	if err != nil {
		return errors.Trace(err)
	}
	etcdCfg.UserHandlers = map[string]http.Handler{
		pdRPCPrefix: s,
	}
	if apiHandler != nil {
		etcdCfg.UserHandlers[pdAPIPrefix] = apiHandler
	}

	log.Info("start embed etcd")

	etcd, err := embed.StartEtcd(etcdCfg)
	if err != nil {
		return errors.Trace(err)
	}

	endpoints := []string{etcdCfg.LCUrls[0].String()}

	log.Infof("create etcd v3 client with endpoints %v", endpoints)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: etcdTimeout,
	})
	if err != nil {
		return errors.Trace(err)
	}

	if err = waitEtcdStart(client, endpoints[0]); err != nil {
		// See https://github.com/coreos/etcd/issues/6067
		// Here may return "not capable" error because we don't start
		// all etcds in initial_cluster at same time, so here just log
		// an error.
		// Note that pd can not work correctly if we don't start all etcds.
		log.Errorf("etcd start failed, err %v", err)
	}

	s.etcd = etcd
	s.client = client
	s.id = uint64(etcd.Server.ID())

	// Server has started.
	atomic.StoreInt64(&s.closed, 0)
	return nil
}

// Close closes the server.
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt64(&s.closed, 0, 1) {
		// server is already closed
		return
	}

	log.Info("closing server")

	s.enableLeader(false)

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
func (s *Server) Run() {
	// We use "127.0.0.1:0" for test and will set correct listening
	// address before run, so we set leader value here.
	s.leaderValue = s.marshalLeader()

	s.wg.Add(1)
	s.leaderLoop()
}

// ServeHTTP hijack the HTTP connection and switch to RPC.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		log.Errorf("server doesn't support hijacking: conn %v", w)
		return
	}

	conn, bufrw, err := hj.Hijack()
	if err != nil {
		log.Error(err)
		return
	}

	err = conn.SetDeadline(time.Time{})
	if err != nil {
		log.Error(err)
		conn.Close()
		return
	}

	c, err := newConn(s, conn, bufrw)
	if err != nil {
		log.Error(err)
		conn.Close()
		return
	}

	s.wg.Add(1)
	go c.run()
}

// GetAddr returns the server urls for clients.
func (s *Server) GetAddr() string {
	return s.cfg.AdvertiseClientUrls
}

// GetEndpoints returns the etcd endpoints for outer use.
func (s *Server) GetEndpoints() []string {
	return s.client.Endpoints()
}

// GetClient returns builtin etcd client.
func (s *Server) GetClient() *clientv3.Client {
	return s.client
}

// ID returns the unique etcd ID for this server in etcd cluster.
func (s *Server) ID() uint64 {
	return s.id
}

// Name returns the unique etcd Name for this server in etcd cluster.
func (s *Server) Name() string {
	return s.cfg.Name
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
