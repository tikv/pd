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
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"golang.org/x/net/context"
)

var _ = Suite(&testJoinServerSuite{})

var (
	errTimeout = errors.New("timeout")
)

type testJoinServerSuite struct {
	m    sync.Mutex
	dirs []string
}

func (s *testJoinServerSuite) TearDownSuite(c *C) {
	// wait a while, avoid `etcdserver: failed to purge snap file`
	time.Sleep(5 * time.Second)

	s.m.Lock()
	for _, d := range s.dirs {
		os.RemoveAll(d)
	}
	s.m.Unlock()
}

func newTestMultiJoinConfig(count int) []*Config {
	cfgs := NewTestMultiConfig(count)
	for i := 0; i < count; i++ {
		cfgs[i].InitialCluster = ""
		if i == 0 {
			continue
		}
		cfgs[i].Join = cfgs[i-1].ClientUrls
	}
	return cfgs
}

func waitMembers(svr *Server, c int) error {
	// maxRetryTime * waitInterval = 5s
	maxRetryCount := 10
	waitInterval := 500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), defaultDialTimeout)
	defer cancel()

	client := svr.GetClient()
	for ; maxRetryCount != 0; maxRetryCount-- {
		listResp, err := client.MemberList(ctx)
		if err != nil {
			continue
		}

		count := 0
		for _, memb := range listResp.Members {
			if len(memb.Name) == 0 {
				// unstarted, see:
				// https://github.com/coreos/etcd/blob/master/etcdctl/ctlv3/command/printer.go#L60
				// https://coreos.com/etcd/docs/latest/runtime-configuration.html#add-a-new-member
				continue
			}
			count++
		}

		if count >= c {
			return nil
		}

		time.Sleep(waitInterval)
	}
	return errors.New("waitMembers Timeout")
}

func waitLeader(svrs []*Server) error {
	// maxRetryTime * waitInterval = 10s
	maxRetryCount := 20
	waitInterval := 500 * time.Millisecond
	for count := 0; count < maxRetryCount; count++ {
		for _, s := range svrs {
			// TODO: a better way of finding leader.
			if s.etcd.Server.Leader() == s.etcd.Server.ID() {
				return nil
			}
		}
		time.Sleep(waitInterval)
	}
	return errTimeout
}

// notice: cfg has changed
func startPdWith(s *testJoinServerSuite, cfg *Config) (*Server, error) {
	svrCh := make(chan *Server)
	errCh := make(chan error)
	go func() {
		svr, err := CreateServer(cfg)
		if err != nil {
			errCh <- errors.Trace(err)
			return
		}
		err = svr.StartEtcd(nil)
		if err != nil {
			errCh <- errors.Trace(err)
			return
		}

		s.m.Lock()
		s.dirs = append(s.dirs, cfg.DataDir)
		s.m.Unlock()

		svrCh <- svr
		svr.Run()
	}()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	select {
	case s := <-svrCh:
		return s, nil
	case e := <-errCh:
		return nil, errors.Trace(e)
	case <-timer.C:
		return nil, errTimeout
	}
}

type cleanUpFunc func()

func mustNewJoinCluster(s *testJoinServerSuite, c *C, num int) ([]*Config, []*Server, cleanUpFunc) {
	svrs := make([]*Server, 0, num)
	cfgs := newTestMultiJoinConfig(num)

	for _, cfg := range cfgs {
		svr, err := startPdWith(s, cfg)
		c.Assert(err, IsNil)
		svrs = append(svrs, svr)
	}

	waitMembers(svrs[rand.Intn(num)], num)

	// clean up
	clean := func() {
		for _, s := range svrs {
			s.Close()
		}
	}

	return cfgs, svrs, clean
}

func alive(target, peer *Server) error {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	ch := make(chan error)
	go func() {
		// put something to cluster
		key := fmt.Sprintf("%d", rand.Int63())
		value := key
		client := peer.GetClient()
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		_, err := client.Put(ctx, key, value)
		if err != nil {
			ch <- errors.Trace(err)
			return
		}

		client = target.GetClient()
		ctx, cancel = context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		resp, err := client.Get(ctx, key)
		if err != nil {
			ch <- errors.Trace(err)
			return
		}
		if string(resp.Kvs[0].Value) != value {
			ch <- errors.Errorf("not match, got: %s, expect: %s", resp.Kvs[0].Value, value)
			return
		}
		ch <- nil
	}()

	select {
	case err := <-ch:
		return err
	case <-timer.C:
		return errTimeout
	}
}

// Case 1. a new pd joins to an existing cluster.
func (s *testJoinServerSuite) TestJoinCase1(c *C) {
	_, svrs, clean := mustNewJoinCluster(s, c, 3)
	defer clean()

	err := waitMembers(svrs[0], 3)
	c.Assert(err, IsNil)
}

// Case 2. a new pd joins itself
func (s *testJoinServerSuite) TestJoinCase2(c *C) {
	cfgs := newTestMultiJoinConfig(1)
	cfgs[0].Join = cfgs[0].AdvertiseClientUrls

	svr, err := startPdWith(s, cfgs[0])
	c.Assert(err, IsNil)
	defer svr.Close()

	err = waitMembers(svr, 1)
	c.Assert(err, IsNil)
}

// Case 3. an failed pd re-joins to previous cluster.
func (s *testJoinServerSuite) TestJoinCase3(c *C) {
	cfgs, svrs, clean := mustNewJoinCluster(s, c, 3)
	defer clean()

	target := 1
	svrs[target].Close()
	time.Sleep(500 * time.Millisecond)
	err := os.RemoveAll(cfgs[target].DataDir)
	c.Assert(err, IsNil)

	cfgs[target].InitialCluster = ""
	_, err = startPdWith(s, cfgs[target])
	c.Assert(err, NotNil)
}

// Case 4. a join self pd failed and it restarted with join while other peers
//         try to connect to it.
func (s *testJoinServerSuite) TestJoinCase4(c *C) {
	cfgs, svrs, clean := mustNewJoinCluster(s, c, 3)
	defer clean()

	err := alive(svrs[2], svrs[1])
	c.Assert(err, IsNil)

	target := 0
	svrs[target].Close()
	err = os.RemoveAll(cfgs[target].DataDir)
	c.Assert(err, IsNil)

	err = waitLeader([]*Server{svrs[2], svrs[1]})
	c.Assert(err, IsNil)

	// put some data
	err = alive(svrs[2], svrs[1])
	c.Assert(err, IsNil)

	cfgs[target].InitialCluster = ""
	cfgs[target].Join = cfgs[target].AdvertiseClientUrls
	_, err = startPdWith(s, cfgs[target])
	c.Assert(err, IsNil)

	err = alive(svrs[0], svrs[2])
	c.Assert(err, NotNil)

	err = alive(svrs[1], svrs[2])
	c.Assert(err, IsNil)
}

// Case 6. a failed pd tries to join to previous cluster but it has been deleted
//         during it's downtime.
func (s *testJoinServerSuite) TestJoinCase6(c *C) {
	cfgs, svrs, clean := mustNewJoinCluster(s, c, 3)
	defer clean()

	target := 2
	svrs[target].Close()
	time.Sleep(500 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), defaultDialTimeout)
	defer cancel()
	client := svrs[0].GetClient()
	client.MemberRemove(ctx, svrs[target].ID())

	cfgs[target].InitialCluster = ""
	_, err := startPdWith(s, cfgs[target])
	// deleted etcd will not start successfully.
	c.Assert(err, Equals, errTimeout)

	list, err := memberList(client)
	c.Assert(err, IsNil)
	c.Assert(len(list.Members), Equals, 2)
}

// Case 7. a deleted pd joins to previous cluster.
func (s *testJoinServerSuite) TestJoinCase7(c *C) {
	cfgs, svrs, clean := mustNewJoinCluster(s, c, 3)
	defer clean()

	target := 2
	ctx, cancel := context.WithTimeout(context.Background(), defaultDialTimeout)
	defer cancel()
	client := svrs[0].GetClient()
	client.MemberRemove(ctx, svrs[target].ID())

	svrs[target].Close()
	time.Sleep(500 * time.Millisecond)

	cfgs[target].InitialCluster = ""
	_, err := startPdWith(s, cfgs[target])
	// deleted etcd will not start successfully.
	c.Assert(err, Equals, errTimeout)

	list, err := memberList(client)
	c.Assert(err, IsNil)
	c.Assert(len(list.Members), Equals, 2)
}

// General join case.
func (s *testJoinServerSuite) TestReJoin(c *C) {
	cfgs, svrs, clean := mustNewJoinCluster(s, c, 3)
	defer clean()

	target := rand.Intn(len(cfgs))
	other := 0
	for {
		if other != target {
			break
		}
		other = rand.Intn(len(cfgs))
	}
	// put some data
	err := alive(svrs[target], svrs[other])
	c.Assert(err, IsNil)

	svrs[target].Close()
	time.Sleep(500 * time.Millisecond)

	cfgs[target].InitialCluster = ""
	re, err := startPdWith(s, cfgs[target])
	c.Assert(err, IsNil)

	svrs = append(svrs[:target], svrs[target+1:]...)
	svrs = append(svrs, re)
	err = waitLeader(svrs)
	c.Assert(err, IsNil)

	err = alive(re, svrs[0])
	c.Assert(err, IsNil)
}
