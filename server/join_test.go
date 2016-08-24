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
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"golang.org/x/net/context"
)

var _ = Suite(&testJoinServerSuite{})

var (
	errTimeout = errors.New("timeout")
)

type testJoinServerSuite struct{}

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

// Notice: cfg has changed.
func startPdWith(cfg *Config) (*Server, error) {
	// wait must less than util.maxCheckEtcdRunningCount * util.checkEtcdRunningDelay
	wait := maxCheckEtcdRunningCount * checkEtcdRunningDelay / 2 // 5 seconds
	svrCh := make(chan *Server)
	errCh := make(chan error, 1)
	abortCh := make(chan struct{}, 1)

	go func() {
		// Etcd may panic, reports: "raft log corrupted, truncated, or lost?"
		defer func() {
			if err := recover(); err != nil {
				errCh <- errors.Errorf("%s", err)
			}
		}()

		svr, err := CreateServer(cfg)
		if err != nil {
			errCh <- errors.Trace(err)
			return
		}
		err = svr.StartEtcd(nil)
		if err != nil {
			errCh <- errors.Trace(err)
			svr.Close()
			return
		}

		select {
		case <-abortCh:
			svr.Close()
			return
		default:
		}

		svrCh <- svr
		svr.Run()
	}()

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case s := <-svrCh:
		return s, nil
	case e := <-errCh:
		return nil, errors.Trace(e)
	case <-timer.C:
		abortCh <- struct{}{}
		return nil, errTimeout
	}
}

type cleanUpFunc func()

func mustNewJoinCluster(c *C, num int) ([]*Config, []*Server, cleanUpFunc) {
	svrs := make([]*Server, 0, num)
	cfgs := newTestMultiJoinConfig(num)

	for i, cfg := range cfgs {
		svr, err := startPdWith(cfg)
		c.Assert(err, IsNil)
		svrs = append(svrs, svr)
		waitMembers(svrs[0], i+1)
	}

	// Clean up.
	clean := func() {
		for _, s := range svrs {
			s.Close()
		}
		for _, cfg := range cfgs {
			cleanConfig(cfg)
		}
	}

	return cfgs, svrs, clean
}

func isConnective(target, peer *Server) error {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	ch := make(chan error)
	go func() {
		// Put something to cluster.
		key := fmt.Sprintf("%d", rand.Int63())
		value := key
		client := peer.GetClient()
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		_, err := client.Put(ctx, key, value)
		cancel()
		if err != nil {
			ch <- errors.Trace(err)
			return
		}

		client = target.GetClient()
		ctx, cancel = context.WithTimeout(context.Background(), requestTimeout)
		resp, err := client.Get(ctx, key)
		cancel()
		if err != nil {
			ch <- errors.Trace(err)
			return
		}
		if len(resp.Kvs) == 0 {
			ch <- errors.Errorf("not match, got: %s, expect: %s", resp.Kvs, value)
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

// A new PD joins an existing cluster.
func (s *testJoinServerSuite) TestNewPDJoinsExistingCluster(c *C) {
	_, svrs, clean := mustNewJoinCluster(c, 3)
	defer clean()

	err := waitMembers(svrs[0], 3)
	c.Assert(err, IsNil)
}

// A new PD joins itself.
func (s *testJoinServerSuite) TestNewPDJoinsItself(c *C) {
	cfgs := newTestMultiJoinConfig(1)
	cfgs[0].Join = cfgs[0].AdvertiseClientUrls

	svr, err := startPdWith(cfgs[0])
	c.Assert(err, IsNil)

	defer func() {
		svr.Close()
		cleanConfig(cfgs[0])
	}()

	err = waitMembers(svr, 1)
	c.Assert(err, IsNil)
}

// A failed PD re-joins the previous cluster.
func (s *testJoinServerSuite) TestFailedPDJoinsPreviousCluster(c *C) {
	cfgs, svrs, clean := mustNewJoinCluster(c, 3)
	defer clean()

	target := 1
	svrs[target].Close()
	time.Sleep(500 * time.Millisecond)
	err := os.RemoveAll(cfgs[target].DataDir)
	c.Assert(err, IsNil)

	cfgs[target].InitialCluster = ""
	_, err = startPdWith(cfgs[target])
	c.Assert(err, NotNil)
}

// A PD starts with join itself and fails, it is restarted with the same
// arguments while other peers try to connect to it.
func (s *testJoinServerSuite) TestJoinSelfPDFiledAndRestarts(c *C) {
	cfgs, svrs, clean := mustNewJoinCluster(c, 3)
	defer clean()

	// Put some data.
	err := isConnective(svrs[2], svrs[1])
	c.Assert(err, IsNil)

	// Close join self PD and remove it's data.
	target := 0
	svrs[target].Close()
	err = os.RemoveAll(cfgs[target].DataDir)
	c.Assert(err, IsNil)

	mustWaitLeader(c, []*Server{svrs[2], svrs[1]})

	// Put some data.
	err = isConnective(svrs[2], svrs[1])
	c.Assert(err, IsNil)

	// Since the original cluster ID is computed by the target PD, so the
	// restarted PD ,with the same config, has the same cluster ID and
	// the same peer ID.
	// Here comes two situation:
	//  1. The restarted PD becomes leader before other peers reach it.
	//     so there are two leaders in two cluster(with same cluster ID),
	//     the leader of old cluster will send messages to the leader of the new
	//     cluster, but the new leader will reject them.
	//  2. Other peers reach the restarted PD before it becomes leader.
	//     The restarted PD joins and becomes a follower, but soon it will find
	//     it has lost data then panic.
	cfgs[target].InitialCluster = ""
	cfgs[target].InitialClusterState = "new"
	cfgs[target].Join = cfgs[target].AdvertiseClientUrls
	svr, _ := startPdWith(cfgs[target])
	if svr != nil {
		err = isConnective(svr, svrs[2])
		c.Assert(err, NotNil)
		svr.Close()
	}

	err = isConnective(svrs[1], svrs[2])
	c.Assert(err, IsNil)
}

// A failed PD tries to join the previous cluster but it has been deleted
// during its downtime.
func (s *testJoinServerSuite) TestFailedAndDeletedPDJoinsPreviousCluster(c *C) {
	cfgs, svrs, clean := mustNewJoinCluster(c, 3)
	defer clean()

	target := 2
	svrs[target].Close()
	time.Sleep(500 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), defaultDialTimeout)
	defer cancel()
	client := svrs[0].GetClient()
	client.MemberRemove(ctx, svrs[target].ID())

	cfgs[target].InitialCluster = ""
	_, err := startPdWith(cfgs[target])
	// Deleted PD will not start successfully.
	c.Assert(err, Equals, errTimeout)

	list, err := memberList(client)
	c.Assert(err, IsNil)
	c.Assert(len(list.Members), Equals, 2)
}

// A deleted PD joins the previous cluster.
func (s *testJoinServerSuite) TestDeletedPDJoinsPreviousCluster(c *C) {
	cfgs, svrs, clean := mustNewJoinCluster(c, 3)
	defer clean()

	target := 2
	ctx, cancel := context.WithTimeout(context.Background(), defaultDialTimeout)
	defer cancel()
	client := svrs[0].GetClient()
	client.MemberRemove(ctx, svrs[target].ID())

	svrs[target].Close()
	time.Sleep(500 * time.Millisecond)

	cfgs[target].InitialCluster = ""
	_, err := startPdWith(cfgs[target])
	// A deleted PD will not start successfully.
	c.Assert(err, Equals, errTimeout)

	list, err := memberList(client)
	c.Assert(err, IsNil)
	c.Assert(len(list.Members), Equals, 2)
}

// General join case.
func (s *testJoinServerSuite) TestGeneralJoin(c *C) {
	cfgs, svrs, clean := mustNewJoinCluster(c, 3)
	defer clean()

	target := rand.Intn(len(cfgs))
	other := 0
	for {
		if other != target {
			break
		}
		other = rand.Intn(len(cfgs))
	}
	// Put some data.
	err := isConnective(svrs[target], svrs[other])
	c.Assert(err, IsNil)

	svrs[target].Close()
	time.Sleep(500 * time.Millisecond)

	cfgs[target].InitialCluster = ""
	re, err := startPdWith(cfgs[target])
	c.Assert(err, IsNil)
	defer re.Close()

	svrs = append(svrs[:target], svrs[target+1:]...)
	svrs = append(svrs, re)
	mustWaitLeader(c, svrs)

	err = isConnective(re, svrs[0])
	c.Assert(err, IsNil)
}
