// Copyright 2021 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package global_config_test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/assertutil"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func Test(t *testing.T) {
	TestingT(t)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

var _ = Suite(&GlobalConfigTestSuite{})
var globalConfigPath = "/global/config/"

type GlobalConfigTestSuite struct {
	server  *server.GrpcServer
	client  *grpc.ClientConn
	cleanup server.CleanupFunc
}

type TestReceiver struct {
	c *C
	grpc.ServerStream
}

func (s TestReceiver) Send(m *pdpb.WatchGlobalConfigResponse) error {
	log.Info("received", zap.Any("received", m.GetChanges()))
	for _, change := range m.GetChanges() {
		s.c.Assert(change.Name, Equals, globalConfigPath+change.Value)
	}
	return nil
}

func (s *GlobalConfigTestSuite) SetUpSuite(c *C) {
	var err error
	var gsi *server.Server
	gsi, s.cleanup, err = server.NewTestServer(assertutil.NewChecker(func() {}))
	s.server = &server.GrpcServer{Server: gsi}
	c.Assert(err, IsNil)
	server.EnableZap = true
	addr := s.server.GetAddr()
	s.client, err = grpc.Dial(strings.TrimPrefix(addr, "http://"), grpc.WithInsecure())
	c.Assert(err, IsNil)
}

func (s *GlobalConfigTestSuite) TearDownSuite(c *C) {
	s.client.Close()
	s.cleanup()
}

func (s *GlobalConfigTestSuite) TestLoad(c *C) {
	defer func() {
		// clean up
		_, err := s.server.GetClient().Delete(s.server.Context(), globalConfigPath+"test")
		c.Assert(err, IsNil)
	}()
	_, err := s.server.GetClient().Put(s.server.Context(), globalConfigPath+"test", "test")
	c.Assert(err, IsNil)
	res, err := s.server.LoadGlobalConfig(s.server.Context(), &pdpb.LoadGlobalConfigRequest{Names: []string{"test"}})
	c.Assert(err, IsNil)
	c.Assert(len(res.Items), Equals, 1)
	c.Assert(res.Items[0].Value, Equals, "test")
}

func (s *GlobalConfigTestSuite) TestLoadError(c *C) {
	res, err := s.server.LoadGlobalConfig(s.server.Context(), &pdpb.LoadGlobalConfigRequest{Names: []string{"test"}})
	c.Assert(err, IsNil)
	c.Assert(res.Items[0].Error, Not(Equals), nil)
}

func (s *GlobalConfigTestSuite) TestStore(c *C) {
	defer func() {
		for i := 1; i <= 3; i++ {
			_, err := s.server.GetClient().Delete(s.server.Context(), globalConfigPath+strconv.Itoa(i))
			c.Assert(err, IsNil)
		}
	}()
	changes := []*pdpb.GlobalConfigItem{{Name: "1", Value: "1"}, {Name: "2", Value: "2"}, {Name: "3", Value: "3"}}
	_, err := s.server.StoreGlobalConfig(s.server.Context(), &pdpb.StoreGlobalConfigRequest{Changes: changes})
	c.Assert(err, IsNil)
	for i := 1; i <= 3; i++ {
		res, err := s.server.GetClient().Get(s.server.Context(), globalConfigPath+strconv.Itoa(i))
		c.Assert(err, IsNil)
		c.Assert(string(res.Kvs[0].Key), Equals, globalConfigPath+string(res.Kvs[0].Value))
	}
}

func (s *GlobalConfigTestSuite) TestWatch(c *C) {
	defer func() {
		for i := 0; i < 3; i++ {
			// clean up
			_, err := s.server.GetClient().Delete(s.server.Context(), globalConfigPath+strconv.Itoa(i))
			c.Assert(err, IsNil)
		}
	}()
	server := TestReceiver{c: c}
	go s.server.WatchGlobalConfig(&pdpb.WatchGlobalConfigRequest{}, server)
	for i := 0; i < 3; i++ {
		_, err := s.server.GetClient().Put(s.server.Context(), globalConfigPath+strconv.Itoa(i), strconv.Itoa(i))
		c.Assert(err, IsNil)
	}
}

func (s *GlobalConfigTestSuite) loadGlobalConfig(ctx context.Context, names []string) ([]*pdpb.GlobalConfigItem, error) {
	res, err := pdpb.NewPDClient(s.client).LoadGlobalConfig(ctx, &pdpb.LoadGlobalConfigRequest{Names: names})
	return res.GetItems(), err
}

func (s *GlobalConfigTestSuite) storeGlobalConfig(ctx context.Context, changes []*pdpb.GlobalConfigItem) error {
	_, err := pdpb.NewPDClient(s.client).StoreGlobalConfig(ctx, &pdpb.StoreGlobalConfigRequest{Changes: changes})
	return err
}

func (s *GlobalConfigTestSuite) watchGlobalConfig(ctx context.Context) (chan []*pdpb.GlobalConfigItem, error) {
	globalConfigWatcherCh := make(chan []*pdpb.GlobalConfigItem, 16)
	res, err := pdpb.NewPDClient(s.client).WatchGlobalConfig(ctx, &pdpb.WatchGlobalConfigRequest{})
	if err != nil {
		close(globalConfigWatcherCh)
		return nil, err
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				return
			}
		}()
		for {
			select {
			case <-ctx.Done():
				close(globalConfigWatcherCh)
				return
			default:
				m, err := res.Recv()
				if err != nil {
					return
				}
				arr := make([]*pdpb.GlobalConfigItem, len(m.Changes))
				for j, i := range m.Changes {
					arr[j] = &pdpb.GlobalConfigItem{Name: i.GetName(), Value: i.GetValue()}
				}
				globalConfigWatcherCh <- arr
			}
		}
	}()
	return globalConfigWatcherCh, err
}

func (s *GlobalConfigTestSuite) TestClientLoad(c *C) {
	defer func() {
		_, err := s.server.GetClient().Delete(s.server.Context(), globalConfigPath+"test")
		c.Assert(err, IsNil)
	}()
	_, err := s.server.GetClient().Put(s.server.Context(), globalConfigPath+"test", "test")
	c.Assert(err, IsNil)
	res, err := s.loadGlobalConfig(s.server.Context(), []string{"test"})
	c.Assert(err, IsNil)
	c.Assert(len(res), Equals, 1)
	c.Assert(res[0], DeepEquals, &pdpb.GlobalConfigItem{Name: "test", Value: "test", Error: nil})
}

func (s *GlobalConfigTestSuite) TestClientLoadError(c *C) {
	res, err := s.loadGlobalConfig(s.server.Context(), []string{"test"})
	c.Assert(err, IsNil)
	c.Assert(res[0].Error, Not(Equals), nil)
}

func (s *GlobalConfigTestSuite) TestClientStore(c *C) {
	defer func() {
		for i := 1; i <= 3; i++ {
			_, err := s.server.GetClient().Delete(s.server.Context(), globalConfigPath+strconv.Itoa(i))
			c.Assert(err, IsNil)
		}
	}()
	err := s.storeGlobalConfig(s.server.Context(), []*pdpb.GlobalConfigItem{{Name: "1", Value: "1"}, {Name: "2", Value: "2"}, {Name: "3", Value: "3"}})
	c.Assert(err, IsNil)
	for i := 1; i <= 3; i++ {
		res, err := s.server.GetClient().Get(s.server.Context(), globalConfigPath+strconv.Itoa(i))
		c.Assert(err, IsNil)
		c.Assert(string(res.Kvs[0].Key), Equals, globalConfigPath+string(res.Kvs[0].Value))
	}
}

func (s *GlobalConfigTestSuite) TestClientWatch(c *C) {
	defer func() {
		for i := 0; i < 3; i++ {
			_, err := s.server.GetClient().Delete(s.server.Context(), globalConfigPath+strconv.Itoa(i))
			c.Assert(err, IsNil)
		}
	}()
	wc, err := s.watchGlobalConfig(s.server.Context())
	c.Assert(err, IsNil)
	for i := 0; i < 3; i++ {
		_, err = s.server.GetClient().Put(s.server.Context(), globalConfigPath+strconv.Itoa(i), strconv.Itoa(i))
		c.Assert(err, IsNil)
	}
	for {
		select {
		case <-time.After(time.Second):
			return
		case res := <-wc:
			for _, r := range res {
				c.Assert(r.Name, Equals, globalConfigPath+r.Value)
			}
		}
	}
}

func (s *GlobalConfigTestSuite) TestClientWatchTimeout(c *C) {
	ctx, cancel := context.WithCancel(s.server.Context())
	wc, _ := s.watchGlobalConfig(ctx)
	cancel()
	_, opened := <-wc
	c.Assert(opened, Equals, false)
}
