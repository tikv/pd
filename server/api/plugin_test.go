// Copyright 2019 PingCAP, Inc.
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

package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
)

var _ = Suite(&testPluginSuite{})

type testPluginSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
	stores    []*metapb.Store
}

func (s *testPluginSuite) SetUpSuite(c *C) {
	s.stores = []*metapb.Store{
		{
			Id:      1,
			Address: "tikv1",
			State:   metapb.StoreState_Up,
			Version: "2.0.0",
		},
		{
			Id:      4,
			Address: "tikv4",
			State:   metapb.StoreState_Up,
			Version: "2.0.0",
		},
		{
			Id:      5,
			Address: "tikv5",
			State:   metapb.StoreState_Up,
			Version: "2.0.0",
		},
		{
			Id:      6,
			Address: "tikv6",
			State:   metapb.StoreState_Up,
			Version: "2.0.0",
		},
		{
			Id:      7,
			Address: "tikv7",
			State:   metapb.StoreState_Up,
			Version: "2.0.0",
		},
	}

	s.svr, s.cleanup = mustNewServer(c)
	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s/pd/api/v1/", addr)

	mustBootstrapCluster(c, s.svr)

	for _, store := range s.stores {
		mustPutStore(c, s.svr, store.Id, store.State, nil)
	}
}

func (s *testPluginSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testPluginSuite) TestPlugin(c *C) {
	r1 := newTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	r2 := newTestRegionInfo(3, 4, []byte("b"), []byte("c"))
	r3 := newTestRegionInfo(4, 5, []byte("c"), []byte("e"))
	r4 := newTestRegionInfo(5, 6, []byte("x"), []byte("z"))
	r5 := newTestRegionInfo(6, 7, []byte("h"), []byte("k"))

	mustRegionHeartbeat(c, s.svr, r1)
	mustRegionHeartbeat(c, s.svr, r2)
	mustRegionHeartbeat(c, s.svr, r3)
	mustRegionHeartbeat(c, s.svr, r4)
	mustRegionHeartbeat(c, s.svr, r5)

	data := map[string]interface{}{
		"plugin-path": "/home/dc/pd/plugin/scheduler_example/evict_leader/evictLeaderPlugin.so",
	}
	reqData, err := json.Marshal(data)
	c.Assert(err, IsNil)
	client := &http.Client{}
	url := s.urlPrefix + "plugin"
	status, _ := doRequest(c, client, url, http.MethodPost, bytes.NewBuffer(reqData))
	c.Assert(status, Equals, http.StatusOK)
}

func doRequest(c *C, client *http.Client, url, method string, data io.Reader) (int, []byte) {
	req, err := http.NewRequest(method, url, data)
	c.Assert(err, IsNil)
	resp, err := client.Do(req)
	c.Assert(err, IsNil)
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	err = resp.Body.Close()
	c.Assert(err, IsNil)

	return resp.StatusCode, body
}
