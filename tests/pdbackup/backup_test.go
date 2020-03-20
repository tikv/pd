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

package pdbackup

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/tests"
	"github.com/pingcap/pd/v4/tools/pd-backup/pdbackup"
	"go.etcd.io/etcd/clientv3"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&backupTestSuite{})

type backupTestSuite struct{}

func (s *backupTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *backupTestSuite) TestBackup(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURL()
	urls := strings.Split(pdAddr, ",")
	defer cluster.Destroy()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   urls,
		DialTimeout: 3 * time.Second,
		TLS:         nil,
	})
	c.Assert(err, IsNil)
	backupInfo, err := pdbackup.GetBackupInfo(client, pdAddr)
	c.Assert(err, IsNil)
	c.Assert(backupInfo, NotNil)
	backBytes, err := json.Marshal(backupInfo)
	c.Assert(err, IsNil)

	var formatBuffer bytes.Buffer
	err = json.Indent(&formatBuffer, []byte(backBytes), "", "    ")
	c.Assert(err, IsNil)
	newInfo := &pdbackup.BackupInfo{}
	err = json.Unmarshal(formatBuffer.Bytes(), newInfo)
	c.Assert(err, IsNil)
	c.Assert(backupInfo, DeepEquals, newInfo)
}
