// Copyright 2019 TiKV Project Authors.
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

package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tools/pd-backup/pdbackup"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestBackup(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	err = cluster.RunInitialServers()
	re.NoError(err)
	re.NotEmpty(cluster.WaitLeader())
	leaderServer := cluster.GetLeaderServer()
	leaderServer.BootstrapCluster()
	pdAddr := cluster.GetConfig().GetClientURL()
	urls := strings.Split(pdAddr, ",")
	defer cluster.Destroy()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   urls,
		DialTimeout: 3 * time.Second,
		TLS:         nil,
	})
	re.NoError(err)
	backupInfo, err := pdbackup.GetBackupInfo(client, pdAddr)
	re.NoError(err)
	re.NotNil(backupInfo)
	backBytes, err := json.Marshal(backupInfo)
	re.NoError(err)

	var formatBuffer bytes.Buffer
	err = json.Indent(&formatBuffer, backBytes, "", "    ")
	re.NoError(err)
	newInfo := &pdbackup.BackupInfo{}
	err = json.Unmarshal(formatBuffer.Bytes(), newInfo)
	re.NoError(err)
	re.Equal(newInfo, backupInfo)
}
