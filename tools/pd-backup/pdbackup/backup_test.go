// Copyright 2022 TiKV Project Authors.
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

package pdbackup

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/goleak"

	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/config"
)

var (
	clusterID         = (uint64(1257894000) << 32) + uint64(rand.Uint32())
	allocIDMax        = uint64(100000000)
	allocTimestampMax = uint64(1257894000)
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type backupTestSuite struct {
	suite.Suite

	etcd       *embed.Etcd
	etcdClient *clientv3.Client

	server       *httptest.Server
	serverConfig *config.Config
}

func TestBackupTestSuite(t *testing.T) {
	servers, etcdClient, clean := etcdutil.NewTestEtcdCluster(t, 1)
	defer clean()

	server, serverConfig := setupServer()
	testSuite := &backupTestSuite{
		etcd:         servers[0],
		etcdClient:   etcdClient,
		server:       server,
		serverConfig: serverConfig,
	}

	suite.Run(t, testSuite)
}

func setupServer() (*httptest.Server, *config.Config) {
	serverConfig := &config.Config{
		ClientUrls:          "example.com:2379",
		PeerUrls:            "example.com:20480",
		AdvertiseClientUrls: "example.com:2380",
		AdvertisePeerUrls:   "example.com:2380",
		Name:                "test-svc",
		DataDir:             string(filepath.Separator) + "data",
		ForceNewCluster:     true,
		EnableGRPCGateway:   true,
		InitialCluster:      "pd1=http://127.0.0.1:10208",
		InitialClusterState: "new",
		InitialClusterToken: "test-token",
		LeaderLease:         int64(1),
		Replication: sc.ReplicationConfig{
			LocationLabels: typeutil.StringSlice{},
		},
		PDServerCfg: config.PDServerConfig{
			RuntimeServices: typeutil.StringSlice{},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, _ *http.Request) {
		b, err := json.Marshal(serverConfig)
		if err != nil {
			res.WriteHeader(http.StatusInternalServerError)
			res.Write([]byte(fmt.Sprintf("failed setting up test server: %s", err)))
			return
		}

		res.WriteHeader(http.StatusOK)
		res.Write(b)
	}))

	return server, serverConfig
}

func (s *backupTestSuite) BeforeTest(string, string) {
	re := s.Require()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	_, err := s.etcdClient.Put(
		ctx,
		pdClusterIDPath,
		string(typeutil.Uint64ToBytes(clusterID)))
	re.NoError(err)

	var (
		rootPath               = path.Join(pdRootPath, strconv.FormatUint(clusterID, 10))
		allocTimestampMaxBytes = typeutil.Uint64ToBytes(allocTimestampMax)
	)
	_, err = s.etcdClient.Put(ctx, keypath.TimestampPath(rootPath), string(allocTimestampMaxBytes))
	re.NoError(err)

	var (
		allocIDPath     = path.Join(rootPath, "alloc_id")
		allocIDMaxBytes = typeutil.Uint64ToBytes(allocIDMax)
	)
	_, err = s.etcdClient.Put(ctx, allocIDPath, string(allocIDMaxBytes))
	re.NoError(err)
}

func (s *backupTestSuite) AfterTest(string, string) {
	s.etcd.Close()
}

func (s *backupTestSuite) TestGetBackupInfo() {
	re := s.Require()
	actual, err := GetBackupInfo(s.etcdClient, s.server.URL)
	re.NoError(err)

	expected := &BackupInfo{
		ClusterID:         clusterID,
		AllocIDMax:        allocIDMax,
		AllocTimestampMax: allocTimestampMax,
		Config:            s.serverConfig,
	}
	re.Equal(expected, actual)

	tmpFile, err := os.CreateTemp("", "pd_tests")
	re.NoError(err)
	defer os.RemoveAll(tmpFile.Name())

	re.NoError(OutputToFile(actual, tmpFile))
	_, err = tmpFile.Seek(0, 0)
	re.NoError(err)

	b, err := io.ReadAll(tmpFile)
	re.NoError(err)

	var restored BackupInfo
	err = json.Unmarshal(b, &restored)
	re.NoError(err)

	re.Equal(expected, &restored)
}
