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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/config"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.uber.org/goleak"
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
	re := require.New(t)

	etcd, etcdClient, err := setupEtcd(t)
	re.NoError(err)

	server, serverConfig := setupServer()
	testSuite := &backupTestSuite{
		etcd:         etcd,
		etcdClient:   etcdClient,
		server:       server,
		serverConfig: serverConfig,
	}

	suite.Run(t, testSuite)
}

func setupEtcd(t *testing.T) (*embed.Etcd, *clientv3.Client, error) {
	etcdCfg := etcdutil.NewTestSingleConfig(t)
	etcd, err := embed.StartEtcd(etcdCfg)
	if err != nil {
		return nil, nil, err
	}

	ep := etcdCfg.ListenClientUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	if err != nil {
		return nil, nil, err
	}

	return etcd, client, nil
}

func setupServer() (*httptest.Server, *config.Config) {
	serverConfig := &config.Config{
		ClientUrls:          "example.com:2379",
		PeerUrls:            "example.com:20480",
		AdvertiseClientUrls: "example.com:2380",
		AdvertisePeerUrls:   "example.com:2380",
		Name:                "test-svc",
		DataDir:             "/data",
		ForceNewCluster:     true,
		EnableGRPCGateway:   true,
		InitialCluster:      "pd1=http://127.0.0.1:10208",
		InitialClusterState: "new",
		InitialClusterToken: "test-token",
		LeaderLease:         int64(1),
		Replication: config.ReplicationConfig{
			LocationLabels: typeutil.StringSlice{},
		},
		PDServerCfg: config.PDServerConfig{
			RuntimeServices: typeutil.StringSlice{},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
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

func (s *backupTestSuite) BeforeTest(suiteName, testName string) {
	// the etcd server is set up in TestBackupTestSuite() before the test suite
	// runs
	<-s.etcd.Server.ReadyNotify()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	_, err := s.etcdClient.Put(
		ctx,
		pdClusterIDPath,
		string(typeutil.Uint64ToBytes(clusterID)))
	s.NoError(err)

	var (
		rootPath               = path.Join(pdRootPath, strconv.FormatUint(clusterID, 10))
		timestampPath          = path.Join(rootPath, "timestamp")
		allocTimestampMaxBytes = typeutil.Uint64ToBytes(allocTimestampMax)
	)
	_, err = s.etcdClient.Put(ctx, timestampPath, string(allocTimestampMaxBytes))
	s.NoError(err)

	var (
		allocIDPath     = path.Join(rootPath, "alloc_id")
		allocIDMaxBytes = typeutil.Uint64ToBytes(allocIDMax)
	)
	_, err = s.etcdClient.Put(ctx, allocIDPath, string(allocIDMaxBytes))
	s.NoError(err)
}

func (s *backupTestSuite) AfterTest(suiteName, testName string) {
	s.etcd.Close()
}

func (s *backupTestSuite) TestGetBackupInfo() {
	actual, err := GetBackupInfo(s.etcdClient, s.server.URL)
	s.NoError(err)

	expected := &BackupInfo{
		ClusterID:         clusterID,
		AllocIDMax:        allocIDMax,
		AllocTimestampMax: allocTimestampMax,
		Config:            s.serverConfig,
	}
	s.Equal(expected, actual)

	tmpFile, err := os.CreateTemp(os.TempDir(), "pd_backup_info_test.json")
	s.NoError(err)
	defer os.Remove(tmpFile.Name())

	s.NoError(OutputToFile(actual, tmpFile))
	_, err = tmpFile.Seek(0, 0)
	s.NoError(err)

	b, err := io.ReadAll(tmpFile)
	s.NoError(err)

	var restored BackupInfo
	err = json.Unmarshal(b, &restored)
	s.NoError(err)

	s.Equal(expected, &restored)
}
