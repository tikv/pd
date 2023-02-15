package tso

import (
	"context"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

func TestMigrateGlobalTSO(t *testing.T) {
	re := require.New(t)

	cfg := etcdutil.NewTestSingleConfig(t)
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)
	defer etcd.Close()

	ep := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	re.NoError(err)
	rootPath := path.Join("/pd", strconv.FormatUint(100, 10))
	s := storage.NewStorageWithEtcdBackend(client, rootPath)

	oldGlobalPath := path.Join(rootPath, "timestamp")
	globalTS := uint64(time.Now().UnixNano())
	_, err = client.Put(context.Background(), oldGlobalPath, string(typeutil.Uint64ToBytes(globalTS)))
	re.NoError(err)
	oldLocalPath := path.Join(rootPath, "lts", "dc1", "timestamp")
	localTS := uint64(time.Now().UnixNano())
	_, err = client.Put(context.Background(), oldLocalPath, string(typeutil.Uint64ToBytes(localTS)))
	re.NoError(err)

	tso := &timestampOracle{
		client:                 client,
		rootPath:               rootPath,
		storage:                s,
		saveInterval:           3 * time.Second,
		updatePhysicalInterval: defaultTSOUpdatePhysicalInterval,
		dcLocation:             GlobalDCLocation,
		tsoMux:                 &tsoObject{},
	}

	err = tso.SyncTimestamp()
	re.NoError(err)

	newGlobalPath := path.Join(rootPath, "ms", "tso", "default", "gts", "timestamp")
	resp, err := client.Get(context.Background(), newGlobalPath)
	re.NoError(err)
	ts, err := typeutil.ParseTimestamp(resp.Kvs[0].Value)
	re.NoError(err)
	re.Greater(uint64(ts.UnixNano()), globalTS)

	resp, err = client.Get(context.Background(), oldGlobalPath)
	re.NoError(err)
	re.NotEmpty(resp.Kvs)

	newLocalPath := path.Join(rootPath, "ms", "tso", "default", "lts", "dc1", "timestamp")
	resp, err = client.Get(context.Background(), newLocalPath)
	re.NoError(err)
	ts1, err := typeutil.ParseTimestamp(resp.Kvs[0].Value)
	re.NoError(err)
	re.Equal(localTS, uint64(ts1.UnixNano()))

	resp, err = client.Get(context.Background(), oldLocalPath)
	re.NoError(err)
	re.NotEmpty(resp.Kvs)
}

func TestMigrateLocalTSO(t *testing.T) {
	re := require.New(t)

	cfg := etcdutil.NewTestSingleConfig(t)
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)
	defer etcd.Close()

	ep := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	re.NoError(err)
	rootPath := path.Join("/pd", strconv.FormatUint(100, 10))
	s := storage.NewStorageWithEtcdBackend(client, rootPath)

	oldGlobalPath := path.Join(rootPath, "timestamp")
	globalTS := uint64(time.Now().UnixNano())
	_, err = client.Put(context.Background(), oldGlobalPath, string(typeutil.Uint64ToBytes(globalTS)))
	re.NoError(err)
	oldLocalPath := path.Join(rootPath, "lts", "dc1", "timestamp")
	localTS := uint64(time.Now().UnixNano())
	_, err = client.Put(context.Background(), oldLocalPath, string(typeutil.Uint64ToBytes(localTS)))
	re.NoError(err)

	tso := &timestampOracle{
		client:                 client,
		rootPath:               rootPath,
		storage:                s,
		saveInterval:           3 * time.Second,
		updatePhysicalInterval: defaultTSOUpdatePhysicalInterval,
		dcLocation:             "dc1",
		tsoMux:                 &tsoObject{},
	}

	err = tso.SyncTimestamp()
	re.NoError(err)

	newGlobalPath := path.Join(rootPath, "ms", "tso", "default", "gts", "timestamp")
	resp, err := client.Get(context.Background(), newGlobalPath)
	re.NoError(err)
	ts, err := typeutil.ParseTimestamp(resp.Kvs[0].Value)
	re.NoError(err)
	re.Equal(globalTS, uint64(ts.UnixNano()))

	resp, err = client.Get(context.Background(), oldGlobalPath)
	re.NoError(err)
	re.NotEmpty(resp.Kvs)

	newLocalPath := path.Join(rootPath, "ms", "tso", "default", "lts", "dc1", "timestamp")
	resp, err = client.Get(context.Background(), newLocalPath)
	re.NoError(err)
	ts1, err := typeutil.ParseTimestamp(resp.Kvs[0].Value)
	re.NoError(err)
	re.Greater(uint64(ts1.UnixNano()), localTS)

	resp, err = client.Get(context.Background(), oldLocalPath)
	re.NoError(err)
	re.NotEmpty(resp.Kvs)
}
