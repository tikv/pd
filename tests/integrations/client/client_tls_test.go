// Copyright 2020 TiKV Project Authors.
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

package client_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"google.golang.org/grpc"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	"github.com/pingcap/kvproto/pkg/pdpb"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/netutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

var certScript = strings.Join([]string{".", "cert_opt.sh"}, string(filepath.Separator))

func newTLSWatchTestServer(t *testing.T) *server.Server {
	t.Helper()
	re := require.New(t)
	certPath := t.TempDir()
	generateCerts(re, certPath)
	tlsInfo := buildTLSInfo(certPath, "pd-server")
	cluster, err := tests.NewTestCluster(t.Context(), 1, func(conf *config.Config, _ string) {
		conf.Security.TLSConfig = grpcutil.TLSConfig{KeyPath: tlsInfo.KeyFile, CertPath: tlsInfo.CertFile, CAPath: tlsInfo.TrustedCAFile}
		conf.AdvertiseClientUrls = strings.ReplaceAll(conf.AdvertiseClientUrls, "http", "https")
		conf.ClientUrls = strings.ReplaceAll(conf.ClientUrls, "http", "https")
		conf.AdvertisePeerUrls = strings.ReplaceAll(conf.AdvertisePeerUrls, "http", "https")
		conf.PeerUrls = strings.ReplaceAll(conf.PeerUrls, "http", "https")
		conf.InitialCluster = strings.ReplaceAll(conf.InitialCluster, "http", "https")
	})
	re.NoError(err)
	t.Cleanup(cluster.Destroy)
	re.NoError(cluster.RunInitialServers())
	name := cluster.WaitLeader()
	re.NotEmpty(name)
	re.NoError(cluster.GetServer(name).BootstrapCluster())
	return cluster.GetServer(name).GetServer()
}

type watchResponseWriter struct {
	http.ResponseWriter
	started  chan struct{}
	finished chan struct{}
	once     *sync.Once
}

func (w *watchResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *watchResponseWriter) Flush() { w.ResponseWriter.(http.Flusher).Flush() }

func (w *watchResponseWriter) Write(data []byte) (int, error) {
	observed := false
	if len(data) > 64<<10 {
		w.once.Do(func() {
			observed = true
			close(w.started)
		})
	}
	if observed {
		defer close(w.finished)
	}
	return w.ResponseWriter.Write(data)
}

func TestTLSWatchServerClose(t *testing.T) {
	for _, method := range []string{"idle", "/keyspacepb.Keyspace/WatchKeyspaces", "/pdpb.PD/WatchGlobalConfig", "/meta_storagepb.MetaStorage/Watch"} {
		t.Run(method, func(t *testing.T) {
			re := require.New(t)
			started, finished := make(chan struct{}), make(chan struct{})
			var once sync.Once
			const point = "github.com/tikv/pd/pkg/utils/grpcutil/watchHTTPResponseWriter"
			re.NoError(failpoint.EnableCall(point, func(path string, writer *http.ResponseWriter) {
				if path == method {
					*writer = &watchResponseWriter{ResponseWriter: *writer, started: started, finished: finished, once: &once}
				}
			}))
			t.Cleanup(func() { re.NoError(failpoint.Disable(point)) })
			srv := newTLSWatchTestServer(t)
			tlsConfig, err := srv.GetConfig().Security.ToClientTLSConfig()
			re.NoError(err)
			// Fixed flow-control windows keep large responses blocked until read.
			conn, err := grpcutil.GetClientConn(t.Context(), srv.GetAddr(), tlsConfig,
				grpc.WithStaticStreamWindowSize(64<<10), grpc.WithStaticConnWindowSize(1<<20))
			re.NoError(err)
			defer conn.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			value := "value"
			if method != "idle" {
				value = strings.Repeat("x", 1<<20)
			}
			key := globalConfigPath + "tls-shutdown"
			put, err := srv.GetClient().Put(ctx, key, value)
			re.NoError(err)
			meta := &keyspacepb.KeyspaceMeta{
				Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 42}, Name: "tls-shutdown", State: keyspacepb.KeyspaceState_ENABLED,
				Config: map[string]string{"watch-test-payload": value},
			}
			data, err := proto.Marshal(meta)
			re.NoError(err)
			_, err = srv.GetClient().Put(ctx, keypath.KeyspaceMetaPath(meta.GetId()), string(data))
			re.NoError(err)
			keyspaces, err := keyspacepb.NewKeyspaceClient(conn).WatchKeyspaces(ctx, &keyspacepb.WatchKeyspacesRequest{Header: testutil.NewRequestHeader(keypath.ClusterID())})
			re.NoError(err)
			configs, err := pdpb.NewPDClient(conn).WatchGlobalConfig(ctx, &pdpb.WatchGlobalConfigRequest{ConfigPath: globalConfigPath, Revision: put.Header.Revision})
			re.NoError(err)
			metas, err := meta_storagepb.NewMetaStorageClient(conn).Watch(ctx, &meta_storagepb.WatchRequest{Key: []byte(key), StartRevision: put.Header.Revision})
			re.NoError(err)
			streams := []struct {
				method  string
				stream  grpc.ClientStream
				message proto.Message
			}{
				{"/keyspacepb.Keyspace/WatchKeyspaces", keyspaces, &keyspacepb.WatchKeyspacesResponse{}},
				{"/pdpb.PD/WatchGlobalConfig", configs, &pdpb.WatchGlobalConfigResponse{}},
				{"/meta_storagepb.MetaStorage/Watch", metas, &meta_storagepb.WatchResponse{}},
			}
			if method == "idle" {
				for _, stream := range streams {
					re.NoError(stream.stream.RecvMsg(stream.message))
				}
				re.NotEmpty(streams[0].message.(*keyspacepb.WatchKeyspacesResponse).Keyspaces)
				configResponse := streams[1].message.(*pdpb.WatchGlobalConfigResponse)
				re.GreaterOrEqual(configResponse.Revision, put.Header.Revision)
				re.Len(configResponse.Changes, 1)
				re.Equal([]byte(value), configResponse.Changes[0].Payload)
				metaResponse := streams[2].message.(*meta_storagepb.WatchResponse)
				re.Len(metaResponse.Events, 1)
				re.Equal([]byte(value), metaResponse.Events[0].Kv.Value)
			} else {
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("TLS watch did not enter the large response write")
				}
				select {
				case <-finished:
					t.Fatal("large response write completed without client reads")
				case <-time.After(100 * time.Millisecond):
				}
			}
			closed := make(chan struct{})
			go func() {
				srv.Close()
				close(closed)
			}()
			select {
			case <-closed:
			case <-time.After(10 * time.Second):
				cancel()
				select {
				case <-closed:
				case <-time.After(5 * time.Second):
					t.Fatal("Server.Close did not finish after client cancellation")
				}
				t.Fatal("live TLS watch streams blocked Server.Close")
			}
			re.NoError(ctx.Err())
			if method != "idle" {
				select {
				case <-finished:
				default:
					t.Fatal("TLS response writer survived Server.Close")
				}
			}
			for _, stream := range streams {
				if method == stream.method {
					re.Error(stream.stream.RecvMsg(stream.message))
					continue
				}
				for err = nil; err == nil; {
					err = stream.stream.RecvMsg(stream.message)
				}
				re.Error(err)
			}
			re.NoError(ctx.Err())
		})
	}
}

// TestTLSReloadAtomicReplace ensures server reloads expired/valid certs
// when all certs are atomically replaced by directory renaming.
// And expects server to reject client requests, and vice versa.
func TestTLSReloadAtomicReplace(t *testing.T) {
	re := require.New(t)

	certPath := t.TempDir()
	certExpiredPath := t.TempDir()
	generateCerts(re, certPath)
	generateCerts(re, certExpiredPath)
	testTLSInfo := buildTLSInfo(certPath, "pd-server")
	testTLSInfoExpired := buildTLSInfo(certExpiredPath, "pd-server")
	testClientTLSInfo := buildTLSInfo(certPath, "client")

	ctx := t.Context()
	tmpDir := t.TempDir()
	re.NoError(os.RemoveAll(tmpDir))

	certsDir := t.TempDir()

	certsDirExp := t.TempDir()

	cloneFunc := func() transport.TLSInfo {
		tlsInfo, terr := copyTLSFiles(testTLSInfo, certsDir)
		re.NoError(terr)
		_, err := copyTLSFiles(testTLSInfoExpired, certsDirExp)
		re.NoError(err)
		return tlsInfo
	}
	replaceFunc := func() {
		err := os.Rename(certsDir, tmpDir)
		re.NoError(err)
		err = os.Rename(certsDirExp, certsDir)
		re.NoError(err)
		// after rename,
		// 'certsDir' contains expired certs
		// 'tmpDir' contains valid certs
		// 'certsDirExp' does not exist
	}
	revertFunc := func() {
		err := os.Rename(tmpDir, certsDirExp)
		re.NoError(err)

		err = os.Rename(certsDir, tmpDir)
		re.NoError(err)

		err = os.Rename(certsDirExp, certsDir)
		re.NoError(err)
	}
	testTLSReload(ctx, re, testClientTLSInfo, cloneFunc, replaceFunc, revertFunc)
}

func buildTLSInfo(path, name string) transport.TLSInfo {
	return transport.TLSInfo{
		KeyFile:       strings.Join([]string{path, name + "-key.pem"}, string(filepath.Separator)),
		CertFile:      strings.Join([]string{path, name + ".pem"}, string(filepath.Separator)),
		TrustedCAFile: strings.Join([]string{path, "ca.pem"}, string(filepath.Separator)),
	}
}

func generateCerts(re *require.Assertions, path string) {
	err := exec.Command(certScript, "generate", path).Run()
	re.NoError(err)
}

func testTLSReload(
	ctx context.Context,
	re *require.Assertions,
	testClientTLSInfo transport.TLSInfo,
	cloneFunc func() transport.TLSInfo,
	replaceFunc func(),
	revertFunc func()) {
	tlsInfo := cloneFunc()
	// 1. start cluster with valid certs
	clus, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.Security.TLSConfig = grpcutil.TLSConfig{
			KeyPath:  tlsInfo.KeyFile,
			CertPath: tlsInfo.CertFile,
			CAPath:   tlsInfo.TrustedCAFile,
		}
		conf.AdvertiseClientUrls = strings.ReplaceAll(conf.AdvertiseClientUrls, "http", "https")
		conf.ClientUrls = strings.ReplaceAll(conf.ClientUrls, "http", "https")
		conf.AdvertisePeerUrls = strings.ReplaceAll(conf.AdvertisePeerUrls, "http", "https")
		conf.PeerUrls = strings.ReplaceAll(conf.PeerUrls, "http", "https")
		conf.InitialCluster = strings.ReplaceAll(conf.InitialCluster, "http", "https")
	})
	re.NoError(err)
	defer clus.Destroy()
	err = clus.RunInitialServers()
	re.NoError(err)
	clus.WaitLeader()

	testServers := clus.GetServers()
	endpoints := make([]string, 0, len(testServers))
	for _, s := range testServers {
		endpoints = append(endpoints, s.GetConfig().AdvertiseClientUrls)
		tlsConfig, err := s.GetConfig().Security.ToClientTLSConfig()
		re.NoError(err)
		httpClient := &http.Client{
			Transport: &http.Transport{
				DisableKeepAlives: true,
				TLSClientConfig:   tlsConfig,
			},
		}
		re.True(netutil.IsEnableHTTPS(httpClient))
	}
	// 2. concurrent client dialing while certs become expired
	errc := make(chan error, 1)
	go func() {
		for {
			dctx, dcancel := context.WithTimeout(ctx, time.Second)
			cli, err := pd.NewClientWithContext(dctx,
				caller.TestComponent,
				endpoints, pd.SecurityOption{
					CAPath:   testClientTLSInfo.TrustedCAFile,
					CertPath: testClientTLSInfo.CertFile,
					KeyPath:  testClientTLSInfo.KeyFile,
				}, opt.WithGRPCDialOptions(grpc.WithBlock())) //nolint:staticcheck
			if err != nil {
				errc <- err
				dcancel()
				return
			}
			cli.Close()
			dcancel()
		}
	}()

	// 3. replace certs with expired ones
	replaceFunc()

	// 4. expect dial time-out when loading expired certs
	select {
	case cerr := <-errc:
		re.Contains(cerr.Error(), "failed to get cluster id")
	case <-time.After(5 * time.Second):
		re.FailNow("failed to receive dial timeout error")
	}

	// 5. replace expired certs back with valid ones
	revertFunc()

	// 6. new requests should trigger listener to reload valid certs
	dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
	cli, err := pd.NewClientWithContext(dctx,
		caller.TestComponent,
		endpoints, pd.SecurityOption{
			CAPath:   testClientTLSInfo.TrustedCAFile,
			CertPath: testClientTLSInfo.CertFile,
			KeyPath:  testClientTLSInfo.KeyFile,
		}, opt.WithGRPCDialOptions(grpc.WithBlock())) //nolint:staticcheck
	re.NoError(err)
	dcancel()
	cli.Close()

	// 7. test use raw bytes to init tls config
	caData, certData, keyData := loadTLSContent(re,
		testClientTLSInfo.TrustedCAFile, testClientTLSInfo.CertFile, testClientTLSInfo.KeyFile)
	ctx1, cancel1 := context.WithTimeout(ctx, 2*time.Second)
	cli, err = pd.NewClientWithContext(ctx1,
		caller.TestComponent,
		endpoints, pd.SecurityOption{
			SSLCABytes:   caData,
			SSLCertBytes: certData,
			SSLKEYBytes:  keyData,
		}, opt.WithGRPCDialOptions(grpc.WithBlock())) //nolint:staticcheck
	re.NoError(err)
	defer cli.Close()
	cancel1()
}

func loadTLSContent(re *require.Assertions, caPath, certPath, keyPath string) (caData, certData, keyData []byte) {
	var err error
	caData, err = os.ReadFile(caPath)
	re.NoError(err)
	certData, err = os.ReadFile(certPath)
	re.NoError(err)
	keyData, err = os.ReadFile(keyPath)
	re.NoError(err)
	return
}

// copyTLSFiles clones certs files to dst directory.
func copyTLSFiles(ti transport.TLSInfo, dst string) (transport.TLSInfo, error) {
	ci := transport.TLSInfo{
		KeyFile:        filepath.Join(dst, "pd-server-key.pem"),
		CertFile:       filepath.Join(dst, "pd-server.pem"),
		TrustedCAFile:  filepath.Join(dst, "ca.pem"),
		ClientCertAuth: ti.ClientCertAuth,
	}
	if err := copyFile(ti.KeyFile, ci.KeyFile); err != nil {
		return transport.TLSInfo{}, err
	}
	if err := copyFile(ti.CertFile, ci.CertFile); err != nil {
		return transport.TLSInfo{}, err
	}
	if err := copyFile(ti.TrustedCAFile, ci.TrustedCAFile); err != nil {
		return transport.TLSInfo{}, err
	}
	return ci, nil
}

func copyFile(src, dst string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Printf("Error closing file: %s\n", err)
		}
	}()

	w, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer w.Close()

	if _, err = io.Copy(w, f); err != nil {
		return err
	}
	return w.Sync()
}

func TestMultiCN(t *testing.T) {
	re := require.New(t)

	certPath := t.TempDir()
	generateCerts(re, certPath)
	testTLSInfo := buildTLSInfo(certPath, "pd-server")
	testClientTLSInfo := buildTLSInfo(certPath, "client")
	testTiDBClientTLSInfo := buildTLSInfo(certPath, "tidb-client")

	ctx := t.Context()

	clus, err := tests.NewTestCluster(ctx, 1, func(conf *config.Config, _ string) {
		conf.Security.TLSConfig = grpcutil.TLSConfig{
			KeyPath:        testTLSInfo.KeyFile,
			CertPath:       testTLSInfo.CertFile,
			CAPath:         testTLSInfo.TrustedCAFile,
			CertAllowedCNs: []string{"tidb", "pd-server"},
		}
		conf.AdvertiseClientUrls = strings.ReplaceAll(conf.AdvertiseClientUrls, "http", "https")
		conf.ClientUrls = strings.ReplaceAll(conf.ClientUrls, "http", "https")
		conf.AdvertisePeerUrls = strings.ReplaceAll(conf.AdvertisePeerUrls, "http", "https")
		conf.PeerUrls = strings.ReplaceAll(conf.PeerUrls, "http", "https")
		conf.InitialCluster = strings.ReplaceAll(conf.InitialCluster, "http", "https")
	})
	re.NoError(err)
	defer clus.Destroy()
	err = clus.RunInitialServers()
	re.NoError(err)
	clus.WaitLeader()

	testServers := clus.GetServers()
	endpoints := make([]string, 0, len(testServers))
	for _, s := range testServers {
		endpoints = append(endpoints, s.GetConfig().AdvertiseClientUrls)
	}

	// cn TiDB is allowed
	re.NoError(testAllowedCN(ctx, endpoints, testTiDBClientTLSInfo))

	// cn client is not allowed
	re.Error(testAllowedCN(ctx, endpoints, testClientTLSInfo))
}

func testAllowedCN(ctx context.Context, endpoints []string, tls transport.TLSInfo) error {
	ctx1, cancel1 := context.WithTimeout(ctx, 3*time.Second)
	defer cancel1()
	cli, err := pd.NewClientWithContext(ctx1,
		caller.TestComponent,
		endpoints, pd.SecurityOption{
			CAPath:   tls.TrustedCAFile,
			CertPath: tls.CertFile,
			KeyPath:  tls.KeyFile,
		}, opt.WithGRPCDialOptions(grpc.WithBlock())) //nolint:staticcheck
	if err != nil {
		return err
	}
	defer cli.Close()
	_, err = cli.GetAllMembers(ctx1)
	return err
}
