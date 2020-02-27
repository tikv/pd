// Copyright 2020 PingCAP, Inc.
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

package client_test

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/pingcap/check"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/pd/v4/pkg/grpcutil"
	"github.com/pingcap/pd/v4/server"
	"github.com/pingcap/pd/v4/server/config"
	"github.com/pingcap/pd/v4/tests"
	"go.etcd.io/etcd/pkg/transport"
	"google.golang.org/grpc"
)

var _ = Suite(&clientTLSTestSuite{})

var (
	testTLSInfo = transport.TLSInfo{
		KeyFile:        "./cert/pd-server-key.pem",
		CertFile:       "./cert/pd-server.pem",
		TrustedCAFile:  "./cert/ca.pem",
		ClientCertAuth: true,
	}

	testClientTLSInfo = transport.TLSInfo{
		KeyFile:        "./cert/client-key.pem",
		CertFile:       "./cert/client.pem",
		TrustedCAFile:  "./cert/ca.pem",
		ClientCertAuth: true,
	}

	testTLSInfoExpired = transport.TLSInfo{
		KeyFile:        "./cert-expired/pd-server-key.pem",
		CertFile:       "./cert-expired/pd-server.pem",
		TrustedCAFile:  "./cert-expired/ca.pem",
		ClientCertAuth: true,
	}
)

type clientTLSTestSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *clientTLSTestSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
}

func (s *clientTLSTestSuite) TearDownSuite(c *C) {
	s.cancel()
}

// TestTLSReloadAtomicReplace ensures server reloads expired/valid certs
// when all certs are atomically replaced by directory renaming.
// And expects server to reject client requests, and vice versa.
func (s *clientTLSTestSuite) TestTLSReloadAtomicReplace(c *C) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "cert-tmp")
	c.Assert(err, IsNil)
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	certsDir, err := ioutil.TempDir(os.TempDir(), "cert-to-load")
	c.Assert(err, IsNil)
	defer os.RemoveAll(certsDir)

	certsDirExp, err := ioutil.TempDir(os.TempDir(), "cert-expired")
	c.Assert(err, IsNil)
	defer os.RemoveAll(certsDirExp)

	cloneFunc := func() transport.TLSInfo {
		tlsInfo, terr := copyTLSFiles(testTLSInfo, certsDir)
		c.Assert(terr, IsNil)
		_, err = copyTLSFiles(testTLSInfoExpired, certsDirExp)
		c.Assert(err, IsNil)
		return tlsInfo

	}
	replaceFunc := func() {
		err = os.Rename(certsDir, tmpDir)
		c.Assert(err, IsNil)
		err = os.Rename(certsDirExp, certsDir)
		c.Assert(err, IsNil)
		// after rename,
		// 'certsDir' contains expired certs
		// 'tmpDir' contains valid certs
		// 'certsDirExp' does not exist

	}
	revertFunc := func() {
		err = os.Rename(tmpDir, certsDirExp)
		c.Assert(err, IsNil)

		err = os.Rename(certsDir, tmpDir)
		c.Assert(err, IsNil)

		err = os.Rename(certsDirExp, certsDir)
		c.Assert(err, IsNil)

	}
	s.testTLSReload(c, cloneFunc, replaceFunc, revertFunc, false)

}

func (s *clientTLSTestSuite) testTLSReload(
	c *C,
	cloneFunc func() transport.TLSInfo,
	replaceFunc func(),
	revertFunc func(),
	useIP bool) {
	tlsInfo := cloneFunc()
	// 1. start cluster with valid certs
	clus, err := tests.NewTestCluster(s.ctx, 1, func(conf *config.Config) {
		conf.Security = grpcutil.SecurityConfig{
			KeyPath:        tlsInfo.KeyFile,
			CertPath:       tlsInfo.CertFile,
			CAPath:         tlsInfo.TrustedCAFile,
			ClientCertAuth: tlsInfo.ClientCertAuth,
		}
		conf.AdvertiseClientUrls = strings.ReplaceAll(conf.AdvertiseClientUrls, "http", "https")
		conf.ClientUrls = strings.ReplaceAll(conf.ClientUrls, "http", "https")
		conf.AdvertisePeerUrls = strings.ReplaceAll(conf.AdvertisePeerUrls, "http", "https")
		conf.PeerUrls = strings.ReplaceAll(conf.PeerUrls, "http", "https")
		conf.InitialCluster = strings.ReplaceAll(conf.InitialCluster, "http", "https")
	})
	c.Assert(err, IsNil)
	defer clus.Destroy()
	err = clus.RunInitialServers()
	c.Assert(err, IsNil)
	clus.WaitLeader()

	var endpoints []string
	for _, s := range clus.GetServers() {
		endpoints = append(endpoints, s.GetConfig().AdvertiseClientUrls)
	}
	// 2. concurrent client dialing while certs become expired
	errc := make(chan error, 1)
	go func() {
		for {
			dctx, dcancel := context.WithTimeout(s.ctx, time.Second)
			cli, err := pd.NewClientWithContext(dctx, endpoints, pd.SecurityOption{
				CAPath:   testClientTLSInfo.TrustedCAFile,
				CertPath: testClientTLSInfo.CertFile,
				KeyPath:  testClientTLSInfo.KeyFile,
			}, pd.WithGRPCDialOptions(grpc.WithBlock()))
			if err != nil {
				errc <- err
				dcancel()
				return
			}
			dcancel()
			cli.Close()
		}
	}()

	// 3. replace certs with expired ones
	replaceFunc()

	// 4. expect dial time-out when loading expired certs
	select {
	case cerr := <-errc:
		c.Assert(strings.Contains(cerr.Error(), "failed to get cluster id"), IsTrue)
	case <-time.After(5 * time.Second):
		c.Fatal("failed to receive dial timeout error")
	}

	// 5. replace expired certs back with valid ones
	revertFunc()

	// 6. new requests should trigger listener to reload valid certs
	dctx, dcancel := context.WithTimeout(s.ctx, 5*time.Second)
	cli, err := pd.NewClientWithContext(dctx, endpoints, pd.SecurityOption{
		CAPath:   testClientTLSInfo.TrustedCAFile,
		CertPath: testClientTLSInfo.CertFile,
		KeyPath:  testClientTLSInfo.KeyFile,
	}, pd.WithGRPCDialOptions(grpc.WithBlock()))
	c.Assert(err, IsNil)
	dcancel()
	cli.Close()
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
	defer f.Close()

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
