// Copyright 2026 TiKV Project Authors.
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

package spiffetls

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/proto/spiffe/workload"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func validConfig(endpoint string) Config {
	return Config{
		Endpoint: endpoint, Timeout: 3 * time.Second,
		ClientIDPattern: "spiffe://example.org/ns/pd/*",
		ServerIDPattern: "spiffe://example.org/ns/tidb/*",
	}
}

func TestConfigValidate(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*Config)
	}{
		{"empty endpoint", func(c *Config) { c.Endpoint = "" }},
		{"path without scheme", func(c *Config) { c.Endpoint = "/tmp/api.sock" }},
		{"relative socket", func(c *Config) { c.Endpoint = "unix:api.sock" }},
		{"authority", func(c *Config) { c.Endpoint = "unix://host/tmp/api.sock" }},
		{"tcp", func(c *Config) { c.Endpoint = "tcp://127.0.0.1:8080" }},
		{"query", func(c *Config) { c.Endpoint += "?x=1" }},
		{"empty query", func(c *Config) { c.Endpoint += "?" }},
		{"fragment", func(c *Config) { c.Endpoint += "#x" }},
		{"empty fragment", func(c *Config) { c.Endpoint += "#" }},
		{"invalid URI", func(c *Config) { c.Endpoint = "unix:///%" }},
		{"zero timeout", func(c *Config) { c.Timeout = 0 }},
		{"negative timeout", func(c *Config) { c.Timeout = -time.Second }},
		{"missing client", func(c *Config) { c.ClientIDPattern = "" }},
		{"missing server", func(c *Config) { c.ServerIDPattern = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := validConfig("unix:///tmp/api.sock")
			test.change(&config)
			require.Error(t, config.Validate())
		})
	}
	require.NoError(t, validConfig("unix:///tmp/api.sock").Validate())
}

func TestPatterns(t *testing.T) {
	for _, test := range []struct {
		name, raw string
		matching  []string
		rejected  []string
	}{
		{"exact", "spiffe://example.org/ns/tidb/pod", []string{"spiffe://example.org/ns/tidb/pod"}, []string{"spiffe://example.org/ns/tidb/other"}},
		{"root identity", "spiffe://example.org", []string{"spiffe://example.org"}, []string{"spiffe://example.org/pod"}},
		{"pod wildcard", "spiffe://example.org/ns/tidb/*", []string{"spiffe://example.org/ns/tidb/pod"}, []string{"spiffe://other.org/ns/tidb/pod", "spiffe://example.org/ns/tidb", "spiffe://example.org/ns/tidb/pod/extra"}},
		{"multiple wildcards", "spiffe://example.org/*/tidb/*", []string{"spiffe://example.org/ns/tidb/pod"}, []string{"spiffe://example.org/ns/pd/pod"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			pattern, err := parsePattern(test.raw)
			require.NoError(t, err)
			for _, raw := range test.matching {
				require.True(t, pattern.matches(spiffeid.RequireFromString(raw)), raw)
			}
			for _, raw := range test.rejected {
				require.False(t, pattern.matches(spiffeid.RequireFromString(raw)), raw)
			}
		})
	}
	for _, raw := range []string{
		"", "https://example.org/ns/pd", "SPIFFE://example.org/ns/pd", "spiffe://EXAMPLE.org/ns/pd",
		"spiffe://example.org:443/ns/pd", "spiffe://*/ns/*", "spiffe://*.example.org/ns/*",
		"spiffe://example.org/ns/pd*", "spiffe://example.org/ns/**", "spiffe://example.org/ns//pd",
		"spiffe://example.org/ns/./*", "spiffe://example.org/ns/../*", "spiffe://example.org/ns/*/",
		"spiffe://example.org/ns/%2A", "spiffe://example.org/ns/%2f/*", "spiffe://example.org/ns/*?x=1",
		"spiffe://example.org/ns/pd?", "spiffe://example.org/ns/pd#x", " spiffe://example.org/ns/pd",
	} {
		t.Run(raw, func(t *testing.T) {
			_, err := parsePattern(raw)
			require.Error(t, err)
		})
	}
}

func TestSVIDSelection(t *testing.T) {
	ca, key := newCA(t)
	client := newSVID(t, ca, key, "spiffe://example.org/ns/pd/pod", 2)
	other := newSVID(t, ca, key, "spiffe://example.org/ns/other/pod", 3)
	expired := *client
	expiredLeaf := *client.Certificates[0]
	expiredLeaf.NotAfter = time.Now().Add(-time.Second)
	expired.Certificates = []*x509.Certificate{&expiredLeaf}
	pattern, err := parsePattern("spiffe://example.org/ns/pd/*")
	require.NoError(t, err)
	for _, test := range []struct {
		name  string
		svids []*x509svid.SVID
		want  *x509svid.SVID
	}{
		{"matching", []*x509svid.SVID{client}, client},
		{"matching after unrelated", []*x509svid.SVID{other, client}, client},
		{"none", nil, nil},
		{"unrelated", []*x509svid.SVID{other}, nil},
		{"ambiguous", []*x509svid.SVID{client, client}, nil},
		{"invalid matching candidate", []*x509svid.SVID{client, &expired}, nil},
		{"nil entry", []*x509svid.SVID{nil}, nil},
	} {
		t.Run(test.name, func(t *testing.T) { require.Same(t, test.want, pattern.pickSVID(test.svids)) })
	}
	for _, test := range []struct {
		name   string
		change func(*x509svid.SVID)
	}{
		{"missing certificates", func(s *x509svid.SVID) { s.Certificates = nil }},
		{"nil certificate", func(s *x509svid.SVID) { s.Certificates = []*x509.Certificate{nil} }},
		{"inconsistent ID", func(s *x509svid.SVID) { s.ID = other.ID }},
		{"expired", func(s *x509svid.SVID) { s.Certificates[0].NotAfter = time.Now().Add(-time.Second) }},
		{"future", func(s *x509svid.SVID) { s.Certificates[0].NotBefore = time.Now().Add(time.Hour) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := *client
			leaf := *client.Certificates[0]
			candidate.Certificates = []*x509.Certificate{&leaf}
			test.change(&candidate)
			require.Error(t, pattern.validateSVID(&candidate))
		})
	}
}

func TestNewRejectsInitialIdentitiesAndClosesWatch(t *testing.T) {
	ca, key := newCA(t)
	client := newSVID(t, ca, key, "spiffe://example.org/ns/pd/pod", 2)
	other := newSVID(t, ca, key, "spiffe://example.org/ns/other/pod", 3)
	for _, test := range []struct {
		name   string
		svids  []*x509svid.SVID
		server string
	}{
		{"no matching client", []*x509svid.SVID{other}, ""},
		{"ambiguous client", []*x509svid.SVID{client, client}, ""},
		{"missing server bundle", []*x509svid.SVID{client}, "spiffe://other.org/ns/tidb/*"},
	} {
		t.Run(test.name, func(t *testing.T) {
			api, endpoint := startWorkloadAPI(t)
			api.updates <- workloadResponse(t, ca, test.svids...)
			config := validConfig(endpoint)
			if test.server != "" {
				config.ServerIDPattern = test.server
			}
			source, err := New(t.Context(), config)
			require.Error(t, err)
			require.Nil(t, source)
			require.Eventually(t, func() bool { return api.stopped.Load() == 1 }, 3*time.Second, time.Millisecond)
		})
	}
}

func TestNewStartupDeadline(t *testing.T) {
	for _, parentDeadline := range []bool{false, true} {
		t.Run("parent-deadline="+strconv.FormatBool(parentDeadline), func(t *testing.T) {
			api, endpoint := startWorkloadAPI(t)
			config := validConfig(endpoint)
			config.Timeout = 150 * time.Millisecond
			ctx := t.Context()
			if parentDeadline {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, config.Timeout)
				defer cancel()
				config.Timeout = 3 * time.Second
			}
			started := time.Now()
			source, err := New(ctx, config)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Nil(t, source)
			require.Less(t, time.Since(started), 3*time.Second)
			require.Eventually(t, func() bool { return api.stopped.Load() == 1 }, 3*time.Second, time.Millisecond)
		})
	}
}

func TestWorkloadAPIRotationAndTLSConfigClones(t *testing.T) {
	clientCA, clientKey := newCA(t)
	serverCA, serverKey := newCA(t)
	rotatedCA, rotatedKey := newCA(t)
	client := newSVID(t, clientCA, clientKey, "spiffe://example.org/ns/pd/pod", 2)
	rotatedClient := newSVID(t, clientCA, clientKey, client.ID.String(), 3)
	server := newSVID(t, serverCA, serverKey, "spiffe://example.org/ns/tidb/pod", 4)
	rotatedServer := newSVID(t, rotatedCA, rotatedKey, server.ID.String(), 5)
	api, endpoint := startWorkloadAPI(t)
	api.updates <- workloadResponse(t, serverCA, client)
	ctx, cancel := context.WithCancel(t.Context())
	source, err := New(ctx, validConfig(endpoint))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	cancel()

	config := source.TLSConfig()
	require.EqualValues(t, tls.VersionTLS13, config.MinVersion)
	// The Dashboard registers its TLS config with the MySQL driver, which
	// clones it when parsing DSNs. Those callbacks must keep following rotation.
	registryName := t.Name()
	require.NoError(t, mysql.RegisterTLSConfig(registryName, config))
	t.Cleanup(func() { mysql.DeregisterTLSConfig(registryName) })
	driverConfig, err := mysql.ParseDSN("dashboard@tcp(localhost:4000)/?tls=" + registryName)
	require.NoError(t, err)
	require.NotNil(t, driverConfig.TLS)
	require.False(t, driverConfig.AllowFallbackToPlaintext)
	cloned := driverConfig.Clone().TLS
	require.NotSame(t, config, cloned)
	state, err := handshake(t, cloned, server, clientCA)
	require.NoError(t, err)
	require.Equal(t, int64(2), state.PeerCertificates[0].SerialNumber.Int64())

	api.updates <- workloadResponse(t, rotatedCA, rotatedClient)
	waitForSVID(t, source, 3)
	state, err = handshake(t, cloned, rotatedServer, clientCA)
	require.NoError(t, err)
	require.Equal(t, int64(3), state.PeerCertificates[0].SerialNumber.Int64())
	_, err = handshake(t, cloned, server, clientCA)
	require.Error(t, err, "the old server CA must no longer be trusted")
	wrongServer := newSVID(t, rotatedCA, rotatedKey, "spiffe://example.org/ns/other/pod", 6)
	_, err = handshake(t, cloned, wrongServer, clientCA)
	require.ErrorContains(t, err, "does not match")
	wrongTrustDomain := newSVID(t, rotatedCA, rotatedKey, "spiffe://other.org/ns/tidb/pod", 7)
	_, err = handshake(t, cloned, wrongTrustDomain, clientCA)
	require.Error(t, err)

	wrongClient := newSVID(t, clientCA, clientKey, "spiffe://example.org/ns/other/pod", 8)
	for _, svids := range [][]*x509svid.SVID{{wrongClient}, {client, rotatedClient}} {
		api.updates <- workloadResponse(t, rotatedCA, svids...)
		require.Eventually(t, func() bool { _, err := source.source.GetX509SVID(); return err != nil }, 3*time.Second, time.Millisecond)
		_, err = handshake(t, cloned, rotatedServer, clientCA)
		require.Error(t, err, "invalid rotation must not retain old client credentials")
		api.updates <- workloadResponse(t, rotatedCA, rotatedClient)
		waitForSVID(t, source, 3)
	}
	state, err = handshake(t, cloned, rotatedServer, clientCA)
	require.NoError(t, err)
	require.Equal(t, int64(3), state.PeerCertificates[0].SerialNumber.Int64())
	require.EqualValues(t, 1, api.started.Load(), "initialization cancellation and rotation must retain the original watch")
	require.Zero(t, api.stopped.Load())

	// Bundle callbacks run concurrently with Workload API updates during SQL
	// reconnects. Exercise that shared source under the race detector.
	readerCtx, cancelReader := context.WithCancel(t.Context())
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for readerCtx.Err() == nil {
			_, _ = source.source.GetX509BundleForTrustDomain(client.ID.TrustDomain())
		}
	}()
	t.Cleanup(func() { cancelReader(); <-readerDone })
	for i := range 20 {
		current := client
		if i%2 == 1 {
			current = rotatedClient
		}
		api.updates <- workloadResponse(t, rotatedCA, current)
		waitForSVID(t, source, current.Certificates[0].SerialNumber.Int64())
	}
	cancelReader()
	<-readerDone

	var closes sync.WaitGroup
	var closeErrors [5]error
	for i := range closeErrors {
		closes.Go(func() { closeErrors[i] = source.Close() })
	}
	closes.Wait()
	for _, err := range closeErrors {
		require.NoError(t, err)
	}
	require.NoError(t, source.Close())
	require.Eventually(t, func() bool { return api.stopped.Load() == 1 }, 3*time.Second, time.Millisecond)
	_, err = handshake(t, cloned, rotatedServer, clientCA)
	require.Error(t, err, "retained TLS config clones must fail after closing the source")
}

type testWorkloadAPI struct {
	workload.UnimplementedSpiffeWorkloadAPIServer
	updates chan *workload.X509SVIDResponse
	started atomic.Int64
	stopped atomic.Int64
}

func (s *testWorkloadAPI) FetchX509SVID(_ *workload.X509SVIDRequest, stream workload.SpiffeWorkloadAPI_FetchX509SVIDServer) error {
	s.started.Add(1)
	defer s.stopped.Add(1)
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case update := <-s.updates:
			if err := stream.Send(update); err != nil {
				return fmt.Errorf("send test Workload API update: %w", err)
			}
		}
	}
}

func startWorkloadAPI(t *testing.T) (*testWorkloadAPI, string) {
	t.Helper()
	// macOS Unix socket paths are short; testing.T.TempDir can exceed the limit.
	directory, err := os.MkdirTemp("/tmp", "pd-spiffe-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(directory)) })
	socket := filepath.Join(directory, "api.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	api := &testWorkloadAPI{updates: make(chan *workload.X509SVIDResponse, 2)}
	server := grpc.NewServer()
	workload.RegisterSpiffeWorkloadAPIServer(server, api)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	return api, "unix://" + socket
}

func workloadResponse(t *testing.T, ca *x509.Certificate, svids ...*x509svid.SVID) *workload.X509SVIDResponse {
	t.Helper()
	response := &workload.X509SVIDResponse{}
	for _, svid := range svids {
		key, err := x509.MarshalPKCS8PrivateKey(svid.PrivateKey)
		require.NoError(t, err)
		var chain []byte
		for _, cert := range svid.Certificates {
			chain = append(chain, cert.Raw...)
		}
		response.Svids = append(response.Svids, &workload.X509SVID{
			SpiffeId: svid.ID.String(), X509Svid: chain, X509SvidKey: key, Bundle: ca.Raw,
		})
	}
	return response
}

func waitForSVID(t *testing.T, source *Source, serial int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		svid, err := source.source.GetX509SVID()
		return err == nil && svid.Certificates[0].SerialNumber.Int64() == serial
	}, 3*time.Second, time.Millisecond)
}

func newCA(t *testing.T) (*x509.Certificate, crypto.Signer) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: t.Name()},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageCertSign, IsCA: true, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	require.NoError(t, err)
	certificate, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return certificate, key
}

func newSVID(t *testing.T, ca *x509.Certificate, caKey crypto.Signer, rawID string, serial int64) *x509svid.SVID {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	id := spiffeid.RequireFromString(rawID)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial), URIs: []*url.URL{id.URL()},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, key.Public(), caKey)
	require.NoError(t, err)
	certificate, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return &x509svid.SVID{ID: id, Certificates: []*x509.Certificate{certificate}, PrivateKey: key}
}

type serverSource struct {
	svid   *x509svid.SVID
	bundle *x509bundle.Bundle
}

func (s serverSource) GetX509SVID() (*x509svid.SVID, error) { return s.svid, nil }
func (s serverSource) GetX509BundleForTrustDomain(spiffeid.TrustDomain) (*x509bundle.Bundle, error) {
	return s.bundle, nil
}

func handshake(t *testing.T, clientConfig *tls.Config, serverSVID *x509svid.SVID, clientCA *x509.Certificate) (tls.ConnectionState, error) {
	t.Helper()
	td := spiffeid.RequireTrustDomainFromString("example.org")
	server := serverSource{svid: serverSVID, bundle: x509bundle.FromX509Authorities(td, []*x509.Certificate{clientCA})}
	serverConfig := tlsconfig.MTLSServerConfig(server, server, tlsconfig.AuthorizeID(spiffeid.RequireFromString("spiffe://example.org/ns/pd/pod")))
	serverConfig.MinVersion = tls.VersionTLS13
	clientPipe, serverPipe := net.Pipe()
	defer func() { require.NoError(t, clientPipe.Close()) }()
	defer func() { require.NoError(t, serverPipe.Close()) }()
	deadline := time.Now().Add(3 * time.Second)
	require.NoError(t, clientPipe.SetDeadline(deadline))
	require.NoError(t, serverPipe.SetDeadline(deadline))
	client := tls.Client(clientPipe, clientConfig)
	tlsServer := tls.Server(serverPipe, serverConfig)
	serverResult := make(chan error, 1)
	go func() { serverResult <- tlsServer.HandshakeContext(t.Context()) }()
	clientErr := client.HandshakeContext(t.Context())
	if clientErr != nil {
		require.NoError(t, clientPipe.Close())
	}
	serverErr := <-serverResult
	return tlsServer.ConnectionState(), errors.Join(clientErr, serverErr)
}
