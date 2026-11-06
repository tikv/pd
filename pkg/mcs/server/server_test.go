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

package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/tikv/pd/pkg/utils/configutil"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

func TestClientCertificateAuthentication(t *testing.T) {
	generateCertificates := func() string {
		certDir := t.TempDir()
		script := filepath.Join("..", "..", "..", "tests", "integrations", "client", "cert_opt.sh")
		output, err := exec.Command("bash", script, "generate", certDir).CombinedOutput()
		require.NoError(t, err, "%s", output)
		return certDir
	}
	certDir, otherCertDir := generateCertificates(), generateCertificates()
	ca, err := os.ReadFile(filepath.Join(certDir, "ca.pem"))
	require.NoError(t, err)
	rootCAs := x509.NewCertPool()
	require.True(t, rootCAs.AppendCertsFromPEM(ca))
	loadCertificate := func(dir, name string) tls.Certificate {
		cert, err := tls.LoadX509KeyPair(filepath.Join(dir, name+".pem"), filepath.Join(dir, name+"-key.pem"))
		require.NoError(t, err)
		return cert
	}
	untrusted := loadCertificate(otherCertDir, "client")
	wrongCN := loadCertificate(certDir, "pd-server")

	for _, testCase := range []struct {
		name        string
		settings    string
		requireCert bool
		restrictCN  bool
		withoutCA   bool
	}{
		{name: "legacy default"},
		{name: "explicit legacy policy", settings: "client-cert-auth = false"},
		{name: "legacy server-only TLS", withoutCA: true},
		{name: "legacy CN policy", settings: "cert-allowed-cn = [\"client\"]", requireCert: true, restrictCN: true},
		{name: "required certificate", settings: "client-cert-auth = true", requireCert: true},
		{name: "required certificate and CN", settings: "client-cert-auth = true\ncert-allowed-cn = [\"client\"]", requireCert: true, restrictCN: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var cfg struct {
				Security configutil.SecurityConfig `toml:"security"`
			}
			meta, err := toml.Decode("[security]\n"+testCase.settings, &cfg)
			require.NoError(t, err)
			require.Empty(t, meta.Undecoded())
			if !testCase.withoutCA {
				cfg.Security.CAPath = filepath.Join(certDir, "ca.pem")
			}
			cfg.Security.CertPath = filepath.Join(certDir, "pd-server.pem")
			cfg.Security.KeyPath = filepath.Join(certDir, "pd-server-key.pem")

			for _, protocol := range []string{"HTTP", "gRPC"} {
				t.Run(protocol, func(t *testing.T) {
					server := NewBaseServer(t.Context())
					require.NoError(t, server.InitListener(&cfg.Security.TLSConfig, "https://127.0.0.1:0"))
					require.True(t, server.IsSecure())
					listener := server.GetListener()
					serveDone := make(chan error, 1)
					if protocol == "HTTP" {
						httpServer := &http.Server{
							ReadHeaderTimeout: 3 * time.Second,
							Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
								w.WriteHeader(http.StatusNoContent)
							}),
						}
						go func() {
							serveDone <- httpServer.Serve(listener)
						}()
						t.Cleanup(func() {
							require.NoError(t, httpServer.Close())
							require.ErrorIs(t, <-serveDone, http.ErrServerClosed)
						})
					} else {
						grpcServer := grpc.NewServer()
						hs := health.NewServer()
						hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
						grpc_health_v1.RegisterHealthServer(grpcServer, hs)
						go func() {
							serveDone <- grpcServer.Serve(listener)
						}()
						t.Cleanup(func() {
							grpcServer.Stop()
							require.NoError(t, <-serveDone)
						})
					}
					for _, client := range []struct {
						name           string
						cert           *tls.Certificate
						allowed        bool
						outboundConfig bool
						maxTLSVersion  uint16
					}{
						{name: "no certificate", allowed: !testCase.requireCert},
						{name: "no certificate over TLS 1.2", allowed: !testCase.requireCert, maxTLSVersion: tls.VersionTLS12},
						{name: "untrusted certificate", cert: &untrusted},
						{name: "untrusted certificate over TLS 1.2", cert: &untrusted, maxTLSVersion: tls.VersionTLS12},
						{name: "other CN", cert: &wrongCN, allowed: !testCase.restrictCN && !testCase.withoutCA},
						{name: "trusted certificate over TLS 1.2", allowed: !testCase.withoutCA, outboundConfig: true, maxTLSVersion: tls.VersionTLS12},
						{name: "trusted certificate after rejected clients", allowed: !testCase.withoutCA, outboundConfig: true},
					} {
						t.Run(client.name, func(t *testing.T) {
							clientTLS := &tls.Config{RootCAs: rootCAs, MinVersion: tls.VersionTLS12}
							if client.cert != nil {
								// Send the untrusted certificate even when its CA is not requested.
								clientTLS.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
									return client.cert, nil
								}
							}
							if client.outboundConfig {
								outbound := cfg.Security.TLSConfig
								outbound.CAPath = filepath.Join(certDir, "ca.pem")
								outbound.CertPath = filepath.Join(certDir, "client.pem")
								outbound.KeyPath = filepath.Join(certDir, "client-key.pem")
								outbound.CertAllowedCNs = []string{"pd-server"}
								clientTLS, err = outbound.ToClientTLSConfig()
								require.NoError(t, err)
							}
							clientTLS.MaxVersion = client.maxTLSVersion
							err := checkTLSHealth(t, protocol, listener.Addr().String(), clientTLS)
							if client.allowed {
								require.NoError(t, err)
							} else {
								require.Error(t, err)
							}
						})
					}
				})
			}
		})
	}
}

func checkTLSHealth(t *testing.T, protocol, address string, clientTLS *tls.Config) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if protocol == "HTTP" {
		transport := &http.Transport{TLSClientConfig: clientTLS}
		defer transport.CloseIdleConnections()
		client := &http.Client{Transport: transport}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+address, nil)
		require.NoError(t, err)
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		require.NoError(t, response.Body.Close())
		if response.StatusCode != http.StatusNoContent {
			return fmt.Errorf("unexpected HTTP status: %s", response.Status)
		}
		return nil
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)))
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	response, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err == nil && response.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		return fmt.Errorf("unexpected gRPC health status: %s", response.GetStatus())
	}
	return err
}
