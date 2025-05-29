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

package grpcutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/errs"
)

var (
	certPath   = filepath.Join("..", "..", "..", "tests", "integrations", "client") + string(filepath.Separator)
	certScript = filepath.Join("..", "..", "..", "tests", "integrations", "client", "cert_opt.sh")
)

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

func TestToClientTLSConfig(t *testing.T) {
	re := require.New(t)
	if err := exec.Command(certScript, "generate", certPath).Run(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := exec.Command(certScript, "cleanup", certPath).Run(); err != nil {
			t.Fatal(err)
		}
	}()

	tlsConfig := TLSConfig{
		KeyPath:  filepath.Join(certPath, "pd-server-key.pem"),
		CertPath: filepath.Join(certPath, "pd-server.pem"),
		CAPath:   filepath.Join(certPath, "ca.pem"),
	}
	// test without bytes
	_, err := tlsConfig.ToClientTLSConfig()
	re.NoError(err)

	// test with bytes
	caData, certData, keyData := loadTLSContent(re, tlsConfig.CAPath, tlsConfig.CertPath, tlsConfig.KeyPath)
	tlsConfig.SSLCABytes = caData
	tlsConfig.SSLCertBytes = certData
	tlsConfig.SSLKEYBytes = keyData
	_, err = tlsConfig.ToClientTLSConfig()
	re.NoError(err)

	// test wrong cert bytes
	tlsConfig.SSLCertBytes = []byte("invalid cert")
	_, err = tlsConfig.ToClientTLSConfig()
	re.True(errors.ErrorEqual(err, errs.ErrCryptoX509KeyPair))

	// test wrong ca bytes
	tlsConfig.SSLCertBytes = certData
	tlsConfig.SSLCABytes = []byte("invalid ca")
	_, err = tlsConfig.ToClientTLSConfig()
	re.True(errors.ErrorEqual(err, errs.ErrCryptoAppendCertsFromPEM))
}

func TestToServerTLSConfig(t *testing.T) {
	re := require.New(t)

	if err := exec.Command(certScript, "generate", certPath).Run(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := exec.Command(certScript, "cleanup", certPath).Run(); err != nil {
			t.Fatal(err)
		}
	}()

	testCases := []struct {
		name          string
		tlsConfig     TLSConfig
		wantErr       bool
		checkConfig   bool
		allowedCNs    []string
		validateProto bool
	}{
		{
			name: "valid certificate configuration",
			tlsConfig: TLSConfig{
				KeyPath:  filepath.Join(certPath, "pd-server-key.pem"),
				CertPath: filepath.Join(certPath, "pd-server.pem"),
				CAPath:   filepath.Join(certPath, "ca.pem"),
			},
			checkConfig:   true,
			validateProto: true,
		},
		{
			name: "with allowed CNs",
			tlsConfig: TLSConfig{
				KeyPath:        filepath.Join(certPath, "pd-server-key.pem"),
				CertPath:       filepath.Join(certPath, "pd-server.pem"),
				CAPath:         filepath.Join(certPath, "ca.pem"),
				CertAllowedCNs: []string{"pd-server"},
			},
			checkConfig: true,
			allowedCNs:  []string{"pd-server"},
		},
		{
			name: "empty cert and key paths",
			tlsConfig: TLSConfig{
				CAPath: filepath.Join(certPath, "ca.pem"),
			},
			wantErr: false, // Should return nil config, not error
		},
		{
			name: "invalid cert path",
			tlsConfig: TLSConfig{
				CAPath:   filepath.Join(certPath, "ca.pem"),
				CertPath: "non-existent.pem",
				KeyPath:  filepath.Join(certPath, "pd-server-key.pem"),
			},
			wantErr: true,
		},
		{
			name: "invalid key path",
			tlsConfig: TLSConfig{
				CAPath:   filepath.Join(certPath, "ca.pem"),
				CertPath: filepath.Join(certPath, "pd-server-key.pem"),
				KeyPath:  "non-existent.pem",
			},
			wantErr: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(_ *testing.T) {
			tlsConfig, err := testCase.tlsConfig.ToServerTLSConfig()
			if testCase.wantErr {
				re.Error(err)
				return
			}
			re.NoError(err)

			if !testCase.checkConfig {
				if testCase.tlsConfig.CertPath == "" && testCase.tlsConfig.KeyPath == "" {
					re.Nil(tlsConfig)
				}
				return
			}

			re.NotNil(tlsConfig)
			re.Equal(tls.VerifyClientCertIfGiven, tlsConfig.ClientAuth)

			if testCase.validateProto {
				re.Contains(tlsConfig.NextProtos, "http/1.1")
				re.Contains(tlsConfig.NextProtos, "h2")
			}

			// Validate allowed CNs
			if len(testCase.allowedCNs) > 0 {
				cert, err := tls.LoadX509KeyPair(testCase.tlsConfig.CertPath, testCase.tlsConfig.KeyPath)
				re.NoError(err)
				x509Cert, err := x509.ParseCertificate(cert.Certificate[0])
				re.NoError(err)
				re.Contains(testCase.allowedCNs, x509Cert.Subject.CommonName)
			}
		})
	}
}
func BenchmarkGetForwardedHost(b *testing.B) {
	// Without forwarded host key
	md := metadata.Pairs("test", "example.com")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	// Run the GetForwardedHost function b.N times
	for range b.N {
		GetForwardedHost(ctx)
	}
}
