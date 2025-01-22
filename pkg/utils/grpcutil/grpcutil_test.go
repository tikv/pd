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

func TestToTLSConfig(t *testing.T) {
	if err := exec.Command(certScript, "generate", certPath).Run(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := exec.Command(certScript, "cleanup", certPath).Run(); err != nil {
			t.Fatal(err)
		}
	}()

	re := require.New(t)
	tlsConfig := TLSConfig{
		KeyPath:  filepath.Join(certPath, "pd-server-key.pem"),
		CertPath: filepath.Join(certPath, "pd-server.pem"),
		CAPath:   filepath.Join(certPath, "ca.pem"),
	}
	// test without bytes
	_, err := tlsConfig.ToTLSConfig()
	re.NoError(err)

	// test with bytes
	caData, certData, keyData := loadTLSContent(re, tlsConfig.CAPath, tlsConfig.CertPath, tlsConfig.KeyPath)
	tlsConfig.SSLCABytes = caData
	tlsConfig.SSLCertBytes = certData
	tlsConfig.SSLKEYBytes = keyData
	_, err = tlsConfig.ToTLSConfig()
	re.NoError(err)

	// test wrong cert bytes
	tlsConfig.SSLCertBytes = []byte("invalid cert")
	_, err = tlsConfig.ToTLSConfig()
	re.True(errors.ErrorEqual(err, errs.ErrCryptoX509KeyPair))

	// test wrong ca bytes
	tlsConfig.SSLCertBytes = certData
	tlsConfig.SSLCABytes = []byte("invalid ca")
	_, err = tlsConfig.ToTLSConfig()
	re.True(errors.ErrorEqual(err, errs.ErrCryptoAppendCertsFromPEM))
}

func BenchmarkGetForwardedHost(b *testing.B) {
	// Without forwarded host key
	md := metadata.Pairs("test", "example.com")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	// Run the GetForwardedHost function b.N times
	for i := 0; i < b.N; i++ {
		GetForwardedHost(ctx)
	}
}
