package grpcutil

import (
	"context"
	"os"
	"os/exec"
	"path"
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/errs"
	"google.golang.org/grpc/metadata"
)

var (
	certPath   = "../../../tests/integrations/client/"
	certScript = "cert_opt.sh"
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
	if err := exec.Command(certPath+certScript, "generate", certPath).Run(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := exec.Command(certPath+certScript, "cleanup", certPath).Run(); err != nil {
			t.Fatal(err)
		}
	}()

	re := require.New(t)
	tlsConfig := TLSConfig{
		KeyPath:  path.Join(certPath, "pd-server-key.pem"),
		CertPath: path.Join(certPath, "pd-server.pem"),
		CAPath:   path.Join(certPath, "ca.pem"),
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
