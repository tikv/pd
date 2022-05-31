package grpcutil

import (
	"os"
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/errs"
)

func loadTLSContent(t *testing.T, caPath, certPath, keyPath string) (caData, certData, keyData []byte) {
	var err error
	caData, err = os.ReadFile(caPath)
	require.NoError(t, err)
	certData, err = os.ReadFile(certPath)
	require.NoError(t, err)
	keyData, err = os.ReadFile(keyPath)
	require.NoError(t, err)
	return
}

func TestToTLSConfig(t *testing.T) {
	tlsConfig := TLSConfig{
		KeyPath:  "../../tests/client/cert/pd-server-key.pem",
		CertPath: "../../tests/client/cert/pd-server.pem",
		CAPath:   "../../tests/client/cert/ca.pem",
	}
	// test without bytes
	_, err := tlsConfig.ToTLSConfig()
	require.NoError(t, err)

	// test with bytes
	caData, certData, keyData := loadTLSContent(t, tlsConfig.CAPath, tlsConfig.CertPath, tlsConfig.KeyPath)
	tlsConfig.SSLCABytes = caData
	tlsConfig.SSLCertBytes = certData
	tlsConfig.SSLKEYBytes = keyData
	_, err = tlsConfig.ToTLSConfig()
	require.NoError(t, err)

	// test wrong cert bytes
	tlsConfig.SSLCertBytes = []byte("invalid cert")
	_, err = tlsConfig.ToTLSConfig()
	require.True(t, errors.ErrorEqual(err, errs.ErrCryptoX509KeyPair))

	// test wrong ca bytes
	tlsConfig.SSLCertBytes = certData
	tlsConfig.SSLCABytes = []byte("invalid ca")
	_, err = tlsConfig.ToTLSConfig()
	require.True(t, errors.ErrorEqual(err, errs.ErrCryptoAppendCertsFromPEM))
}
