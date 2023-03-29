package grpcutil

import (
	"os"
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/errs"
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
	t.Parallel()
	re := require.New(t)
	tlsConfig := TLSConfig{
		KeyPath:  "../../../tests/integrations/client/cert/pd-server-key.pem",
		CertPath: "../../../tests/integrations/client/cert/pd-server.pem",
		CAPath:   "../../../tests/integrations/client/cert/ca.pem",
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
