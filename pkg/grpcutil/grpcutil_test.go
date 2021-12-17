package grpcutil

import (
	"os"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/tikv/pd/pkg/errs"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&gRPCUtilSuite{})

type gRPCUtilSuite struct{}

func loadTLSContent(c *C, caPath, certPath, keyPath string) (caData, certData, keyData []byte) {
	var err error
	caData, err = os.ReadFile(caPath)
	c.Assert(err, IsNil)
	certData, err = os.ReadFile(certPath)
	c.Assert(err, IsNil)
	keyData, err = os.ReadFile(keyPath)
	c.Assert(err, IsNil)
	return
}

func (s *gRPCUtilSuite) TestToTLSConfig(c *C) {
	tlsConfig := TLSConfig{
		KeyPath:  "../../tests/client/cert/pd-server-key.pem",
		CertPath: "../../tests/client/cert/pd-server.pem",
		CAPath:   "../../tests/client/cert/ca.pem",
	}
	// test without bytes
	_, err := tlsConfig.ToTLSConfig()
	c.Assert(err, IsNil)

	// test with bytes
	caData, certData, keyData := loadTLSContent(c, tlsConfig.CAPath, tlsConfig.CertPath, tlsConfig.KeyPath)
	tlsConfig.SSLCABytes = caData
	tlsConfig.SSLCertBytes = certData
	tlsConfig.SSLKEYBytes = keyData
	_, err = tlsConfig.ToTLSConfig()
	c.Assert(err, IsNil)

	// test wrong cert bytes
	tlsConfig.SSLCertBytes = []byte("invalid cert")
	_, err = tlsConfig.ToTLSConfig()
	c.Assert(errors.ErrorEqual(err, errs.ErrCryptoX509KeyPair), IsTrue)

	// test wrong ca bytes
	tlsConfig.SSLCertBytes = certData
	tlsConfig.SSLCABytes = []byte("invalid ca")
	_, err = tlsConfig.ToTLSConfig()
	c.Assert(errors.ErrorEqual(err, errs.ErrCryptoAppendCertsFromPEM), IsTrue)
}
