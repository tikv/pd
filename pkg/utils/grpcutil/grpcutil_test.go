package grpcutil

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/errs"
)

var (
	certPath   = "../../../tests/integrations/client/cert"
	certScript = "./cert_opt.sh"
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

func cmdCert(certsDir, script, args string) error {
	currentDir, _ := os.Getwd()
	// Change working directory
	os.Chdir(certsDir)
	defer os.Chdir(currentDir)

	// Run the script
	if err := exec.Command(script, args).Run(); err != nil {
		fmt.Println("Error running script:", err)
		return err
	}
	return nil
}

func TestToTLSConfig(t *testing.T) {
	if err := cmdCert(certPath, certScript, "generate"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cmdCert(certPath, certScript, "cleanup"); err != nil {
			t.Fatal(err)
		}
	}()

	t.Parallel()
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
