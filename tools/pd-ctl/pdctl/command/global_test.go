// Copyright 2024 TiKV Project Authors.
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

package command

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestParseTLSConfig(t *testing.T) {
	re := require.New(t)

	rootCmd := &cobra.Command{
		Use:           "pd-ctl",
		Short:         "Placement Driver control",
		SilenceErrors: true,
	}
	certPath := filepath.Join("..", "..", "tests", "cert")
	rootCmd.Flags().String("cacert", filepath.Join(certPath, "ca.pem"), "path of file that contains list of trusted SSL CAs")
	rootCmd.Flags().String("cert", filepath.Join(certPath, "client.pem"), "path of file that contains X509 certificate in PEM format")
	rootCmd.Flags().String("key", filepath.Join(certPath, "client-key.pem"), "path of file that contains X509 key in PEM format")

	// generate certs
	if err := os.Mkdir(certPath, 0755); err != nil {
		t.Fatal(err)
	}
	certScript := filepath.Join("..", "..", "tests", "cert_opt.sh")
	if err := exec.Command(certScript, "generate", certPath).Run(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := exec.Command(certScript, "cleanup", certPath).Run(); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(certPath); err != nil {
			t.Fatal(err)
		}
	}()

	tlsConfig, err := parseTLSConfig(rootCmd)
	re.NoError(err)
	re.NotNil(tlsConfig)
}
