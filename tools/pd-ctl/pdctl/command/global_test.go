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
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestURLWithTrailingSlash(t *testing.T) {
	for _, suffix := range []string{"", "/", "///", "/proxy%2Ftenant/"} {
		t.Run(suffix, func(t *testing.T) {
			re := require.New(t)
			as := assert.New(t)
			requestPath := "/pd/api/v1/config?ttlSecond=5"
			if suffix == "/proxy%2Ftenant/" {
				requestPath = "/proxy%2Ftenant" + requestPath
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				as.Equal(http.MethodPost, r.Method)
				as.Equal(requestPath, r.RequestURI)
				body, err := io.ReadAll(r.Body)
				as.NoError(err)
				as.JSONEq(`{"leader-schedule-limit":8}`, string(body))
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			endpoint := server.URL + suffix
			cmd := &cobra.Command{}
			cmd.Flags().String("pd", endpoint, "")
			var output bytes.Buffer
			cmd.SetOut(&output)
			for _, prefix := range []string{"pd/api/v1/config?ttlSecond=5", "/pd/api/v1/config?ttlSecond=5"} {
				requestJSON(cmd, http.MethodPost, prefix, map[string]any{"leader-schedule-limit": 8})
				re.Contains(output.String(), "Success!")
				output.Reset()
				var response string
				re.NoError(do(endpoint, prefix, http.MethodPost, &response, nil,
					&bodyOption{body: bytes.NewBufferString(`{"leader-schedule-limit":8}`)}))
			}
		})
	}
}

func TestParseTLSConfig(t *testing.T) {
	re := require.New(t)

	rootCmd := &cobra.Command{
		Use:           "pd-ctl",
		Short:         "Placement Driver control",
		SilenceErrors: true,
	}
	certPath := t.TempDir()
	rootCmd.Flags().String("cacert", filepath.Join(certPath, "ca.pem"), "path of file that contains list of trusted SSL CAs")
	rootCmd.Flags().String("cert", filepath.Join(certPath, "client.pem"), "path of file that contains X509 certificate in PEM format")
	rootCmd.Flags().String("key", filepath.Join(certPath, "client-key.pem"), "path of file that contains X509 key in PEM format")

	certScript := filepath.Join("..", "..", "tests", "cert_opt.sh")
	if err := exec.Command(certScript, "generate", certPath).Run(); err != nil {
		t.Fatal(err)
	}

	tlsConfig, err := parseTLSConfig(rootCmd)
	re.NoError(err)
	re.NotNil(tlsConfig)
}
