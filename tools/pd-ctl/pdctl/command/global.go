// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package command

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/pkg/transport"
)

var (
	dialClient = &http.Client{}
	pingPrefix = "pd/ping"
)

// InitHTTPSClient creates https client with ca file
func InitHTTPSClient(CAPath, CertPath, KeyPath string) error {
	tlsInfo := transport.TLSInfo{
		CertFile:      CertPath,
		KeyFile:       KeyPath,
		TrustedCAFile: CAPath,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return errors.WithStack(err)
	}

	dialClient = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}

	return nil
}

type bodyOption struct {
	contentType string
	body        io.Reader
}

// BodyOption sets the type and content of the body
type BodyOption func(*bodyOption)

// WithBody returns a BodyOption
func WithBody(contentType string, body io.Reader) BodyOption {
	return func(bo *bodyOption) {
		bo.contentType = contentType
		bo.body = body
	}
}
func doRequest(cmd *cobra.Command, prefix string, method string,
	opts ...BodyOption) (string, error) {
	b := &bodyOption{}
	for _, o := range opts {
		o(b)
	}
	var resp string
	var err error
	tryURLs(cmd, func(endpoint string) error {
		url := endpoint + "/" + prefix
		if method == "" {
			method = http.MethodGet
		}
		var req *http.Request
		req, err = http.NewRequest(method, url, b.body)
		if err != nil {
			return err
		}
		if b.contentType != "" {
			req.Header.Set("Content-Type", b.contentType)
		}
		// the resp would be returned by the outer function
		resp, err = dail(req)
		if err != nil {
			return err
		}
		return nil
	})
	return resp, err
}

func dail(req *http.Request) (string, error) {
	resp, err := dialClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var msg []byte
		msg, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("[%d] %s", resp.StatusCode, msg), nil
	}

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

// DoFunc receives an endpoint which you can issue request to
type DoFunc func(endpoint string) error

// tryURLs issues requests to each URL and tries next one if there
// is an error
func tryURLs(cmd *cobra.Command, f DoFunc) {
	addrs, err := cmd.Flags().GetString("pd")
	if err != nil {
		cmd.Println("get pd address failed, should set flag with '-u'")
		os.Exit(1)
	}
	endpoints := strings.Split(addrs, ",")
	for _, endpoint := range endpoints {
		var u *url.URL
		u, err = url.Parse(endpoint)
		if err != nil {
			cmd.Println("address format is wrong, should like 'http://127.0.0.1:2379' or '127.0.0.1:2379'")
			os.Exit(1)
		}
		// tolerate some schemes that will be used by users, the TiKV SDK
		// use 'tikv' as the scheme, it is really confused if we do not
		// support it by pdctl
		if u.Scheme == "" || u.Scheme == "pd" || u.Scheme == "tikv" {
			u.Scheme = "http"
		}

		endpoint = u.String()
		err = f(endpoint)
		if err != nil {
			continue
		}
		break
	}
	if err != nil {
		cmd.Println("after trying all endpoints, no endpoint is available, the last error we met:", err)
	}
}

func printResponseError(r *http.Response) error {
	fmt.Printf("[%d]:", r.StatusCode)
	_, err := io.Copy(os.Stdout, r.Body)
	return err
}

func postJSON(cmd *cobra.Command, prefix string, input map[string]interface{}) {
	data, err := json.Marshal(input)
	if err != nil {
		cmd.Println(err)
		return
	}

	tryURLs(cmd, func(endpoint string) error {
		url := endpoint + "/" + prefix
		r, err := dialClient.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			return err
		}
		defer r.Body.Close()
		if r.StatusCode != http.StatusOK {
			if err := printResponseError(r); err != nil {
				cmd.Println(err)
			}
			return nil
		}
		cmd.Println("success!")
		return nil
	})

}

// UsageTemplate will used to generate a help information
const UsageTemplate = `Usage:{{if .Runnable}}
  {{if .HasAvailableFlags}}{{appendIfNotPresent .UseLine ""}}{{else}}{{.UseLine}}{{end}}{{end}}{{if .HasAvailableSubCommands}}
  {{if .HasParent}}{{ .Name}} [command]{{else}}[command]{{end}}{{end}}{{if gt .Aliases 0}}

Aliases:
  {{.NameAndAliases}}
{{end}}{{if .HasExample}}

Examples:
{{ .Example }}{{end}}{{ if .HasAvailableSubCommands}}

Available Commands:{{range .Commands}}{{if .IsAvailableCommand}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{ if .HasAvailableLocalFlags}}

Additional help topics:{{range .Commands}}{{if .IsAdditionalHelpTopicCommand}}
  {{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{ if .HasAvailableSubCommands }}

Use "{{if .HasParent}}help {{.Name}} [command] {{else}}help [command]{{end}}" for more information about a command.{{end}}
`
