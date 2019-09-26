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

func getRequest(cmd *cobra.Command, prefix string, method string, bodyType string, body io.Reader) (*http.Request, error) {
	if method == "" {
		method = http.MethodGet
	}
	url := getAddressFromCmd(cmd, prefix)
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", bodyType)
	return req, err
}

func dail(req *http.Request) (string, error) {
	var res string
	reps, err := dialClient.Do(req)
	if err != nil {
		return res, err
	}
	defer reps.Body.Close()
	if reps.StatusCode != http.StatusOK {
		return res, genResponseError(reps)
	}

	r, err := ioutil.ReadAll(reps.Body)
	if err != nil {
		return res, err
	}
	res = string(r)
	return res, nil
}

func doRequest(cmd *cobra.Command, prefix string, method string) (string, error) {
	req, err := getRequest(cmd, prefix, method, "", nil)
	if err != nil {
		return "", err
	}
	return dail(req)
}

func genResponseError(r *http.Response) error {
	res, _ := ioutil.ReadAll(r.Body)
	return errors.Errorf("[%d] %s", r.StatusCode, res)
}

func getAddressFromCmd(cmd *cobra.Command, prefix string) string {
	p, err := cmd.Flags().GetString("pd")
	if err != nil {
		cmd.Println("get pd address error, should set flag with '-u'")
		os.Exit(1)
	}

	if p != "" && !strings.HasPrefix(p, "http") {
		p = "http://" + p
	}

	u, err := url.Parse(p)
	if err != nil {
		cmd.Println("address format is wrong, should like 'http://127.0.0.1:2379' or '127.0.0.1:2379'")
	}

	s := fmt.Sprintf("%s/%s", u, prefix)
	return s
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

	url := getAddressFromCmd(cmd, prefix)
	r, err := dialClient.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		cmd.Println(err)
		return
	}
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		if err := printResponseError(r); err != nil {
			cmd.Println(err)
		}
	} else {
		cmd.Println("Success!")
	}
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
