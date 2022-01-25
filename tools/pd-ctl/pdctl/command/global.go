// Copyright 2016 TiKV Project Authors.
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
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
	"github.com/tikv/pd/pkg/apiutil"
	"go.etcd.io/etcd/pkg/transport"
)

var (
	pdControllerComponentName = "pdctl"
	dialClient                = &http.Client{
		Transport: apiutil.NewComponentSignatureRoundTripper(http.DefaultTransport, pdControllerComponentName),
	}
	pingPrefix = "pd/api/v1/ping"
)

// InitHTTPSClient creates https client with ca file
func InitHTTPSClient(caPath, certPath, keyPath string) error {
	tlsInfo := transport.TLSInfo{
		CertFile:      certPath,
		KeyFile:       keyPath,
		TrustedCAFile: caPath,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return errors.WithStack(err)
	}

	dialClient = &http.Client{
		Transport: apiutil.NewComponentSignatureRoundTripper(
			&http.Transport{TLSClientConfig: tlsConfig}, pdControllerComponentName),
	}

	return nil
}

type convertOutputFunc func(string) string

type bodyOption struct {
	body io.Reader
}

// BodyOption sets the type and content of the body
type BodyOption func(*bodyOption)

// WithBody returns a BodyOption
func WithBody(body io.Reader) BodyOption {
	return func(bo *bodyOption) {
		bo.body = body
	}
}

func doRequest(cmd *cobra.Command, prefix string, method string, customHeader http.Header,
	opts ...BodyOption) (string, error) {
	b := &bodyOption{}
	for _, o := range opts {
		o(b)
	}
	var resp string

	endpoints := getEndpoints(cmd)
	err := tryURLs(cmd, endpoints, func(endpoint string) error {
		return doGet(endpoint, prefix, method, &resp, customHeader, b)
	})
	return resp, err
}

func doRequestSingleEndpoint(cmd *cobra.Command, endpoint, prefix, method string, customHeader http.Header,
	opts ...BodyOption) (string, error) {
	b := &bodyOption{}
	for _, o := range opts {
		o(b)
	}
	var resp string

	err := requestURL(cmd, endpoint, func(endpoint string) error {
		return doGet(endpoint, prefix, method, &resp, customHeader, b)
	})
	return resp, err
}

func dial(req *http.Request) (string, error) {
	resp, err := dialClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var msg []byte
		msg, err = io.ReadAll(resp.Body)
		if err != nil {
			return "", err
		}
		return "", errors.Errorf("[%d] %s", resp.StatusCode, msg)
	}

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

// DoFunc receives an endpoint which you can issue request to
type DoFunc func(endpoint string) error

// tryURLs issues requests to each URL and tries next one if there
// is an error
func tryURLs(cmd *cobra.Command, endpoints []string, f DoFunc) error {
	var err error
	for _, endpoint := range endpoints {
		endpoint, err = checkURL(endpoint)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		err = f(endpoint)
		if err != nil {
			continue
		}
		break
	}
	if len(endpoints) > 1 && err != nil {
		err = errors.Errorf("after trying all endpoints, no endpoint is available, the last error we met: %s", err)
	}
	return err
}

func requestURL(cmd *cobra.Command, endpoint string, f DoFunc) error {
	endpoint, err := checkURL(endpoint)
	if err != nil {
		cmd.Println(err.Error())
		os.Exit(1)
	}
	return f(endpoint)
}

func getEndpoints(cmd *cobra.Command) []string {
	addrs, err := cmd.Flags().GetString("pd")
	if err != nil {
		cmd.Println("get pd address failed, should set flag with '-u'")
		os.Exit(1)
	}
	return strings.Split(addrs, ",")
}

func postJSON(cmd *cobra.Command, prefix string, input map[string]interface{}) {
	data, err := json.Marshal(input)
	if err != nil {
		cmd.Println(err)
		return
	}

	endpoints := getEndpoints(cmd)
	err = tryURLs(cmd, endpoints, func(endpoint string) error {
		var msg []byte
		var r *http.Response
		url := endpoint + "/" + prefix
		r, err = dialClient.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			return err
		}
		defer r.Body.Close()
		if r.StatusCode != http.StatusOK {
			msg, err = io.ReadAll(r.Body)
			if err != nil {
				return err
			}
			return errors.Errorf("[%d] %s", r.StatusCode, msg)
		}
		return nil
	})
	if err != nil {
		cmd.Printf("Failed! %s", err)
		return
	}
	cmd.Println("Success!")
}

// doGet send a get request to server.
func doGet(endpoint, prefix, method string, resp *string, customHeader http.Header, b *bodyOption) error {
	var err error
	url := endpoint + "/" + prefix
	if method == "" {
		method = http.MethodGet
	}
	var req *http.Request

	req, err = http.NewRequest(method, url, b.body)
	if err != nil {
		return err
	}

	for key, values := range customHeader {
		for _, v := range values {
			req.Header.Add(key, v)
		}
	}
	// the resp would be returned by the outer function
	*resp, err = dial(req)
	if err != nil {
		return err
	}
	return nil
}

func checkURL(endpoint string) (string, error) {
	if j := strings.Index(endpoint, "//"); j == -1 {
		endpoint = "//" + endpoint
	}
	var u *url.URL
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", errors.Errorf("address format is wrong, should like 'http://127.0.0.1:2379' or '127.0.0.1:2379'")
	}
	// tolerate some schemes that will be used by users, the TiKV SDK
	// use 'tikv' as the scheme, it is really confused if we do not
	// support it by pd-ctl
	if u.Scheme == "" || u.Scheme == "pd" || u.Scheme == "tikv" {
		u.Scheme = "http"
	}

	return u.String(), nil
}
