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
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"go.etcd.io/etcd/client/pkg/v3/transport"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"

	pd "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/pkg/utils/apiutil"
)

const (
	// PDControlCallerID is used to set the caller ID for PD client
	PDControlCallerID = "pd-ctl"
	clusterPrefix     = "pd/api/v1/cluster"
)

func initTLSConfig(caPath, certPath, keyPath string) (*tls.Config, error) {
	tlsInfo := transport.TLSInfo{
		CertFile:      certPath,
		KeyFile:       keyPath,
		TrustedCAFile: caPath,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return tlsConfig, nil
}

// PDCli is a pd HTTP client
var PDCli pd.Client

func requirePDClient(cmd *cobra.Command, _ []string) error {
	var (
		tlsConfig *tls.Config
		err       error
	)
	tlsConfig, err = parseTLSConfig(cmd)
	if err != nil {
		return err
	}

	return initNewPDClient(cmd, pd.WithTLSConfig(tlsConfig))
}

// shouldInitPDClient checks whether we should create a new PD client according to the cluster information.
func shouldInitPDClient(cmd *cobra.Command) (bool, error) {
	// Get the cluster information the current command assigned to.
	newClusterInfoJSON, err := doRequest(cmd, clusterPrefix, http.MethodGet, http.Header{})
	if err != nil {
		return false, err
	}
	newClusterInfo := &metapb.Cluster{}
	err = json.Unmarshal([]byte(newClusterInfoJSON), newClusterInfo)
	if err != nil {
		return false, err
	}
	// If the PD client is nil and we get the cluster information successfully,
	// we should initialize the PD client directly.
	if PDCli == nil {
		return true, nil
	}
	// Get current cluster information that the PD client connects to.
	currentClusterInfo, err := PDCli.GetCluster(cmd.Context())
	if err != nil {
		return true, nil
	}
	// Compare the cluster ID to determine whether we should re-initialize the PD client.
	return currentClusterInfo.GetId() == 0 || newClusterInfo.GetId() != currentClusterInfo.GetId(), nil
}

func initNewPDClient(cmd *cobra.Command, opts ...pd.ClientOption) error {
	if should, err := shouldInitPDClient(cmd); !should || err != nil {
		return err
	}
	if PDCli != nil {
		PDCli.Close()
	}
	PDCli = pd.NewClient(PDControlCallerID, getEndpoints(cmd), opts...).WithCallerID(PDControlCallerID)
	return nil
}

// TODO: replace dialClient with the PD HTTP client completely.
var dialClient = &http.Client{
	Transport: apiutil.NewCallerIDRoundTripper(http.DefaultTransport, PDControlCallerID),
}

func parseTLSConfig(cmd *cobra.Command) (*tls.Config, error) {
	caPath, err := cmd.Flags().GetString("cacert")
	if err != nil || len(caPath) == 0 {
		return nil, err
	}
	certPath, err := cmd.Flags().GetString("cert")
	if err != nil {
		return nil, err
	}
	keyPath, err := cmd.Flags().GetString("key")
	if err != nil {
		return nil, err
	}
	tlsConfig, err := initTLSConfig(caPath, certPath, keyPath)
	if err != nil {
		return nil, err
	}

	return tlsConfig, nil
}

// RequireHTTPSClient creates a HTTPS client if the related flags are set
func RequireHTTPSClient(cmd *cobra.Command, _ []string) error {
	tlsConfig, err := parseTLSConfig(cmd)
	if err != nil || tlsConfig == nil {
		return err
	}
	dialClient = &http.Client{
		Transport: apiutil.NewCallerIDRoundTripper(
			&http.Transport{TLSClientConfig: tlsConfig}, PDControlCallerID),
	}
	return nil
}

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
		return do(endpoint, prefix, method, &resp, customHeader, b)
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
		return do(endpoint, prefix, method, &resp, customHeader, b)
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
	if req.Header.Get(apiutil.XForbiddenForwardToMicroserviceHeader) == "true" {
		if resp.Header.Get(apiutil.XForwardedToMicroserviceHeader) == "true" {
			return string(content), errors.Errorf("the request is forwarded to microservice unexpectedly")
		}
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

func requestJSON(cmd *cobra.Command, method, prefix string, input map[string]any) {
	data, err := json.Marshal(input)
	if err != nil {
		cmd.Println(err)
		return
	}

	endpoints := getEndpoints(cmd)
	var msg []byte
	err = tryURLs(cmd, endpoints, func(endpoint string) error {
		var req *http.Request
		var resp *http.Response
		url := endpoint + "/" + prefix
		switch method {
		case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodGet:
			req, err = http.NewRequest(method, url, bytes.NewBuffer(data))
			if err != nil {
				return err
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err = dialClient.Do(req)
		default:
			err := errors.Errorf("method %s not supported", method)
			return err
		}
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		msg, err = io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return errors.Errorf("[%d] %s", resp.StatusCode, msg)
		}
		return nil
	})
	if err != nil {
		cmd.Printf("Failed! %s\n", strings.TrimSpace(err.Error()))
		return
	}
	msg = bytes.Trim(bytes.TrimSpace(msg), "\"")
	cmd.Printf("Success! %s\n", string(msg))
}

func postJSON(cmd *cobra.Command, prefix string, input map[string]any) {
	requestJSON(cmd, http.MethodPost, prefix, input)
}

func patchJSON(cmd *cobra.Command, prefix string, input map[string]any) {
	requestJSON(cmd, http.MethodPatch, prefix, input)
}

// do send a request to server. Default is Get.
func do(endpoint, prefix, method string, resp *string, customHeader http.Header, b *bodyOption) error {
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

func jsonPrint(cmd *cobra.Command, val any) {
	jsonBytes, err := json.MarshalIndent(val, "", "  ")
	if err != nil {
		cmd.Printf("Failed to marshal the data to json: %s\n", err)
		return
	}

	cmd.Println(string(jsonBytes))
}
