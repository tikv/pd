// Copyright 2023 TiKV Project Authors.
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

package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const (
	// defaultCallerID marks the default caller ID of the PD HTTP client.
	defaultCallerID = "pd-http-client"
	// defaultInnerCallerID marks the default caller ID of the inner PD HTTP client.
	// It's used to distinguish the requests sent by the inner client via some internal logic.
	defaultInnerCallerID = "pd-http-client-inner"
	httpScheme           = "http"
	httpsScheme          = "https"
	networkErrorStatus   = "network error"

	defaultMembersInfoUpdateInterval = time.Minute
	defaultTimeout                   = 30 * time.Second
)

// respHandleFunc is the function to handle the HTTP response.
type respHandleFunc func(resp *http.Response, res interface{}) error

// clientInner is the inner implementation of the PD HTTP client, which contains some fundamental fields.
// It is wrapped by the `client` struct to make sure the inner implementation won't be exposed and could
// be consistent during the copy.
type clientInner struct {
	ctx    context.Context
	cancel context.CancelFunc

	sync.RWMutex

	sd pd.ServiceDiscovery

	// source is used to mark the source of the client creation,
	// it will also be used in the caller ID of the inner client.
	source  string
	tlsConf *tls.Config
	cli     *http.Client

	requestCounter    *prometheus.CounterVec
	executionDuration *prometheus.HistogramVec
}

func newClientInner(ctx context.Context, cancel context.CancelFunc, source string) *clientInner {
	return &clientInner{ctx: ctx, cancel: cancel, source: source}
}

func (ci *clientInner) init(sd pd.ServiceDiscovery) {
	// Init the HTTP client if it's not configured.
	if ci.cli == nil {
		ci.cli = &http.Client{Timeout: defaultTimeout}
		if ci.tlsConf != nil {
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.TLSClientConfig = ci.tlsConf
			ci.cli.Transport = transport
		}
	}
	ci.sd = sd
}

func (ci *clientInner) close() {
	ci.cancel()
	if ci.cli != nil {
		ci.cli.CloseIdleConnections()
	}
}

func (ci *clientInner) reqCounter(name, status string) {
	if ci.requestCounter == nil {
		return
	}
	ci.requestCounter.WithLabelValues(name, status).Inc()
}

func (ci *clientInner) execDuration(name string, duration time.Duration) {
	if ci.executionDuration == nil {
		return
	}
	ci.executionDuration.WithLabelValues(name).Observe(duration.Seconds())
}

// requestWithRetry will first try to send the request to the PD leader, if it fails, it will try to send
// the request to the other PD followers to gain a better availability.
// TODO: support custom retry logic, e.g. retry with customizable backoffer.
func (ci *clientInner) requestWithRetry(
	ctx context.Context,
	reqInfo *requestInfo,
	headerOpts ...HeaderOption,
) error {
	var err error
	clients := ci.sd.GetAllServiceClients()
	for _, cli := range clients {
		addr := cli.GetHTTPAddress()
		err = ci.doRequest(ctx, addr, reqInfo, headerOpts...)
		if err == nil {
			break
		}
		log.Debug("[pd] request addr failed",
			zap.String("source", ci.source), zap.Bool("is-leader", cli.IsConnectedToLeader()), zap.String("addr", addr), zap.Error(err))
	}
	return err
}

func (ci *clientInner) doRequest(
	ctx context.Context,
	addr string, reqInfo *requestInfo,
	headerOpts ...HeaderOption,
) error {
	var (
		source      = ci.source
		callerID    = reqInfo.callerID
		name        = reqInfo.name
		url         = reqInfo.getURL(addr)
		method      = reqInfo.method
		body        = reqInfo.body
		res         = reqInfo.res
		respHandler = reqInfo.respHandler
	)
	logFields := []zap.Field{
		zap.String("source", source),
		zap.String("name", name),
		zap.String("url", url),
		zap.String("method", method),
		zap.String("caller-id", callerID),
	}
	log.Debug("[pd] request the http url", logFields...)
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewBuffer(body))
	if err != nil {
		log.Error("[pd] create http request failed", append(logFields, zap.Error(err))...)
		return errors.Trace(err)
	}
	for _, opt := range headerOpts {
		opt(req.Header)
	}
	req.Header.Set(xCallerIDKey, callerID)

	start := time.Now()
	resp, err := ci.cli.Do(req)
	if err != nil {
		ci.reqCounter(name, networkErrorStatus)
		log.Error("[pd] do http request failed", append(logFields, zap.Error(err))...)
		return errors.Trace(err)
	}
	ci.execDuration(name, time.Since(start))
	ci.reqCounter(name, resp.Status)

	// Give away the response handling to the caller if the handler is set.
	if respHandler != nil {
		return respHandler(resp, res)
	}

	defer func() {
		err = resp.Body.Close()
		if err != nil {
			log.Warn("[pd] close http response body failed", append(logFields, zap.Error(err))...)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		logFields = append(logFields, zap.String("status", resp.Status))

		bs, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			logFields = append(logFields, zap.NamedError("read-body-error", err))
		} else {
			logFields = append(logFields, zap.ByteString("body", bs))
		}

		log.Error("[pd] request failed with a non-200 status", logFields...)
		return errors.Errorf("request pd http api failed with status: '%s'", resp.Status)
	}

	if res == nil {
		return nil
	}

	err = json.NewDecoder(resp.Body).Decode(res)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

type client struct {
	inner *clientInner

	callerID    string
	respHandler respHandleFunc
}

// ClientOption configures the HTTP client.
type ClientOption func(c *client)

// WithHTTPClient configures the client with the given initialized HTTP client.
func WithHTTPClient(cli *http.Client) ClientOption {
	return func(c *client) {
		c.inner.cli = cli
	}
}

// WithTLSConfig configures the client with the given TLS config.
// This option won't work if the client is configured with WithHTTPClient.
func WithTLSConfig(tlsConf *tls.Config) ClientOption {
	return func(c *client) {
		c.inner.tlsConf = tlsConf
	}
}

// WithMetrics configures the client with metrics.
func WithMetrics(
	requestCounter *prometheus.CounterVec,
	executionDuration *prometheus.HistogramVec,
) ClientOption {
	return func(c *client) {
		c.inner.requestCounter = requestCounter
		c.inner.executionDuration = executionDuration
	}
}

// WithLoggerRedirection configures the client with the given logger redirection.
func WithLoggerRedirection(logLevel, fileName string) ClientOption {
	cfg := &log.Config{}
	cfg.Level = logLevel
	if fileName != "" {
		f, _ := os.CreateTemp(".", fileName)
		fname := f.Name()
		f.Close()
		cfg.File.Filename = fname
	}
	lg, p, _ := log.InitLogger(cfg)
	log.ReplaceGlobals(lg, p)
	return func(c *client) {}
}

// NewClientWithServiceDiscovery creates a PD HTTP client with the given PD service discovery.
func NewClientWithServiceDiscovery(
	source string,
	sd pd.ServiceDiscovery,
	opts ...ClientOption,
) Client {
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{inner: newClientInner(ctx, cancel, source), callerID: defaultCallerID}
	// Apply the options first.
	for _, opt := range opts {
		opt(c)
	}
	c.inner.init(sd)
	return c
}

// NewClient creates a PD HTTP client with the given PD addresses and TLS config.
func NewClient(
	source string,
	pdAddrs []string,
	opts ...ClientOption,
) Client {
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{inner: newClientInner(ctx, cancel, source), callerID: defaultCallerID}
	// Apply the options first.
	for _, opt := range opts {
		opt(c)
	}
	sd := pd.NewDefaultPDServiceDiscovery(ctx, cancel, pdAddrs, c.inner.tlsConf)
	c.inner.init(sd)
	return c
}

// Close gracefully closes the HTTP client.
func (c *client) Close() {
	c.inner.close()
	log.Info("[pd] http client closed", zap.String("source", c.inner.source))
}

// WithCallerID sets and returns a new client with the given caller ID.
func (c *client) WithCallerID(callerID string) Client {
	newClient := *c
	newClient.callerID = callerID
	return &newClient
}

// WithRespHandler sets and returns a new client with the given HTTP response handler.
func (c *client) WithRespHandler(
	handler func(resp *http.Response, res interface{}) error,
) Client {
	newClient := *c
	newClient.respHandler = handler
	return &newClient
}

// Header key definition constants.
const (
	pdAllowFollowerHandleKey = "PD-Allow-Follower-Handle"
	xCallerIDKey             = "X-Caller-ID"
)

// HeaderOption configures the HTTP header.
type HeaderOption func(header http.Header)

// WithAllowFollowerHandle sets the header field to allow a PD follower to handle this request.
func WithAllowFollowerHandle() HeaderOption {
	return func(header http.Header) {
		header.Set(pdAllowFollowerHandleKey, "true")
	}
}

func (c *client) request(ctx context.Context, reqInfo *requestInfo, headerOpts ...HeaderOption) error {
	return c.inner.requestWithRetry(ctx, reqInfo.
		WithCallerID(c.callerID).
		WithRespHandler(c.respHandler),
		headerOpts...)
}
