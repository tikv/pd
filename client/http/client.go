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
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
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
	pdAddrs       []string
	leaderAddrIdx int

	// source is used to mark the source of the client creation,
	// it will also be used in the caller ID of the inner client.
	source  string
	tlsConf *tls.Config
	cli     *http.Client

	requestCounter    *prometheus.CounterVec
	executionDuration *prometheus.HistogramVec
}

func newClientInner(source string) *clientInner {
	ctx, cancel := context.WithCancel(context.Background())
	return &clientInner{ctx: ctx, cancel: cancel, leaderAddrIdx: -1, source: source}
}

func (ci *clientInner) init() {
	// Init the HTTP client if it's not configured.
	if ci.cli == nil {
		ci.cli = &http.Client{Timeout: defaultTimeout}
		if ci.tlsConf != nil {
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.TLSClientConfig = ci.tlsConf
			ci.cli.Transport = transport
		}
	}
	// Start the members info updater daemon.
	go ci.membersInfoUpdater(ci.ctx)
}

func (ci *clientInner) close() {
	ci.cancel()
	if ci.cli != nil {
		ci.cli.CloseIdleConnections()
	}
}

// getPDAddrs returns the current PD addresses and the index of the leader address.
func (ci *clientInner) getPDAddrs() ([]string, int) {
	ci.RLock()
	defer ci.RUnlock()
	return ci.pdAddrs, ci.leaderAddrIdx
}

func (ci *clientInner) setPDAddrs(pdAddrs []string, leaderAddrIdx int) {
	ci.Lock()
	defer ci.Unlock()
	// Normalize the addresses with correct scheme prefix.
	var scheme string
	if ci.tlsConf == nil {
		scheme = httpScheme
	} else {
		scheme = httpsScheme
	}
	for i, addr := range pdAddrs {
		if strings.HasPrefix(addr, httpScheme) {
			continue
		}
		pdAddrs[i] = fmt.Sprintf("%s://%s", scheme, addr)
	}
	ci.pdAddrs = pdAddrs
	ci.leaderAddrIdx = leaderAddrIdx
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
	var (
		err                    error
		addr                   string
		pdAddrs, leaderAddrIdx = ci.getPDAddrs()
	)
	// Try to send the request to the PD leader first.
	if leaderAddrIdx != -1 {
		addr = pdAddrs[leaderAddrIdx]
		err = ci.doRequest(ctx, addr, reqInfo, headerOpts...)
		if err == nil {
			return nil
		}
		log.Debug("[pd] request leader addr failed",
			zap.String("source", ci.source), zap.Int("leader-idx", leaderAddrIdx), zap.String("addr", addr), zap.Error(err))
	}
	// Try to send the request to the other PD followers.
	for idx := 0; idx < len(pdAddrs) && idx != leaderAddrIdx; idx++ {
		addr = ci.pdAddrs[idx]
		err = ci.doRequest(ctx, addr, reqInfo, headerOpts...)
		if err == nil {
			break
		}
		log.Debug("[pd] request follower addr failed",
			zap.String("source", ci.source), zap.Int("idx", idx), zap.String("addr", addr), zap.Error(err))
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

func (ci *clientInner) membersInfoUpdater(ctx context.Context) {
	ci.updateMembersInfo(ctx)
	log.Info("[pd] http client member info updater started", zap.String("source", ci.source))
	ticker := time.NewTicker(defaultMembersInfoUpdateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("[pd] http client member info updater stopped", zap.String("source", ci.source))
			return
		case <-ticker.C:
			ci.updateMembersInfo(ctx)
		}
	}
}

func (ci *clientInner) updateMembersInfo(ctx context.Context) {
	var membersInfo MembersInfo
	err := ci.requestWithRetry(ctx, newRequestInfo().
		WithCallerID(fmt.Sprintf("%s-%s", ci.source, defaultInnerCallerID)).
		WithName(getMembersName).
		WithURI(membersPrefix).
		WithMethod(http.MethodGet).
		WithResp(&membersInfo))
	if err != nil {
		log.Error("[pd] http client get members info failed", zap.String("source", ci.source), zap.Error(err))
		return
	}
	if len(membersInfo.Members) == 0 {
		log.Error("[pd] http client get empty members info", zap.String("source", ci.source))
		return
	}
	var (
		newPDAddrs       []string
		newLeaderAddrIdx int = -1
	)
	for _, member := range membersInfo.Members {
		if membersInfo.Leader != nil && member.GetMemberId() == membersInfo.Leader.GetMemberId() {
			newLeaderAddrIdx = len(newPDAddrs)
		}
		newPDAddrs = append(newPDAddrs, member.GetClientUrls()...)
	}
	// Prevent setting empty addresses.
	if len(newPDAddrs) == 0 {
		log.Error("[pd] http client get empty member addresses", zap.String("source", ci.source))
		return
	}
	oldPDAddrs, oldLeaderAddrIdx := ci.getPDAddrs()
	ci.setPDAddrs(newPDAddrs, newLeaderAddrIdx)
	// Log the member info change if it happens.
	var oldPDLeaderAddr, newPDLeaderAddr string
	if oldLeaderAddrIdx != -1 {
		oldPDLeaderAddr = oldPDAddrs[oldLeaderAddrIdx]
	}
	if newLeaderAddrIdx != -1 {
		newPDLeaderAddr = newPDAddrs[newLeaderAddrIdx]
	}
	oldMemberNum, newMemberNum := len(oldPDAddrs), len(newPDAddrs)
	if oldPDLeaderAddr != newPDLeaderAddr || oldMemberNum != newMemberNum {
		log.Info("[pd] http client members info changed", zap.String("source", ci.source),
			zap.Int("old-member-num", oldMemberNum), zap.Int("new-member-num", newMemberNum),
			zap.Strings("old-addrs", oldPDAddrs), zap.Strings("new-addrs", newPDAddrs),
			zap.Int("old-leader-addr-idx", oldLeaderAddrIdx), zap.Int("new-leader-addr-idx", newLeaderAddrIdx),
			zap.String("old-leader-addr", oldPDLeaderAddr), zap.String("new-leader-addr", newPDLeaderAddr))
	}
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

// NewClient creates a PD HTTP client with the given PD addresses and TLS config.
func NewClient(
	source string,
	pdAddrs []string,
	opts ...ClientOption,
) Client {
	c := &client{inner: newClientInner(source), callerID: defaultCallerID}
	// Apply the options first.
	for _, opt := range opts {
		opt(c)
	}
	c.inner.setPDAddrs(pdAddrs, -1)
	c.inner.init()
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

// UpdateMembersInfo updates the members info of the PD cluster in the inner client.
// Exported for testing.
func (c *client) UpdateMembersInfo() {
	c.inner.updateMembersInfo(c.inner.ctx)
}
