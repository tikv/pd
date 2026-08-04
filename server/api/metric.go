// Copyright 2019 TiKV Project Authors.
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

package api

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/ratelimit"
	"github.com/tikv/pd/server"
)

const (
	metricQueryTimeout            = 30 * time.Second
	metricQueryDialAttemptTimeout = 2 * time.Second
	metricQueryErrorBody          = `{"status":"error","errorType":"proxy","error":"metric query failed"}`
)

// Keep one warning per minute; repeated failures remain available at debug level.
var metricQueryErrorLogLimiter = ratelimit.NewRateLimiter(1.0/time.Minute.Seconds(), 1)

type metricIPResolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

type queryMetric struct {
	s             *server.Server
	transportMu   sync.Mutex
	transport     *http.Transport
	closed        bool
	getHTTPClient func() *http.Client
	resolver      metricIPResolver
}

type metricStorageURL struct {
	URL      *url.URL
	Hostname string
	Port     string
}

type metricTarget struct {
	*metricStorageURL
	addresses []netip.Addr
}

type metricTargetContextKey struct{}

func newQueryMetric(s *server.Server) *queryMetric {
	return &queryMetric{
		s:             s,
		getHTTPClient: s.GetHTTPClient,
		resolver:      net.DefaultResolver,
	}
}

func (h *queryMetric) queryMetric(w http.ResponseWriter, r *http.Request) {
	transport, err := h.getTransport()
	if err != nil {
		writeMetricQueryError(w, err)
		return
	}
	proxyMetricQuery(
		w,
		r,
		h.s.GetConfig().PDServerCfg.MetricStorage,
		transport,
		h.resolver,
	)
}

func (h *queryMetric) getTransport() (*http.Transport, error) {
	h.transportMu.Lock()
	defer h.transportMu.Unlock()
	if h.closed {
		return nil, errors.New("metric query transport is closed")
	}
	if h.transport != nil {
		return h.transport, nil
	}
	if h.getHTTPClient == nil {
		return nil, errors.New("metric query HTTP client is not initialized")
	}
	baseClient := h.getHTTPClient()
	if baseClient == nil {
		return nil, errors.New("metric query HTTP client is not initialized")
	}
	transport, err := newMetricQueryTransport(baseClient)
	if err != nil {
		return nil, err
	}
	h.transport = transport
	return transport, nil
}

func (h *queryMetric) close() {
	h.transportMu.Lock()
	defer h.transportMu.Unlock()
	h.closed = true
	if h.transport != nil {
		h.transport.CloseIdleConnections()
	}
}

func proxyMetricQuery(
	w http.ResponseWriter,
	r *http.Request,
	metricStorage string,
	transport http.RoundTripper,
	resolver metricIPResolver,
) {
	ctx, cancel := context.WithTimeout(r.Context(), metricQueryTimeout)
	defer cancel()

	target, err := resolveMetricStorageTarget(ctx, metricStorage, resolver)
	if err != nil {
		writeMetricQueryError(w, err)
		return
	}
	ctx = context.WithValue(ctx, metricTargetContextKey{}, target)

	proxy := newMetricReverseProxy(target, transport)
	proxy.ServeHTTP(&metricQueryResponseWriter{ResponseWriter: w}, r.WithContext(ctx))
}

func newMetricReverseProxy(target *metricTarget, transport http.RoundTripper) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(request *httputil.ProxyRequest) {
			request.Out.URL.Scheme = target.URL.Scheme
			request.Out.URL.Host = target.URL.Host
			request.Out.URL.Path = strings.Replace(request.In.URL.Path, "/pd/api/v1/metric", "/api/v1", 1)
			request.Out.URL.RawPath = ""
			// ReverseProxy sanitizes malformed queries before Rewrite. Restore the
			// original value to retain the historical transparent proxy contract.
			request.Out.URL.RawQuery = request.In.URL.RawQuery
			request.Out.Host = target.URL.Host
			// ReverseProxy already removes hop-by-hop headers, except headers needed
			// for protocol upgrades and TE trailers. The metric API supports neither.
			request.Out.Header.Del("Connection")
			request.Out.Header.Del("Upgrade")
			request.Out.Header.Del("Te")
			request.Out.Trailer = nil
			request.Out.Header.Del("Accept-Encoding")
			request.Out.Header.Set("Accept", "application/json")
		},
		Transport:      transport,
		ModifyResponse: normalizeMetricQueryResponse,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			writeMetricQueryError(w, errors.Annotate(err, "metric storage request failed"))
		},
	}
}

func normalizeMetricQueryResponse(response *http.Response) error {
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return errors.Errorf("metric storage returned status %d", response.StatusCode)
	}
	response.Header = make(http.Header, 2)
	response.Header.Set("Content-Type", "application/json; charset=utf-8")
	response.Header.Set("Cache-Control", "no-store")
	response.Trailer = nil
	return nil
}

// metricQueryResponseWriter suppresses informational responses so that no
// upstream headers are exposed before the final response is normalized.
type metricQueryResponseWriter struct {
	http.ResponseWriter
	wroteHeader bool
	lateHeader  http.Header
}

// Header returns a private header map after the final response headers have
// been written, preventing late upstream trailers from reaching the client.
func (w *metricQueryResponseWriter) Header() http.Header {
	if !w.wroteHeader {
		return w.ResponseWriter.Header()
	}
	if w.lateHeader == nil {
		w.lateHeader = make(http.Header)
	}
	return w.lateHeader
}

// WriteHeader discards informational responses and forwards final responses.
func (w *metricQueryResponseWriter) WriteHeader(statusCode int) {
	if statusCode >= 100 && statusCode < 200 {
		return
	}
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *metricQueryResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func resolveMetricStorageTarget(
	ctx context.Context,
	rawURL string,
	resolver metricIPResolver,
) (*metricTarget, error) {
	storageURL, err := parseMetricStorageURL(rawURL)
	if err != nil {
		return nil, err
	}
	target := &metricTarget{metricStorageURL: storageURL}

	var addresses []netip.Addr
	if address, ok := target.literalAddress(); ok {
		addresses = []netip.Addr{address}
	} else {
		addresses, err = resolver.LookupNetIP(ctx, "ip", target.Hostname)
		if err != nil {
			return nil, errors.Annotate(err, "failed to resolve metric-storage hostname")
		}
	}
	if len(addresses) == 0 {
		return nil, errors.New("metric-storage hostname resolved to no addresses")
	}

	for i, address := range addresses {
		addresses[i] = address.Unmap()
		if !passesMetricStorageAddressBaseline(addresses[i]) {
			return nil, errors.New("metric-storage resolved to an address outside the permitted baseline")
		}
	}

	target.addresses = addresses
	return target, nil
}

func parseMetricStorageURL(rawURL string) (*metricStorageURL, error) {
	targetURL, err := url.Parse(rawURL)
	if err != nil || targetURL.Opaque != "" || targetURL.Hostname() == "" || targetURL.User != nil ||
		targetURL.Fragment != "" || targetURL.RawFragment != "" {
		return nil, errors.New("invalid metric-storage URL")
	}
	targetURL.Scheme = strings.ToLower(targetURL.Scheme)
	if targetURL.Scheme != "http" && targetURL.Scheme != "https" {
		return nil, errors.New("metric-storage must use HTTP or HTTPS")
	}
	port := targetURL.Port()
	if port == "" {
		if strings.HasSuffix(targetURL.Host, ":") {
			return nil, errors.New("metric-storage URL has an empty port")
		}
		if targetURL.Scheme == "http" {
			port = "80"
		} else {
			port = "443"
		}
	} else {
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return nil, errors.New("metric-storage URL has an invalid port")
		}
		port = strconv.Itoa(portNumber)
	}
	return &metricStorageURL{URL: targetURL, Hostname: targetURL.Hostname(), Port: port}, nil
}

func (target *metricStorageURL) literalAddress() (netip.Addr, bool) {
	address, err := netip.ParseAddr(target.Hostname)
	if err != nil {
		return netip.Addr{}, false
	}
	return address.Unmap(), true
}

func passesMetricStorageAddressBaseline(address netip.Addr) bool {
	address = address.Unmap()
	if !address.IsValid() || address.Zone() != "" {
		return false
	}
	blocked := address.IsLoopback() ||
		address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() ||
		address.IsUnspecified() || address.IsMulticast() || !address.IsGlobalUnicast()
	if address.Is4() {
		octets := address.As4()
		blocked = blocked || octets[0] == 0 || octets[0] >= 240
	}
	// Block well-known cloud instance metadata endpoints explicitly, including
	// addresses that are not covered by the generic IP classifications above.
	switch address.String() {
	case "169.254.169.254", "100.100.100.200", "192.0.0.192", "fd00:ec2::254", "fd20:ce::254":
		blocked = true
	}
	return !blocked
}

func metricQueryDialDeadline(now, deadline time.Time, addressesRemaining int) time.Time {
	if addressesRemaining == 1 {
		return deadline
	}
	// Bound earlier attempts so one unreachable address cannot consume the overall timeout.
	timeout := min(deadline.Sub(now)/time.Duration(addressesRemaining), metricQueryDialAttemptTimeout)
	return now.Add(timeout)
}

func newMetricQueryTransport(baseClient *http.Client) (*http.Transport, error) {
	baseTransport := http.DefaultTransport
	if baseClient != nil && baseClient.Transport != nil {
		baseTransport = baseClient.Transport
	}
	transport, ok := baseTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("metric query requires an HTTP transport")
	}
	transport = transport.Clone()
	// Do not inherit a custom dialer: it could ignore the validated address.
	dialContext := (&net.Dialer{
		Timeout:   metricQueryTimeout,
		KeepAlive: 30 * time.Second,
	}).DialContext
	// A proxy would resolve and dial the target outside this transport, bypassing
	// the destination validation below.
	transport.Proxy = nil
	transport.DialTLS = nil //nolint:staticcheck // Clear the deprecated hook as well to prevent a validation bypass.
	transport.DialTLSContext = nil
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		target, ok := ctx.Value(metricTargetContextKey{}).(*metricTarget)
		if !ok {
			return nil, errors.New("metric query target is not validated")
		}
		dialDeadline, ok := ctx.Deadline()
		if !ok {
			dialDeadline = time.Now().Add(metricQueryTimeout)
		}

		var lastErr error
		for i, address := range target.addresses {
			addressesRemaining := len(target.addresses) - i
			attemptCtx, attemptCancel := context.WithDeadline(
				ctx,
				metricQueryDialDeadline(time.Now(), dialDeadline, addressesRemaining),
			)
			connection, err := dialContext(attemptCtx, network, net.JoinHostPort(address.String(), target.Port))
			attemptCancel()
			if err == nil && connection != nil {
				return connection, nil
			}
			if connection != nil {
				_ = connection.Close()
			}
			lastErr = err
			if lastErr == nil {
				lastErr = errors.New("metric-storage dial returned an empty connection")
			}
		}
		return nil, errors.Annotate(lastErr, "failed to dial metric-storage target")
	}

	return transport, nil
}

func writeMetricQueryError(w http.ResponseWriter, err error) {
	logMetricQueryError("failed to query metric storage", err)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusBadGateway)
	if _, writeErr := io.WriteString(w, metricQueryErrorBody); writeErr != nil {
		logMetricQueryError("failed to write metric query error response", writeErr)
	}
}

func logMetricQueryError(message string, err error) {
	if metricQueryErrorLogLimiter.Allow() {
		log.Warn(message, zap.Error(err))
	} else {
		log.Debug(message, zap.Error(err))
	}
}
