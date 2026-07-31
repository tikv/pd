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
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/server"
)

const (
	metricQueryTimeout             = 30 * time.Second
	metricQueryDialFallbackDelay   = 300 * time.Millisecond
	maxMetricQueryResponseBodySize = int64(32 << 20)
	metricQueryErrorBody           = `{"status":"error","errorType":"proxy","error":"metric query failed"}`
)

type metricIPResolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

type queryMetric struct {
	s            *server.Server
	transport    *http.Transport
	transportErr error
	resolver     metricIPResolver
}

type resolvedMetricTarget struct {
	url       *url.URL
	port      string
	addresses []netip.Addr
}

type metricTargetContextKey struct{}

func newqueryMetric(s *server.Server) *queryMetric {
	transport, err := newMetricQueryTransport(s.GetHTTPClient())
	return &queryMetric{
		s:            s,
		transport:    transport,
		transportErr: err,
		resolver:     net.DefaultResolver,
	}
}

func (h *queryMetric) queryMetric(w http.ResponseWriter, r *http.Request) {
	if h.transportErr != nil {
		writeMetricQueryError(w, h.transportErr)
		return
	}
	proxyMetricQuery(
		w,
		r,
		h.s.GetConfig().PDServerCfg.MetricStorage,
		h.transport,
		h.resolver,
	)
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

	request := r.Clone(ctx)
	request.RequestURI = ""
	request.URL.Scheme = target.url.Scheme
	request.URL.Host = target.url.Host
	request.URL.Path = strings.Replace(r.URL.Path, "/pd/api/v1/metric", "/api/v1", 1)
	// Preserve end-to-end headers for authenticated metric storage, matching the
	// historical proxy contract, but never forward connection-specific headers.
	request.Header = r.Header.Clone()
	removeHopByHopHeaders(request.Header)
	request.Header.Del("Accept-Encoding")
	request.Header.Set("Accept", "application/json")

	// RoundTrip does not follow redirects. The target is resolved and validated before dialing.
	response, err := transport.RoundTrip(request) //nolint:gosec
	if err != nil {
		writeMetricQueryError(w, errors.Annotate(err, "metric storage request failed"))
		return
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		writeMetricQueryError(w, errors.Errorf("metric storage returned status %d", response.StatusCode))
		return
	}

	body, err := readMetricQueryResponse(response.Body, maxMetricQueryResponseBodySize)
	if err != nil {
		writeMetricQueryError(w, err)
		return
	}
	if err := validatePrometheusQueryResponse(body); err != nil {
		writeMetricQueryError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(body); err != nil {
		log.Warn("failed to write metric query response", zap.Error(err))
	}
}

func resolveMetricStorageTarget(
	ctx context.Context,
	rawURL string,
	resolver metricIPResolver,
) (*resolvedMetricTarget, error) {
	targetURL, err := url.Parse(rawURL)
	if err != nil || targetURL.Opaque != "" || targetURL.Hostname() == "" || targetURL.User != nil || targetURL.Fragment != "" {
		return nil, errors.New("invalid metric-storage URL")
	}
	targetURL.Scheme = strings.ToLower(targetURL.Scheme)
	if targetURL.Scheme != "http" && targetURL.Scheme != "https" {
		return nil, errors.New("metric-storage must use HTTP or HTTPS")
	}
	hostname := targetURL.Hostname()
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
	} else if portNumber, err := strconv.Atoi(port); err != nil || portNumber < 1 || portNumber > 65535 {
		return nil, errors.New("metric-storage URL has an invalid port")
	}

	var addresses []netip.Addr
	if address, err := netip.ParseAddr(hostname); err == nil {
		addresses = []netip.Addr{address}
	} else {
		addresses, err = resolver.LookupNetIP(ctx, "ip", hostname)
		if err != nil {
			return nil, errors.Annotate(err, "failed to resolve metric-storage hostname")
		}
	}
	if len(addresses) == 0 {
		return nil, errors.New("metric-storage hostname resolved to no addresses")
	}

	for i, address := range addresses {
		addresses[i] = address.Unmap()
		if !isSafeMetricTargetIP(addresses[i]) {
			return nil, errors.New("metric-storage resolved to an unsafe address")
		}
	}

	return &resolvedMetricTarget{
		url:       targetURL,
		port:      port,
		addresses: addresses,
	}, nil
}

func isSafeMetricTargetIP(address netip.Addr) bool {
	address = address.Unmap()
	unsafe := !address.IsValid() || address.Zone() != "" || address.IsLoopback() ||
		address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() ||
		address.IsUnspecified() || address.IsMulticast() || !address.IsGlobalUnicast()
	if address.Is4() {
		octets := address.As4()
		unsafe = unsafe || octets[0] == 0 || octets[0] >= 240
	}
	switch address.String() {
	case "169.254.169.254", "100.100.100.200", "fd00:ec2::254":
		unsafe = true
	}
	return !unsafe
}

func metricQueryDialDeadline(now, deadline time.Time, addressesRemaining int) time.Time {
	timeRemaining := deadline.Sub(now)
	timeout := timeRemaining / time.Duration(addressesRemaining)
	if timeout < 2*time.Second {
		timeout = min(timeRemaining, 2*time.Second)
	}
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
	dialContext := transport.DialContext
	if dialContext == nil {
		dialContext = (&net.Dialer{}).DialContext
	}
	// A proxy would resolve and dial the target outside this transport, bypassing
	// the destination validation below.
	transport.Proxy = nil
	transport.DialTLS = nil //nolint:staticcheck // Clear the deprecated hook as well to prevent a validation bypass.
	transport.DialTLSContext = nil
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		target, ok := ctx.Value(metricTargetContextKey{}).(*resolvedMetricTarget)
		if !ok {
			return nil, errors.New("metric query target is not validated")
		}
		type dialResult struct {
			connection net.Conn
			err        error
		}
		dialCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		dialDeadline, ok := dialCtx.Deadline()
		if !ok {
			dialDeadline = time.Now().Add(metricQueryTimeout)
		}
		// Keep fast fallback bounded while preserving the standard Dialer's multi-address behavior.
		results := make(chan dialResult, 2)
		next, inFlight := 0, 0
		launch := func() {
			address := target.addresses[next]
			addressesRemaining := len(target.addresses) - next
			next++
			inFlight++
			attemptCtx, attemptCancel := context.WithDeadline(
				dialCtx,
				metricQueryDialDeadline(time.Now(), dialDeadline, addressesRemaining),
			)
			go func() {
				defer attemptCancel()
				connection, err := dialContext(attemptCtx, network, net.JoinHostPort(address.String(), target.port))
				results <- dialResult{connection: connection, err: err}
			}()
		}
		launch()

		var lastErr error
		for inFlight > 0 {
			var fallback <-chan time.Time
			if next < len(target.addresses) && inFlight < cap(results) {
				fallback = time.After(metricQueryDialFallbackDelay)
			}
			select {
			case result := <-results:
				inFlight--
				if result.err == nil && result.connection != nil {
					cancel()
					for inFlight > 0 {
						result := <-results
						inFlight--
						if result.connection != nil {
							_ = result.connection.Close()
						}
					}
					return result.connection, nil
				}
				lastErr = result.err
				if lastErr == nil {
					lastErr = errors.New("metric-storage dial returned an empty connection")
				}
				if next < len(target.addresses) {
					launch()
				}
			case <-fallback:
				launch()
			}
		}
		return nil, errors.Annotate(lastErr, "failed to dial metric-storage target")
	}

	return transport, nil
}

func removeHopByHopHeaders(header http.Header) {
	for _, connectionHeader := range header.Values("Connection") {
		for _, name := range strings.Split(connectionHeader, ",") {
			header.Del(strings.TrimSpace(name))
		}
	}
	for _, name := range []string{
		"Connection",
		"Proxy-Connection",
		"Keep-Alive",
		"Proxy-Authenticate",
		"Proxy-Authorization",
		"Te",
		"Trailer",
		"Transfer-Encoding",
		"Upgrade",
	} {
		header.Del(name)
	}
}

func readMetricQueryResponse(body io.Reader, limit int64) ([]byte, error) {
	limitedBody := io.LimitReader(body, limit+1)
	data, err := io.ReadAll(limitedBody)
	if err != nil {
		return nil, errors.Annotate(err, "failed to read metric query response")
	}
	if int64(len(data)) > limit {
		return nil, errors.New("metric query response exceeds the size limit")
	}
	return data, nil
}

func validatePrometheusQueryResponse(body []byte) error {
	response := struct {
		Status string `json:"status"`
		Data   *struct {
			ResultType string          `json:"resultType"`
			Result     json.RawMessage `json:"result"`
		} `json:"data"`
	}{}
	if err := json.Unmarshal(body, &response); err != nil {
		return errors.Annotate(err, "metric storage returned invalid JSON")
	}
	if response.Status != "success" || response.Data == nil {
		return errors.New("metric storage returned a non-success Prometheus response")
	}
	result := strings.TrimSpace(string(response.Data.Result))
	if len(result) < 2 || result[0] != '[' || result[len(result)-1] != ']' {
		return errors.New("metric storage returned an invalid Prometheus result")
	}
	switch response.Data.ResultType {
	case "matrix", "vector", "scalar", "string":
		return nil
	default:
		return errors.New("metric storage returned an invalid Prometheus result type")
	}
}

func writeMetricQueryError(w http.ResponseWriter, err error) {
	log.Warn("failed to query metric storage", zap.Error(err))
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusBadGateway)
	if _, writeErr := io.WriteString(w, metricQueryErrorBody); writeErr != nil {
		log.Warn("failed to write metric query error response", zap.Error(writeErr))
	}
}
