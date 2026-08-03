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
	"sync"
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
	metricStorageConfigKey         = "metric-storage"
	prefixedMetricStorageConfigKey = "pd-server.metric-storage"
)

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

type resolvedMetricTarget struct {
	url       *url.URL
	port      string
	addresses []netip.Addr
}

type metricTargetContextKey struct{}

func newqueryMetric(s *server.Server) *queryMetric {
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

	request := r.Clone(ctx)
	request.RequestURI = ""
	request.URL.Scheme = target.url.Scheme
	request.URL.Host = target.url.Host
	request.Host = target.url.Host
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
	targetURL, hostname, port, err := parseMetricStorageURL(rawURL)
	if err != nil {
		return nil, err
	}

	var addresses []netip.Addr
	allowLoopback := false
	if address, err := netip.ParseAddr(hostname); err == nil {
		address = address.Unmap()
		addresses = []netip.Addr{address}
		// An explicit loopback address in the startup configuration is trusted for
		// compatibility with local Prometheus deployments. Runtime updates cannot
		// introduce or switch to a different loopback origin.
		allowLoopback = address.IsLoopback()
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
		if !isSafeMetricTargetIP(addresses[i], allowLoopback) {
			return nil, errors.New("metric-storage resolved to an unsafe address")
		}
	}

	return &resolvedMetricTarget{
		url:       targetURL,
		port:      port,
		addresses: addresses,
	}, nil
}

func parseMetricStorageURL(rawURL string) (targetURL *url.URL, hostname, port string, err error) {
	targetURL, err = url.Parse(rawURL)
	if err != nil || targetURL.Opaque != "" || targetURL.Hostname() == "" || targetURL.User != nil ||
		targetURL.Fragment != "" || targetURL.RawFragment != "" {
		return nil, "", "", errors.New("invalid metric-storage URL")
	}
	targetURL.Scheme = strings.ToLower(targetURL.Scheme)
	if targetURL.Scheme != "http" && targetURL.Scheme != "https" {
		return nil, "", "", errors.New("metric-storage must use HTTP or HTTPS")
	}
	hostname = targetURL.Hostname()
	port = targetURL.Port()
	if port == "" {
		if strings.HasSuffix(targetURL.Host, ":") {
			return nil, "", "", errors.New("metric-storage URL has an empty port")
		}
		if targetURL.Scheme == "http" {
			port = "80"
		} else {
			port = "443"
		}
	} else {
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return nil, "", "", errors.New("metric-storage URL has an invalid port")
		}
		port = strconv.Itoa(portNumber)
	}
	return targetURL, hostname, port, nil
}

func validateMetricStorageConfigUpdate(current string, conf map[string]any) error {
	updated, found, err := metricStorageConfigUpdate(conf)
	if err != nil || !found || updated == current || updated == "" {
		return err
	}

	updatedURL, updatedHostname, updatedPort, err := parseMetricStorageURL(updated)
	if err != nil {
		return err
	}
	updatedAddress, err := netip.ParseAddr(updatedHostname)
	if err != nil || !updatedAddress.Unmap().IsLoopback() {
		return nil
	}
	if current != "" {
		currentURL, currentHostname, currentPort, currentErr := parseMetricStorageURL(current)
		currentAddress, addressErr := netip.ParseAddr(currentHostname)
		if currentErr == nil && addressErr == nil && currentAddress.Unmap().IsLoopback() &&
			currentURL.Scheme == updatedURL.Scheme && currentAddress.Unmap() == updatedAddress.Unmap() &&
			currentPort == updatedPort {
			return nil
		}
	}
	return errors.New("changing metric-storage to a loopback target is not allowed")
}

func metricStorageConfigUpdate(conf map[string]any) (string, bool, error) {
	var (
		updated string
		found   bool
	)
	for _, key := range []string{metricStorageConfigKey, prefixedMetricStorageConfigKey} {
		value, ok := conf[key]
		if !ok {
			continue
		}
		valueString, ok := value.(string)
		if !ok {
			return "", false, errors.Errorf("config item %s must be a string", key)
		}
		if found && valueString != updated {
			return "", false, errors.New("metric-storage is specified with conflicting values")
		}
		updated = valueString
		found = true
	}
	return updated, found, nil
}

func isSafeMetricTargetIP(address netip.Addr, allowLoopback bool) bool {
	address = address.Unmap()
	if !address.IsValid() || address.Zone() != "" {
		return false
	}
	if allowLoopback && address.IsLoopback() {
		return true
	}
	unsafe := address.IsLoopback() ||
		address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() ||
		address.IsUnspecified() || address.IsMulticast() || !address.IsGlobalUnicast()
	if address.Is4() {
		octets := address.As4()
		unsafe = unsafe || octets[0] == 0 || octets[0] >= 240
	}
	// Block well-known cloud instance metadata endpoints explicitly, including
	// addresses that are not covered by the generic IP classifications above.
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
