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
	"bytes"
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
	maxMetricQueryResponseBodySize = int64(32 << 20)
	metricQueryErrorBody           = `{"status":"error","errorType":"proxy","error":"metric query failed"}`
)

type metricIPResolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

type metricDialContext func(context.Context, string, string) (net.Conn, error)

type metricProxyOptions struct {
	resolver            metricIPResolver
	dialContext         metricDialContext
	timeout             time.Duration
	maxResponseBodySize int64
}

type queryMetric struct {
	s *server.Server
}

type resolvedMetricTarget struct {
	url       *url.URL
	hostname  string
	port      string
	addresses []netip.Addr
}

func newqueryMetric(s *server.Server) *queryMetric {
	return &queryMetric{s: s}
}

func (h *queryMetric) queryMetric(w http.ResponseWriter, r *http.Request) {
	proxyMetricQuery(w, r, h.s.GetConfig().PDServerCfg.MetricStorage, h.s.GetHTTPClient(), metricProxyOptions{})
}

func proxyMetricQuery(
	w http.ResponseWriter,
	r *http.Request,
	metricStorage string,
	baseClient *http.Client,
	options metricProxyOptions,
) {
	metricPath, ok := prometheusMetricPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	options = options.withDefaults()
	ctx, cancel := context.WithTimeout(r.Context(), options.timeout)
	defer cancel()

	target, err := resolveMetricStorageTarget(ctx, metricStorage, options.resolver)
	if err != nil {
		writeMetricQueryError(w, err)
		return
	}
	client, closeIdleConnections, err := newMetricQueryHTTPClient(baseClient, target, options)
	if err != nil {
		writeMetricQueryError(w, err)
		return
	}
	defer closeIdleConnections()

	requestURL := &url.URL{
		Scheme:   target.url.Scheme,
		Host:     target.url.Host,
		Path:     metricPath,
		RawQuery: r.URL.RawQuery,
	}
	request, err := http.NewRequestWithContext(ctx, r.Method, requestURL.String(), r.Body)
	if err != nil {
		writeMetricQueryError(w, errors.Annotate(err, "failed to build metric query request"))
		return
	}
	request.ContentLength = r.ContentLength
	request.Header = r.Header.Clone()
	request.Header.Del("Accept-Encoding")
	request.Header.Set("Accept", "application/json")

	response, err := client.Do(request) //nolint:gosec // The target is resolved and validated before dialing.
	if err != nil {
		writeMetricQueryError(w, errors.Annotate(err, "metric storage request failed"))
		return
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		writeMetricQueryError(w, errors.Errorf("metric storage returned status %d", response.StatusCode))
		return
	}

	body, err := readMetricQueryResponse(response.Body, options.maxResponseBodySize)
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

func (options metricProxyOptions) withDefaults() metricProxyOptions {
	if options.resolver == nil {
		options.resolver = net.DefaultResolver
	}
	if options.dialContext == nil {
		dialer := &net.Dialer{}
		options.dialContext = dialer.DialContext
	}
	if options.timeout <= 0 {
		options.timeout = metricQueryTimeout
	}
	if options.maxResponseBodySize <= 0 {
		options.maxResponseBodySize = maxMetricQueryResponseBodySize
	}
	return options
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

	validatedAddresses := make([]netip.Addr, 0, len(addresses))
	seen := make(map[netip.Addr]struct{}, len(addresses))
	for _, address := range addresses {
		address = address.Unmap()
		if err := validateMetricTargetIP(address); err != nil {
			return nil, err
		}
		if _, ok := seen[address]; ok {
			continue
		}
		seen[address] = struct{}{}
		validatedAddresses = append(validatedAddresses, address)
	}

	return &resolvedMetricTarget{
		url:       targetURL,
		hostname:  canonicalMetricHostname(hostname),
		port:      port,
		addresses: validatedAddresses,
	}, nil
}

func parseMetricStorageURL(rawURL string) (targetURL *url.URL, hostname, port string, err error) {
	targetURL, err = url.Parse(rawURL)
	if err != nil {
		return nil, "", "", errors.Annotate(err, "invalid metric-storage URL")
	}
	targetURL.Scheme = strings.ToLower(targetURL.Scheme)
	if targetURL.Scheme != "http" && targetURL.Scheme != "https" {
		return nil, "", "", errors.Errorf("unsupported metric-storage URL scheme %q", targetURL.Scheme)
	}
	if targetURL.Opaque != "" || targetURL.Host == "" || targetURL.Hostname() == "" {
		return nil, "", "", errors.New("metric-storage must be an absolute HTTP or HTTPS URL")
	}
	if targetURL.User != nil {
		return nil, "", "", errors.New("metric-storage URL must not contain user information")
	}
	if targetURL.Fragment != "" || targetURL.RawFragment != "" {
		return nil, "", "", errors.New("metric-storage URL must not contain a fragment")
	}

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
			return nil, "", "", errors.Errorf("metric-storage URL has invalid port %q", port)
		}
	}

	hostname = canonicalMetricHostname(targetURL.Hostname())
	if hostname == "" {
		return nil, "", "", errors.New("metric-storage URL must contain a hostname")
	}
	return targetURL, hostname, port, nil
}

func validateMetricTargetIP(address netip.Addr) error {
	if !address.IsValid() || address.Zone() != "" {
		return errors.New("metric-storage resolved to an invalid address")
	}
	address = address.Unmap()
	if isMetadataServiceIP(address) {
		return errors.New("metric-storage resolved to a metadata service address")
	}
	if address.IsLoopback() {
		return errors.New("metric-storage resolved to a loopback address")
	}
	if address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() {
		return errors.New("metric-storage resolved to a link-local address")
	}
	if address.IsUnspecified() {
		return errors.New("metric-storage resolved to an unspecified address")
	}
	if address.IsMulticast() {
		return errors.New("metric-storage resolved to a multicast address")
	}
	if address.Is4() {
		octets := address.As4()
		if octets[0] == 0 || octets[0] >= 240 {
			return errors.New("metric-storage resolved to a reserved or broadcast address")
		}
	}
	if !address.IsGlobalUnicast() {
		return errors.New("metric-storage resolved to a non-unicast address")
	}
	return nil
}

func isMetadataServiceIP(address netip.Addr) bool {
	switch address.String() {
	case "169.254.169.254", "100.100.100.200", "fd00:ec2::254":
		return true
	default:
		return false
	}
}

func newMetricQueryHTTPClient(
	baseClient *http.Client,
	target *resolvedMetricTarget,
	options metricProxyOptions,
) (*http.Client, func(), error) {
	baseTransport := http.DefaultTransport
	if baseClient != nil && baseClient.Transport != nil {
		baseTransport = baseClient.Transport
	}
	transport, ok := baseTransport.(*http.Transport)
	if !ok {
		return nil, nil, errors.New("metric query requires an HTTP transport")
	}
	transport = transport.Clone()
	// A proxy would resolve and dial the target outside this transport, bypassing
	// the destination validation below.
	transport.Proxy = nil
	transport.DialTLS = nil //nolint:staticcheck // Clear the deprecated hook as well to prevent a validation bypass.
	transport.DialTLSContext = nil
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		hostname, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, errors.Annotate(err, "invalid metric query dial address")
		}
		if canonicalMetricHostname(hostname) != target.hostname || port != target.port {
			return nil, errors.New("metric query attempted to dial an unvalidated target")
		}

		var lastErr error
		for _, targetAddress := range target.addresses {
			connection, err := options.dialContext(ctx, network, net.JoinHostPort(targetAddress.String(), port))
			if err == nil {
				return connection, nil
			}
			lastErr = err
		}
		return nil, errors.Annotate(lastErr, "failed to dial metric-storage target")
	}

	timeout := options.timeout
	if baseClient != nil && baseClient.Timeout > 0 && baseClient.Timeout < timeout {
		timeout = baseClient.Timeout
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return client, transport.CloseIdleConnections, nil
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
		Status string          `json:"status"`
		Data   json.RawMessage `json:"data"`
	}{}
	if err := json.Unmarshal(body, &response); err != nil {
		return errors.Annotate(err, "metric storage returned invalid JSON")
	}
	data := bytes.TrimSpace(response.Data)
	if response.Status != "success" || len(data) == 0 || bytes.Equal(data, []byte("null")) {
		return errors.New("metric storage returned a non-success Prometheus response")
	}
	queryData := struct {
		ResultType string          `json:"resultType"`
		Result     json.RawMessage `json:"result"`
	}{}
	if err := json.Unmarshal(data, &queryData); err != nil {
		return errors.Annotate(err, "metric storage returned invalid Prometheus query data")
	}
	switch queryData.ResultType {
	case "matrix", "vector", "scalar", "string":
	default:
		return errors.New("metric storage returned an invalid Prometheus result type")
	}
	result := bytes.TrimSpace(queryData.Result)
	if len(result) < 2 || result[0] != '[' || result[len(result)-1] != ']' {
		return errors.New("metric storage returned an invalid Prometheus result")
	}
	return nil
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

func prometheusMetricPath(requestPath string) (string, bool) {
	switch requestPath {
	case "/pd/api/v1/metric/query":
		return "/api/v1/query", true
	case "/pd/api/v1/metric/query_range":
		return "/api/v1/query_range", true
	default:
		return "", false
	}
}

func canonicalMetricHostname(hostname string) string {
	return strings.TrimSuffix(strings.ToLower(hostname), ".")
}
