// Copyright 2026 TiKV Project Authors.
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
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tikv/pd/pkg/utils/apiutil"
)

func TestParseMetricStorageURL(t *testing.T) {
	testCases := []struct {
		name   string
		rawURL string
		origin string
		err    bool
	}{
		{
			name:   "HTTP",
			rawURL: "http://127.0.0.1:9090",
			origin: "http://127.0.0.1:9090",
		},
		{
			name:   "HTTPSDefaultPort",
			rawURL: "HTTPS://Prometheus.Example./prometheus?tenant=1",
			origin: "https://prometheus.example:443",
		},
		{
			name:   "IPv6",
			rawURL: "http://[2001:db8::1]:9090",
			origin: "http://[2001:db8::1]:9090",
		},
		{name: "Empty", err: true},
		{name: "UnsupportedScheme", rawURL: "file:///tmp/prometheus", err: true},
		{name: "MissingHost", rawURL: "http:///prometheus", err: true},
		{name: "UserInfo", rawURL: "http://user@prometheus:9090", err: true},
		{name: "Fragment", rawURL: "http://prometheus:9090/#fragment", err: true},
		{name: "InvalidPort", rawURL: "http://prometheus:65536", err: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			re := require.New(t)
			target, origin, err := parseMetricStorageURL(testCase.rawURL)
			if testCase.err {
				re.Error(err)
				return
			}
			re.NoError(err)
			re.NotNil(target)
			re.Equal(testCase.origin, origin)
		})
	}
}

func TestValidateMetricStorageConfigUpdate(t *testing.T) {
	re := require.New(t)
	request := httptest.NewRequest(http.MethodPost, "http://pd/pd/api/v1/config", nil)

	statusCode, err := validateMetricStorageConfigUpdate(request, "", map[string]any{"schedule.leader-schedule-limit": 1})
	re.NoError(err)
	re.Zero(statusCode)

	statusCode, err = validateMetricStorageConfigUpdate(request, "http://prometheus:9090", map[string]any{
		metricStorageConfigKey: "http://prometheus:9090",
	})
	re.NoError(err)
	re.Zero(statusCode)

	statusCode, err = validateMetricStorageConfigUpdate(request, "http://prometheus:80", map[string]any{
		prefixedMetricStorageConfigKey: "HTTP://PROMETHEUS./prometheus",
	})
	re.NoError(err)
	re.Zero(statusCode)

	statusCode, err = validateMetricStorageConfigUpdate(request, "http://prometheus:9090", map[string]any{
		metricStorageConfigKey: "http://other-prometheus:9090",
	})
	re.EqualError(err, "changing metric-storage target requires a mutually authenticated TLS connection")
	re.Equal(http.StatusForbidden, statusCode)

	statusCode, err = validateMetricStorageConfigUpdate(request, "", map[string]any{
		metricStorageConfigKey: "file:///tmp/prometheus",
	})
	re.Error(err)
	re.Equal(http.StatusBadRequest, statusCode)

	statusCode, err = validateMetricStorageConfigUpdate(request, "", map[string]any{
		metricStorageConfigKey: 9090,
	})
	re.EqualError(err, "config item metric-storage must be a string")
	re.Equal(http.StatusBadRequest, statusCode)

	statusCode, err = validateMetricStorageConfigUpdate(request, "", map[string]any{
		metricStorageConfigKey:         "http://prometheus:9090",
		prefixedMetricStorageConfigKey: "http://other-prometheus:9090",
	})
	re.EqualError(err, "metric-storage is specified with conflicting values")
	re.Equal(http.StatusBadRequest, statusCode)

	certificate := &x509.Certificate{}
	request.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{certificate},
		VerifiedChains:   [][]*x509.Certificate{{certificate}},
	}
	statusCode, err = validateMetricStorageConfigUpdate(request, "http://prometheus:9090", map[string]any{
		metricStorageConfigKey: "http://other-prometheus:9090",
	})
	re.NoError(err)
	re.Zero(statusCode)

	statusCode, err = validateMetricStorageConfigUpdate(request, "http://prometheus:9090", map[string]any{
		metricStorageConfigKey: "",
	})
	re.NoError(err)
	re.Zero(statusCode)
}

func TestMetricReverseProxy(t *testing.T) {
	type observedRequest struct {
		body   string
		header http.Header
		host   string
		method string
		path   string
		query  string
	}

	var observed observedRequest
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		observed = observedRequest{
			body:   string(body),
			header: r.Header.Clone(),
			host:   r.Host,
			method: r.Method,
			path:   r.URL.Path,
			query:  r.URL.RawQuery,
		}
		return &http.Response{
			StatusCode: http.StatusCreated,
			Status:     "201 Created",
			Header: http.Header{
				"Content-Type":          {"application/json"},
				"X-Prometheus-Response": {"true"},
			},
			Body:    io.NopCloser(strings.NewReader(`{"status":"success"}`)),
			Request: r,
		}, nil
	})

	target, err := url.Parse("http://prometheus.internal:9090")
	require.NoError(t, err)
	proxy := newMetricReverseProxy(target, "/api/v1/query_range", transport)

	request := httptest.NewRequest(
		http.MethodPost,
		"http://pd/pd/api/v1/metric/query_range?query=up&start=1",
		strings.NewReader("query=up"),
	)
	request.Header.Set("Authorization", "Bearer pd-credential")
	request.Header.Set("Component", "pdctl")
	request.Header.Set("Cookie", "session=secret")
	request.Header.Set("Forwarded", "for=192.0.2.1")
	request.Header.Set("Proxy-Authorization", "Basic secret")
	request.Header.Set(apiutil.PDRedirectorHeader, "pd-1")
	request.Header.Set(apiutil.PDAllowFollowerHandleHeader, "true")
	request.Header.Set(apiutil.XCallerIDHeader, "caller")
	request.Header.Set(apiutil.XForbiddenForwardToMicroserviceHeader, "true")
	request.Header.Set(apiutil.XForwardedToMicroserviceHeader, "true")
	request.Header.Set(apiutil.XPDHandleHeader, "pd-1")
	request.Header.Set(apiutil.XRealIPHeader, "192.0.2.1")
	request.Header.Set(apiutil.XForwardedForHeader, "192.0.2.1")
	request.Header.Set("X-Forwarded-Proto", "https")
	request.Header.Set("X-Scope-OrgID", "tenant-1")

	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, request) //nolint:gosec // The test target uses a mock transport.
	response := recorder.Result()
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, response.StatusCode)
	require.Equal(t, "true", response.Header.Get("X-Prometheus-Response"))
	require.JSONEq(t, `{"status":"success"}`, string(responseBody))

	require.Equal(t, "query=up", observed.body)
	require.Equal(t, target.Host, observed.host)
	require.Equal(t, http.MethodPost, observed.method)
	require.Equal(t, "/api/v1/query_range", observed.path)
	require.Equal(t, "query=up&start=1", observed.query)
	require.Equal(t, "tenant-1", observed.header.Get("X-Scope-OrgID"))
	for _, header := range []string{
		"Authorization",
		"Component",
		"Cookie",
		"Forwarded",
		"Proxy-Authorization",
		apiutil.PDRedirectorHeader,
		apiutil.PDAllowFollowerHandleHeader,
		apiutil.XCallerIDHeader,
		apiutil.XForbiddenForwardToMicroserviceHeader,
		apiutil.XForwardedToMicroserviceHeader,
		apiutil.XPDHandleHeader,
		apiutil.XRealIPHeader,
		apiutil.XForwardedForHeader,
		"X-Forwarded-Proto",
	} {
		require.Empty(t, observed.header.Values(header), header)
	}
}

func TestMetricReverseProxyDoesNotFollowRedirects(t *testing.T) {
	requests := 0
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		return &http.Response{
			StatusCode: http.StatusFound,
			Status:     "302 Found",
			Header: http.Header{
				"Location": {"http://169.254.169.254/latest/meta-data"},
			},
			Body:    io.NopCloser(strings.NewReader("redirect")),
			Request: r,
		}, nil
	})
	target, err := url.Parse("http://prometheus.internal:9090")
	require.NoError(t, err)
	proxy := newMetricReverseProxy(target, "/api/v1/query", transport)
	request := httptest.NewRequest(http.MethodGet, "http://pd/pd/api/v1/metric/query?query=up", nil)
	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, request) //nolint:gosec // The test target uses a mock transport.
	response := recorder.Result()
	response.Body.Close()
	require.Equal(t, http.StatusFound, response.StatusCode)
	require.Equal(t, "http://169.254.169.254/latest/meta-data", response.Header.Get("Location"))
	require.Equal(t, 1, requests)
}

func TestPrometheusMetricPath(t *testing.T) {
	path, ok := prometheusMetricPath("/pd/api/v1/metric/query")
	require.True(t, ok)
	require.Equal(t, "/api/v1/query", path)

	path, ok = prometheusMetricPath("/pd/api/v1/metric/query_range")
	require.True(t, ok)
	require.Equal(t, "/api/v1/query_range", path)

	path, ok = prometheusMetricPath("/pd/api/v1/metric/other")
	require.False(t, ok)
	require.Empty(t, path)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
