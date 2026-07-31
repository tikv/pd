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
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveMetricStorageTarget(t *testing.T) {
	resolver := safeMetricResolver()
	for _, rawURL := range []string{
		"", "file:///tmp/metrics", "http:metrics.example", "http:///metrics",
		"http://user:pass@metrics.example", "http://metrics.example:",
		"http://metrics.example:70000", "http://metrics.example/#secret",
	} {
		_, err := resolveMetricStorageTarget(context.Background(), rawURL, resolver)
		require.Error(t, err, rawURL)
	}
	for rawURL, port := range map[string]string{
		"http://metrics.example": "80", "HTTPS://metrics.example": "443", "http://[2001:db8::1]": "80",
	} {
		target, err := resolveMetricStorageTarget(context.Background(), rawURL, resolver)
		require.NoError(t, err)
		require.Equal(t, port, target.port)
	}

	unsafeAddresses := []string{
		"127.0.0.1",
		"::1",
		"0.0.0.0",
		"169.254.169.254",
		"fe80::1",
		"224.0.0.1",
		"240.0.0.1",
		"100.100.100.200",
		"fd00:ec2::254",
	}
	for _, address := range unsafeAddresses {
		resolver := staticMetricResolver{addresses: []netip.Addr{netip.MustParseAddr(address)}}
		_, err := resolveMetricStorageTarget(context.Background(), "http://metrics.example", resolver)
		require.Error(t, err, address)
	}

	for _, address := range []string{"10.0.0.1", "192.168.0.1", "fc00::1", "192.0.2.1"} {
		resolver := staticMetricResolver{addresses: []netip.Addr{netip.MustParseAddr(address)}}
		target, err := resolveMetricStorageTarget(context.Background(), "http://metrics.example", resolver)
		require.NoError(t, err)
		require.Equal(t, resolver.addresses, target.addresses)
	}

	resolver = staticMetricResolver{addresses: []netip.Addr{
		netip.MustParseAddr("192.0.2.1"),
		netip.MustParseAddr("127.0.0.1"),
	}}
	_, err := resolveMetricStorageTarget(context.Background(), "http://metrics.example", resolver)
	require.Error(t, err, "every resolved address must be safe")
}

func TestMetricQueryPinsValidatedTarget(t *testing.T) {
	dialAddresses := make(chan string, 2)
	var peer net.Conn
	baseTransport := http.DefaultTransport.(*http.Transport).Clone()
	baseTransport.DialContext = func(ctx context.Context, _, address string) (net.Conn, error) {
		dialAddresses <- address
		if strings.HasPrefix(address, "192.0.2.10:") {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		connection, otherEnd := net.Pipe()
		peer = otherEnd
		return connection, nil
	}
	transport, err := newMetricQueryTransport(&http.Client{Transport: baseTransport})
	require.NoError(t, err)
	defer transport.CloseIdleConnections()
	resolver := staticMetricResolver{addresses: []netip.Addr{
		netip.MustParseAddr("192.0.2.10"),
		netip.MustParseAddr("192.0.2.11"),
	}}
	target, err := resolveMetricStorageTarget(context.Background(), "http://metrics.example:9090", resolver)
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), metricTargetContextKey{}, target)
	connection, err := transport.DialContext(ctx, "tcp", "metrics.example:9090")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"192.0.2.10:9090", "192.0.2.11:9090"}, []string{<-dialAddresses, <-dialAddresses})
	require.NoError(t, connection.Close())
	require.NoError(t, peer.Close())
}

func TestMetricQueryResponseNormalization(t *testing.T) {
	testCases := []struct {
		name       string
		statusCode int
		body       string
		wantOK     bool
	}{
		{name: "Success", statusCode: http.StatusOK, body: successMetricResponse, wantOK: true},
		{name: "Non2xx", statusCode: http.StatusUnauthorized, body: "upstream secret"},
		{name: "Redirect", statusCode: http.StatusFound, body: "redirect secret"},
		{name: "InvalidJSON", statusCode: http.StatusOK, body: "not json"},
		{name: "PrometheusError", statusCode: http.StatusOK, body: `{"status":"error","error":"secret"}`},
		{name: "InvalidResult", statusCode: http.StatusOK, body: `{"status":"success","data":{"resultType":"vector","result":{}}}`},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var authorization string
			var requestPath string
			var removedHeader string
			transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
				authorization = r.Header.Get("Authorization")
				requestPath = r.URL.Path
				removedHeader = r.Header.Get("X-Remove")
				return &http.Response{
					StatusCode: testCase.statusCode,
					Header: http.Header{
						"Server":     []string{"private-prometheus"},
						"Set-Cookie": []string{"secret=value"},
						"Location":   []string{"http://169.254.169.254/secret"},
					},
					Body: io.NopCloser(strings.NewReader(testCase.body)),
				}, nil
			})
			request := httptest.NewRequest(http.MethodGet, "http://pd/pd/api/v1/metric/query?query=up", nil)
			request.Header.Set("Authorization", "Bearer token")
			request.Header.Set("Connection", "X-Remove")
			request.Header.Set("X-Remove", "secret")
			recorder := httptest.NewRecorder()
			proxyMetricQuery(recorder, request, "http://metrics.example:9090", transport, safeMetricResolver())

			require.Equal(t, "Bearer token", authorization)
			require.Equal(t, "/api/v1/query", requestPath)
			require.Empty(t, removedHeader)
			require.Empty(t, recorder.Header().Get("Server"))
			require.Empty(t, recorder.Header().Get("Set-Cookie"))
			require.Empty(t, recorder.Header().Get("Location"))
			if testCase.wantOK {
				require.Equal(t, http.StatusOK, recorder.Code)
				require.JSONEq(t, successMetricResponse, recorder.Body.String())
				return
			}
			requireGenericMetricQueryError(t, recorder, testCase.body)
		})
	}
}

func TestMetricQueryHidesPolicyAndNetworkErrors(t *testing.T) {
	testCases := []struct {
		metricStorage string
		resolver      metricIPResolver
		networkError  error
	}{
		{metricStorage: "http://127.0.0.1:9090", resolver: safeMetricResolver()},
		{metricStorage: "http://metrics.example:9090", resolver: safeMetricResolver(), networkError: errors.New("private network error")},
	}
	for _, testCase := range testCases {
		recorder := httptest.NewRecorder()
		transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, testCase.networkError
		})
		proxyMetricQuery(recorder, httptest.NewRequest(http.MethodGet, "/pd/api/v1/metric/query", nil),
			testCase.metricStorage, transport, testCase.resolver)
		requireGenericMetricQueryError(t, recorder, "private", "127.0.0.1")
	}
}

func TestMetricQueryResponseLimit(t *testing.T) {
	data, err := readMetricQueryResponse(strings.NewReader("1234"), 4)
	require.NoError(t, err)
	require.Equal(t, "1234", string(data))
	_, err = readMetricQueryResponse(strings.NewReader("12345"), 4)
	require.Error(t, err)
}

const successMetricResponse = `{"status":"success","data":{"resultType":"vector","result":[]}}`

type staticMetricResolver struct {
	addresses []netip.Addr
}

func (resolver staticMetricResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return resolver.addresses, nil
}

func safeMetricResolver() staticMetricResolver {
	return staticMetricResolver{addresses: []netip.Addr{netip.MustParseAddr("192.0.2.10")}}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (roundTrip roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

func requireGenericMetricQueryError(t *testing.T, recorder *httptest.ResponseRecorder, secrets ...string) {
	require.Equal(t, http.StatusBadGateway, recorder.Code)
	require.JSONEq(t, metricQueryErrorBody, recorder.Body.String())
	for _, secret := range secrets {
		require.NotContains(t, recorder.Body.String(), secret)
	}
	require.Equal(t, "application/json; charset=utf-8", recorder.Header().Get("Content-Type"))
	require.Equal(t, "no-store", recorder.Header().Get("Cache-Control"))
}
