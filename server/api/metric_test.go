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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseMetricStorageURL(t *testing.T) {
	testCases := []struct {
		name     string
		rawURL   string
		hostname string
		port     string
		err      bool
	}{
		{
			name:     "HTTP",
			rawURL:   "http://192.0.2.1:9090",
			hostname: "192.0.2.1",
			port:     "9090",
		},
		{
			name:     "HTTPSDefaultPort",
			rawURL:   "HTTPS://Prometheus.Example./prometheus?tenant=1",
			hostname: "Prometheus.Example.",
			port:     "443",
		},
		{
			name:     "IPv6",
			rawURL:   "http://[2001:db8::1]:9090",
			hostname: "2001:db8::1",
			port:     "9090",
		},
		{name: "Empty", err: true},
		{name: "UnsupportedScheme", rawURL: "file:///tmp/prometheus", err: true},
		{name: "MissingHost", rawURL: "http:///prometheus", err: true},
		{name: "UserInfo", rawURL: "http://user:pass@prometheus:9090", err: true},
		{name: "Fragment", rawURL: "http://prometheus:9090/#fragment", err: true},
		{name: "EmptyPort", rawURL: "http://prometheus:", err: true},
		{name: "InvalidPort", rawURL: "http://prometheus:65536", err: true},
		{name: "Malformed", rawURL: "http://prometheus%41:9090", err: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			target, hostname, port, err := parseMetricStorageURL(testCase.rawURL)
			if testCase.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, target)
			require.Equal(t, testCase.hostname, hostname)
			require.Equal(t, testCase.port, port)
		})
	}
}

func TestResolveMetricStorageTargetRejectsUnsafeAddresses(t *testing.T) {
	blockedTargets := []string{
		"http://127.0.0.1:9090",
		"http://[::1]:9090",
		"http://169.254.169.254:80",
		"http://[fe80::1]:9090",
		"http://0.0.0.0:9090",
		"http://[::]:9090",
		"http://224.0.0.1:9090",
		"http://[ff02::1]:9090",
		"http://255.255.255.255:9090",
		"http://100.100.100.200:80",
		"http://[fd00:ec2::254]:80",
	}
	for _, target := range blockedTargets {
		t.Run(target, func(t *testing.T) {
			_, err := resolveMetricStorageTarget(context.Background(), target, staticMetricResolver{})
			require.Error(t, err)
		})
	}
}

func TestResolveMetricStorageTargetAllowsPrivateAddresses(t *testing.T) {
	for _, target := range []string{
		"http://10.0.0.1:9090",
		"http://172.16.0.1:9090",
		"http://192.168.0.1:9090",
		"http://[fd00::1]:9090",
	} {
		t.Run(target, func(t *testing.T) {
			resolved, err := resolveMetricStorageTarget(context.Background(), target, staticMetricResolver{})
			require.NoError(t, err)
			require.Len(t, resolved.addresses, 1)
		})
	}
}

func TestResolveMetricStorageTargetValidatesAllDNSAddresses(t *testing.T) {
	resolver := staticMetricResolver{addresses: map[string][]netip.Addr{
		"prometheus.example": {
			netip.MustParseAddr("192.0.2.10"),
			netip.MustParseAddr("127.0.0.1"),
		},
	}}
	_, err := resolveMetricStorageTarget(
		context.Background(),
		"http://prometheus.example:9090",
		resolver,
	)
	require.Error(t, err)

	resolver.addresses["prometheus.example"] = []netip.Addr{
		netip.MustParseAddr("192.0.2.10"),
		netip.MustParseAddr("192.0.2.11"),
		netip.MustParseAddr("192.0.2.10"),
	}
	resolved, err := resolveMetricStorageTarget(
		context.Background(),
		"http://prometheus.example:9090",
		resolver,
	)
	require.NoError(t, err)
	require.Equal(t, []netip.Addr{
		netip.MustParseAddr("192.0.2.10"),
		netip.MustParseAddr("192.0.2.11"),
	}, resolved.addresses)
}

func TestResolveMetricStorageTargetPreservesAbsoluteHostname(t *testing.T) {
	resolver := staticMetricResolver{addresses: map[string][]netip.Addr{
		"Prometheus.Example.": {netip.MustParseAddr("192.0.2.10")},
	}}
	resolved, err := resolveMetricStorageTarget(
		context.Background(),
		"https://Prometheus.Example.:9090",
		resolver,
	)
	require.NoError(t, err)
	require.Equal(t, "prometheus.example", resolved.hostname)
	require.Equal(t, []netip.Addr{netip.MustParseAddr("192.0.2.10")}, resolved.addresses)
}

func TestMetricQueryReturnsNormalizedPrometheusResponse(t *testing.T) {
	type observedRequest struct {
		body   string
		header http.Header
		host   string
		method string
		path   string
		query  string
	}
	var observed observedRequest
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read request body: %v", err)
			return
		}
		observed = observedRequest{
			body:   string(body),
			header: r.Header.Clone(),
			host:   r.Host,
			method: r.Method,
			path:   r.URL.Path,
			query:  r.URL.RawQuery,
		}
		w.Header().Set("Content-Type", "application/private+json")
		w.Header().Set("Server", "secret-upstream")
		w.Header().Set("Set-Cookie", "session=secret")
		w.Header().Set("WWW-Authenticate", "Basic realm=secret")
		w.Header().Set("Location", "http://169.254.169.254/")
		w.Header().Set("X-Upstream", "secret")
		if _, err = io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`); err != nil {
			t.Errorf("failed to write response body: %v", err)
		}
	})

	var dialAddress string
	options := safeMetricProxyOptions(upstream)
	baseDialContext := options.dialContext
	options.dialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		dialAddress = address
		return baseDialContext(ctx, network, address)
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"http://pd/pd/api/v1/metric/query_range?query=up&start=1",
		strings.NewReader("query=up"),
	)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Authorization", "Bearer prometheus-credential")
	request.Header.Set("X-Scope-OrgID", "tenant-1")
	request.Header.Set("Connection", "X-Remove-Me")
	request.Header.Set("X-Remove-Me", "secret hop-by-hop value")
	request.Header.Set("Proxy-Authorization", "secret proxy credential")

	recorder := httptest.NewRecorder()
	proxyMetricQuery(
		recorder,
		request,
		"http://prometheus.example:9090/base/path",
		http.DefaultClient,
		options,
	)
	response := recorder.Result()
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(t, "application/json; charset=utf-8", response.Header.Get("Content-Type"))
	require.Equal(t, "no-store", response.Header.Get("Cache-Control"))
	for _, header := range []string{"Server", "Set-Cookie", "WWW-Authenticate", "Location", "X-Upstream"} {
		require.Empty(t, response.Header.Values(header), header)
	}
	require.JSONEq(t, `{"status":"success","data":{"resultType":"vector","result":[]}}`, string(responseBody))

	require.Equal(t, "query=up", observed.body)
	require.Equal(t, "prometheus.example:9090", observed.host)
	require.Equal(t, http.MethodPost, observed.method)
	require.Equal(t, "/api/v1/query_range", observed.path)
	require.Equal(t, "query=up&start=1", observed.query)
	require.Equal(t, "application/json", observed.header.Get("Accept"))
	require.Equal(t, "Bearer prometheus-credential", observed.header.Get("Authorization"))
	require.Equal(t, "tenant-1", observed.header.Get("X-Scope-OrgID"))
	require.Empty(t, observed.header.Get("Connection"))
	require.Empty(t, observed.header.Get("X-Remove-Me"))
	require.Empty(t, observed.header.Get("Proxy-Authorization"))
	require.Equal(t, "192.0.2.10:9090", dialAddress)
}

func TestMetricQueryFallsBackFromUnreachableAddress(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, err := io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
		require.NoError(t, err)
	})
	workingDialContext := pipeHTTPDialContext(upstream)
	firstDialCanceled := make(chan struct{})
	options := metricProxyOptions{
		resolver: staticMetricResolver{addresses: map[string][]netip.Addr{
			"prometheus.example": {
				netip.MustParseAddr("192.0.2.10"),
				netip.MustParseAddr("192.0.2.11"),
			},
		}},
		dialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			if strings.HasPrefix(address, "192.0.2.10:") {
				<-ctx.Done()
				close(firstDialCanceled)
				return nil, ctx.Err()
			}
			return workingDialContext(ctx, network, address)
		},
		timeout:             time.Second,
		maxResponseBodySize: 1024,
	}
	recorder := httptest.NewRecorder()
	proxyMetricQuery(
		recorder,
		httptest.NewRequest(http.MethodGet, "http://pd/pd/api/v1/metric/query?query=up", nil),
		"http://prometheus.example:9090",
		http.DefaultClient,
		options,
	)
	require.Equal(t, http.StatusOK, recorder.Code)
	select {
	case <-firstDialCanceled:
	case <-time.After(time.Second):
		require.Fail(t, "the losing dial attempt was not canceled")
	}
}

func TestMetricQueryReusesValidatedConnection(t *testing.T) {
	var requests atomic.Int32
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, err := io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
		require.NoError(t, err)
	})
	options := safeMetricProxyOptions(upstream)
	var dials atomic.Int32
	var dialAddresses []string
	workingDialContext := options.dialContext
	options.dialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		dialAddresses = append(dialAddresses, address)
		return workingDialContext(ctx, network, address)
	}
	cache := &metricQueryClientCache{}
	defer cache.close()
	options.clientCache = cache

	for range 2 {
		recorder := httptest.NewRecorder()
		proxyMetricQuery(
			recorder,
			httptest.NewRequest(http.MethodGet, "http://pd/pd/api/v1/metric/query?query=up", nil),
			"http://prometheus.example:9090",
			http.DefaultClient,
			options,
		)
		require.Equal(t, http.StatusOK, recorder.Code)
	}
	require.Equal(t, int32(2), requests.Load())
	require.Equal(t, int32(1), dials.Load())

	resolver := options.resolver.(staticMetricResolver)
	resolver.addresses["prometheus.example"] = []netip.Addr{netip.MustParseAddr("192.0.2.11")}
	recorder := httptest.NewRecorder()
	proxyMetricQuery(
		recorder,
		httptest.NewRequest(http.MethodGet, "http://pd/pd/api/v1/metric/query?query=up", nil),
		"http://prometheus.example:9090",
		http.DefaultClient,
		options,
	)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, int32(3), requests.Load())
	require.Equal(t, int32(2), dials.Load())
	require.Equal(t, []string{"192.0.2.10:9090", "192.0.2.11:9090"}, dialAddresses)
}

func TestMetricQueryClientCacheDefersCloseUntilRelease(t *testing.T) {
	options := safeMetricProxyOptions(http.NotFoundHandler()).withDefaults()
	cache := &metricQueryClientCache{}
	options.clientCache = cache
	target, err := resolveMetricStorageTarget(
		context.Background(),
		"http://prometheus.example:9090",
		options.resolver,
	)
	require.NoError(t, err)

	clientA, releaseA, err := newMetricQueryHTTPClient(http.DefaultClient, target, options)
	require.NoError(t, err)
	entryA := cache.current
	require.Equal(t, 1, entryA.references)

	targetWithNewAddress := *target
	targetWithNewAddress.addresses = []netip.Addr{netip.MustParseAddr("192.0.2.11")}
	clientB, releaseB, err := newMetricQueryHTTPClient(http.DefaultClient, &targetWithNewAddress, options)
	require.NoError(t, err)
	require.NotSame(t, clientA, clientB)
	require.True(t, entryA.retired)
	require.Equal(t, 1, entryA.references)
	require.NotNil(t, entryA.closeIdleConnections)

	releaseA()
	require.Zero(t, entryA.references)
	require.Nil(t, entryA.closeIdleConnections)

	entryB := cache.current
	cache.close()
	require.True(t, cache.closed)
	require.Nil(t, cache.current)
	require.True(t, entryB.retired)
	require.Equal(t, 1, entryB.references)
	require.NotNil(t, entryB.closeIdleConnections)

	releaseB()
	require.Zero(t, entryB.references)
	require.Nil(t, entryB.closeIdleConnections)
	_, _, err = newMetricQueryHTTPClient(http.DefaultClient, target, options)
	require.ErrorContains(t, err, "cache is closed")
}

func TestMetricQueryDoesNotFollowRedirects(t *testing.T) {
	var requests atomic.Int32
	redirectTarget := "http://169.254.169.254/secret"
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Location", redirectTarget)
		w.WriteHeader(http.StatusFound)
		if _, err := io.WriteString(w, "secret redirect body"); err != nil {
			t.Errorf("failed to write response body: %v", err)
		}
	})

	recorder := httptest.NewRecorder()
	proxyMetricQuery(
		recorder,
		httptest.NewRequest(http.MethodGet, "http://pd/pd/api/v1/metric/query?query=up", nil),
		"http://prometheus.example:9090",
		http.DefaultClient,
		safeMetricProxyOptions(upstream),
	)
	requireGenericMetricQueryError(t, recorder, "secret", redirectTarget)
	require.Equal(t, int32(1), requests.Load())
	require.Empty(t, recorder.Header().Values("Location"))
}

func TestMetricQueryNormalizesFailures(t *testing.T) {
	testCases := []struct {
		name       string
		statusCode int
		body       string
		maxBody    int64
	}{
		{name: "Non2xx", statusCode: http.StatusUnauthorized, body: "secret upstream error"},
		{name: "InvalidJSON", statusCode: http.StatusOK, body: "secret non-json response"},
		{name: "NonPrometheusJSON", statusCode: http.StatusOK, body: `{"message":"secret"}`},
		{name: "PrometheusError", statusCode: http.StatusOK, body: `{"status":"error","error":"secret"}`},
		{name: "InvalidPrometheusData", statusCode: http.StatusOK, body: `{"status":"success","data":"secret"}`},
		{name: "InvalidPrometheusResultType", statusCode: http.StatusOK, body: `{"status":"success","data":{"resultType":"secret","result":[]}}`},
		{name: "InvalidPrometheusResult", statusCode: http.StatusOK, body: `{"status":"success","data":{"resultType":"vector","result":"secret"}}`},
		{name: "Oversized", statusCode: http.StatusOK, body: `{"status":"success","data":"secret oversized"}`, maxBody: 16},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Server", "secret-upstream")
				w.WriteHeader(testCase.statusCode)
				if _, err := io.WriteString(w, testCase.body); err != nil {
					t.Errorf("failed to write response body: %v", err)
				}
			})

			options := safeMetricProxyOptions(upstream)
			if testCase.maxBody > 0 {
				options.maxResponseBodySize = testCase.maxBody
			}
			recorder := httptest.NewRecorder()
			proxyMetricQuery(
				recorder,
				httptest.NewRequest(http.MethodGet, "http://pd/pd/api/v1/metric/query?query=up", nil),
				"http://prometheus.example:9090",
				http.DefaultClient,
				options,
			)
			requireGenericMetricQueryError(t, recorder, "secret", testCase.body)
			require.Empty(t, recorder.Header().Values("Server"))
		})
	}
}

func TestMetricQueryHidesPolicyNetworkAndTimeoutErrors(t *testing.T) {
	t.Run("Policy", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		proxyMetricQuery(
			recorder,
			httptest.NewRequest(http.MethodGet, "http://pd/pd/api/v1/metric/query?query=up", nil),
			"http://127.0.0.1:9090",
			http.DefaultClient,
			metricProxyOptions{},
		)
		requireGenericMetricQueryError(t, recorder, "loopback", "127.0.0.1")
	})

	t.Run("DNS", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		proxyMetricQuery(
			recorder,
			httptest.NewRequest(http.MethodGet, "http://pd/pd/api/v1/metric/query?query=up", nil),
			"http://prometheus.example:9090",
			http.DefaultClient,
			metricProxyOptions{resolver: staticMetricResolver{err: errors.New("secret DNS detail")}},
		)
		requireGenericMetricQueryError(t, recorder, "secret", "DNS")
	})

	t.Run("Dial", func(t *testing.T) {
		options := metricProxyOptions{
			resolver: staticMetricResolver{addresses: map[string][]netip.Addr{
				"prometheus.example": {netip.MustParseAddr("192.0.2.10")},
			}},
			dialContext: func(context.Context, string, string) (net.Conn, error) {
				return nil, errors.New("secret connection detail")
			},
			timeout:             time.Second,
			maxResponseBodySize: 1024,
		}
		recorder := httptest.NewRecorder()
		proxyMetricQuery(
			recorder,
			httptest.NewRequest(http.MethodGet, "http://pd/pd/api/v1/metric/query?query=up", nil),
			"http://prometheus.example:9090",
			http.DefaultClient,
			options,
		)
		requireGenericMetricQueryError(t, recorder, "secret", "connection")
	})

	t.Run("TLS", func(t *testing.T) {
		options := metricProxyOptions{
			resolver: staticMetricResolver{addresses: map[string][]netip.Addr{
				"prometheus.example": {netip.MustParseAddr("192.0.2.10")},
			}},
			dialContext: func(context.Context, string, string) (net.Conn, error) {
				clientConnection, serverConnection := net.Pipe()
				go func() {
					defer serverConnection.Close()
					_, _ = io.WriteString(serverConnection, "HTTP/1.1 200 OK\r\n\r\nsecret TLS detail")
				}()
				return clientConnection, nil
			},
			timeout:             time.Second,
			maxResponseBodySize: 1024,
		}
		recorder := httptest.NewRecorder()
		proxyMetricQuery(
			recorder,
			httptest.NewRequest(http.MethodGet, "http://pd/pd/api/v1/metric/query?query=up", nil),
			"https://prometheus.example:9090",
			http.DefaultClient,
			options,
		)
		requireGenericMetricQueryError(t, recorder, "secret", "TLS")
	})

	t.Run("Timeout", func(t *testing.T) {
		upstream := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		})

		options := safeMetricProxyOptions(upstream)
		options.timeout = 20 * time.Millisecond
		recorder := httptest.NewRecorder()
		proxyMetricQuery(
			recorder,
			httptest.NewRequest(http.MethodGet, "http://pd/pd/api/v1/metric/query?query=up", nil),
			"http://prometheus.example:9090",
			http.DefaultClient,
			options,
		)
		requireGenericMetricQueryError(t, recorder, "deadline", "timeout")
	})
}

func TestReadMetricQueryResponseLimit(t *testing.T) {
	body, err := readMetricQueryResponse(strings.NewReader("1234"), 4)
	require.NoError(t, err)
	require.Equal(t, "1234", string(body))

	_, err = readMetricQueryResponse(strings.NewReader("12345"), 4)
	require.Error(t, err)
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

type staticMetricResolver struct {
	addresses map[string][]netip.Addr
	err       error
}

func (resolver staticMetricResolver) LookupNetIP(
	_ context.Context,
	_ string,
	hostname string,
) ([]netip.Addr, error) {
	if resolver.err != nil {
		return nil, resolver.err
	}
	return resolver.addresses[hostname], nil
}

func safeMetricProxyOptions(upstream http.Handler) metricProxyOptions {
	return metricProxyOptions{
		resolver: staticMetricResolver{addresses: map[string][]netip.Addr{
			"prometheus.example": {netip.MustParseAddr("192.0.2.10")},
		}},
		dialContext:         pipeHTTPDialContext(upstream),
		timeout:             time.Second,
		maxResponseBodySize: 1024,
	}
}

func pipeHTTPDialContext(handler http.Handler) metricDialContext {
	return func(context.Context, string, string) (net.Conn, error) {
		clientConnection, serverConnection := net.Pipe()
		listener := &singleConnectionListener{connection: serverConnection}
		go func() {
			server := &http.Server{Handler: handler, ReadHeaderTimeout: time.Second}
			_ = server.Serve(listener)
		}()
		return clientConnection, nil
	}
}

type singleConnectionListener struct {
	connection net.Conn
}

func (listener *singleConnectionListener) Accept() (net.Conn, error) {
	if listener.connection == nil {
		return nil, net.ErrClosed
	}
	connection := listener.connection
	listener.connection = nil
	return connection, nil
}

func (listener *singleConnectionListener) Close() error {
	if listener.connection == nil {
		return nil
	}
	err := listener.connection.Close()
	listener.connection = nil
	return err
}

func (*singleConnectionListener) Addr() net.Addr {
	return pipeAddress("metric-test")
}

type pipeAddress string

func (pipeAddress) Network() string        { return "pipe" }
func (address pipeAddress) String() string { return string(address) }

func requireGenericMetricQueryError(t *testing.T, recorder *httptest.ResponseRecorder, secrets ...string) {
	response := recorder.Result()
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadGateway, response.StatusCode)
	require.Equal(t, "application/json; charset=utf-8", response.Header.Get("Content-Type"))
	require.JSONEq(t, metricQueryErrorBody, string(body))
	for _, secret := range secrets {
		require.NotContains(t, string(body), secret)
	}
}
