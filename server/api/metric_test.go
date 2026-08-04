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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/netip"
	"net/textproto"
	"strings"
	"testing"
	"time"

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
		require.Equal(t, port, target.Port)
	}
	for _, rawURL := range []string{
		"http://127.0.0.1:9090",
		"http://127.1.2.3:9090",
		"http://[::1]:9090",
		"http://[::ffff:127.0.0.1]:9090",
	} {
		_, err := resolveMetricStorageTarget(context.Background(), rawURL, resolver)
		require.Error(t, err, rawURL)
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

func TestValidateMetricStorageConfigUpdate(t *testing.T) {
	const legacyTarget = "file:///tmp/legacy-metrics"
	testCases := []struct {
		name    string
		conf    map[string]any
		current string
		wantErr bool
	}{
		{name: "Unrelated", conf: map[string]any{"schedule.leader-schedule-limit": 1}},
		{name: "PrivateTarget", conf: map[string]any{metricStorageConfigKey: "http://192.168.0.1:9090"}},
		{name: "InvalidTarget", conf: map[string]any{metricStorageConfigKey: "file:///tmp/metrics"}, wantErr: true},
		{name: "UnchangedLegacyTarget", conf: map[string]any{metricStorageConfigKey: legacyTarget}, current: legacyTarget},
		{name: "UnchangedPrefixedLegacyTarget", conf: map[string]any{prefixedMetricStorageConfigKey: legacyTarget}, current: legacyTarget},
		{name: "ChangedLegacyTarget", conf: map[string]any{metricStorageConfigKey: "http://127.0.0.1:9090"}, current: legacyTarget, wantErr: true},
		{name: "NewLoopback", conf: map[string]any{metricStorageConfigKey: "http://127.0.0.1:9090"}, wantErr: true},
		{name: "NewPrefixedLoopback", conf: map[string]any{prefixedMetricStorageConfigKey: "http://[::1]:9090"}, wantErr: true},
		{name: "Localhost", conf: map[string]any{metricStorageConfigKey: "http://localhost:9090"}, wantErr: true},
		{name: "Metadata", conf: map[string]any{metricStorageConfigKey: "http://100.100.100.200"}, wantErr: true},
		{name: "Unspecified", conf: map[string]any{metricStorageConfigKey: "http://0.0.0.0"}, wantErr: true},
		{name: "ClearLoopback", conf: map[string]any{metricStorageConfigKey: ""}},
		{
			name: "DifferentKeys",
			conf: map[string]any{
				metricStorageConfigKey:         "http://192.168.0.1:9090",
				prefixedMetricStorageConfigKey: "http://192.168.0.2:9090",
			},
		},
		{
			name: "UnsafeSecondKey",
			conf: map[string]any{
				metricStorageConfigKey:         "http://192.168.0.1:9090",
				prefixedMetricStorageConfigKey: "http://127.0.0.1:9090",
			},
			wantErr: true,
		},
		{name: "NonString", conf: map[string]any{metricStorageConfigKey: 1}, wantErr: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateMetricStorageConfigUpdate(testCase.conf, testCase.current)
			if testCase.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestMetricQueryTransportInitializesAfterHTTPClient(t *testing.T) {
	var baseClient *http.Client
	handler := &queryMetric{
		getHTTPClient: func() *http.Client { return baseClient },
	}
	_, err := handler.getTransport()
	require.Error(t, err)

	rootCAs := x509.NewCertPool()
	baseTransport := http.DefaultTransport.(*http.Transport).Clone()
	baseTransport.TLSClientConfig = &tls.Config{
		Certificates: []tls.Certificate{{}},
		RootCAs:      rootCAs,
		MinVersion:   tls.VersionTLS12,
	}
	baseClient = &http.Client{Transport: baseTransport}
	transport, err := handler.getTransport()
	require.NoError(t, err)
	require.NotSame(t, baseTransport, transport)
	require.Same(t, rootCAs, transport.TLSClientConfig.RootCAs)
	require.Len(t, transport.TLSClientConfig.Certificates, 1)
	require.Equal(t, uint16(tls.VersionTLS12), transport.TLSClientConfig.MinVersion)

	cachedTransport, err := handler.getTransport()
	require.NoError(t, err)
	require.Same(t, transport, cachedTransport)
	handler.close()
	_, err = handler.getTransport()
	require.Error(t, err)
}

func TestMetricQueryPinsValidatedTarget(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	_, port, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)

	baseDialerCalled := false
	baseTransport := http.DefaultTransport.(*http.Transport).Clone()
	baseTransport.DialContext = func(context.Context, string, string) (net.Conn, error) {
		baseDialerCalled = true
		return nil, errors.New("custom dialer must not be used")
	}
	transport, err := newMetricQueryTransport(&http.Client{Transport: baseTransport})
	require.NoError(t, err)
	defer transport.CloseIdleConnections()
	target, err := resolveMetricStorageTarget(
		context.Background(),
		"http://metrics.example:"+port,
		safeMetricResolver(),
	)
	require.NoError(t, err)
	target.addresses = []netip.Addr{
		netip.MustParseAddr("127.0.0.2"),
		netip.MustParseAddr("127.0.0.1"),
	}
	ctx, cancel := context.WithTimeout(context.Background(), metricQueryTimeout)
	defer cancel()
	ctx = context.WithValue(ctx, metricTargetContextKey{}, target)
	var dialAddresses []string
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		ConnectStart: func(_, address string) {
			dialAddresses = append(dialAddresses, address)
		},
	})
	connection, err := transport.DialContext(ctx, "tcp", "metrics.example:9090")
	require.NoError(t, err)
	require.False(t, baseDialerCalled)
	require.Equal(t, []string{"127.0.0.2:" + port, "127.0.0.1:" + port}, dialAddresses)
	require.Equal(t, listener.Addr().String(), connection.RemoteAddr().String())
	require.NoError(t, connection.Close())
}

func TestMetricQueryDialDeadline(t *testing.T) {
	now := time.Unix(0, 0)
	deadline := now.Add(metricQueryTimeout)
	require.Equal(t, now.Add(metricQueryDialAttemptTimeout), metricQueryDialDeadline(now, deadline, 3))
	require.Equal(t, deadline, metricQueryDialDeadline(now, deadline, 1))

	shortDeadline := now.Add(time.Second)
	require.Equal(t, now.Add(500*time.Millisecond), metricQueryDialDeadline(now, shortDeadline, 2))
}

func TestMetricQueryResponseNormalization(t *testing.T) {
	testCases := []struct {
		name       string
		statusCode int
		body       string
		wantOK     bool
	}{
		{name: "Success", statusCode: http.StatusOK, body: successMetricResponse, wantOK: true},
		{name: "Accepted", statusCode: http.StatusAccepted, body: successMetricResponse, wantOK: true},
		{name: "Non2xx", statusCode: http.StatusUnauthorized, body: "upstream secret"},
		{name: "Redirect", statusCode: http.StatusFound, body: "redirect secret"},
		{name: "InvalidJSON", statusCode: http.StatusOK, body: "not json"},
		{name: "PrometheusError", statusCode: http.StatusOK, body: `{"status":"error","error":"secret"}`},
		{name: "InvalidResult", statusCode: http.StatusOK, body: `{"status":"success","data":{"resultType":"vector","result":{}}}`},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var authorization string
			var tenant string
			var requestBody string
			var requestMethod string
			var requestPath string
			var requestQuery string
			var requestHost string
			var removedHeader string
			var removedTrailer string
			transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
				authorization = r.Header.Get("Authorization")
				tenant = r.Header.Get("X-Scope-OrgID")
				body, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				requestBody = string(body)
				requestMethod = r.Method
				requestPath = r.URL.Path
				requestQuery = r.URL.RawQuery
				requestHost = r.Host
				removedHeader = r.Header.Get("X-Remove")
				removedTrailer = r.Trailer.Get("X-Secret-Trailer")
				return &http.Response{
					StatusCode: testCase.statusCode,
					Header: http.Header{
						"Server":     []string{"private-prometheus"},
						"Set-Cookie": []string{"secret=value"},
						"Location":   []string{"http://169.254.169.254/secret"},
					},
					Body:    io.NopCloser(strings.NewReader(testCase.body)),
					Trailer: http.Header{"X-Secret-Trailer": []string{"secret"}},
				}, nil
			})
			request := httptest.NewRequest(
				http.MethodPost,
				"http://pd/pd/api/v1/metric/query?query=up;down&tenant=a+b",
				strings.NewReader("query=up"),
			)
			request.Host = "admin.internal"
			request.Header.Set("Authorization", "Bearer token")
			request.Header.Set("X-Scope-OrgID", "tenant-1")
			request.Header.Set("Connection", "X-Remove")
			request.Header.Set("X-Remove", "secret")
			request.Trailer = http.Header{"X-Secret-Trailer": []string{"secret"}}
			recorder := httptest.NewRecorder()
			proxyMetricQuery(recorder, request, "http://metrics.example:9090", transport, safeMetricResolver())

			require.Equal(t, "Bearer token", authorization)
			require.Equal(t, "tenant-1", tenant)
			require.Equal(t, "query=up", requestBody)
			require.Equal(t, http.MethodPost, requestMethod)
			require.Equal(t, "/api/v1/query", requestPath)
			require.Equal(t, "query=up;down&tenant=a+b", requestQuery)
			require.Equal(t, "metrics.example:9090", requestHost)
			require.Empty(t, removedHeader)
			require.Empty(t, removedTrailer)
			require.Empty(t, recorder.Header().Get("Server"))
			require.Empty(t, recorder.Header().Get("Set-Cookie"))
			require.Empty(t, recorder.Header().Get("Location"))
			require.Empty(t, recorder.Header().Get("X-Secret-Trailer"))
			if testCase.wantOK {
				require.Equal(t, testCase.statusCode, recorder.Code)
				require.JSONEq(t, successMetricResponse, recorder.Body.String())
				return
			}
			requireGenericMetricQueryError(t, recorder, testCase.body)
		})
	}
}

func TestMetricQuerySuppressesInformationalResponse(t *testing.T) {
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		trace := httptrace.ContextClientTrace(request.Context())
		require.NotNil(t, trace)
		require.NotNil(t, trace.Got1xxResponse)
		err := trace.Got1xxResponse(http.StatusEarlyHints, textproto.MIMEHeader{
			"Link": []string{"<http://internal.example/secret>"},
		})
		require.NoError(t, err)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(successMetricResponse)),
		}, nil
	})
	recorder := httptest.NewRecorder()
	proxyMetricQuery(
		recorder,
		httptest.NewRequest(http.MethodGet, "/pd/api/v1/metric/query", nil),
		"http://metrics.example:9090",
		transport,
		safeMetricResolver(),
	)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Empty(t, recorder.Header().Get("Link"))
	require.JSONEq(t, successMetricResponse, recorder.Body.String())
}

func TestMetricQueryRejectsExplicitLoopback(t *testing.T) {
	for _, metricStorage := range []string{"http://127.0.0.1:9090", "http://[::1]:9090"} {
		t.Run(metricStorage, func(t *testing.T) {
			transportCalled := false
			transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
				transportCalled = true
				return nil, errors.New("unexpected metric-storage request")
			})
			recorder := httptest.NewRecorder()
			proxyMetricQuery(
				recorder,
				httptest.NewRequest(http.MethodGet, "/pd/api/v1/metric/query", nil),
				metricStorage,
				transport,
				safeMetricResolver(),
			)
			require.False(t, transportCalled)
			requireGenericMetricQueryError(t, recorder, metricStorage)
		})
	}
}

func TestMetricQueryRejectsInvalidConfiguredURL(t *testing.T) {
	for _, metricStorage := range []string{"file:///tmp/metrics", "http://user:pass@metrics.example", "not a URL"} {
		t.Run(metricStorage, func(t *testing.T) {
			transportCalled := false
			transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
				transportCalled = true
				return nil, errors.New("unexpected metric-storage request")
			})
			recorder := httptest.NewRecorder()
			proxyMetricQuery(
				recorder,
				httptest.NewRequest(http.MethodGet, "/pd/api/v1/metric/query", nil),
				metricStorage,
				transport,
				safeMetricResolver(),
			)
			require.False(t, transportCalled)
			requireGenericMetricQueryError(t, recorder, metricStorage)
		})
	}
}

func TestMetricQueryHidesPolicyAndNetworkErrors(t *testing.T) {
	testCases := []struct {
		metricStorage string
		resolver      metricIPResolver
		networkError  error
	}{
		{metricStorage: "http://metrics.example:9090", resolver: staticMetricResolver{addresses: []netip.Addr{netip.MustParseAddr("127.0.0.1")}}},
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

func TestValidatePrometheusQueryResponseResultShape(t *testing.T) {
	testCases := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{name: "EmptyArray", body: `{"status":"success","data":{"resultType":"vector","result":[]}}`},
		{name: "NestedArray", body: `{"status":"success","data":{"resultType":"vector","result":[[1], {"value":2}]}}`},
		{name: "Whitespace", body: "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\": \n[1, 2]\t}}"},
		{name: "Missing", body: `{"status":"success","data":{"resultType":"vector"}}`, wantErr: true},
		{name: "Null", body: `{"status":"success","data":{"resultType":"vector","result":null}}`, wantErr: true},
		{name: "Object", body: `{"status":"success","data":{"resultType":"vector","result":{}}}`, wantErr: true},
		{name: "String", body: `{"status":"success","data":{"resultType":"vector","result":"[]"}}`, wantErr: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := validatePrometheusQueryResponse([]byte(testCase.body))
			if testCase.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func BenchmarkValidatePrometheusQueryResponseLargeResult(b *testing.B) {
	body := []byte(`{"status":"success","data":{"resultType":"vector","result":[` +
		strings.Repeat("0,", 1<<19) + `0]}}`)
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := validatePrometheusQueryResponse(body); err != nil {
			b.Fatal(err)
		}
	}
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
