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
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/server"
)

const (
	metricStorageConfigKey         = "metric-storage"
	prefixedMetricStorageConfigKey = "pd-server.metric-storage"
)

type queryMetric struct {
	s *server.Server
}

func newqueryMetric(s *server.Server) *queryMetric {
	return &queryMetric{s: s}
}

func (h *queryMetric) queryMetric(w http.ResponseWriter, r *http.Request) {
	metricAddr := h.s.GetConfig().PDServerCfg.MetricStorage
	if metricAddr == "" {
		http.Error(w, "metric storage doesn't set", http.StatusInternalServerError)
		return
	}
	target, _, err := parseMetricStorageURL(metricAddr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	metricPath, ok := prometheusMetricPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}

	client := h.s.GetHTTPClient()
	var transport http.RoundTripper
	if client != nil {
		transport = client.Transport
	}
	// The target comes from the validated metric-storage configuration, not from this request.
	newMetricReverseProxy(target, metricPath, transport).ServeHTTP(w, r) //nolint:gosec // G704 is a false positive.
}

func validateMetricStorageConfigUpdate(
	r *http.Request,
	current string,
	conf map[string]any,
) (int, error) {
	updated, ok, err := metricStorageConfigUpdate(conf)
	if err != nil {
		return http.StatusBadRequest, err
	}
	if !ok || updated == current {
		return 0, nil
	}

	updatedOrigin := ""
	if updated != "" {
		_, updatedOrigin, err = parseMetricStorageURL(updated)
		if err != nil {
			return http.StatusBadRequest, err
		}
	}
	if current != "" {
		_, currentOrigin, currentErr := parseMetricStorageURL(current)
		if currentErr == nil && currentOrigin == updatedOrigin {
			return 0, nil
		}
	}
	if isMutuallyAuthenticated(r) {
		return 0, nil
	}
	return http.StatusForbidden, errors.New("changing metric-storage target requires a mutually authenticated TLS connection")
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

func isMutuallyAuthenticated(r *http.Request) bool {
	return r.TLS != nil && len(r.TLS.PeerCertificates) > 0 && len(r.TLS.VerifiedChains) > 0
}

func parseMetricStorageURL(rawURL string) (*url.URL, string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, "", errors.Annotate(err, "invalid metric-storage URL")
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return nil, "", errors.Errorf("unsupported metric-storage URL scheme %q", u.Scheme)
	}
	if u.Opaque != "" || u.Host == "" || u.Hostname() == "" {
		return nil, "", errors.New("metric-storage must be an absolute HTTP or HTTPS URL")
	}
	if u.User != nil {
		return nil, "", errors.New("metric-storage URL must not contain user information")
	}
	if u.Fragment != "" || u.RawFragment != "" {
		return nil, "", errors.New("metric-storage URL must not contain a fragment")
	}

	port := u.Port()
	if port == "" {
		if scheme == "http" {
			port = "80"
		} else {
			port = "443"
		}
	} else {
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return nil, "", errors.Errorf("metric-storage URL has invalid port %q", port)
		}
		port = strconv.Itoa(portNumber)
	}

	hostname := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if hostname == "" {
		return nil, "", errors.New("metric-storage URL must contain a hostname")
	}
	u.Scheme = scheme
	return u, scheme + "://" + net.JoinHostPort(hostname, port), nil
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

func newMetricReverseProxy(target *url.URL, metricPath string, transport http.RoundTripper) *httputil.ReverseProxy {
	origin := &url.URL{Scheme: target.Scheme, Host: target.Host}
	proxy := httputil.NewSingleHostReverseProxy(origin)
	proxy.Transport = transport
	director := proxy.Director
	proxy.Director = func(req *http.Request) {
		director(req)
		req.URL.Path = metricPath
		req.URL.RawPath = ""
		req.Host = origin.Host
		removeMetricProxySensitiveHeaders(req.Header)
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
		log.Warn("failed to query metric storage", zap.Error(err))
		http.Error(w, "failed to query metric storage", http.StatusBadGateway)
	}
	return proxy
}

func removeMetricProxySensitiveHeaders(header http.Header) {
	for key := range header {
		canonicalKey := http.CanonicalHeaderKey(key)
		switch {
		case canonicalKey == "Authorization",
			canonicalKey == "Component",
			canonicalKey == "Cookie",
			canonicalKey == "Forwarded",
			canonicalKey == "Proxy-Authorization",
			canonicalKey == http.CanonicalHeaderKey(apiutil.PDRedirectorHeader),
			canonicalKey == http.CanonicalHeaderKey(apiutil.PDAllowFollowerHandleHeader),
			canonicalKey == http.CanonicalHeaderKey(apiutil.XCallerIDHeader),
			canonicalKey == http.CanonicalHeaderKey(apiutil.XForbiddenForwardToMicroserviceHeader),
			canonicalKey == http.CanonicalHeaderKey(apiutil.XForwardedToMicroserviceHeader),
			canonicalKey == http.CanonicalHeaderKey(apiutil.XPDHandleHeader),
			canonicalKey == http.CanonicalHeaderKey(apiutil.XRealIPHeader),
			strings.HasPrefix(canonicalKey, "X-Forwarded-"):
			header.Del(key)
		}
	}
	// A nil value tells httputil.ReverseProxy not to add the client address back
	// after the Director returns.
	header[apiutil.XForwardedForHeader] = nil
}
