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

package config

import (
	"net/netip"
	"net/url"
	"strconv"
	"strings"

	"github.com/pingcap/errors"

	"github.com/tikv/pd/pkg/errs"
)

// MetricStorageURL is a parsed metric-storage endpoint.
type MetricStorageURL struct {
	URL      *url.URL
	Hostname string
	Port     string
}

// ParseMetricStorageURL parses and validates the syntax of a metric-storage URL.
func ParseMetricStorageURL(rawURL string) (*MetricStorageURL, error) {
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
	return &MetricStorageURL{URL: targetURL, Hostname: targetURL.Hostname(), Port: port}, nil
}

// LiteralAddress returns the metric-storage host as an IP address when it is a literal.
func (target *MetricStorageURL) LiteralAddress() (netip.Addr, bool) {
	address, err := netip.ParseAddr(target.Hostname)
	if err != nil {
		return netip.Addr{}, false
	}
	return address.Unmap(), true
}

// PassesMetricStorageAddressBaseline reports whether an IP passes the
// metric-storage destination baseline. Private unicast addresses are allowed.
func PassesMetricStorageAddressBaseline(address netip.Addr) bool {
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
	case "169.254.169.254", "100.100.100.200", "fd00:ec2::254":
		blocked = true
	}
	return !blocked
}

// ValidateMetricStorageTarget checks a metric-storage URL against the address
// baseline without resolving its hostname.
func ValidateMetricStorageTarget(rawURL string) error {
	if rawURL == "" {
		return nil
	}
	target, err := ParseMetricStorageURL(rawURL)
	if err != nil {
		return err
	}
	hostname := strings.TrimSuffix(strings.ToLower(target.Hostname), ".")
	if hostname == "localhost" || strings.HasSuffix(hostname, ".localhost") {
		return errors.New("metric-storage target is not allowed")
	}
	if address, literal := target.LiteralAddress(); literal && !PassesMetricStorageAddressBaseline(address) {
		return errors.New("metric-storage target is not allowed")
	}
	// Other hostnames are resolved and checked immediately before every query,
	// so configuration validation cannot become a stale DNS trust decision.
	return nil
}

// ValidateURLWithScheme checks the format of the URL.
func ValidateURLWithScheme(rawURL string) error {
	u, err := url.ParseRequestURI(rawURL)
	if err != nil {
		return err
	}
	if u.Scheme == "" || u.Host == "" {
		return errors.Errorf("%s has no scheme", rawURL)
	}
	return nil
}

// parseUrls parse a string into multiple urls.
func parseUrls(s string) ([]url.URL, error) {
	items := strings.Split(s, ",")
	urls := make([]url.URL, 0, len(items))
	for _, item := range items {
		u, err := url.Parse(item)
		if err != nil {
			return nil, errs.ErrURLParse.Wrap(err).GenWithStackByCause()
		}

		urls = append(urls, *u)
	}

	return urls, nil
}
