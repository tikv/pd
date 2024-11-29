// Copyright 2024 TiKV Project Authors.
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

package tlsutil

import (
	"crypto/tls"
	"net/url"
	"strings"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	httpScheme        = "http"
	httpsScheme       = "https"
	httpSchemePrefix  = "http://"
	httpsSchemePrefix = "https://"
)

// AddrsToURLs converts a list of addresses to a list of URLs.
func AddrsToURLs(addrs []string, tlsCfg *tls.Config) []string {
	// Add default schema "http://" to addrs.
	urls := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		urls = append(urls, ModifyURLScheme(addr, tlsCfg))
	}
	return urls
}

// ModifyURLScheme modifies the scheme of the URL based on the TLS config.
func ModifyURLScheme(url string, tlsCfg *tls.Config) string {
	if tlsCfg == nil {
		if strings.HasPrefix(url, httpsSchemePrefix) {
			url = httpSchemePrefix + strings.TrimPrefix(url, httpsSchemePrefix)
		} else if !strings.HasPrefix(url, httpSchemePrefix) {
			url = httpSchemePrefix + url
		}
	} else {
		if strings.HasPrefix(url, httpSchemePrefix) {
			url = httpsSchemePrefix + strings.TrimPrefix(url, httpSchemePrefix)
		} else if !strings.HasPrefix(url, httpsSchemePrefix) {
			url = httpsSchemePrefix + url
		}
	}
	return url
}

// PickMatchedURL picks the matched URL based on the TLS config.
// Note: please make sure the URLs are valid.
func PickMatchedURL(urls []string, tlsCfg *tls.Config) string {
	for _, uStr := range urls {
		u, err := url.Parse(uStr)
		if err != nil {
			continue
		}
		if tlsCfg != nil && u.Scheme == httpsScheme {
			return uStr
		}
		if tlsCfg == nil && u.Scheme == httpScheme {
			return uStr
		}
	}
	ret := ModifyURLScheme(urls[0], tlsCfg)
	log.Warn("[pd] no matched url found", zap.Strings("urls", urls),
		zap.Bool("tls-enabled", tlsCfg != nil),
		zap.String("attempted-url", ret))
	return ret
}

// TrimHTTPPrefix trims the HTTP/HTTPS prefix from the string.
func TrimHTTPPrefix(str string) string {
	str = strings.TrimPrefix(str, httpSchemePrefix)
	str = strings.TrimPrefix(str, httpsSchemePrefix)
	return str
}
