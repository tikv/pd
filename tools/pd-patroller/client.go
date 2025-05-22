// Copyright 2025 TiKV Project Authors.
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

// It is a copy from pd

package main

import (
	"crypto/tls"
	"strings"
)

const (
	httpScheme        = "http"
	httpsScheme       = "https"
	httpSchemePrefix  = "http://"
	httpsSchemePrefix = "https://"
)

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

// TrimHTTPPrefix trims the HTTP/HTTPS prefix from the string.
func TrimHTTPPrefix(str string) string {
	str = strings.TrimPrefix(str, httpSchemePrefix)
	str = strings.TrimPrefix(str, httpsSchemePrefix)
	return str
}
