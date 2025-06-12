// Copyright 2020 TiKV Project Authors.
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

package adapter

import (
	"crypto/tls"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/pingcap/tidb-dashboard/pkg/apiserver"
	"github.com/pingcap/tidb-dashboard/pkg/utils"

	"github.com/tikv/pd/pkg/utils/syncutil"
)

const (
	proxyHeader = "Via"
)

// Redirector is used to redirect when the dashboard is started in another PD.
type Redirector struct {
	mu syncutil.RWMutex

	name      string
	tlsConfig *tls.Config

	address string
	proxy   *httputil.ReverseProxy
	// The status of the dashboard in the cluster.
	// It is not equal to `apiserver.Service.status`.
	status *utils.ServiceStatus
}

// NewRedirector creates a new Redirector.
func NewRedirector(name string, tlsConfig *tls.Config) *Redirector {
	return &Redirector{
		name:      name,
		tlsConfig: tlsConfig,
		status:    utils.NewServiceStatus(),
	}
}

// SetAddress is used to set a new address to be redirected.
func (h *Redirector) SetAddress(addr string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.address == addr {
		return
	}

	if addr == "" {
		h.status.Stop()
		h.address = ""
		h.proxy = nil
		return
	}

	h.status.Start()
	h.address = addr
	target, _ := url.Parse(addr) // error has been handled
	h.proxy = httputil.NewSingleHostReverseProxy(target)

	defaultDirector := h.proxy.Director
	h.proxy.Director = func(r *http.Request) {
		defaultDirector(r)
		r.Header.Add(proxyHeader, h.name)
	}

	if h.tlsConfig != nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = h.tlsConfig
		h.proxy.Transport = transport
	}
}

// GetAddress is used to get the address to be redirected.
func (h *Redirector) GetAddress() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.address
}

// GetProxy is used to get the reverse proxy arriving at address.
func (h *Redirector) GetProxy() *httputil.ReverseProxy {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.proxy
}

// TemporaryRedirect sends the status code 307 to the client, and the client redirects itself.
func (h *Redirector) TemporaryRedirect(w http.ResponseWriter, r *http.Request) {
	addr := h.GetAddress()
	if addr == "" {
		apiserver.StoppedHandler.ServeHTTP(w, r)
		return
	}
	http.Redirect(w, r, addr+r.RequestURI, http.StatusTemporaryRedirect)
}

// ReverseProxy forwards the request to address and returns the response to the client.
func (h *Redirector) ReverseProxy(w http.ResponseWriter, r *http.Request) {
	proxy := h.GetProxy()
	if proxy == nil {
		apiserver.StoppedHandler.ServeHTTP(w, r)
		return
	}

	proxySources := r.Header.Values(proxyHeader)
	for _, proxySource := range proxySources {
		if proxySource == h.name {
			w.WriteHeader(http.StatusLoopDetected)
			return
		}
	}

	proxy.ServeHTTP(w, r)
}

// NewStatusAwareHandler returns a Handler that switches between different Handlers based on status.
func (h *Redirector) NewStatusAwareHandler(handler http.Handler) http.Handler {
	return h.status.NewStatusAwareHandler(handler, apiserver.StoppedHandler)
}
