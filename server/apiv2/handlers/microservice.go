// Copyright 2023 TiKV Project Authors.
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

package handlers

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/gin-gonic/gin"

	"github.com/tikv/pd/pkg/mcs/discovery"
	mcs "github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/middlewares"
)

// tsoPrimaryEvictPath is the tso-node-local endpoint that transfers away every
// keyspace group primary held by the node.
const tsoPrimaryEvictPath = "/tso/api/v1/primary/evict"

// RegisterMicroservice registers microservice handler to the router.
func RegisterMicroservice(r *gin.RouterGroup) {
	router := r.Group("ms")
	router.GET("members/:service", GetMembers)
	router.GET("primary/:service", GetPrimary)
	router.POST("tso/primary/evict", EvictTSOPrimary)
}

// GetMembers gets all members of the cluster for the specified service.
//
//	@Tags		members
//	@Summary	Get all members of the cluster for the specified service.
//	@Produce	json
//	@Success	200	{object}	[]discovery.ServiceRegistryEntry
//	@Router		/ms/members/{service} [get]
func GetMembers(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	if !svr.IsKeyspaceGroupEnabled() {
		c.AbortWithStatusJSON(http.StatusNotFound, "not support microservice")
		return
	}

	if service := c.Param("service"); len(service) > 0 {
		entries, err := discovery.GetMSMembers(service, svr.GetClient())
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}
		c.IndentedJSON(http.StatusOK, entries)
		return
	}

	c.AbortWithStatusJSON(http.StatusInternalServerError, "please specify service")
}

// GetPrimary gets the primary member of the specified service.
//
//	@Tags		primary
//	@Summary	Get the primary member of the specified service.
//	@Produce	json
//	@Success	200	{object}	string
//	@Router		/ms/primary/{service} [get]
func GetPrimary(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	if !svr.IsKeyspaceGroupEnabled() {
		c.AbortWithStatusJSON(http.StatusNotFound, "not support microservice")
		return
	}

	if service := c.Param("service"); len(service) > 0 {
		addr, _ := svr.GetServicePrimaryAddr(c.Request.Context(), service)
		c.IndentedJSON(http.StatusOK, addr)
		return
	}

	c.AbortWithStatusJSON(http.StatusInternalServerError, "please specify service")
}

// EvictTSOPrimary forwards a request that evicts all keyspace group primaries
// held by the given tso node to that node. The eviction is node-local, so the
// caller only needs to reach PD, which proxies the request to the target node.
//
//	@Tags		primary
//	@Summary	Evict all keyspace group primaries held by the given tso node.
//	@Param		node	query	string	true	"the target tso node address"
//	@Produce	json
//	@Success	200	{object}	map[string]string
//	@Failure	400	{string}	string	"The input is invalid."
//	@Failure	500	{string}	string	"PD server failed to proceed the request."
//	@Router		/ms/tso/primary/evict [post]
func EvictTSOPrimary(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	if !svr.IsKeyspaceGroupEnabled() {
		c.AbortWithStatusJSON(http.StatusNotFound, "not support microservice")
		return
	}
	node := c.Query("node")
	if len(node) == 0 {
		c.AbortWithStatusJSON(http.StatusBadRequest, "please specify the tso node")
		return
	}
	// Only forward to a node that is a known tso member so that PD is not turned
	// into an open proxy to an arbitrary address.
	entries, err := discovery.GetMSMembers(mcs.TSOServiceName, svr.GetClient())
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	// Match by base URL so that a scheme (http/https) or advertise-listen-addr
	// difference does not cause a false negative, and forward to the member's
	// canonical address rather than the raw client input.
	target := ""
	for _, entry := range entries {
		if typeutil.EqualBaseURLs(entry.ServiceAddr, node) {
			target = entry.ServiceAddr
			break
		}
	}
	if target == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, fmt.Sprintf("%s is not a tso node", node))
		return
	}
	// The registered address may be a bare advertise-listen-addr without a scheme
	// (e.g. 127.0.0.1:3379), which url.Parse cannot turn into a usable proxy
	// target, so default a missing scheme from the server's TLS setup.
	u, err := resolveProxyURL(target, httpSchemeFromTLS(svr.GetTLSConfig()))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	// NewCustomReverseProxies keeps the request path, so rewrite it to the tso
	// node's evict endpoint and drop the query before forwarding.
	c.Request.URL.Path = tsoPrimaryEvictPath
	c.Request.URL.RawQuery = ""
	apiutil.NewCustomReverseProxies(svr.GetHTTPClient(), []url.URL{*u}).ServeHTTP(c.Writer, c.Request)
	c.Abort()
}

// httpSchemeFromTLS returns the URL scheme PD uses to reach its microservice
// nodes: "https" when TLS is configured, "http" otherwise.
func httpSchemeFromTLS(tls *grpcutil.TLSConfig) string {
	if tls != nil && len(tls.CertPath) != 0 {
		return "https"
	}
	return "http"
}

// resolveProxyURL turns a registry service address into a URL usable by the
// reverse proxy. A registered address may be stored without a scheme (a bare
// advertise-listen-addr such as 127.0.0.1:3379), in which case url.Parse yields
// an empty Host; the given default scheme is applied so the proxy target is
// valid. An address that already carries a scheme is kept as-is.
func resolveProxyURL(serviceAddr, defaultScheme string) (*url.URL, error) {
	if u, err := url.Parse(serviceAddr); err == nil && u.Host != "" {
		return u, nil
	}
	return url.Parse(defaultScheme + "://" + typeutil.TrimScheme(serviceAddr))
}
