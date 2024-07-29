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

package multiservicesapi

import (
	"net/http"
	"net/url"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"go.uber.org/zap"
)

const (
	// ServiceAllowDirectHandle is the header key to allow direct handle.
	ServiceAllowDirectHandle = "service-allow-direct-handle"
	// ServiceRedirectorHeader is the header key to indicate the request is redirected.
	ServiceRedirectorHeader = "service-redirector"
	// ServiceContextKey is the key to get service server from gin.Context.
	ServiceContextKey = "service"
)

// ServiceRedirector is a middleware to redirect the request to the right place.
func ServiceRedirector() gin.HandlerFunc {
	return func(c *gin.Context) {
		svr := c.MustGet(ServiceContextKey).(bs.Server)
		allowDirectHandle := len(c.Request.Header.Get(ServiceAllowDirectHandle)) > 0
		isServing := svr.IsServing()
		if allowDirectHandle || isServing {
			c.Next()
			return
		}

		// Prevent more than one redirection.
		if name := c.Request.Header.Get(ServiceRedirectorHeader); len(name) != 0 {
			log.Error("redirect but server is not primary", zap.String("from", name), zap.String("server", svr.Name()), errs.ZapError(errs.ErrRedirectToNotPrimary))
			c.AbortWithStatusJSON(http.StatusInternalServerError, errs.ErrRedirectToNotPrimary.FastGenByArgs().Error())
			return
		}

		c.Request.Header.Set(ServiceRedirectorHeader, svr.Name())

		listenUrls := svr.GetLeaderListenUrls()
		if listenUrls == nil {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, errs.ErrLeaderNil.FastGenByArgs().Error())
			return
		}
		urls := make([]url.URL, 0, len(listenUrls))
		for _, item := range listenUrls {
			u, err := url.Parse(item)
			if err != nil {
				c.AbortWithStatusJSON(http.StatusInternalServerError, errs.ErrURLParse.Wrap(err).GenWithStackByCause().Error())
				return
			}

			urls = append(urls, *u)
		}

		client := svr.GetHTTPClient()
		apiutil.NewCustomReverseProxies(client, urls).ServeHTTP(c.Writer, c.Request)
		c.Abort()
	}
}
