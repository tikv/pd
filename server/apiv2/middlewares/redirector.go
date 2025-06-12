// Copyright 2022 TiKV Project Authors.
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

package middlewares

import (
	"net/http"
	"net/url"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/server"
)

// Redirector is a middleware to redirect the request to the right place.
func Redirector() gin.HandlerFunc {
	return func(c *gin.Context) {
		svr := c.MustGet(ServerContextKey).(*server.Server)

		if svr.IsClosed() {
			c.AbortWithStatusJSON(http.StatusInternalServerError, errs.ErrServerNotStarted.FastGenByArgs().Error())
			return
		}
		allowFollowerHandle := len(c.Request.Header.Get(apiutil.PDAllowFollowerHandleHeader)) > 0
		if allowFollowerHandle || svr.GetMember().IsLeader() {
			c.Next()
			return
		}

		// Prevent more than one redirection.
		if name := c.Request.Header.Get(apiutil.PDRedirectorHeader); len(name) != 0 {
			log.Error("redirect but server is not leader", zap.String("from", name), zap.String("server", svr.Name()), errs.ZapError(errs.ErrRedirectToNotLeader))
			c.AbortWithStatusJSON(http.StatusInternalServerError, errs.ErrRedirectToNotLeader.FastGenByArgs().Error())
			return
		}

		c.Request.Header.Set(apiutil.PDRedirectorHeader, svr.Name())

		if svr.GetMember().GetLeader() == nil {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, errs.ErrLeaderNil.FastGenByArgs().Error())
			return
		}
		clientUrls := svr.GetMember().GetLeader().GetClientUrls()
		urls := make([]url.URL, 0, len(clientUrls))
		for _, item := range clientUrls {
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
