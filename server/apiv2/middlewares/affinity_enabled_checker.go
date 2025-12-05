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

package middlewares

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server"
)

// AffinitySchedulingEnabledChecker blocks requests when affinity scheduling is disabled.
func AffinitySchedulingEnabledChecker() gin.HandlerFunc {
	return func(c *gin.Context) {
		svr := c.MustGet(ServerContextKey).(*server.Server)
		if !svr.GetPersistOptions().IsAffinitySchedulingEnabled() {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, errs.ErrAffinityDisabled.FastGenByArgs().Error())
			return
		}
		c.Next()
	}
}
