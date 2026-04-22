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

package alloc

import (
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/utils/tempurl"
)

var statusAddress = flag.String("status-addr", "0.0.0.0:0", "status address")

// RunHTTPServer runs a HTTP server to provide alloc address.
func RunHTTPServer() *http.Server {
	err := os.Setenv(tempurl.AllocURLFromUT, fmt.Sprintf("http://%s/alloc", *statusAddress))
	if err != nil {
		fmt.Println(err)
	}

	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())

	engine.GET("alloc", func(c *gin.Context) {
		addr := Alloc()
		c.String(http.StatusOK, addr)
	})

	srv := &http.Server{Addr: *statusAddress, Handler: engine.Handler(), ReadHeaderTimeout: 3 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal("server listen error", zap.Error(err))
		}
	}()

	return srv
}
