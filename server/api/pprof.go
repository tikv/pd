// Copyright 2021 TiKV Project Authors.
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
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	pp "net/http/pprof"
	"runtime"
	"runtime/pprof"
	"strconv"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/versioninfo"
	"github.com/unrolled/render"
	"go.uber.org/zap"
)

// pprofHandler pprof handler
type pprofHandler struct {
	svr *server.Server
	rd  *render.Render
}

// newPprofHandler constructor for ProfHandler
func newPprofHandler(svr *server.Server, rd *render.Render) *pprofHandler {
	return &pprofHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags     debug
// @Summary  debug zip of PD servers.
// @Produce  application/octet-stream
// @Router   /debug/pprof/zip [get]
func (h *pprofHandler) PProfZip(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Disposition", `attachment; filename="pd_debug"`+time.Now().Format("20060102_150405")+".zip")

	// dump goroutine/heap/mutex
	items := []struct {
		name   string
		gc     int
		debug  int
		second int
	}{
		{name: "goroutine", debug: 2},
		{name: "heap", gc: 1},
		{name: "mutex"},
		{name: "allocs"},
	}
	zw := zip.NewWriter(w)
	for _, item := range items {
		p := pprof.Lookup(item.name)
		if p == nil {
			h.rd.JSON(w, http.StatusNotFound, fmt.Sprintf("pprof can not find name: %s", item.name))
			return
		}
		if item.gc > 0 {
			runtime.GC()
		}
		fw, err := zw.Create(item.name)
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, fmt.Sprintf("Create zipped %s fail: %v", item.name, err))
			return
		}
		err = p.WriteTo(fw, item.debug)
		if err != nil {
			log.Error("write failed", zap.Error(err))
		}
	}

	// dump profile
	fw, err := zw.Create("profile")
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, fmt.Sprintf("create zip %s failed: %v", "profile", err))
		return
	}
	if err := pprof.StartCPUProfile(fw); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, fmt.Sprintf("Could not enable CPU profiling: %v", err))
		return
	}
	sec, err := strconv.ParseInt(r.FormValue("seconds"), 10, 64)
	if sec <= 0 || err != nil {
		sec = 30
	}
	sleepWithCtx(r.Context(), time.Duration(sec)*time.Second)
	pprof.StopCPUProfile()

	// dump config
	fw, err = zw.Create("config")
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, fmt.Sprintf("Create zipped %s fail: %v", "config", err))
		return
	}
	// dump config all
	config := h.svr.GetConfig()
	js, err := json.Marshal(config)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, fmt.Sprintf("json marshal config info fail: %v", err))
		return
	}
	if _, err = fw.Write(js); err != nil {
		log.Error("write config failed", zap.Error(err))
	}

	// dump version
	fw, err = zw.Create("version")
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, fmt.Sprintf("Create zipped %s fail: %v", "version", err))
		return
	}
	versions, err := json.Marshal(&version{
		Version:   versioninfo.PDReleaseVersion,
		Branch:    versioninfo.PDGitBranch,
		BuildTime: versioninfo.PDBuildTS,
		Hash:      versioninfo.PDGitHash,
	})
	if err != nil {
		log.Error("json marshal version failed", zap.Error(err))
	}
	if _, err = fw.Write(versions); err != nil {
		log.Error("write version failed", zap.Error(err))
	}

	if err = zw.Close(); err != nil {
		log.Error("zip close error", zap.Error(err))
	}
}

// @Tags     debug
// @Summary  debug profile of PD servers.
// @Router   /debug/pprof/profile [get]
func (h *pprofHandler) PProfProfile(w http.ResponseWriter, r *http.Request) {
	pp.Profile(w, r)
}

// @Tags     debug
// @Summary  debug trace of PD servers.
// @Router   /debug/pprof/trace [get]
func (h *pprofHandler) PProfTrace(w http.ResponseWriter, r *http.Request) {
	pp.Trace(w, r)
}

// @Tags     debug
// @Summary  debug symbol of PD servers.
// @Router   /debug/pprof/symbol [get]
func (h *pprofHandler) PProfSymbol(w http.ResponseWriter, r *http.Request) {
	pp.Symbol(w, r)
}

// @Tags     debug
// @Summary  debug heap of PD servers.
// @Router   /debug/pprof/heap [get]
func (h *pprofHandler) PProfHeap(w http.ResponseWriter, r *http.Request) {
	pp.Handler("heap").ServeHTTP(w, r)
}

// @Tags     debug
// @Summary  debug mutex of PD servers.
// @Router   /debug/pprof/mutex [get]
func (h *pprofHandler) PProfMutex(w http.ResponseWriter, r *http.Request) {
	pp.Handler("mutex").ServeHTTP(w, r)
}

// @Tags     debug
// @Summary  debug allocs of PD servers.
// @Router   /debug/pprof/allocs [get]
func (h *pprofHandler) PProfAllocs(w http.ResponseWriter, r *http.Request) {
	pp.Handler("allocs").ServeHTTP(w, r)
}

// @Tags     debug
// @Summary  debug block of PD servers.
// @Router   /debug/pprof/block [get]
func (h *pprofHandler) PProfBlock(w http.ResponseWriter, r *http.Request) {
	pp.Handler("block").ServeHTTP(w, r)
}

// @Tags     debug
// @Summary  debug goroutine of PD servers.
// @Router   /debug/pprof/goroutine [get]
func (h *pprofHandler) PProfGoroutine(w http.ResponseWriter, r *http.Request) {
	pp.Handler("goroutine").ServeHTTP(w, r)
}

// @Tags     debug
// @Summary  debug threadcreate of PD servers.
// @Router   /debug/pprof/threadcreate [get]
func (h *pprofHandler) PProfThreadcreate(w http.ResponseWriter, r *http.Request) {
	pp.Handler("threadcreate").ServeHTTP(w, r)
}

func sleepWithCtx(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}
