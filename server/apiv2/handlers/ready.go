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

package handlers

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/middlewares"
)

// RegisterReadyCheck registers the ready check handlers.
func RegisterReadyCheck(r *gin.RouterGroup) {
	// --- Liveness Checks ---
	livezRegistry := NewCheckRegistry()
	livezRegistry.register("etcd-serializable-read", checkEtcdSerializableRead)
	livezRegistry.installHandlers(r, "/livez")

	// --- Readiness Checks ---
	readyzRegistry := NewCheckRegistry()
	readyzRegistry.register("leader-promotion", checkLeaderPromotion)
	readyzRegistry.register("etcd-data-corruption", checkEtcdDataCorruption)
	readyzRegistry.register("etcd-serializable-read", checkEtcdSerializableRead)
	readyzRegistry.register("etcd-linearizable-read", checkEtcdLinearizableRead)
	readyzRegistry.register("etcd-non-learner", checkEtcdNonLearner)
	readyzRegistry.installHandlers(r, "/readyz")

	// --- Backward Compatibility ---
	// The old /ready endpoint is now an alias for the leader-promotion readiness check.
	r.GET("ready", Ready)
}

// checkFunction defines the function signature for a single check.
// It returns an error if the check fails.
type checkFunction func(ctx context.Context, svr *server.Server) error

// checkRegistry manages a set of checks for a specific endpoint (e.g., "livez", "readyz").
type checkRegistry struct {
	checks map[string]checkFunction
}

// NewCheckRegistry creates a new registry.
func NewCheckRegistry() *checkRegistry {
	return &checkRegistry{
		checks: make(map[string]checkFunction),
	}
}

// register adds a new check to the registry.
func (reg *checkRegistry) register(name string, check checkFunction) {
	if _, exists := reg.checks[name]; exists {
		log.Warn("check function is already registered", zap.String("name", name))
	}
	reg.checks[name] = check
}

// installHandlers registers the HTTP handlers for the root endpoint and all individual checks.
func (reg *checkRegistry) installHandlers(r *gin.RouterGroup, basePath string) {
	checkNames := make([]string, 0, len(reg.checks))
	for name := range reg.checks {
		checkNames = append(checkNames, name)
	}
	sort.Strings(checkNames) // ensure consistent order

	rootHandler := reg.createRootHandler(checkNames)
	r.GET(basePath, rootHandler)

	for _, name := range checkNames {
		checkName := name
		checkFunc := reg.checks[checkName]
		fullCheckPath := basePath + "/" + checkName // e.g., "/readyz/leader-promotion"
		r.GET(fullCheckPath, func(c *gin.Context) {
			svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
			if err := checkFunc(c.Request.Context(), svr); err != nil {
				c.String(http.StatusServiceUnavailable, "[-]%s failed: %v\n", checkName, err)
				return
			}
			c.Status(http.StatusOK)
			if _, verbose := c.GetQuery("verbose"); verbose {
				fmt.Fprintf(c.Writer, "[+]%s ok\n", checkName)
			}
			fmt.Fprintf(c.Writer, "ok\n")
		})
	}
}

// createRootHandler creates the main handler that runs multiple checks.
func (reg *checkRegistry) createRootHandler(allChecks []string) gin.HandlerFunc {
	return func(c *gin.Context) {
		svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
		excludeSet := make(map[string]struct{})
		if excluded, ok := c.GetQueryArray("exclude"); ok {
			for _, item := range excluded {
				excludeSet[item] = struct{}{}
			}
		}

		checksToRun := make([]string, 0, len(allChecks))
		for _, name := range allChecks {
			if _, excluded := excludeSet[name]; !excluded {
				checksToRun = append(checksToRun, name)
			}
		}

		var overallStatus = http.StatusOK
		var output bytes.Buffer

		for _, name := range checksToRun {
			check := reg.checks[name]
			if err := check(c.Request.Context(), svr); err != nil {
				overallStatus = http.StatusServiceUnavailable
				fmt.Fprintf(&output, "[-]%s failed: %v\n", name, err)
			} else {
				fmt.Fprintf(&output, "[+]%s ok\n", name)
			}
		}

		if overallStatus != http.StatusOK {
			c.String(overallStatus, output.String())
			return
		}

		c.Status(http.StatusOK)
		if _, verbose := c.GetQuery("verbose"); verbose {
			fmt.Fprint(c.Writer, output.String())
		}
		fmt.Fprintf(c.Writer, "ok\n")
	}
}

// checkLeaderPromotion checks if the pd follower is ready to become leader.
func checkLeaderPromotion(_ context.Context, svr *server.Server) error {
	s := svr.GetStorage()
	if !storage.IsBootstrapped(s) {
		// TODO: Do we still need it after introducing readyz?
		// Not bootstrapped is considered ready for this specific check.
		return nil
	}

	regionLoaded := storage.AreRegionsLoaded(s)
	failpoint.Inject("loadRegionSlow", func(val failpoint.Value) {
		if addr, ok := val.(string); ok && svr.GetAddr() == addr {
			regionLoaded = false
		}
	})

	if !regionLoaded {
		return errors.New("regions not loaded")
	}
	return nil
}

// checkEtcdSerializableRead checks if a local etcd read is ok.
func checkEtcdSerializableRead(ctx context.Context, svr *server.Server) error {
	failpoint.Inject("etcdSerializableReadError", func(val failpoint.Value) {
		if addr, ok := val.(string); ok && addr == svr.GetAddr() {
			failpoint.Return(errors.New("injected serializable read error"))
		}
	})
	// Ref: https://github.com/etcd-io/etcd/blob/9a5533382d84999e4e79642e1ec0f8bfa9b70ba8/server/etcdserver/api/etcdhttp/health.go#L454
	etcdServer := svr.GetMember().Etcd().Server
	_, err := etcdServer.Range(ctx, &etcdserverpb.RangeRequest{KeysOnly: true, Limit: 1, Serializable: true})
	if err != nil {
		return errors.New("etcd serializable read failed: " + err.Error())
	}
	return nil
}

// checkEtcdLinearizableRead checks if there is consensus in the cluster.
func checkEtcdLinearizableRead(ctx context.Context, svr *server.Server) error {
	failpoint.Inject("etcdLinearizableReadError", func(val failpoint.Value) {
		if addr, ok := val.(string); ok && addr == svr.GetAddr() {
			failpoint.Return(errors.New("injected linearizable read error"))
		}
	})
	// Ref: https://github.com/etcd-io/etcd/blob/9a5533382d84999e4e79642e1ec0f8bfa9b70ba8/server/etcdserver/api/etcdhttp/health.go#L454
	etcdServer := svr.GetMember().Etcd().Server
	_, err := etcdServer.Range(ctx, &etcdserverpb.RangeRequest{KeysOnly: true, Limit: 1, Serializable: false})
	if err != nil {
		return errors.New("etcd linearizable read failed: " + err.Error())
	}
	return nil
}

// checkEtcdDataCorruption checks if there is an active data corruption alarm.
func checkEtcdDataCorruption(_ context.Context, svr *server.Server) error {
	failpoint.Inject("etcdDataCorruptionAlarm", func(val failpoint.Value) {
		if addr, ok := val.(string); ok && addr == svr.GetAddr() {
			failpoint.Return(errors.New("injected data corruption alarm"))
		}
	})
	// Ref: https://github.com/etcd-io/etcd/blob/9a5533382d84999e4e79642e1ec0f8bfa9b70ba8/server/etcdserver/api/etcdhttp/health.go#L284
	etcdServer := svr.GetMember().Etcd().Server
	for _, alarm := range etcdServer.Alarms() {
		if alarm.Alarm == etcdserverpb.AlarmType_CORRUPT {
			return errors.New("etcd data corruption alarm is active")
		}
	}
	return nil
}

// checkEtcdNonLearner checks if the member is not a learner.
func checkEtcdNonLearner(_ context.Context, svr *server.Server) error {
	failpoint.Inject("etcdIsLearner", func(val failpoint.Value) {
		if addr, ok := val.(string); ok && addr == svr.GetAddr() {
			failpoint.Return(errors.New("injected learner state"))
		}
	})
	// Ref: https://github.com/etcd-io/etcd/commit/989c556645115201fdfcb4ba3026867f03709975
	etcdServer := svr.GetMember().Etcd().Server
	if etcdServer.IsLearner() {
		return errors.New("member is a learner")
	}
	return nil
}

// ReadyStatus reflects the cluster's ready status.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type ReadyStatus struct {
	RegionLoaded   bool `json:"region_loaded"`
	IsBootstrapped bool `json:"is_bootstrapped"`
}

// Ready checks if the region is loaded.
//
//	@Description	It will return whether pd follower is ready to became leader.
//	@Deprecated		This endpoint is deprecated. Please use /readyz/leader-promotion instead.
//	@Summary		It will return whether pd follower is ready to became leader. This request is always served by the instance that receives it and never forwarded to the leader.
//	@Router			/ready [get]
//	@Param			verbose	query	bool	false	"Whether to return details."
//	@Success		200
//	@Failure		500
func Ready(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	err := checkLeaderPromotion(c.Request.Context(), svr)

	if _, ok := c.GetQuery("verbose"); !ok {
		if err != nil {
			c.Status(http.StatusInternalServerError)
		} else {
			c.Status(http.StatusOK)
		}

		return
	}

	s := svr.GetStorage()
	isBootstrapped := storage.IsBootstrapped(s)
	regionLoaded := isBootstrapped && err == nil
	resp := &ReadyStatus{
		RegionLoaded:   regionLoaded,
		IsBootstrapped: isBootstrapped,
	}
	if !isBootstrapped || regionLoaded {
		c.IndentedJSON(http.StatusOK, resp)
	} else {
		c.AbortWithStatusJSON(http.StatusInternalServerError, resp)
	}
}
