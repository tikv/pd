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

package controller

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/metastorage"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/resource_group/controller/metrics"
)

const (
	defaultResourceGroupName = "default"
	controllerConfigPath     = "resource_group/controller"
	maxNotificationChanLen   = 200
	needTokensAmplification  = 1.1
	trickleReserveDuration   = 1250 * time.Millisecond
	slowNotifyFilterDuration = 10 * time.Millisecond

	watchRetryInterval = 30 * time.Second

	bigRequestThreshold = 4 * 1024 * 1024 // 4MB -> 16 RRU
)

type selectType int

const (
	periodicReport selectType = 0
	lowToken       selectType = 1
)

var enableControllerTraceLog atomic.Bool

func logControllerTrace(msg string, fields ...zap.Field) {
	if enableControllerTraceLog.Load() {
		log.Info(msg, fields...)
	}
}

// ResourceGroupKVInterceptor is used as quota limit controller for resource group using kv store.
type ResourceGroupKVInterceptor interface {
	// OnRequestWait is used to check whether resource group has enough tokens. It maybe needs to wait some time.
	OnRequestWait(ctx context.Context, resourceGroupName string, info RequestInfo) (*rmpb.Consumption, *rmpb.Consumption, time.Duration, uint32, error)
	// OnResponse is used to consume tokens after receiving response.
	OnResponse(resourceGroupName string, req RequestInfo, resp ResponseInfo) (*rmpb.Consumption, error)
	// OnResponseWait is used to consume tokens after receiving a response. If the response requires many tokens, we need to wait for the tokens.
	// This is an optimized version of OnResponse for cases where the response requires many tokens, making the debt smaller and smoother.
	OnResponseWait(ctx context.Context, resourceGroupName string, req RequestInfo, resp ResponseInfo) (*rmpb.Consumption, time.Duration, error)
	// IsBackgroundRequest If the resource group has background jobs, we should not record consumption and wait for it.
	IsBackgroundRequest(ctx context.Context, resourceGroupName, requestResource string) bool
}

// ResourceGroupProvider provides some api to interact with resource manager server.
type ResourceGroupProvider interface {
	GetResourceGroup(ctx context.Context, resourceGroupName string, opts ...pd.GetResourceGroupOption) (*rmpb.ResourceGroup, error)
	ListResourceGroups(ctx context.Context, opts ...pd.GetResourceGroupOption) ([]*rmpb.ResourceGroup, error)
	AddResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error)
	ModifyResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error)
	DeleteResourceGroup(ctx context.Context, resourceGroupName string) (string, error)
	AcquireTokenBuckets(ctx context.Context, request *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error)
	LoadResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, int64, error)

	metastorage.Client
}

// ResourceControlCreateOption create a ResourceGroupsController with the optional settings.
type ResourceControlCreateOption func(controller *ResourceGroupsController)

// EnableSingleGroupByKeyspace is the option to enable single group by keyspace feature.
func EnableSingleGroupByKeyspace() ResourceControlCreateOption {
	return func(controller *ResourceGroupsController) {
		controller.ruConfig.isSingleGroupByKeyspace = true
	}
}

// WithMaxWaitDuration is the option to set the max wait duration for acquiring token buckets.
func WithMaxWaitDuration(d time.Duration) ResourceControlCreateOption {
	return func(controller *ResourceGroupsController) {
		controller.ruConfig.LTBMaxWaitDuration = d
	}
}

// WithWaitRetryInterval is the option to set the retry interval when waiting for the token.
func WithWaitRetryInterval(d time.Duration) ResourceControlCreateOption {
	return func(controller *ResourceGroupsController) {
		controller.ruConfig.WaitRetryInterval = d
	}
}

// WithWaitRetryTimes is the option to set the times to retry when waiting for the token.
func WithWaitRetryTimes(times int) ResourceControlCreateOption {
	return func(controller *ResourceGroupsController) {
		controller.ruConfig.WaitRetryTimes = times
	}
}

// WithDegradedModeWaitDuration is the option to set the wait duration for degraded mode.
func WithDegradedModeWaitDuration(d time.Duration) ResourceControlCreateOption {
	return func(controller *ResourceGroupsController) {
		controller.ruConfig.DegradedModeWaitDuration = d
	}
}

// WithDegradedRUSettings is the option to set the degraded RU settings for the resource group.
func WithDegradedRUSettings(degradedRUSettings *rmpb.GroupRequestUnitSettings) ResourceControlCreateOption {
	return func(controller *ResourceGroupsController) {
		controller.degradedRUSettings = degradedRUSettings
	}
}

var _ ResourceGroupKVInterceptor = (*ResourceGroupsController)(nil)

// ResourceGroupsController implements ResourceGroupKVInterceptor.
type ResourceGroupsController struct {
	clientUniqueID   uint64
	provider         ResourceGroupProvider
	groupsController sync.Map
	ruConfig         *RUConfig
	keyspaceID       uint32

	loopCtx    context.Context
	loopCancel func()

	calculators []ResourceCalculator

	// When a signal is received, it means the number of available token is low.
	lowTokenNotifyChan chan notifyMsg
	// When a token bucket response received from server, it will be sent to the channel.
	tokenResponseChan chan []*rmpb.TokenBucketResponse
	// When the token bucket of a resource group is updated, it will be sent to the channel.
	tokenBucketUpdateChan chan *groupCostController
	responseDeadlineCh    <-chan time.Time

	run struct {
		responseDeadline *time.Timer
		inDegradedMode   bool
		// currentRequests is used to record the request and resource group.
		// Currently, we don't do multiple `AcquireTokenBuckets`` at the same time, so there are no concurrency problems with `currentRequests`.
		currentRequests []*rmpb.TokenBucketRequest
	}

	opts []ResourceControlCreateOption

	// a cache for ru config and make concurrency safe.
	safeRuConfig atomic.Pointer[RUConfig]

	degradedRUSettings *rmpb.GroupRequestUnitSettings

	wg sync.WaitGroup
}

// NewResourceGroupController returns a new ResourceGroupsController which impls ResourceGroupKVInterceptor
func NewResourceGroupController(
	ctx context.Context,
	clientUniqueID uint64,
	provider ResourceGroupProvider,
	requestUnitConfig *RequestUnitConfig,
	keyspaceID uint32,
	opts ...ResourceControlCreateOption,
) (*ResourceGroupsController, error) {
	config, err := loadServerConfig(ctx, provider)
	if err != nil {
		return nil, err
	}
	if requestUnitConfig != nil {
		config.RequestUnit = *requestUnitConfig
	}

	ruConfig := GenerateRUConfig(config)
	controller := &ResourceGroupsController{
		clientUniqueID:        clientUniqueID,
		provider:              provider,
		keyspaceID:            keyspaceID,
		ruConfig:              ruConfig,
		lowTokenNotifyChan:    make(chan notifyMsg, 1),
		tokenResponseChan:     make(chan []*rmpb.TokenBucketResponse, 1),
		tokenBucketUpdateChan: make(chan *groupCostController, maxNotificationChanLen),
		opts:                  opts,
	}
	for _, opt := range opts {
		opt(controller)
	}
	log.Info("load resource controller config", zap.Reflect("config", config), zap.Reflect("ru-config", controller.ruConfig), zap.Uint32("keyspace-id", keyspaceID))
	controller.calculators = []ResourceCalculator{newKVCalculator(controller.ruConfig), newSQLCalculator(controller.ruConfig)}
	controller.safeRuConfig.Store(controller.ruConfig)
	enableControllerTraceLog.Store(config.EnableControllerTraceLog)
	return controller, nil
}

func loadServerConfig(ctx context.Context, provider ResourceGroupProvider) (*Config, error) {
	resp, err := provider.Get(ctx, []byte(controllerConfigPath))
	if err != nil {
		return nil, err
	}
	config := DefaultConfig()
	defer config.Adjust()
	kvs := resp.GetKvs()
	if len(kvs) == 0 {
		log.Warn("[resource group controller] server does not save config, load config failed")
		return config, nil
	}
	err = json.Unmarshal(kvs[0].GetValue(), config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

// GetConfig returns the config of controller.
func (c *ResourceGroupsController) GetConfig() *RUConfig {
	return c.safeRuConfig.Load()
}

// Source List
const (
	FromPeriodReport = "period_report"
	FromLowRU        = "low_ru"
)

// Start starts ResourceGroupController service.
func (c *ResourceGroupsController) Start(ctx context.Context) {
	c.loopCtx, c.loopCancel = context.WithCancel(ctx)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		if c.ruConfig.DegradedModeWaitDuration > 0 {
			c.run.responseDeadline = time.NewTimer(c.ruConfig.DegradedModeWaitDuration)
			c.run.responseDeadline.Stop()
			defer c.run.responseDeadline.Stop()
		}
		cleanupTicker := time.NewTicker(defaultGroupCleanupInterval)
		defer cleanupTicker.Stop()
		stateUpdateTicker := time.NewTicker(defaultGroupStateUpdateInterval)
		defer stateUpdateTicker.Stop()
		emergencyTokenAcquisitionTicker := time.NewTicker(defaultTargetPeriod)
		defer emergencyTokenAcquisitionTicker.Stop()

		failpoint.Inject("fastCleanup", func() {
			cleanupTicker.Reset(100 * time.Millisecond)
			// because of checking `gc.run.consumption` in cleanupTicker,
			// so should also change the stateUpdateTicker.
			stateUpdateTicker.Reset(200 * time.Millisecond)
		})
		failpoint.Inject("acceleratedReportingPeriod", func() {
			stateUpdateTicker.Reset(time.Millisecond * 100)
		})

		_, metaRevision, err := c.provider.LoadResourceGroups(ctx)
		if err != nil {
			log.Warn("load resource group revision failed", zap.Error(err))
		}
		resp, err := c.provider.Get(ctx, []byte(controllerConfigPath))
		if err != nil {
			log.Warn("load resource group revision failed", zap.Error(err))
		}
		cfgRevision := resp.GetHeader().GetRevision()
		var watchMetaChannel, watchConfigChannel chan []*meta_storagepb.Event
		if !c.ruConfig.isSingleGroupByKeyspace {
			// Use WithPrevKV() to get the previous key-value pair when get Delete Event.
			prefix := pd.GroupSettingsPathPrefixBytes(c.keyspaceID)
			watchMetaChannel, err = c.provider.Watch(ctx, prefix, opt.WithRev(metaRevision), opt.WithPrefix(), opt.WithPrevKV())
			if err != nil {
				log.Warn("watch resource group meta failed", zap.Error(err))
			}
		}

		watchConfigChannel, err = c.provider.Watch(ctx, pd.ControllerConfigPathPrefixBytes, opt.WithRev(cfgRevision), opt.WithPrefix())
		if err != nil {
			log.Warn("watch resource group config failed", zap.Error(err))
		}
		watchRetryTimer := time.NewTimer(watchRetryInterval)
		defer watchRetryTimer.Stop()

		for {
			select {
			/* tickers */
			case <-cleanupTicker.C:
				c.cleanUpResourceGroup()
			case <-stateUpdateTicker.C:
				c.executeOnAllGroups((*groupCostController).updateRunState)
				c.executeOnAllGroups((*groupCostController).updateAvgRequestResourcePerSec)
				if len(c.run.currentRequests) == 0 {
					c.collectTokenBucketRequests(c.loopCtx, FromPeriodReport, periodicReport /* select resource groups which should be reported periodically */, notifyMsg{})
				}
			case <-watchRetryTimer.C:
				if !c.ruConfig.isSingleGroupByKeyspace && watchMetaChannel == nil {
					// Use WithPrevKV() to get the previous key-value pair when get Delete Event.
					prefix := pd.GroupSettingsPathPrefixBytes(c.keyspaceID)
					watchMetaChannel, err = c.provider.Watch(ctx, prefix, opt.WithRev(metaRevision), opt.WithPrefix(), opt.WithPrevKV())
					if err != nil {
						log.Warn("watch resource group meta failed", zap.Error(err))
						watchRetryTimer.Reset(watchRetryInterval)
						failpoint.Inject("watchStreamError", func() {
							watchRetryTimer.Reset(20 * time.Millisecond)
						})
					}
				}
				if watchConfigChannel == nil {
					watchConfigChannel, err = c.provider.Watch(ctx, pd.ControllerConfigPathPrefixBytes, opt.WithRev(cfgRevision), opt.WithPrefix())
					if err != nil {
						log.Warn("watch resource group config failed", zap.Error(err))
						watchRetryTimer.Reset(watchRetryInterval)
					}
				}
			case <-emergencyTokenAcquisitionTicker.C:
				c.executeOnAllGroups((*groupCostController).resetEmergencyTokenAcquisition)
			/* channels */
			case <-c.loopCtx.Done():
				metrics.ResourceGroupStatusGauge.Reset()
				return
			case <-c.responseDeadlineCh:
				c.run.inDegradedMode = true
				c.executeOnAllGroups((*groupCostController).applyDegradedMode)
				log.Warn("[resource group controller] enter degraded mode")
			case resp := <-c.tokenResponseChan:
				if resp != nil {
					c.executeOnAllGroups((*groupCostController).updateRunState)
					c.handleTokenBucketResponse(resp)
				}
				c.run.currentRequests = nil
			case notifyMsg := <-c.lowTokenNotifyChan:
				c.executeOnAllGroups((*groupCostController).updateRunState)
				c.executeOnAllGroups((*groupCostController).updateAvgRequestResourcePerSec)
				c.collectTokenBucketRequests(c.loopCtx, FromLowRU, lowToken /* select low tokens resource group */, notifyMsg)
				if c.run.inDegradedMode {
					c.executeOnAllGroups((*groupCostController).applyDegradedMode)
				}
			case resp, ok := <-watchMetaChannel:
				failpoint.Inject("disableWatch", func() {
					if c.ruConfig.isSingleGroupByKeyspace {
						panic("disableWatch")
					}
				})
				if !ok {
					watchMetaChannel = nil
					watchRetryTimer.Reset(watchRetryInterval)
					failpoint.Inject("watchStreamError", func() {
						watchRetryTimer.Reset(20 * time.Millisecond)
					})
					continue
				}
				for _, item := range resp {
					metaRevision = item.Kv.ModRevision
					group := &rmpb.ResourceGroup{}
					switch item.Type {
					case meta_storagepb.Event_PUT:
						if err = proto.Unmarshal(item.Kv.Value, group); err != nil {
							continue
						}
						name := group.GetName()
						gc, ok := c.loadGroupController(name)
						if !ok {
							continue
						}
						if !gc.tombstone.Load() {
							gc.modifyMeta(group)
							continue
						}
						// If the resource group is marked as tombstone before, re-create the resource group controller.
						newGC, err := newGroupCostController(group, c.ruConfig, c.lowTokenNotifyChan, c.tokenBucketUpdateChan)
						if err != nil {
							log.Warn("[resource group controller] re-create resource group cost controller for tombstone failed",
								zap.String("name", name), zap.Error(err))
							continue
						}
						if c.groupsController.CompareAndSwap(name, gc, newGC) {
							log.Info("[resource group controller] re-create resource group cost controller for tombstone",
								zap.String("name", name))
						}
					case meta_storagepb.Event_DELETE:
						// Prev-kv is compacted means there must have been a delete event before this event,
						// which means that this is just a duplicated event, so we can just ignore it.
						if item.PrevKv == nil {
							log.Info("[resource group controller] previous key-value pair has been compacted",
								zap.String("required-key", string(item.Kv.Key)), zap.String("value", string(item.Kv.Value)))
							continue
						}
						if err = proto.Unmarshal(item.PrevKv.Value, group); err != nil {
							continue
						}
						c.tombstoneGroupCostController(group.GetName())
					}
				}
			case resp, ok := <-watchConfigChannel:
				if !ok {
					watchConfigChannel = nil
					watchRetryTimer.Reset(watchRetryInterval)
					failpoint.Inject("watchStreamError", func() {
						watchRetryTimer.Reset(20 * time.Millisecond)
					})
					continue
				}
				for _, item := range resp {
					cfgRevision = item.Kv.ModRevision
					config := DefaultConfig()
					if err := json.Unmarshal(item.Kv.Value, config); err != nil {
						continue
					}
					config.Adjust()
					c.ruConfig = GenerateRUConfig(config)

					// Stay compatible with serverless
					for _, opt := range c.opts {
						opt(c)
					}
					copyCfg := *c.ruConfig
					c.safeRuConfig.Store(&copyCfg)
					if enableControllerTraceLog.Load() != config.EnableControllerTraceLog {
						enableControllerTraceLog.Store(config.EnableControllerTraceLog)
					}
					log.Info("load resource controller config after config changed", zap.Reflect("config", config), zap.Reflect("ruConfig", c.ruConfig))
				}
			case gc := <-c.tokenBucketUpdateChan:
				go gc.handleTokenBucketUpdateEvent(c.loopCtx)
			}
		}
	}()
}

// Stop stops ResourceGroupController service.
func (c *ResourceGroupsController) Stop() error {
	if c.loopCancel == nil {
		return errors.Errorf("resource groups controller does not start")
	}
	c.loopCancel()
	c.wg.Wait()
	return nil
}

// loadGroupController just wraps the `Load` method of `sync.Map`.
func (c *ResourceGroupsController) loadGroupController(name string) (*groupCostController, bool) {
	tmp, ok := c.groupsController.Load(name)
	if !ok {
		return nil, false
	}
	return tmp.(*groupCostController), true
}

// loadOrStoreGroupController just wraps the `LoadOrStore` method of `sync.Map`.
func (c *ResourceGroupsController) loadOrStoreGroupController(name string, gc *groupCostController) (*groupCostController, bool) {
	tmp, loaded := c.groupsController.LoadOrStore(name, gc)
	return tmp.(*groupCostController), loaded
}

// NewResourceGroupNotExistErr returns a new error that indicates the resource group does not exist.
// It's exported for testing.
func NewResourceGroupNotExistErr(name string) error {
	return &errs.ErrClientGetResourceGroup{ResourceGroupName: name, Cause: "resource group does not exist"}
}

func (c *ResourceGroupsController) getDegradedResourceGroup(resourceGroupName string) *rmpb.ResourceGroup {
	group := &rmpb.ResourceGroup{
		Name:       resourceGroupName,
		Mode:       rmpb.GroupMode_RUMode,
		RUSettings: c.degradedRUSettings,
	}
	return group
}

// tryGetResourceGroupController will try to get the resource group controller from local cache first.
// If the local cache misses, it will then call gRPC to fetch the resource group info from the remote server.
// If `useTombstone` is true, it will return the resource group controller even if it is marked as tombstone.
func (c *ResourceGroupsController) tryGetResourceGroupController(
	ctx context.Context, name string, useTombstone bool,
) (*groupCostController, error) {
	// Get from the local cache first.
	gc, ok := c.loadGroupController(name)
	if ok {
		if !useTombstone && gc.tombstone.Load() {
			return nil, NewResourceGroupNotExistErr(name)
		}
		return gc, nil
	}

	isUseDegradedResourceGroup := false
	// Call gRPC to fetch the resource group info.
	group, err := c.provider.GetResourceGroup(ctx, name)
	if err != nil {
		if c.degradedRUSettings != nil {
			isUseDegradedResourceGroup = true
			group = c.getDegradedResourceGroup(name)
		} else {
			return nil, err
		}
	}
	if group == nil {
		return nil, NewResourceGroupNotExistErr(name)
	}
	// Check again to prevent initializing the same resource group concurrently.
	if gc, ok = c.loadGroupController(name); ok {
		return gc, nil
	}
	// Initialize the resource group controller.
	gc, err = newGroupCostController(group, c.ruConfig, c.lowTokenNotifyChan, c.tokenBucketUpdateChan)
	if err != nil {
		return nil, err
	}
	if !isUseDegradedResourceGroup {
		// Check again to prevent initializing the same resource group concurrently.
		_, loaded := c.loadOrStoreGroupController(name, gc)
		if !loaded {
			metrics.ResourceGroupStatusGauge.WithLabelValues(name, group.Name).Set(1)
			log.Info("[resource group controller] create resource group cost controller", zap.String("name", name))
		}
	}
	return gc, nil
}

// Do not delete the resource group immediately to prevent from interrupting the ongoing request,
// mark it as tombstone and create a default resource group controller for it.
func (c *ResourceGroupsController) tombstoneGroupCostController(name string) {
	_, ok := c.loadGroupController(name)
	if !ok {
		return
	}
	// The default resource group controller should never be deleted.
	if name == defaultResourceGroupName {
		return
	}
	// Try to get the default group meta first.
	defaultGC, err := c.tryGetResourceGroupController(c.loopCtx, defaultResourceGroupName, false)
	if err != nil || defaultGC == nil {
		log.Warn("[resource group controller] get default resource group meta for tombstone failed",
			zap.String("name", name), zap.Error(err))
		// Directly delete the resource group controller if the default group is not available.
		c.groupsController.Delete(name)
		return
	}
	// Create a default resource group controller for the tombstone resource group independently.
	gc, err := newGroupCostController(defaultGC.getMeta(), c.ruConfig, c.lowTokenNotifyChan, c.tokenBucketUpdateChan)
	if err != nil {
		log.Warn("[resource group controller] create default resource group cost controller for tombstone failed",
			zap.String("name", name), zap.Error(err))
		// Directly delete the resource group controller if the default group controller cannot be created.
		c.groupsController.Delete(name)
		return
	}
	gc.tombstone.Store(true)
	c.groupsController.Store(name, gc)
	// Its metrics will be deleted in the cleanup process.
	metrics.ResourceGroupStatusGauge.WithLabelValues(name, name).Set(2)
	log.Info("[resource group controller] default resource group controller cost created for tombstone",
		zap.String("name", name))
}

func (c *ResourceGroupsController) cleanUpResourceGroup() {
	c.groupsController.Range(func(key, value any) bool {
		resourceGroupName := key.(string)
		gc := value.(*groupCostController)
		// Check for stale resource groups, which will be deleted when consumption is continuously unchanged.
		gc.mu.Lock()
		latestConsumption := *gc.mu.consumption
		gc.mu.Unlock()
		if equalRU(latestConsumption, *gc.run.consumption) {
			if gc.inactive || gc.tombstone.Load() {
				c.groupsController.Delete(resourceGroupName)
				metrics.ResourceGroupStatusGauge.DeleteLabelValues(resourceGroupName, resourceGroupName)
				return true
			}
			gc.inactive = true
		} else {
			gc.inactive = false
		}
		return true
	})
}

func (c *ResourceGroupsController) executeOnAllGroups(f func(controller *groupCostController)) {
	c.groupsController.Range(func(_, value any) bool {
		f(value.(*groupCostController))
		return true
	})
}

func (c *ResourceGroupsController) handleTokenBucketResponse(resp []*rmpb.TokenBucketResponse) {
	if c.responseDeadlineCh != nil {
		if c.run.responseDeadline.Stop() {
			select {
			case <-c.run.responseDeadline.C:
			default:
			}
		}
		c.responseDeadlineCh = nil
	}
	c.run.inDegradedMode = false
	for _, res := range resp {
		name := res.GetResourceGroupName()
		gc, ok := c.loadGroupController(name)
		if !ok {
			log.Warn("[resource group controller] a non-existent resource group was found when handle token response", zap.String("name", name))
			continue
		}
		gc.handleTokenBucketResponse(res)
	}
}

func (c *ResourceGroupsController) collectTokenBucketRequests(ctx context.Context, source string, typ selectType, notifyMsg notifyMsg) {
	c.run.currentRequests = make([]*rmpb.TokenBucketRequest, 0)
	c.groupsController.Range(func(_, value any) bool {
		gc := value.(*groupCostController)
		request := gc.collectRequestAndConsumption(typ)
		if request != nil {
			request.KeyspaceId = &rmpb.KeyspaceIDValue{
				Value: c.keyspaceID,
			}
			c.run.currentRequests = append(c.run.currentRequests, request)
			gc.metrics.tokenRequestCounter.Inc()
		}
		return true
	})
	if len(c.run.currentRequests) > 0 {
		c.sendTokenBucketRequests(ctx, c.run.currentRequests, source, notifyMsg)
	}
}

func (c *ResourceGroupsController) sendTokenBucketRequests(ctx context.Context, requests []*rmpb.TokenBucketRequest, source string, notifyMsg notifyMsg) {
	now := time.Now()
	req := &rmpb.TokenBucketsRequest{
		Requests:              requests,
		TargetRequestPeriodMs: uint64(defaultTargetPeriod / time.Millisecond),
		ClientUniqueId:        c.clientUniqueID,
	}
	if c.ruConfig.DegradedModeWaitDuration > 0 && c.responseDeadlineCh == nil {
		c.run.responseDeadline.Reset(c.ruConfig.DegradedModeWaitDuration)
		c.responseDeadlineCh = c.run.responseDeadline.C
	}
	go func() {
		logControllerTrace("[resource group controller] send token bucket request", zap.Time("now", now), zap.Any("req", req.Requests), zap.String("source", source))
		resp, err := c.provider.AcquireTokenBuckets(ctx, req)
		latency := time.Since(now)
		if err != nil {
			// Don't log any errors caused by the stopper canceling the context.
			if !errors.ErrorEqual(err, context.Canceled) {
				log.Warn("[resource group controller] token bucket rpc error", zap.Error(err))
			}
			resp = nil
			metrics.FailedTokenRequestDuration.Observe(latency.Seconds())
		} else {
			metrics.SuccessfulTokenRequestDuration.Observe(latency.Seconds())
		}
		if !notifyMsg.startTime.IsZero() && time.Since(notifyMsg.startTime) > slowNotifyFilterDuration {
			log.Warn("[resource group controller] slow token bucket request", zap.String("source", source), zap.Duration("cost", time.Since(notifyMsg.startTime)))
		}
		logControllerTrace("[resource group controller] token bucket response", zap.Time("now", time.Now()), zap.Any("resp", resp), zap.String("source", source), zap.Duration("latency", latency))
		c.tokenResponseChan <- resp
	}()
}

// OnRequestWait is used to check whether resource group has enough tokens. It maybe needs to wait some time.
func (c *ResourceGroupsController) OnRequestWait(
	ctx context.Context, resourceGroupName string, info RequestInfo,
) (delta, penalty *rmpb.Consumption, waitDuration time.Duration, priority uint32, err error) {
	gc, err := c.tryGetResourceGroupController(ctx, resourceGroupName, true)
	if err != nil {
		return nil, nil, time.Duration(0), 0, err
	}
	return gc.onRequestWaitImpl(ctx, info)
}

// OnResponse is used to consume tokens after receiving response
func (c *ResourceGroupsController) OnResponse(
	resourceGroupName string, req RequestInfo, resp ResponseInfo,
) (*rmpb.Consumption, error) {
	gc, ok := c.loadGroupController(resourceGroupName)
	if !ok {
		log.Warn("[resource group controller] resource group name does not exist", zap.String("name", resourceGroupName))
		return &rmpb.Consumption{}, nil
	}
	return gc.onResponseImpl(req, resp)
}

// OnResponseWait is used to consume tokens after receiving response
func (c *ResourceGroupsController) OnResponseWait(
	ctx context.Context, resourceGroupName string, req RequestInfo, resp ResponseInfo,
) (*rmpb.Consumption, time.Duration, error) {
	gc, ok := c.loadGroupController(resourceGroupName)
	if !ok {
		log.Warn("[resource group controller] resource group name does not exist", zap.String("name", resourceGroupName))
		return &rmpb.Consumption{}, time.Duration(0), nil
	}
	return gc.onResponseWaitImpl(ctx, req, resp)
}

// IsBackgroundRequest If the resource group has background jobs, we should not record consumption and wait for it.
func (c *ResourceGroupsController) IsBackgroundRequest(ctx context.Context,
	resourceGroupName, requestResource string) bool {
	gc, err := c.tryGetResourceGroupController(ctx, resourceGroupName, false)
	if err != nil {
		return false
	}

	return c.checkBackgroundSettings(ctx, gc.getMeta().BackgroundSettings, requestResource)
}

func (c *ResourceGroupsController) checkBackgroundSettings(ctx context.Context, bg *rmpb.BackgroundSettings, requestResource string) bool {
	// fallback to default resource group.
	if bg == nil {
		gc, err := c.tryGetResourceGroupController(ctx, defaultResourceGroupName, false)
		if err != nil {
			return false
		}
		bg = gc.getMeta().BackgroundSettings
	}

	if bg == nil || len(requestResource) == 0 || len(bg.JobTypes) == 0 {
		return false
	}

	if idx := strings.LastIndex(requestResource, "_"); idx != -1 {
		return slices.Contains(bg.JobTypes, requestResource[idx+1:])
	}

	return false
}

// GetResourceGroup returns the meta setting of the given resource group name.
func (c *ResourceGroupsController) GetResourceGroup(resourceGroupName string) (*rmpb.ResourceGroup, error) {
	gc, err := c.tryGetResourceGroupController(c.loopCtx, resourceGroupName, false)
	if err != nil {
		return nil, err
	}
	return gc.getMeta(), nil
}

// ReportConsumption is used to report ru consumption directly.
//
// Currently, this interface is used to report the consumption for TiFlash MPP cost
// after the query is finished.
func (c *ResourceGroupsController) ReportConsumption(resourceGroupName string, consumption *rmpb.Consumption) {
	gc, ok := c.loadGroupController(resourceGroupName)
	if !ok {
		log.Warn("[resource group controller] resource group name does not exist", zap.String("name", resourceGroupName))
		return
	}

	gc.addRUConsumption(consumption)
}
