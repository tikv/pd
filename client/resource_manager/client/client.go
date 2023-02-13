// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,g
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	defaultMaxWaitDuration = time.Second
	maxRetry               = 3
	maxNotificationChanLen = 200
)

// ResourceGroupKVInterceptor is used as quato limit controller for resource group using kv store.
type ResourceGroupKVInterceptor interface {
	// OnRequestWait is used to check whether resource group has enough tokens. It maybe needs wait some time.
	OnRequestWait(ctx context.Context, resourceGroupName string, info RequestInfo) error
	// OnResponse is used to consume tokens atfer receiving response
	OnResponse(ctx context.Context, resourceGroupName string, req RequestInfo, resp ResponseInfo) error
}

// ResourceGroupProvider provides some api to interact with resource manager server。
type ResourceGroupProvider interface {
	ListResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, error)
	GetResourceGroup(ctx context.Context, resourceGroupName string) (*rmpb.ResourceGroup, error)
	AddResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error)
	ModifyResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error)
	DeleteResourceGroup(ctx context.Context, resourceGroupName string) (string, error)
	AcquireTokenBuckets(ctx context.Context, request *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error)
}

var _ ResourceGroupKVInterceptor = (*ResourceGroupsController)(nil)

// ResourceGroupsController impls ResourceGroupKVInterceptor.
type ResourceGroupsController struct {
	clientUniqueID   uint64
	provider         ResourceGroupProvider
	groupsController sync.Map
	config           *Config

	loopCtx    context.Context
	loopCancel func()

	calculators []ResourceCalculator

	// tokenResponseChan receives token bucket response from server.
	// And it handles all resource group and runs in main loop
	tokenResponseChan chan []*rmpb.TokenBucketResponse

	groupNotificationCh chan *groupCostController

	// lowTokenNotifyChan receives chan notification when the number of available token is low
	lowTokenNotifyChan chan struct{}

	run struct {
		now             time.Time
		lastRequestTime time.Time

		// requestInProgress is true if we are in the process of sending a request.
		// It gets set to false when we receives the response in the main loop,
		// even in error cases.
		requestInProgress bool

		// requestNeedsRetry is set if the last token bucket request encountered an
		// error. This triggers a retry attempt on the next tick.
		//
		// Note: requestNeedsRetry and requestInProgress are never true at the same time.
		requestNeedsRetry bool

		// targetPeriod indicate how long it is expected to cost token when acquiring token.
		// last update.
		targetPeriod time.Duration
	}
}

// NewResourceGroupController returns a new ResourceGroupsController which impls ResourceGroupKVInterceptor
func NewResourceGroupController(clientUniqueID uint64, provider ResourceGroupProvider, requestUnitConfig *RequestUnitConfig) (*ResourceGroupsController, error) {
	var config *Config
	if requestUnitConfig != nil {
		config = generateConfig(requestUnitConfig)
	} else {
		config = DefaultConfig()
	}
	return &ResourceGroupsController{
		clientUniqueID:      clientUniqueID,
		provider:            provider,
		config:              config,
		lowTokenNotifyChan:  make(chan struct{}, 1),
		tokenResponseChan:   make(chan []*rmpb.TokenBucketResponse, 1),
		groupNotificationCh: make(chan *groupCostController, maxNotificationChanLen),
		calculators:         []ResourceCalculator{newKVCalculator(config), newSQLCalculator(config)},
	}, nil
}

// Start starts ResourceGroupController service.
func (c *ResourceGroupsController) Start(ctx context.Context) {
	if err := c.updateAllResourceGroups(ctx); err != nil {
		log.Error("update ResourceGroup failed", zap.Error(err))
	}
	c.initRunState()
	c.loopCtx, c.loopCancel = context.WithCancel(ctx)
	go c.mainLoop(c.loopCtx)
}

// Stop stops ResourceGroupController service.
func (c *ResourceGroupsController) Stop() error {
	if c.loopCancel == nil {
		return errors.Errorf("resourceGroupsController does not start.")
	}
	c.loopCancel()
	return nil
}

func (c *ResourceGroupsController) putResourceGroup(ctx context.Context, name string) (*groupCostController, error) {
	// ref https://github.com/tikv/pd/issues/5955, if we don't introduce a lock, we need check the groupsController as much as possible to avoid
	// getting or creating resource group. We check it in L139, L147 and L157.
	if tmp, ok := c.groupsController.Load(name); ok {
		gc := tmp.(*groupCostController)
		return gc, nil
	}
	group, err := c.provider.GetResourceGroup(ctx, name)
	if err != nil {
		return nil, err
	}
	if tmp, ok := c.groupsController.Load(name); ok {
		gc := tmp.(*groupCostController)
		return gc, nil
	}
	gc, err := newGroupCostController(group, c.config, c.lowTokenNotifyChan, c.groupNotificationCh)
	if err != nil {
		return nil, err
	}
	// A future case: If user change mode from RU to RAW mode. How to re-init?
	gc.initRunState()
	tmp, loaded := c.groupsController.LoadOrStore(group.GetName(), gc)
	if !loaded {
		log.Info("create resource group cost controller", zap.String("name", group.GetName()))
	}
	gc = tmp.(*groupCostController)
	return gc, nil
}

func (c *ResourceGroupsController) updateAllResourceGroups(ctx context.Context) error {
	groups, err := c.provider.ListResourceGroups(ctx)
	if err != nil {
		return err
	}
	latestGroups := make(map[string]struct{})
	for _, group := range groups {
		gc, err := newGroupCostController(group, c.config, c.lowTokenNotifyChan, c.groupNotificationCh)
		if err != nil {
			log.Warn("failed to create resource group cost controller", zap.Error(errors.Errorf("[resource group controller] %s is not existed or miss necessary configuration.", group.GetName())))
			continue
		}
		log.Info("create resource group cost controller", zap.String("name", group.GetName()))
		c.groupsController.Store(group.GetName(), gc)
		latestGroups[group.GetName()] = struct{}{}
	}
	c.groupsController.Range(func(key, value any) bool {
		resourceGroupName := key.(string)
		if _, ok := latestGroups[resourceGroupName]; !ok {
			c.groupsController.Delete(key)
		}
		return true
	})
	return nil
}

func (c *ResourceGroupsController) initRunState() {
	now := time.Now()
	c.run.now = now
	c.run.lastRequestTime = now
	c.run.targetPeriod = c.config.targetPeriod
	c.groupsController.Range(func(name, value any) bool {
		gc := value.(*groupCostController)
		gc.initRunState()
		return true
	})
}

func (c *ResourceGroupsController) updateRunState(ctx context.Context) {
	c.run.now = time.Now()
	c.groupsController.Range(func(name, value any) bool {
		gc := value.(*groupCostController)
		gc.updateRunState(ctx)
		return true
	})
}

func (c *ResourceGroupsController) shouldReportConsumption() bool {
	if c.run.requestInProgress {
		return false
	}
	timeSinceLastRequest := c.run.now.Sub(c.run.lastRequestTime)
	if timeSinceLastRequest >= c.run.targetPeriod {
		if timeSinceLastRequest >= extendedReportingPeriodFactor*c.run.targetPeriod {
			return true
		}
		ret := false
		c.groupsController.Range(func(name, value any) bool {
			gc := value.(*groupCostController)
			ret = ret || gc.shouldReportConsumption()
			return !ret
		})
		return ret
	}
	return false
}

func (c *ResourceGroupsController) updateAvgRequestResourcePerSec() {
	c.groupsController.Range(func(name, value any) bool {
		gc := value.(*groupCostController)
		gc.updateAvgRequestResourcePerSec()
		return true
	})
}

func (c *ResourceGroupsController) handleTokenBucketResponse(resp []*rmpb.TokenBucketResponse) {
	for _, res := range resp {
		name := res.GetResourceGroupName()
		v, ok := c.groupsController.Load(name)
		if !ok {
			log.Warn("A non-existent resource group was found when handle token response.", zap.String("name", name))
			continue
		}
		gc := v.(*groupCostController)
		gc.handleTokenBucketResponse(res)
	}
}

func (c *ResourceGroupsController) collectTokenBucketRequests(ctx context.Context, source string, low bool) {
	requests := make([]*rmpb.TokenBucketRequest, 0)
	c.groupsController.Range(func(name, value any) bool {
		gc := value.(*groupCostController)
		request := gc.collectRequestAndConsumption(low)
		if request != nil {
			requests = append(requests, request)
		}
		return true
	})
	if len(requests) > 0 {
		c.sendTokenBucketRequests(ctx, requests, source)
	}
}

func (c *ResourceGroupsController) sendTokenBucketRequests(ctx context.Context, requests []*rmpb.TokenBucketRequest, source string) {
	now := time.Now()
	c.run.lastRequestTime = now
	c.run.requestInProgress = true
	req := &rmpb.TokenBucketsRequest{
		Requests:              requests,
		TargetRequestPeriodMs: uint64(c.config.targetPeriod / time.Millisecond),
	}
	go func() {
		log.Debug("[resource group controller] send token bucket request", zap.Time("now", now), zap.Any("req", req.Requests), zap.String("source", source))
		resp, err := c.provider.AcquireTokenBuckets(ctx, req)
		if err != nil {
			// Don't log any errors caused by the stopper canceling the context.
			if !errors.ErrorEqual(err, context.Canceled) {
				log.L().Sugar().Infof("TokenBucket RPC error: %v", err)
			}
			resp = nil
		}
		log.Debug("[resource group controller] token bucket response", zap.Time("now", time.Now()), zap.Any("resp", resp), zap.String("source", source), zap.Duration("latency", time.Since(now)))
		c.tokenResponseChan <- resp
	}()
}

func (c *ResourceGroupsController) mainLoop(ctx context.Context) {
	interval := c.config.groupLoopUpdateInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	c.updateRunState(ctx)
	c.collectTokenBucketRequests(ctx, "init", false /* select all */)

	for {
		select {
		case <-ctx.Done():
			return
		case resp := <-c.tokenResponseChan:
			c.run.requestInProgress = false
			if resp != nil {
				c.updateRunState(ctx)
				c.handleTokenBucketResponse(resp)
			} else {
				// A nil response indicates a failure (which would have been logged).
				c.run.requestNeedsRetry = true
			}
		case <-ticker.C:
			c.updateRunState(ctx)
			c.updateAvgRequestResourcePerSec()
			if c.run.requestNeedsRetry || c.shouldReportConsumption() {
				c.run.requestNeedsRetry = false
				c.collectTokenBucketRequests(ctx, "report", false /* select all */)
			}
		case <-c.lowTokenNotifyChan:
			c.updateRunState(ctx)
			c.updateAvgRequestResourcePerSec()
			if !c.run.requestInProgress {
				c.collectTokenBucketRequests(ctx, "low_ru", true /* only select low tokens resource group */)
			}
		case gc := <-c.groupNotificationCh:
			now := gc.run.now
			go gc.handleTokenBucketTrickEvent(ctx, now)
		}
	}
}

// OnRequestWait is used to check whether resource group has enough tokens. It maybe needs wait some time.
func (c *ResourceGroupsController) OnRequestWait(
	ctx context.Context, resourceGroupName string, info RequestInfo,
) (err error) {
	var gc *groupCostController
	if tmp, ok := c.groupsController.Load(resourceGroupName); ok {
		gc = tmp.(*groupCostController)
	} else {
		gc, err = c.putResourceGroup(ctx, resourceGroupName)
		if err != nil {
			return errors.Errorf("[resource group] %s is not existed or miss necessary configuration.", resourceGroupName)
		}
	}
	err = gc.onRequestWait(ctx, info)
	return err
}

// OnResponse is used to consume tokens after receiving response
func (c *ResourceGroupsController) OnResponse(_ context.Context, resourceGroupName string, req RequestInfo, resp ResponseInfo) error {
	tmp, ok := c.groupsController.Load(resourceGroupName)
	if !ok {
		log.Warn("[resource group] resourceGroupName is not existed.", zap.String("resourceGroupName", resourceGroupName))
	}
	gc := tmp.(*groupCostController)
	gc.onResponse(req, resp)
	return nil
}

type groupCostController struct {
	*rmpb.ResourceGroup
	mainCfg     *Config
	calculators []ResourceCalculator
	mode        rmpb.GroupMode

	handleRespFunc func(*rmpb.TokenBucketResponse)

	mu struct {
		sync.Mutex
		consumption *rmpb.Consumption
	}

	// fast path to make once token limit with un-limit burst.
	burstable *atomic.Bool

	lowRUNotifyChan chan struct{}

	groupNotificationCh chan *groupCostController
	// run contains the state that is updated by the main loop.
	run struct {
		now time.Time

		// targetPeriod stores the value of the TargetPeriodSetting setting at the
		// last update.
		targetPeriod time.Duration

		// consumptions stores the last value of mu.consumption.
		// requestUnitConsumptions []*rmpb.RequestUnitItem
		// resourceConsumptions    []*rmpb.ResourceItem
		consumption *rmpb.Consumption

		// lastRequestUnitConsumptions []*rmpb.RequestUnitItem
		// lastResourceConsumptions    []*rmpb.ResourceItem
		lastRequestConsumption *rmpb.Consumption

		// initialRequestCompleted is set to true when the first token bucket
		// request completes successfully.
		initialRequestCompleted bool

		resourceTokens    map[rmpb.RawResourceType]*tokenCounter
		requestUnitTokens map[rmpb.RequestUnitType]*tokenCounter
	}
}

type tokenCounter struct {
	// avgRUPerSec is an exponentially-weighted moving average of the RU
	// consumption per second; used to estimate the RU requirements for the next
	// request.
	avgRUPerSec float64
	// lastSecRU is the consumption.RU value when avgRUPerSec was last updated.
	avgRUPerSecLastRU float64
	avgLastTime       time.Time

	notify struct {
		mu                         sync.Mutex
		setupNotificationCh        <-chan time.Time
		setupNotificationThreshold float64
		setupNotificationTimer     *time.Timer
	}

	lastDeadline time.Time
	lastRate     float64

	limiter *Limiter
}

func newGroupCostController(group *rmpb.ResourceGroup, mainCfg *Config, lowRUNotifyChan chan struct{}, groupNotificationCh chan *groupCostController) (*groupCostController, error) {
	switch group.Mode {
	case rmpb.GroupMode_RUMode:
		if group.RUSettings.RU == nil || group.RUSettings.RU.Settings == nil {
			return nil, errors.Errorf("the resource group is not configured")
		}
	default:
		return nil, errors.New("not supports the resource type")
	}

	gc := &groupCostController{
		ResourceGroup:       group,
		mainCfg:             mainCfg,
		calculators:         []ResourceCalculator{newKVCalculator(mainCfg), newSQLCalculator(mainCfg)},
		mode:                group.GetMode(),
		groupNotificationCh: groupNotificationCh,
		lowRUNotifyChan:     lowRUNotifyChan,
		burstable:           &atomic.Bool{},
	}

	switch gc.mode {
	case rmpb.GroupMode_RUMode:
		gc.handleRespFunc = gc.handleRUTokenResponse
	case rmpb.GroupMode_RawMode:
		gc.handleRespFunc = gc.handleRawResourceTokenResponse
	}

	gc.mu.consumption = &rmpb.Consumption{}
	return gc, nil
}

func (gc *groupCostController) initRunState() {
	now := time.Now()
	gc.run.now = now
	gc.run.targetPeriod = gc.mainCfg.targetPeriod

	gc.run.consumption = &rmpb.Consumption{}

	gc.run.lastRequestConsumption = &rmpb.Consumption{}

	cfgFunc := func(tb *rmpb.TokenBucket) tokenBucketReconfigureArgs {
		cfg := tokenBucketReconfigureArgs{
			NewTokens: initialRequestUnits,
			NewBurst:  tb.Settings.BurstLimit,
			// This is to trigger token requests as soon as resource group start consuming tokens.
			NotifyThreshold: math.Max(initialRequestUnits-float64(tb.Settings.FillRate)*0.2, 1),
		}
		if cfg.NewBurst >= 0 {
			cfg.NewBurst = 0
		}
		return cfg
	}

	switch gc.mode {
	case rmpb.GroupMode_RUMode:
		gc.run.requestUnitTokens = make(map[rmpb.RequestUnitType]*tokenCounter)
		for typ := range requestUnitLimitTypeList {
			tb := getRUTokenBucketSetting(gc.ResourceGroup, typ)
			cfg := cfgFunc(tb)
			limiter := NewLimiterWithCfg(now, cfg, gc.lowRUNotifyChan)
			counter := &tokenCounter{
				limiter:     limiter,
				avgRUPerSec: 0,
				avgLastTime: now,
			}
			gc.run.requestUnitTokens[typ] = counter
		}
	case rmpb.GroupMode_RawMode:
		gc.run.resourceTokens = make(map[rmpb.RawResourceType]*tokenCounter)
		for typ := range requestResourceLimitTypeList {
			tb := getRawResourceTokenBucketSetting(gc.ResourceGroup, typ)
			cfg := cfgFunc(tb)
			limiter := NewLimiterWithCfg(now, cfg, gc.lowRUNotifyChan)
			counter := &tokenCounter{
				limiter:     limiter,
				avgRUPerSec: 0,
				avgLastTime: now,
			}
			gc.run.resourceTokens[typ] = counter
		}
	}
}

func (gc *groupCostController) updateRunState(ctx context.Context) {
	newTime := time.Now()
	deltaConsumption := &rmpb.Consumption{}
	for _, calc := range gc.calculators {
		calc.Trickle(ctx, deltaConsumption)
	}
	gc.mu.Lock()
	add(gc.mu.consumption, deltaConsumption)
	*gc.run.consumption = *gc.mu.consumption
	gc.mu.Unlock()
	// remove tokens
	switch gc.mode {
	case rmpb.GroupMode_RUMode:
		for typ, counter := range gc.run.requestUnitTokens {
			if v := getRUValueFromConsumption(deltaConsumption, typ); v > 0 {
				counter.limiter.RemoveTokens(newTime, v)
			}
		}
	case rmpb.GroupMode_RawMode:
		for typ, counter := range gc.run.resourceTokens {
			if v := getRawResourceValueFromConsumption(deltaConsumption, typ); v > 0 {
				counter.limiter.RemoveTokens(newTime, v)
			}
		}
	}
	log.Debug("update run state", zap.Any("request unit consumption", gc.run.consumption))
	gc.run.now = newTime
}

func (gc *groupCostController) updateAvgRequestResourcePerSec() {
	switch gc.mode {
	case rmpb.GroupMode_RawMode:
		gc.updateAvgRaWResourcePerSec()
	case rmpb.GroupMode_RUMode:
		gc.updateAvgRUPerSec()
	}
}

func (gc *groupCostController) handleTokenBucketTrickEvent(ctx context.Context, now time.Time) {
	switch gc.mode {
	case rmpb.GroupMode_RawMode:
		for _, counter := range gc.run.resourceTokens {
			counter.notify.mu.Lock()
			ch := counter.notify.setupNotificationCh
			counter.notify.mu.Unlock()
			if ch == nil {
				continue
			}
			select {
			case <-ch:
				counter.notify.mu.Lock()
				counter.notify.setupNotificationTimer = nil
				counter.notify.setupNotificationCh = nil
				threshold := counter.notify.setupNotificationThreshold
				counter.notify.mu.Unlock()
				counter.limiter.SetupNotificationThreshold(now, threshold)
			case <-ctx.Done():
				return
			}
		}

	case rmpb.GroupMode_RUMode:
		for _, counter := range gc.run.requestUnitTokens {
			counter.notify.mu.Lock()
			ch := counter.notify.setupNotificationCh
			counter.notify.mu.Unlock()
			if ch == nil {
				continue
			}
			select {
			case <-ch:
				counter.notify.mu.Lock()
				counter.notify.setupNotificationTimer = nil
				counter.notify.setupNotificationCh = nil
				threshold := counter.notify.setupNotificationThreshold
				counter.notify.mu.Unlock()
				counter.limiter.SetupNotificationThreshold(now, threshold)
			case <-ctx.Done():
				return
			}
		}
	}
}

func (gc *groupCostController) updateAvgRaWResourcePerSec() {
	isBurstable := true
	for typ, counter := range gc.run.resourceTokens {
		if counter.limiter.GetBurst() >= 0 {
			isBurstable = false
		}
		if !gc.calcAvg(counter, getRawResourceValueFromConsumption(gc.run.consumption, typ)) {
			continue
		}
		log.Debug("[resource group controller] update avg raw resource per sec", zap.String("name", gc.Name), zap.String("type", rmpb.RawResourceType_name[int32(typ)]), zap.Float64("avgRUPerSec", counter.avgRUPerSec))
	}
	gc.burstable.Store(isBurstable)
}

func (gc *groupCostController) updateAvgRUPerSec() {
	isBurstable := true
	for typ, counter := range gc.run.requestUnitTokens {
		if counter.limiter.GetBurst() >= 0 {
			isBurstable = false
		}
		if !gc.calcAvg(counter, getRUValueFromConsumption(gc.run.consumption, typ)) {
			continue
		}
		log.Debug("[resource group controller] update avg ru per sec", zap.String("name", gc.Name), zap.String("type", rmpb.RequestUnitType_name[int32(typ)]), zap.Float64("avgRUPerSec", counter.avgRUPerSec))
	}
	gc.burstable.Store(isBurstable)
}

func (gc *groupCostController) calcAvg(counter *tokenCounter, new float64) bool {
	deltaDuration := gc.run.now.Sub(counter.avgLastTime)
	if deltaDuration <= 500*time.Millisecond {
		return false
	}
	delta := (new - counter.avgRUPerSecLastRU) / deltaDuration.Seconds()
	counter.avgRUPerSec = movingAvgFactor*counter.avgRUPerSec + (1-movingAvgFactor)*delta
	counter.avgLastTime = gc.run.now
	counter.avgRUPerSecLastRU = new
	return true
}

func (gc *groupCostController) shouldReportConsumption() bool {
	switch gc.Mode {
	case rmpb.GroupMode_RUMode:
		for typ := range requestUnitLimitTypeList {
			if getRUValueFromConsumption(gc.run.consumption, typ)-getRUValueFromConsumption(gc.run.lastRequestConsumption, typ) >= consumptionsReportingThreshold {
				return true
			}
		}
	case rmpb.GroupMode_RawMode:
		for typ := range requestResourceLimitTypeList {
			if getRawResourceValueFromConsumption(gc.run.consumption, typ)-getRawResourceValueFromConsumption(gc.run.lastRequestConsumption, typ) >= consumptionsReportingThreshold {
				return true
			}
		}
	}
	return false
}

func (gc *groupCostController) handleTokenBucketResponse(resp *rmpb.TokenBucketResponse) {
	gc.handleRespFunc(resp)
	if !gc.run.initialRequestCompleted {
		gc.run.initialRequestCompleted = true
		// This is the first successful request. Take back the initial RUs that we
		// used to pre-fill the bucket.
		for _, counter := range gc.run.resourceTokens {
			counter.limiter.RemoveTokens(gc.run.now, initialRequestUnits)
		}
		for _, counter := range gc.run.requestUnitTokens {
			counter.limiter.RemoveTokens(gc.run.now, initialRequestUnits)
		}
	}
}

func (gc *groupCostController) handleRawResourceTokenResponse(resp *rmpb.TokenBucketResponse) {
	for _, grantedTB := range resp.GetGrantedResourceTokens() {
		typ := grantedTB.GetType()
		counter, ok := gc.run.resourceTokens[typ]
		if !ok {
			log.Warn("not support this resource type", zap.String("type", rmpb.RawResourceType_name[int32(typ)]))
			continue
		}
		gc.modifyTokenCounter(counter, grantedTB.GetGrantedTokens(), grantedTB.GetTrickleTimeMs())
	}
}

func (gc *groupCostController) handleRUTokenResponse(resp *rmpb.TokenBucketResponse) {
	for _, grantedTB := range resp.GetGrantedRUTokens() {
		typ := grantedTB.GetType()
		counter, ok := gc.run.requestUnitTokens[typ]
		if !ok {
			log.Warn("not support this resource type", zap.String("type", rmpb.RawResourceType_name[int32(typ)]))
			continue
		}
		gc.modifyTokenCounter(counter, grantedTB.GetGrantedTokens(), grantedTB.GetTrickleTimeMs())
	}
}

func (gc *groupCostController) modifyTokenCounter(counter *tokenCounter, bucket *rmpb.TokenBucket, trickleTimeMs int64) {
	granted := bucket.Tokens
	if !counter.lastDeadline.IsZero() {
		// If last request came with a trickle duration, we may have RUs that were
		// not made available to the bucket yet; throw them together with the newly
		// granted RUs.
		if since := counter.lastDeadline.Sub(gc.run.now); since > 0 {
			granted += counter.lastRate * since.Seconds()
		}
	}
	counter.notify.mu.Lock()
	if counter.notify.setupNotificationTimer != nil {
		counter.notify.setupNotificationTimer.Stop()
		counter.notify.setupNotificationTimer = nil
		counter.notify.setupNotificationCh = nil
	}
	counter.notify.mu.Unlock()
	notifyThreshold := granted * notifyFraction
	if notifyThreshold < bufferRUs {
		notifyThreshold = bufferRUs
	}

	var cfg tokenBucketReconfigureArgs
	cfg.NewBurst = bucket.GetSettings().GetBurstLimit()
	// when trickleTimeMs equals zero, server has enough tokens and does not need to
	// limit client consume token. So all token is granted to client right now.
	if trickleTimeMs == 0 {
		cfg.NewTokens = granted
		cfg.NewRate = float64(bucket.GetSettings().FillRate)
		cfg.NotifyThreshold = notifyThreshold
		counter.lastDeadline = time.Time{}
		// In the non-trickle case, clients can be allowed to accumulate more tokens.
		if cfg.NewBurst >= 0 {
			cfg.NewBurst = 0
		}
	} else {
		// Otherwise the granted token is delivered to the client by fill rate.
		cfg.NewTokens = 0
		trickleDuration := time.Duration(trickleTimeMs) * time.Millisecond
		deadline := gc.run.now.Add(trickleDuration)
		cfg.NewRate = float64(bucket.GetSettings().FillRate) + granted/trickleDuration.Seconds()

		timerDuration := trickleDuration - time.Second
		if timerDuration <= 0 {
			timerDuration = (trickleDuration + time.Second) / 2
		}
		counter.notify.mu.Lock()
		counter.notify.setupNotificationTimer = time.NewTimer(timerDuration)
		counter.notify.setupNotificationCh = counter.notify.setupNotificationTimer.C
		counter.notify.setupNotificationThreshold = notifyThreshold
		counter.notify.mu.Unlock()
		counter.lastDeadline = deadline
		select {
		case gc.groupNotificationCh <- gc:
		default:
		}
	}

	counter.lastRate = cfg.NewRate
	counter.limiter.Reconfigure(gc.run.now, cfg)
}

func (gc *groupCostController) collectRequestAndConsumption(low bool) *rmpb.TokenBucketRequest {
	req := &rmpb.TokenBucketRequest{
		ResourceGroupName: gc.ResourceGroup.GetName(),
	}
	// collect request resource
	selected := !low
	switch gc.mode {
	case rmpb.GroupMode_RawMode:
		requests := make([]*rmpb.RawResourceItem, 0, len(requestResourceLimitTypeList))
		for typ, counter := range gc.run.resourceTokens {
			if low && counter.limiter.IsLowTokens() {
				selected = true
			}
			request := &rmpb.RawResourceItem{
				Type:  typ,
				Value: gc.calcRequest(counter),
			}
			requests = append(requests, request)
		}
		req.Request = &rmpb.TokenBucketRequest_RawResourceItems{
			RawResourceItems: &rmpb.TokenBucketRequest_RequestRawResource{
				RequestRawResource: requests,
			},
		}
	case rmpb.GroupMode_RUMode:
		requests := make([]*rmpb.RequestUnitItem, 0, len(requestUnitLimitTypeList))
		for typ, counter := range gc.run.requestUnitTokens {
			if low && counter.limiter.IsLowTokens() {
				selected = true
			}
			request := &rmpb.RequestUnitItem{
				Type:  typ,
				Value: gc.calcRequest(counter),
			}
			requests = append(requests, request)
		}
		req.Request = &rmpb.TokenBucketRequest_RuItems{
			RuItems: &rmpb.TokenBucketRequest_RequestRU{
				RequestRU: requests,
			},
		}
	}
	if !selected {
		return nil
	}

	deltaConsumption := &rmpb.Consumption{}
	*deltaConsumption = *gc.run.consumption
	sub(deltaConsumption, gc.run.lastRequestConsumption)
	req.ConsumptionSinceLastRequest = deltaConsumption

	*gc.run.lastRequestConsumption = *gc.run.consumption
	return req
}

func (gc *groupCostController) calcRequest(counter *tokenCounter) float64 {
	value := counter.avgRUPerSec*gc.run.targetPeriod.Seconds() + bufferRUs
	value -= counter.limiter.AvailableTokens(gc.run.now)
	if value < 0 {
		value = 0
	}
	return value
}

func (gc *groupCostController) onRequestWait(
	ctx context.Context, info RequestInfo,
) (err error) {
	delta := &rmpb.Consumption{}
	for _, calc := range gc.calculators {
		calc.BeforeKVRequest(delta, info)
	}
	now := time.Now()
	if gc.burstable.Load() {
		goto ret
	}
	// retry
retryLoop:
	for i := 0; i < maxRetry; i++ {
		switch gc.mode {
		case rmpb.GroupMode_RawMode:
			res := make([]*Reservation, 0, len(requestResourceLimitTypeList))
			for typ, counter := range gc.run.resourceTokens {
				if v := getRawResourceValueFromConsumption(delta, typ); v > 0 {
					res = append(res, counter.limiter.Reserve(ctx, defaultMaxWaitDuration, now, v))
				}
			}
			if err = WaitReservations(ctx, now, res); err == nil {
				break retryLoop
			}
		case rmpb.GroupMode_RUMode:
			res := make([]*Reservation, 0, len(requestUnitLimitTypeList))
			for typ, counter := range gc.run.requestUnitTokens {
				if v := getRUValueFromConsumption(delta, typ); v > 0 {
					res = append(res, counter.limiter.Reserve(ctx, defaultMaxWaitDuration, now, v))
				}
			}
			if err = WaitReservations(ctx, now, res); err == nil {
				break retryLoop
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		return err
	}
ret:
	gc.mu.Lock()
	add(gc.mu.consumption, delta)
	gc.mu.Unlock()
	return nil
}

func (gc *groupCostController) onResponse(req RequestInfo, resp ResponseInfo) {
	delta := &rmpb.Consumption{}
	for _, calc := range gc.calculators {
		calc.AfterKVRequest(delta, req, resp)
	}
	if gc.burstable.Load() {
		goto ret
	}
	switch gc.mode {
	case rmpb.GroupMode_RawMode:
		for typ, counter := range gc.run.resourceTokens {
			if v := getRawResourceValueFromConsumption(delta, typ); v > 0 {
				counter.limiter.RemoveTokens(time.Now(), v)
			}
		}
	case rmpb.GroupMode_RUMode:
		for typ, counter := range gc.run.requestUnitTokens {
			if v := getRUValueFromConsumption(delta, typ); v > 0 {
				counter.limiter.RemoveTokens(time.Now(), v)
			}
		}
	}
ret:
	gc.mu.Lock()
	add(gc.mu.consumption, delta)
	gc.mu.Unlock()
}
