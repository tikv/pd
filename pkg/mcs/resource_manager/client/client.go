// Copyright 2022 TiKV Project Authors.
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
	"sync"
	"time"

	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var defaultWhiteList = map[string]struct{}{"default": {}}

type ResourceGroupKVInterceptor interface {
	OnRequestWait(ctx context.Context, resourceGroupName string, info RequestInfo) error
	OnResponse(ctx context.Context, resourceGroupName string, req RequestInfo, resp ResponseInfo) error
}

type ResourceGroupProvider interface {
	ListResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, error)
	GetResourceGroup(ctx context.Context, resourceGroupName string) (*rmpb.ResourceGroup, error)
	AddResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error)
	ModifyResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error)
	DeleteResourceGroup(ctx context.Context, resourceGroupName string) (string, error)
	AcquireTokenBuckets(ctx context.Context, request *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error)
}

func NewResourceGroupController(
	clientUniqueId uint64,
	provider ResourceGroupProvider,
	requestUnitConfig *RequestUnitConfig,
) (*resourceGroupsController, error) {
	return newResourceGroupController(clientUniqueId, provider, requestUnitConfig)
}

var _ ResourceGroupKVInterceptor = (*resourceGroupsController)(nil)

type resourceGroupsController struct {
	clientUniqueId   uint64
	provider         ResourceGroupProvider
	groupsController sync.Map
	config           *Config

	calculators []ResourceCalculator

	// tokenResponseChan receives token bucket response from server.
	// And it handles all resource group and runs in main loop
	tokenResponseChan chan []*rmpb.TokenBucketResponse

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

func newResourceGroupController(clientUniqueId uint64, provider ResourceGroupProvider, requestUnitConfig *RequestUnitConfig) (*resourceGroupsController, error) {
	var config *Config
	if requestUnitConfig != nil {
		config = generateConfig(requestUnitConfig)
	} else {
		config = DefaultConfig()
	}
	return &resourceGroupsController{
		clientUniqueId:     clientUniqueId,
		provider:           provider,
		config:             config,
		lowTokenNotifyChan: make(chan struct{}, 1),
		tokenResponseChan:  make(chan []*rmpb.TokenBucketResponse, 1),
		calculators:        []ResourceCalculator{newKVCalculator(config), newSQLLayerCPUCalculateor(config)},
	}, nil
}

func (c *resourceGroupsController) Start(ctx context.Context) error {
	if err := c.updateAllResourceGroups(ctx); err != nil {
		log.Error("update ResourceGroup failed", zap.Error(err))
	}
	c.initRunState(ctx)
	go c.mainLoop(ctx)
	return nil
}

func (c *resourceGroupsController) putResourceGroup(ctx context.Context, name string) (*groupCostController, error) {
	group, err := c.provider.GetResourceGroup(ctx, name)
	if err != nil {
		return nil, err
	}
	log.Info("create resource group cost controller", zap.String("name", group.GetName()))
	gc := newGroupCostController(ctx, group, c.config, c.lowTokenNotifyChan)
	c.groupsController.Store(group.GetName(), gc)
	gc.initRunState(ctx)
	return gc, nil
}

func (c *resourceGroupsController) updateAllResourceGroups(ctx context.Context) error {
	groups, err := c.provider.ListResourceGroups(ctx)
	if err != nil {
		return err
	}
	lastedGroups := make(map[string]struct{})
	for _, group := range groups {
		log.Info("create resource group cost controller", zap.String("name", group.GetName()))
		gc := newGroupCostController(ctx, group, c.config, c.lowTokenNotifyChan)
		c.groupsController.Store(group.GetName(), gc)
		lastedGroups[group.GetName()] = struct{}{}
	}
	c.groupsController.Range(func(key, value any) bool {
		resourceGroupName := key.(string)
		if _, ok := lastedGroups[resourceGroupName]; !ok {
			c.groupsController.Delete(key)
		}
		return true
	})
	return nil
}

func (c *resourceGroupsController) initRunState(ctx context.Context) {
	now := time.Now()
	c.run.now = now
	c.run.lastRequestTime = now
	c.run.targetPeriod = c.config.targetPeriod
	c.groupsController.Range(func(name, value any) bool {
		gc := value.(*groupCostController)
		gc.initRunState(ctx)
		return true
	})
}

func (c *resourceGroupsController) updateRunState(ctx context.Context) {
	c.run.now = time.Now()
	c.groupsController.Range(func(name, value any) bool {
		gc := value.(*groupCostController)
		gc.updateRunState(ctx)
		return true
	})
}

func (c *resourceGroupsController) shouldReportConsumption() bool {
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

func (c *resourceGroupsController) updateAvgRequestResourcePerSec(ctx context.Context) {
	c.groupsController.Range(func(name, value any) bool {
		gc := value.(*groupCostController)
		gc.updateAvgRequestResourcePerSec(ctx)
		return true
	})
}

func (c *resourceGroupsController) handleTokenBucketResponse(ctx context.Context, resp []*rmpb.TokenBucketResponse) {
	for _, res := range resp {
		name := res.GetResourceGroupName()
		v, ok := c.groupsController.Load(name)
		if !ok {
			log.Warn("A non-existent resource group was found when handle token response.", zap.String("name", name))
		}
		gc := v.(*groupCostController)
		gc.handleTokenBucketResponse(ctx, res)
	}
}

func (c *resourceGroupsController) collectTokenBucketRequests(ctx context.Context, source string, low bool) {
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

func (c *resourceGroupsController) sendTokenBucketRequests(ctx context.Context, requests []*rmpb.TokenBucketRequest, source string) {
	now := time.Now()
	c.run.lastRequestTime = now
	c.run.requestInProgress = true
	req := &rmpb.TokenBucketsRequest{
		Requests:              requests,
		TargetRequestPeriodMs: uint64(c.config.targetPeriod / time.Millisecond),
	}
	go func() {
		log.Info("[resource group controllor] send token bucket request", zap.Time("now", now), zap.Any("req", req.Requests), zap.String("source", source))
		resp, err := c.provider.AcquireTokenBuckets(ctx, req)
		if err != nil {
			// Don't log any errors caused by the stopper canceling the context.
			if !errors.ErrorEqual(err, context.Canceled) {
				log.L().Sugar().Infof("TokenBucket RPC error: %v", err)
			}
			resp = nil
		}
		log.Info("[resource group controllor] token bucket response", zap.Time("now", time.Now()), zap.Any("resp", resp), zap.String("source", source), zap.Duration("latency", time.Since(now)))
		c.tokenResponseChan <- resp
	}()
}

func (c *resourceGroupsController) handleTokenBucketTrickEvent(ctx context.Context) {
	c.groupsController.Range(func(name, value any) bool {
		gc := value.(*groupCostController)
		gc.handleTokenBucketTrickEvent(ctx)
		return true
	})
}

func (c *resourceGroupsController) mainLoop(ctx context.Context) {
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
				c.handleTokenBucketResponse(ctx, resp)
			} else {
				// A nil response indicates a failure (which would have been logged).
				c.run.requestNeedsRetry = true
			}
		case <-ticker.C:
			c.updateRunState(ctx)
			c.updateAvgRequestResourcePerSec(ctx)
			if c.run.requestNeedsRetry || c.shouldReportConsumption() {
				c.run.requestNeedsRetry = false
				c.collectTokenBucketRequests(ctx, "report", false /* select all */)
			}
		case <-c.lowTokenNotifyChan:
			c.updateRunState(ctx)
			c.updateAvgRequestResourcePerSec(ctx)
			if !c.run.requestInProgress {
				c.collectTokenBucketRequests(ctx, "low_ru", false /* only select low tokens resource group */)
				//c.collectTokenBucketRequests(ctx, "low_ru", true /* only select low tokens resource group */)
			}
		default:
			c.handleTokenBucketTrickEvent(ctx)
		}
	}
}

func (c *resourceGroupsController) OnRequestWait(
	ctx context.Context, resourceGroupName string, info RequestInfo,
) (err error) {
	if _, ok := defaultWhiteList[resourceGroupName]; ok {
		return nil
	}
	var gc *groupCostController
	if tmp, ok := c.groupsController.Load(resourceGroupName); ok {
		gc = tmp.(*groupCostController)
	} else {
		gc, err = c.putResourceGroup(ctx, resourceGroupName)
		if err != nil {
			return errors.Errorf("[resource group] resourceGroupName %s is not existed.", resourceGroupName)
		}
	}
	err = gc.OnRequestWait(ctx, info)
	return err
}

func (c *resourceGroupsController) OnResponse(ctx context.Context, resourceGroupName string, req RequestInfo, resp ResponseInfo) error {
	if _, ok := defaultWhiteList[resourceGroupName]; ok {
		return nil
	}
	tmp, ok := c.groupsController.Load(resourceGroupName)
	if !ok {
		log.Warn("[resource group] resourceGroupName is not existed.", zap.String("resourceGroupName", resourceGroupName))
	}
	gc := tmp.(*groupCostController)
	gc.OnResponse(ctx, req, resp)
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

	lowRUNotifyChan chan struct{}
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

		resourceTokens    map[rmpb.ResourceType]*tokenCounter
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

	setupNotificationCh        <-chan time.Time
	setupNotificationThreshold float64
	setupNotificationTimer     *time.Timer

	lastDeadline time.Time
	lastRate     float64

	limiter *Limiter
}

func newGroupCostController(ctx context.Context, group *rmpb.ResourceGroup, mainCfg *Config, lowRUNotifyChan chan struct{}) *groupCostController {
	gc := &groupCostController{
		ResourceGroup:   group,
		mainCfg:         mainCfg,
		calculators:     []ResourceCalculator{newKVCalculator(mainCfg), newSQLLayerCPUCalculateor(mainCfg)},
		mode:            group.GetMode(),
		lowRUNotifyChan: lowRUNotifyChan,
	}

	switch gc.mode {
	case rmpb.GroupMode_RUMode:
		gc.handleRespFunc = gc.handleRUTokenResponse
	case rmpb.GroupMode_RawMode:
		gc.handleRespFunc = gc.handleResourceTokenResponse
	}

	gc.mu.consumption = &rmpb.Consumption{}
	return gc
}

func (gc *groupCostController) initRunState(ctx context.Context) {
	now := time.Now()
	gc.run.now = now
	gc.run.targetPeriod = gc.mainCfg.targetPeriod

	gc.run.consumption = &rmpb.Consumption{}

	gc.run.lastRequestConsumption = &rmpb.Consumption{}

	switch gc.mode {
	case rmpb.GroupMode_RUMode:
		gc.run.requestUnitTokens = make(map[rmpb.RequestUnitType]*tokenCounter)
		for typ := range requestUnitList {
			counter := &tokenCounter{
				limiter:     NewLimiter(0, initialRequestUnits, gc.lowRUNotifyChan),
				avgRUPerSec: initialRequestUnits / gc.run.targetPeriod.Seconds() * 2,
				avgLastTime: now,
			}
			gc.run.requestUnitTokens[typ] = counter
		}
	case rmpb.GroupMode_RawMode:
		gc.run.resourceTokens = make(map[rmpb.ResourceType]*tokenCounter)
		for typ := range requestResourceList {
			counter := &tokenCounter{
				limiter:     NewLimiter(0, initialRequestUnits, gc.lowRUNotifyChan),
				avgRUPerSec: initialRequestUnits / gc.run.targetPeriod.Seconds() * 2,
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
		calc.Trickle(deltaConsumption, ctx)
	}
	gc.mu.Lock()
	Add(gc.mu.consumption, deltaConsumption)
	*gc.run.consumption = *gc.mu.consumption
	gc.mu.Unlock()
	// remove tokens
	switch gc.mode {
	case rmpb.GroupMode_RUMode:
		for typ, counter := range gc.run.requestUnitTokens {
			if v := GetRUValueFromConsumption(deltaConsumption, typ); v > 0 {
				counter.limiter.RemoveTokens(newTime, v)
			}
		}
	case rmpb.GroupMode_RawMode:
		for typ, counter := range gc.run.resourceTokens {
			if v := GetResourceValueFromConsumption(deltaConsumption, typ); v > 0 {
				counter.limiter.RemoveTokens(newTime, v)
			}
		}
	}
	log.Info("update run state", zap.Any("request unit comsumption", gc.run.consumption))
	gc.run.now = newTime
}

func (gc *groupCostController) updateAvgRequestResourcePerSec(ctx context.Context) {
	switch gc.mode {
	case rmpb.GroupMode_RawMode:
		gc.updateAvgResourcePerSec(ctx)
	case rmpb.GroupMode_RUMode:
		gc.updateAvgRUPerSec(ctx)
	}
}

func (gc *groupCostController) handleTokenBucketTrickEvent(ctx context.Context) {
	switch gc.mode {
	case rmpb.GroupMode_RawMode:
		for _, counter := range gc.run.resourceTokens {
			select {
			case <-counter.setupNotificationCh:
				counter.setupNotificationTimer = nil
				counter.setupNotificationCh = nil
				counter.limiter.SetupNotificationAt(gc.run.now, float64(counter.setupNotificationThreshold))
				gc.updateRunState(ctx)
			default:
			}
		}
	case rmpb.GroupMode_RUMode:
		for _, counter := range gc.run.requestUnitTokens {
			select {
			case <-counter.setupNotificationCh:
				counter.setupNotificationTimer = nil
				counter.setupNotificationCh = nil
				counter.limiter.SetupNotificationAt(gc.run.now, float64(counter.setupNotificationThreshold))
				gc.updateRunState(ctx)
			default:
			}
		}
	}
}

func (gc *groupCostController) updateAvgResourcePerSec(ctx context.Context) {
	for typ, counter := range gc.run.resourceTokens {
		if !gc.calcAvg(counter, GetResourceValueFromConsumption(gc.run.consumption, typ)) {
			continue
		}
		log.Info("[resource group controllor] update avg ru per sec", zap.String("name", gc.Name), zap.String("type", rmpb.ResourceType_name[int32(typ)]), zap.Float64("avgRUPerSec", counter.avgRUPerSec))
	}
}

func (gc *groupCostController) updateAvgRUPerSec(ctx context.Context) {
	for typ, counter := range gc.run.requestUnitTokens {
		if !gc.calcAvg(counter, GetRUValueFromConsumption(gc.run.consumption, typ)) {
			continue
		}
		log.Info("[resource group controllor] update avg ru per sec", zap.String("name", gc.Name), zap.String("type", rmpb.RequestUnitType_name[int32(typ)]), zap.Float64("avgRUPerSec", counter.avgRUPerSec))
	}
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
		for typ := range requestUnitList {
			if GetRUValueFromConsumption(gc.run.consumption, typ)-GetRUValueFromConsumption(gc.run.lastRequestConsumption, typ) >= consumptionsReportingThreshold {
				return true
			}
		}
	case rmpb.GroupMode_RawMode:
		for typ := range requestResourceList {
			if GetResourceValueFromConsumption(gc.run.consumption, typ)-GetResourceValueFromConsumption(gc.run.lastRequestConsumption, typ) >= consumptionsReportingThreshold {
				return true
			}
		}
	}
	return false
}

func (gc *groupCostController) handleTokenBucketResponse(ctx context.Context, resp *rmpb.TokenBucketResponse) {
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

func (gc *groupCostController) handleResourceTokenResponse(resp *rmpb.TokenBucketResponse) {
	for _, grantedTB := range resp.GetGrantedResourceTokens() {
		typ := grantedTB.GetType()
		// todo: check whether grant = 0
		counter, ok := gc.run.resourceTokens[typ]
		if !ok {
			log.Warn("not support this resource type", zap.String("type", rmpb.ResourceType_name[int32(typ)]))
			continue
		}
		gc.modifyTokenCounter(counter, grantedTB.GetGrantedTokens(), grantedTB.GetTrickleTimeMs())
	}
}

func (gc *groupCostController) handleRUTokenResponse(resp *rmpb.TokenBucketResponse) {
	for _, grantedTB := range resp.GetGrantedRUTokens() {
		typ := grantedTB.GetType()
		// todo: check whether grant = 0
		counter, ok := gc.run.requestUnitTokens[typ]
		if !ok {
			log.Warn("not support this resource type", zap.String("type", rmpb.ResourceType_name[int32(typ)]))
			continue
		}
		gc.modifyTokenCounter(counter, grantedTB.GetGrantedTokens(), grantedTB.GetTrickleTimeMs())
	}
}

func (gc *groupCostController) modifyTokenCounter(counter *tokenCounter, bucket *rmpb.TokenBucket, trickleTimeMs int64) {
	granted := bucket.Tokens
	remainder := 0.
	if !counter.lastDeadline.IsZero() {
		// If last request came with a trickle duration, we may have RUs that were
		// not made available to the bucket yet; throw them together with the newly
		// granted RUs.
		if since := counter.lastDeadline.Sub(gc.run.now); since > 0 {
			remainder = counter.lastRate * since.Seconds()
		}
	}
	if counter.setupNotificationTimer != nil {
		counter.setupNotificationTimer.Stop()
		counter.setupNotificationTimer = nil
		counter.setupNotificationCh = nil
	}
	notifyThreshold := granted * notifyFraction
	if notifyThreshold < bufferRUs {
		notifyThreshold = bufferRUs
	}

	var cfg tokenBucketReconfigureArgs
	if trickleTimeMs == 0 {
		cfg.NewTokens = granted
		cfg.NewRate = float64(bucket.GetSettings().FillRate)
		cfg.NotifyThreshold = notifyThreshold
		counter.lastDeadline = time.Time{}
	} else {
		cfg.NewTokens = remainder
		trickleDuration := time.Duration(trickleTimeMs) * time.Millisecond
		deadline := gc.run.now.Add(trickleDuration)
		cfg.NewRate = float64(bucket.GetSettings().FillRate) + bucket.Tokens/trickleDuration.Seconds()

		timerDuration := trickleDuration - time.Second
		if timerDuration <= 0 {
			timerDuration = (trickleDuration + time.Second) / 2
		}
		counter.setupNotificationTimer = time.NewTimer(timerDuration)
		counter.setupNotificationCh = counter.setupNotificationTimer.C
		counter.setupNotificationThreshold = notifyThreshold

		counter.lastDeadline = deadline
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
		requests := make([]*rmpb.ResourceItem, 0, len(requestResourceList))
		for typ, counter := range gc.run.resourceTokens {
			if low && counter.limiter.IsLowTokens() {
				selected = true
			}
			request := &rmpb.ResourceItem{
				Type:  typ,
				Value: gc.calcRequest(counter),
			}
			requests = append(requests, request)
		}
		req.Request = &rmpb.TokenBucketRequest_ResourceItems{
			ResourceItems: &rmpb.TokenBucketRequest_RequestResource{
				RequestResource: requests,
			},
		}
	case rmpb.GroupMode_RUMode:
		requests := make([]*rmpb.RequestUnitItem, 0, len(requestUnitList))
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
	Sub(deltaConsumption, gc.run.lastRequestConsumption)
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

func (gc *groupCostController) OnRequestWait(
	ctx context.Context, info RequestInfo,
) error {
	delta := &rmpb.Consumption{}
	for _, calc := range gc.calculators {
		calc.BeforeKVRequest(delta, info)
	}
	now := time.Now()
	switch gc.mode {
	case rmpb.GroupMode_RawMode:
		res := make([]*Reservation, 0, len(requestResourceList))
		for typ, counter := range gc.run.resourceTokens {
			if v := GetResourceValueFromConsumption(delta, typ); v > 0 {
				res = append(res, counter.limiter.ReserveN(ctx, now, int(v)))
			}
		}
		if err := waitReservations(now, ctx, res); err != nil {
			return err
		}
	case rmpb.GroupMode_RUMode:
		res := make([]*Reservation, 0, len(requestUnitList))
		for typ, counter := range gc.run.requestUnitTokens {
			if v := GetRUValueFromConsumption(delta, typ); v > 0 {
				res = append(res, counter.limiter.ReserveN(ctx, now, int(v)))
			}
		}
		if err := waitReservations(now, ctx, res); err != nil {
			return err
		}
	}
	gc.mu.Lock()
	Add(gc.mu.consumption, delta)
	gc.mu.Unlock()
	return nil
}

func (gc *groupCostController) OnResponse(ctx context.Context, req RequestInfo, resp ResponseInfo) {
	delta := &rmpb.Consumption{}
	for _, calc := range gc.calculators {
		calc.AfterKVRequest(delta, req, resp)
	}

	switch gc.mode {
	case rmpb.GroupMode_RawMode:
		for typ, counter := range gc.run.resourceTokens {
			if v := GetResourceValueFromConsumption(delta, typ); v > 0 {
				counter.limiter.RemoveTokens(time.Now(), float64(v))
			}
		}
	case rmpb.GroupMode_RUMode:
		for typ, counter := range gc.run.requestUnitTokens {
			if v := GetRUValueFromConsumption(delta, typ); v > 0 {
				counter.limiter.RemoveTokens(time.Now(), float64(v))
			}
		}
	}
	gc.mu.Lock()
	Add(gc.mu.consumption, delta)
	gc.mu.Unlock()
}
