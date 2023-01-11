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

package tenantclient

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type TenantSideKVInterceptor interface {
	OnRequestWait(ctx context.Context, resourceGroupName string, info RequestInfo) error
	OnResponse(ctx context.Context, resourceGroupName string, req RequestInfo, resp ResponseInfo) error
}

type ResourceGroupProvider interface {
	ListResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, error)
	GetResourceGroup(ctx context.Context, resourceGroupName string) (*rmpb.ResourceGroup, error)
	AddResourceGroup(ctx context.Context, resourceGroupName string, settings *rmpb.GroupSettings) (string, error)
	ModifyResourceGroup(ctx context.Context, resourceGroupName string, settings *rmpb.GroupSettings) (string, error)
	DeleteResourceGroup(ctx context.Context, resourceGroupName string) (string, error)
	AcquireTokenBuckets(ctx context.Context, request *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error)
}

func NewResourceGroupController(
	provider ResourceGroupProvider,
) (*resourceGroupsController, error) {
	return newTenantSideCostController(provider)
}

var _ TenantSideKVInterceptor = (*resourceGroupsController)(nil)

type resourceGroupsController struct {
	instanceFingerprint string
	provider            ResourceGroupProvider
	groupsController    sync.Map
	config              *Config

	// responseChan is used to receive results from token bucket requests, which
	// are run in a separate goroutine. A nil response indicates an error.
	responseChan chan []*rmpb.TokenBucketResponse

	// lowRUNotifyChan is used when the number of available resource is running low and
	// we need to send an early token bucket request.
	lowRUNotifyChan chan struct{}

	run struct {
		now             time.Time
		lastRequestTime time.Time
		// requestInProgress is true if we are in the process of sending a request;
		// it gets set to false when we process the response (in the main loop),
		// even in error cases.
		requestInProgress bool

		// requestNeedsRetry is set if the last token bucket request encountered an
		// error. This triggers a retry attempt on the next tick.
		//
		// Note: requestNeedsRetry and requestInProgress are never true at the same
		// time.
		requestNeedsRetry bool

		// targetPeriod stores the value of the TargetPeriodSetting setting at the
		// last update.
		targetPeriod time.Duration
	}
}

func newTenantSideCostController(provider ResourceGroupProvider) (*resourceGroupsController, error) {
	return &resourceGroupsController{
		provider:        provider,
		config:          DefaultConfig(),
		lowRUNotifyChan: make(chan struct{}, 1),
		responseChan:    make(chan []*rmpb.TokenBucketResponse, 1),
	}, nil
}

func (c *resourceGroupsController) Start(ctx context.Context, instanceFingerprint string) error {
	if len(instanceFingerprint) == 0 {
		return errors.New("invalid SQLInstanceID")
	}
	c.instanceFingerprint = instanceFingerprint
	// just for demo
	if err := c.addDemoResourceGroup(ctx); err != nil {
		log.Error("add Demo ResourceGroup failed", zap.Error(err))
	}
	if err := c.updateAllResourceGroups(ctx); err != nil {
		log.Error("update ResourceGroup failed", zap.Error(err))
	}

	c.initRunState(ctx)
	go c.mainLoop(ctx)
	return nil
}

func (c *resourceGroupsController) updateAllResourceGroups(ctx context.Context) error {
	groups, err := c.provider.ListResourceGroups(ctx)
	if err != nil {
		return err
	}
	lastedGroups := make(map[string]struct{})
	for _, group := range groups {
		// todo: check add or modify
		log.Info("create resource group cost controller", zap.String("name", group.GetName()))
		gc := newGroupCostController(ctx, group, c.config, c.lowRUNotifyChan)
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
	requests := make([]*rmpb.TokenBucketRequst, 0)
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

func (c *resourceGroupsController) sendTokenBucketRequests(ctx context.Context, requests []*rmpb.TokenBucketRequst, source string) {
	c.run.requestInProgress = true
	req := &rmpb.TokenBucketsRequest{
		Requests:              requests,
		TargetRequestPeriodMs: uint64(c.config.targetPeriod / time.Millisecond),
	}
	go func() {
		now := time.Now()
		log.Info("[tenant controllor] send token bucket request", zap.Time("now", now), zap.Any("req", req.Requests), zap.String("source", source))
		resp, err := c.provider.AcquireTokenBuckets(ctx, req)
		if err != nil {
			// Don't log any errors caused by the stopper canceling the context.
			if !errors.ErrorEqual(err, context.Canceled) {
				log.L().Sugar().Infof("TokenBucket RPC error: %v", err)
			}
			resp = nil
		}
		log.Info("[tenant controllor] token bucket response", zap.Time("now", time.Now()), zap.Any("resp", resp), zap.String("source", source), zap.Duration("latency", time.Since(now)))
		c.responseChan <- resp
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
		case resp := <-c.responseChan:
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
		case <-c.lowRUNotifyChan:
			c.updateRunState(ctx)
			if !c.run.requestInProgress {
				c.collectTokenBucketRequests(ctx, "low_ru", true /* only select low tokens resource group */)
			}
		default:
			c.handleTokenBucketTrickEvent(ctx)
		}
	}
}

func (c *resourceGroupsController) OnRequestWait(
	ctx context.Context, resourceGroupName string, info RequestInfo,
) error {
	tmp, ok := c.groupsController.Load(resourceGroupName)
	if !ok {
		return errors.Errorf("[resource group] resourceGroupName %s is not existed.", resourceGroupName)
	}
	gc := tmp.(*groupCostController)
	err := gc.OnRequestWait(ctx, info)
	return err
}

func (c *resourceGroupsController) OnResponse(ctx context.Context, resourceGroupName string, req RequestInfo, resp ResponseInfo) error {
	tmp, ok := c.groupsController.Load(resourceGroupName)
	if !ok {
		log.Warn("[resource group] resourceGroupName is not existed.", zap.String("resourceGroupName", resourceGroupName))
	}
	gc := tmp.(*groupCostController)
	gc.OnResponse(ctx, req, resp)
	return nil
}

type groupCostController struct {
	resourceGroupName string
	mainCfg           *Config
	groupSettings     *rmpb.GroupSettings
	calculators       []ResourceCalculator
	mode              rmpb.GroupMode

	handleRespFunc func(*rmpb.TokenBucketResponse)

	mu struct {
		sync.Mutex
		requestUnitConsumptions []*rmpb.RequestUnitItem
		resourceConsumptions    []*rmpb.ResourceItem
	}

	lowRUNotifyChan chan struct{}
	// run contains the state that is updated by the main loop.
	run struct {
		now time.Time

		// targetPeriod stores the value of the TargetPeriodSetting setting at the
		// last update.
		targetPeriod time.Duration

		// consumptions stores the last value of mu.consumption.
		requestUnitConsumptions []*rmpb.RequestUnitItem
		resourceConsumptions    []*rmpb.ResourceItem

		lastRequestUnitConsumptions []*rmpb.RequestUnitItem
		lastResourceConsumptions    []*rmpb.ResourceItem

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
		resourceGroupName: group.GetName(),
		mainCfg:           mainCfg,
		groupSettings:     group.Settings,
		calculators:       []ResourceCalculator{newKVCalculator(mainCfg), newSQLLayerCPUCalculateor(mainCfg)},
		mode:              group.Settings.GetMode(),
		lowRUNotifyChan:   lowRUNotifyChan,
	}

	switch gc.mode {
	case rmpb.GroupMode_RUMode:
		gc.handleRespFunc = gc.handleRUTokenResponse
	case rmpb.GroupMode_NativeMode:
		gc.handleRespFunc = gc.handleResourceTokenResponse
	}

	gc.mu.requestUnitConsumptions = make([]*rmpb.RequestUnitItem, ruLen)
	for typ := range gc.mu.requestUnitConsumptions {
		gc.mu.requestUnitConsumptions[typ] = &rmpb.RequestUnitItem{
			Type: rmpb.RequestUnitType(typ),
		}
	}
	gc.mu.resourceConsumptions = make([]*rmpb.ResourceItem, resourceLen)
	for typ := range gc.mu.resourceConsumptions {
		gc.mu.resourceConsumptions[typ] = &rmpb.ResourceItem{
			Type: rmpb.ResourceType(typ),
		}
	}
	return gc
}

func (gc *groupCostController) initRunState(ctx context.Context) {
	now := time.Now()
	gc.run.now = now
	gc.run.targetPeriod = gc.mainCfg.targetPeriod

	gc.run.requestUnitConsumptions = make([]*rmpb.RequestUnitItem, ruLen)
	for typ := range gc.run.requestUnitConsumptions {
		gc.run.requestUnitConsumptions[typ] = &rmpb.RequestUnitItem{
			Type: rmpb.RequestUnitType(typ),
		}
	}
	gc.run.resourceConsumptions = make([]*rmpb.ResourceItem, resourceLen)
	for typ := range gc.run.resourceConsumptions {
		gc.run.resourceConsumptions[typ] = &rmpb.ResourceItem{
			Type: rmpb.ResourceType(typ),
		}
	}

	gc.run.lastRequestUnitConsumptions = make([]*rmpb.RequestUnitItem, ruLen)
	for typ := range gc.run.lastRequestUnitConsumptions {
		gc.run.lastRequestUnitConsumptions[typ] = &rmpb.RequestUnitItem{
			Type: rmpb.RequestUnitType(typ),
		}
	}
	gc.run.lastResourceConsumptions = make([]*rmpb.ResourceItem, resourceLen)
	for typ := range gc.run.lastResourceConsumptions {
		gc.run.lastResourceConsumptions[typ] = &rmpb.ResourceItem{
			Type: rmpb.ResourceType(typ),
		}
	}

	switch gc.mode {
	case rmpb.GroupMode_RUMode:
		gc.run.requestUnitTokens = make(map[rmpb.RequestUnitType]*tokenCounter)
		for typ := range requestUnitList {
			counter := &tokenCounter{
				limiter:     NewLimiter(0, initialRequestUnits, gc.lowRUNotifyChan),
				avgRUPerSec: initialRequestUnits / gc.run.targetPeriod.Seconds(),
				avgLastTime: now,
			}
			gc.run.requestUnitTokens[typ] = counter
		}
	case rmpb.GroupMode_NativeMode:
		gc.run.resourceTokens = make(map[rmpb.ResourceType]*tokenCounter)
		for typ := range requestResourceList {
			counter := &tokenCounter{
				limiter:     NewLimiter(0, initialRequestUnits, gc.lowRUNotifyChan),
				avgRUPerSec: initialRequestUnits / gc.run.targetPeriod.Seconds(),
				avgLastTime: now,
			}
			gc.run.resourceTokens[typ] = counter
		}
	}
}

func (gc *groupCostController) updateRunState(ctx context.Context) {
	newTime := time.Now()
	deltaResource := make(map[rmpb.ResourceType]float64)
	deltaRequestUnit := make(map[rmpb.RequestUnitType]float64)
	for _, calc := range gc.calculators {
		calc.Trickle(deltaResource, deltaRequestUnit, ctx)
	}
	gc.mu.Lock()
	for typ, detail := range deltaRequestUnit {
		gc.mu.requestUnitConsumptions[typ].Value += detail
	}
	copy(gc.run.requestUnitConsumptions, gc.mu.requestUnitConsumptions)
	copy(gc.run.resourceConsumptions, gc.mu.resourceConsumptions)
	gc.mu.Unlock()

	// remove tokens
	switch gc.mode {
	case rmpb.GroupMode_RUMode:
		for typ, counter := range gc.run.requestUnitTokens {
			v, ok := deltaRequestUnit[typ]
			if ok {
				value := v
				counter.limiter.RemoveTokens(newTime, value)
			}
		}
	case rmpb.GroupMode_NativeMode:
		for typ, counter := range gc.run.resourceTokens {
			v, ok := deltaResource[typ]
			if ok {
				value := v
				counter.limiter.RemoveTokens(newTime, value)
			}
		}
	}

	log.Info("update run state", zap.Any("request unit comsumption", gc.run.requestUnitConsumptions), zap.Any("resource comsumption", gc.run.resourceConsumptions))
	gc.run.now = newTime
}

func (gc *groupCostController) updateAvgRequestResourcePerSec(ctx context.Context) {
	switch gc.mode {
	case rmpb.GroupMode_NativeMode:
		gc.updateAvgResourcePerSec(ctx)
	case rmpb.GroupMode_RUMode:
		gc.updateAvgRUPerSec(ctx)
	}
}

func (gc *groupCostController) handleTokenBucketTrickEvent(ctx context.Context) {
	switch gc.mode {
	case rmpb.GroupMode_NativeMode:
		for _, counter := range gc.run.resourceTokens {
			select {
			case <-counter.setupNotificationCh:
				counter.setupNotificationTimer = nil
				counter.setupNotificationCh = nil
				counter.limiter.SetupNotification(gc.run.now, counter.setupNotificationThreshold)
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
				counter.limiter.SetupNotification(gc.run.now, counter.setupNotificationThreshold)
				gc.updateRunState(ctx)
			default:
			}
		}
	}
}

func (gc *groupCostController) updateAvgResourcePerSec(ctx context.Context) {
	for typ, counter := range gc.run.resourceTokens {
		if !gc.calcAvg(counter, gc.run.resourceConsumptions[typ].Value) {
			continue
		}
		log.Info("[resource group controllor] update avg ru per sec", zap.String("name", gc.resourceGroupName), zap.String("type", rmpb.ResourceType_name[int32(typ)]), zap.Float64("avgRUPerSec", counter.avgRUPerSec))
	}
}

func (gc *groupCostController) updateAvgRUPerSec(ctx context.Context) {
	for typ, counter := range gc.run.requestUnitTokens {
		if !gc.calcAvg(counter, gc.run.resourceConsumptions[typ].Value) {
			continue
		}
		log.Info("[resource group controllor] update avg ru per sec", zap.String("name", gc.resourceGroupName), zap.String("type", rmpb.RequestUnitType_name[int32(typ)]), zap.Float64("avgRUPerSec", counter.avgRUPerSec))
	}
}

func (gc *groupCostController) calcAvg(counter *tokenCounter, new float64) bool {
	deltaDuration := gc.run.now.Sub(counter.avgLastTime)
	if deltaDuration <= 10*time.Millisecond {
		return false
	}
	delta := (new - counter.avgRUPerSecLastRU) / deltaDuration.Seconds()
	counter.avgRUPerSec = movingAvgFactor*counter.avgRUPerSec + (1-movingAvgFactor)*delta
	counter.avgLastTime = gc.run.now
	counter.avgRUPerSecLastRU = new
	return true
}

func (gc *groupCostController) shouldReportConsumption() bool {
	for typ := range requestUnitList {
		if gc.run.requestUnitConsumptions[typ].Value-gc.run.lastRequestUnitConsumptions[typ].Value >= consumptionsReportingThreshold {
			return true
		}
	}
	for typ := range requestResourceList {
		if gc.run.resourceConsumptions[typ].Value-gc.run.lastResourceConsumptions[typ].Value >= consumptionsReportingThreshold {
			return true
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
		cfg.NewRate = float64(bucket.GetSettings().Fillrate)
		cfg.NewBrust = int(granted + 1)
		cfg.NotifyThreshold = notifyThreshold
		counter.lastDeadline = time.Time{}
	} else {
		cfg.NewTokens = remainder
		trickleDuration := time.Duration(trickleTimeMs) * time.Millisecond
		deadline := gc.run.now.Add(trickleDuration)
		cfg.NewRate = float64(bucket.GetSettings().Fillrate) + bucket.Tokens/trickleDuration.Seconds()

		timerDuration := trickleDuration - time.Second
		if timerDuration <= 0 {
			timerDuration = (trickleDuration + time.Second) / 2
		}
		log.Info("QQQ2 ", zap.Duration("timerDuration", timerDuration), zap.Float64("cfg.NewRate", cfg.NewRate))
		counter.setupNotificationTimer = time.NewTimer(timerDuration)
		counter.setupNotificationCh = counter.setupNotificationTimer.C
		counter.setupNotificationThreshold = notifyThreshold

		counter.lastDeadline = deadline
	}
	counter.lastRate = cfg.NewRate
	counter.limiter.Reconfigure(gc.run.now, cfg)
}

func (gc *groupCostController) collectRequestAndConsumption(low bool) *rmpb.TokenBucketRequst {
	req := &rmpb.TokenBucketRequst{
		ResourceGroupName: gc.resourceGroupName,
	}
	// collect request resource
	selected := !low
	switch gc.mode {
	case rmpb.GroupMode_NativeMode:
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
		req.Request = &rmpb.TokenBucketRequst_ResourceItems{
			ResourceItems: &rmpb.TokenBucketRequst_RequestResource{
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
		req.Request = &rmpb.TokenBucketRequst_RuItems{
			RuItems: &rmpb.TokenBucketRequst_RequestRU{
				RequestRU: requests,
			},
		}
	}
	if !selected {
		return nil
	}

	// collect resource consumption
	deltaResourceConsumption := make([]*rmpb.ResourceItem, resourceLen)
	for typ, cons := range gc.run.resourceConsumptions {
		deltaResourceConsumption[typ] = &rmpb.ResourceItem{
			Type:  rmpb.ResourceType(typ),
			Value: Sub(cons.Value, gc.run.lastResourceConsumptions[typ].Value),
		}
	}
	// collect request unit consumption
	deltaRequestUnitConsumption := make([]*rmpb.RequestUnitItem, ruLen)
	for typ, cons := range gc.run.requestUnitConsumptions {
		deltaRequestUnitConsumption[typ] = &rmpb.RequestUnitItem{
			Type:  rmpb.RequestUnitType(typ),
			Value: Sub(cons.Value, gc.run.lastRequestUnitConsumptions[typ].Value),
		}
	}
	req.ConsumptionResourceSinceLastRequest = deltaResourceConsumption
	req.ConsumptionRUSinceLastRequest = deltaRequestUnitConsumption

	copy(gc.run.lastRequestUnitConsumptions, gc.run.requestUnitConsumptions)
	copy(gc.run.lastResourceConsumptions, gc.run.resourceConsumptions)
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
	deltaResource := make(map[rmpb.ResourceType]float64)
	deltaRequestUnit := make(map[rmpb.RequestUnitType]float64)
	for _, calc := range gc.calculators {
		calc.BeforeKVRequest(deltaResource, deltaRequestUnit, info)
	}
	var wg sync.WaitGroup
	var errReturn error
	switch gc.mode {
	case rmpb.GroupMode_NativeMode:
		wg.Add(len(requestResourceList))
		for typ, counter := range gc.run.resourceTokens {
			v, ok := deltaResource[typ]
			if ok {
				go func(value float64, counter *tokenCounter) {
					if ok {
						err := counter.limiter.WaitN(ctx, int(v))
						if err != nil {
							errReturn = err
						}
					}
					wg.Done()
				}(v, counter)
			} else {
				wg.Done()
			}
		}
		wg.Wait()
		if errReturn != nil {
			return errReturn
		}
		gc.mu.Lock()
		for typ, detail := range deltaResource {
			gc.mu.requestUnitConsumptions[typ].Value += detail
		}
		gc.mu.Unlock()
	case rmpb.GroupMode_RUMode:
		wg.Add(len(requestUnitList))
		for typ, counter := range gc.run.requestUnitTokens {
			v, ok := deltaRequestUnit[typ]
			if ok {
				go func(value float64, counter *tokenCounter) {
					if ok {
						err := counter.limiter.WaitN(ctx, int(v))
						if err != nil {
							errReturn = err
						}
					}
					wg.Done()
				}(v, counter)
			} else {
				wg.Done()
			}
		}
		wg.Wait()
		if errReturn != nil {
			return errReturn
		}
		gc.mu.Lock()
		for typ, detail := range deltaRequestUnit {
			gc.mu.resourceConsumptions[typ].Value += detail
		}
		gc.mu.Unlock()
	}

	return nil
}

func (gc *groupCostController) OnResponse(ctx context.Context, req RequestInfo, resp ResponseInfo) {
	deltaResource := make(map[rmpb.ResourceType]float64)
	deltaRequestUnit := make(map[rmpb.RequestUnitType]float64)
	for _, calc := range gc.calculators {
		calc.AfterKVRequest(deltaResource, deltaRequestUnit, req, resp)
	}

	switch gc.mode {
	case rmpb.GroupMode_NativeMode:
		for typ, counter := range gc.run.resourceTokens {
			v, ok := deltaResource[typ]
			if ok {
				counter.limiter.RemoveTokens(time.Now(), v)
			}
		}
		gc.mu.Lock()
		for typ, detail := range deltaResource {
			gc.mu.requestUnitConsumptions[typ].Value += detail
		}
		gc.mu.Unlock()
	case rmpb.GroupMode_RUMode:
		for typ, counter := range gc.run.requestUnitTokens {
			v, ok := deltaRequestUnit[typ]
			if ok {
				counter.limiter.RemoveTokens(time.Now(), v)
			}
		}
		gc.mu.Lock()
		for typ, detail := range deltaRequestUnit {
			gc.mu.resourceConsumptions[typ].Value += detail
		}
		gc.mu.Unlock()
	}
}

func (c *resourceGroupsController) addDemoResourceGroup(ctx context.Context) error {
	setting := &rmpb.GroupSettings{
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RRU: &rmpb.TokenBucket{
				Tokens: 200000,
				Settings: &rmpb.TokenLimitSettings{
					Fillrate:   2000,
					BurstLimit: 20000000,
				},
			},
			WRU: &rmpb.TokenBucket{
				Tokens: 200000,
				Settings: &rmpb.TokenLimitSettings{
					Fillrate:   20000,
					BurstLimit: 2000000,
				},
			},
		},
	}
	context, err := c.provider.AddResourceGroup(ctx, "demo", setting)
	if err != nil {
		return err
	}
	log.Info("add resource group", zap.String("resp", context), zap.Any("setting", setting))
	return err
}
