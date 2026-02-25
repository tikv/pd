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

package controller

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"

	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/resource_group/controller/metrics"
)

type groupCostController struct {
	// invariant attributes
	name    string
	mode    rmpb.GroupMode
	mainCfg *RUConfig
	// meta info
	meta     *rmpb.ResourceGroup
	metaLock sync.RWMutex

	// following fields are used for token limiter.
	calculators []ResourceCalculator

	// metrics
	metrics *groupMetricsCollection
	mu      struct {
		sync.Mutex
		consumption   *rmpb.Consumption
		storeCounter  map[uint64]*rmpb.Consumption
		globalCounter *rmpb.Consumption
	}

	// fast path to make once token limit with un-limit burst.
	burstable *atomic.Bool
	// is throttled
	isThrottled *atomic.Bool

	lowRUNotifyChan       chan<- notifyMsg
	tokenBucketUpdateChan chan<- *groupCostController

	// run contains the state that is updated by the main loop.
	run struct {
		now             time.Time
		lastRequestTime time.Time

		// requestInProgress is set true when sending token bucket request.
		// And it is set false when receiving token bucket response.
		// This triggers a retry attempt on the next tick.
		requestInProgress bool

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

		requestUnitTokens *tokenCounter
	}

	// tombstone is set to true when the resource group is deleted.
	tombstone atomic.Bool
	// inactive is set to true when the resource group has not been updated for a long time.
	inactive bool
}

type groupMetricsCollection struct {
	successfulRequestDuration         prometheus.Observer
	failedLimitReserveDuration        prometheus.Observer
	requestRetryCounter               prometheus.Counter
	failedRequestCounterWithOthers    prometheus.Counter
	failedRequestCounterWithThrottled prometheus.Counter
	tokenRequestCounter               prometheus.Counter
	runningKVRequestCounter           prometheus.Gauge
	consumeTokenHistogram             prometheus.Observer
}

func initMetrics(oldName, name string) *groupMetricsCollection {
	const (
		otherType     = "others"
		throttledType = "throttled"
	)
	return &groupMetricsCollection{
		successfulRequestDuration:         metrics.SuccessfulRequestDuration.WithLabelValues(oldName, name),
		failedLimitReserveDuration:        metrics.FailedLimitReserveDuration.WithLabelValues(oldName, name),
		failedRequestCounterWithOthers:    metrics.FailedRequestCounter.WithLabelValues(oldName, name, otherType),
		failedRequestCounterWithThrottled: metrics.FailedRequestCounter.WithLabelValues(oldName, name, throttledType),
		requestRetryCounter:               metrics.RequestRetryCounter.WithLabelValues(oldName, name),
		tokenRequestCounter:               metrics.ResourceGroupTokenRequestCounter.WithLabelValues(oldName, name),
		runningKVRequestCounter:           metrics.GroupRunningKVRequestCounter.WithLabelValues(name),
		consumeTokenHistogram:             metrics.TokenConsumedHistogram.WithLabelValues(name),
	}
}

type tokenCounter struct {
	fillRate uint64

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

	inDegradedMode bool
}

func newGroupCostController(
	group *rmpb.ResourceGroup,
	mainCfg *RUConfig,
	lowRUNotifyChan chan notifyMsg,
	tokenBucketUpdateChan chan *groupCostController,
) (*groupCostController, error) {
	switch group.Mode {
	case rmpb.GroupMode_RUMode:
		if group.RUSettings.RU == nil || group.RUSettings.RU.Settings == nil {
			return nil, errs.ErrClientResourceGroupConfigUnavailable.FastGenByArgs("not configured")
		}
	default:
		return nil, errs.ErrClientResourceGroupConfigUnavailable.FastGenByArgs("not supports the resource type")
	}
	ms := initMetrics(group.Name, group.Name)
	gc := &groupCostController{
		meta:    group,
		name:    group.Name,
		mainCfg: mainCfg,
		mode:    group.GetMode(),
		metrics: ms,
		calculators: []ResourceCalculator{
			newKVCalculator(mainCfg),
			newSQLCalculator(mainCfg),
		},
		tokenBucketUpdateChan: tokenBucketUpdateChan,
		lowRUNotifyChan:       lowRUNotifyChan,
		burstable:             &atomic.Bool{},
		isThrottled:           &atomic.Bool{},
	}

	gc.mu.consumption = &rmpb.Consumption{}
	gc.mu.storeCounter = make(map[uint64]*rmpb.Consumption)
	gc.mu.globalCounter = &rmpb.Consumption{}
	// TODO: re-init the state if user change mode from RU to RAW mode.
	gc.initRunState()
	return gc, nil
}

func (gc *groupCostController) initRunState() {
	now := time.Now()
	gc.run.now = now
	gc.run.lastRequestTime = now.Add(-defaultTargetPeriod)
	gc.run.targetPeriod = defaultTargetPeriod
	gc.run.consumption = &rmpb.Consumption{}
	gc.run.lastRequestConsumption = &rmpb.Consumption{SqlLayerCpuTimeMs: getSQLProcessCPUTime(gc.mainCfg.isSingleGroupByKeyspace)}

	isBurstable := true
	cfgFunc := func(tb *rmpb.TokenBucket) tokenBucketReconfigureArgs {
		initialToken := float64(tb.Settings.FillRate)
		cfg := tokenBucketReconfigureArgs{
			newTokens: initialToken,
			newBurst:  tb.Settings.BurstLimit,
			// This is to trigger token requests as soon as resource group start consuming tokens.
			newNotifyThreshold: math.Max(initialToken*tokenReserveFraction, 1),
		}
		if cfg.newBurst >= 0 {
			cfg.newBurst = 0
		}
		if tb.Settings.BurstLimit >= 0 {
			isBurstable = false
		}
		return cfg
	}

	gc.metaLock.RLock()
	defer gc.metaLock.RUnlock()
	limiter := NewLimiterWithCfg(gc.name, now, cfgFunc(gc.meta.RUSettings.RU), gc.lowRUNotifyChan)
	counter := &tokenCounter{
		limiter:     limiter,
		avgRUPerSec: 0,
		avgLastTime: now,
		fillRate:    gc.meta.RUSettings.RU.Settings.FillRate,
	}
	gc.run.requestUnitTokens = counter
	gc.burstable.Store(isBurstable)
}

// applyDegradedMode is used to apply degraded mode for resource group which is in low-process.
func (gc *groupCostController) applyDegradedMode() {
	counter := gc.run.requestUnitTokens
	if !counter.limiter.IsLowTokens() {
		return
	}
	if counter.inDegradedMode {
		return
	}
	counter.inDegradedMode = true
	initCounterNotify(counter)
	var cfg tokenBucketReconfigureArgs
	cfg.newBurst = int64(counter.fillRate)
	cfg.newFillRate = float64(counter.fillRate)
	failpoint.Inject("degradedModeRU", func() {
		cfg.newFillRate = 99999999
	})
	counter.limiter.Reconfigure(gc.run.now, cfg, resetLowProcess())
	log.Info("[resource group controller] resource token bucket enter degraded mode", zap.String("name", gc.name))
}

func (gc *groupCostController) updateRunState() {
	newTime := time.Now()
	gc.mu.Lock()
	for _, calc := range gc.calculators {
		calc.Trickle(gc.mu.consumption)
	}
	*gc.run.consumption = *gc.mu.consumption
	gc.mu.Unlock()
	logControllerTrace("[resource group controller] update run state", zap.String("name", gc.name), zap.Any("request-unit-consumption", gc.run.consumption), zap.Bool("is-throttled", gc.isThrottled.Load()))
	gc.run.now = newTime
}

func (gc *groupCostController) updateAvgRequestResourcePerSec() {
	isBurstable := true
	counter := gc.run.requestUnitTokens
	if counter.limiter.GetBurst() >= 0 {
		isBurstable = false
	}
	if !gc.calcAvg(counter, getRUValueFromConsumption(gc.run.consumption)) {
		return
	}
	logControllerTrace("[resource group controller] update avg ru per sec", zap.String("name", gc.name), zap.Float64("avg-ru-per-sec", counter.avgRUPerSec), zap.Bool("is-throttled", gc.isThrottled.Load()))
	gc.burstable.Store(isBurstable)
}

func (gc *groupCostController) resetEmergencyTokenAcquisition() {
	gc.run.requestUnitTokens.limiter.ResetRemainingNotifyTimes()
}

func (gc *groupCostController) handleTokenBucketUpdateEvent(ctx context.Context) {
	counter := gc.run.requestUnitTokens
	counter.notify.mu.Lock()
	ch := counter.notify.setupNotificationCh
	counter.notify.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case <-ch:
		counter.notify.mu.Lock()
		counter.notify.setupNotificationTimer = nil
		counter.notify.setupNotificationCh = nil
		threshold := counter.notify.setupNotificationThreshold
		counter.notify.mu.Unlock()
		counter.limiter.SetupNotificationThreshold(threshold)
	case <-ctx.Done():
		return
	}
}

func (gc *groupCostController) calcAvg(counter *tokenCounter, new float64) bool {
	deltaDuration := gc.run.now.Sub(counter.avgLastTime)
	failpoint.Inject("acceleratedReportingPeriod", func() {
		deltaDuration = 100 * time.Millisecond
	})
	delta := (new - counter.avgRUPerSecLastRU) / deltaDuration.Seconds()
	counter.avgRUPerSec = movingAvgFactor*counter.avgRUPerSec + (1-movingAvgFactor)*delta
	failpoint.Inject("acceleratedSpeedTrend", func() {
		if delta > 0 {
			counter.avgRUPerSec = 1000
		} else {
			counter.avgRUPerSec = 0
		}
	})
	counter.avgLastTime = gc.run.now
	counter.avgRUPerSecLastRU = new
	return true
}

func (gc *groupCostController) shouldReportConsumption() bool {
	if !gc.run.initialRequestCompleted {
		return true
	}
	timeSinceLastRequest := gc.run.now.Sub(gc.run.lastRequestTime)
	failpoint.Inject("acceleratedReportingPeriod", func() {
		timeSinceLastRequest = extendedReportingPeriodFactor * defaultTargetPeriod
	})
	// Due to `gc.run.lastRequestTime` update operations late in this logic,
	// so `timeSinceLastRequest` is less than defaultGroupStateUpdateInterval a little bit, lead to actual report period is greater than defaultTargetPeriod.
	// Add defaultGroupStateUpdateInterval/2 as duration buffer to avoid it.
	if timeSinceLastRequest+defaultGroupStateUpdateInterval/2 >= defaultTargetPeriod {
		if timeSinceLastRequest >= extendedReportingPeriodFactor*defaultTargetPeriod {
			return true
		}
		if getRUValueFromConsumption(gc.run.consumption)-getRUValueFromConsumption(gc.run.lastRequestConsumption) >= consumptionsReportingThreshold {
			return true
		}
	}
	return false
}

func (gc *groupCostController) handleTokenBucketResponse(resp *rmpb.TokenBucketResponse) {
	gc.run.requestInProgress = false
	gc.handleRUTokenResponse(resp)
	gc.run.initialRequestCompleted = true
}

func (gc *groupCostController) handleRUTokenResponse(resp *rmpb.TokenBucketResponse) {
	for _, grantedTB := range resp.GetGrantedRUTokens() {
		gc.modifyTokenCounter(gc.run.requestUnitTokens, grantedTB.GetGrantedTokens(), grantedTB.GetTrickleTimeMs())
	}
}

func (gc *groupCostController) modifyTokenCounter(counter *tokenCounter, bucket *rmpb.TokenBucket, trickleTimeMs int64) {
	granted := bucket.GetTokens()
	if !counter.lastDeadline.IsZero() {
		// If last request came with a trickle duration, we may have RUs that were
		// not made available to the bucket yet; throw them together with the newly
		// granted RUs.
		if since := counter.lastDeadline.Sub(gc.run.now); since > 0 {
			granted += counter.lastRate * since.Seconds()
		}
	}
	initCounterNotify(counter)
	counter.inDegradedMode = false
	var cfg tokenBucketReconfigureArgs
	cfg.newBurst = bucket.GetSettings().GetBurstLimit()
	// When trickleTimeMs equals zero, server has enough tokens and does not need to
	// limit client consume token. So all token is granted to client right now.
	if trickleTimeMs == 0 {
		cfg.newTokens = granted
		cfg.newFillRate = float64(bucket.GetSettings().FillRate)
		counter.lastDeadline = time.Time{}
		cfg.newNotifyThreshold = math.Min(granted+counter.limiter.AvailableTokens(gc.run.now), counter.avgRUPerSec*defaultTargetPeriod.Seconds()) * notifyFraction
		if cfg.newBurst < 0 {
			cfg.newTokens = float64(counter.fillRate)
		}
		gc.isThrottled.Store(false)
	} else {
		// Otherwise the granted token is delivered to the client by fill rate.
		cfg.newTokens = 0
		trickleDuration := time.Duration(trickleTimeMs) * time.Millisecond
		deadline := gc.run.now.Add(trickleDuration)
		cfg.newFillRate = float64(bucket.GetSettings().FillRate) + granted/trickleDuration.Seconds()

		timerDuration := trickleDuration - trickleReserveDuration
		if timerDuration <= 0 {
			timerDuration = (trickleDuration + trickleReserveDuration) / 2
		}
		counter.notify.mu.Lock()
		if counter.notify.setupNotificationTimer != nil {
			counter.notify.setupNotificationTimer.Stop()
		}
		counter.notify.setupNotificationTimer = time.NewTimer(timerDuration)
		counter.notify.setupNotificationCh = counter.notify.setupNotificationTimer.C
		counter.notify.setupNotificationThreshold = 1
		counter.notify.mu.Unlock()
		counter.lastDeadline = deadline
		gc.isThrottled.Store(true)
		select {
		case gc.tokenBucketUpdateChan <- gc:
		default:
		}
	}

	counter.lastRate = cfg.newFillRate
	counter.limiter.Reconfigure(gc.run.now, cfg, resetLowProcess())
}

func initCounterNotify(counter *tokenCounter) {
	counter.notify.mu.Lock()
	if counter.notify.setupNotificationTimer != nil {
		counter.notify.setupNotificationTimer.Stop()
		counter.notify.setupNotificationTimer = nil
		counter.notify.setupNotificationCh = nil
	}
	counter.notify.mu.Unlock()
}

func (gc *groupCostController) collectRequestAndConsumption(selectTyp selectType) *rmpb.TokenBucketRequest {
	req := &rmpb.TokenBucketRequest{
		ResourceGroupName: gc.name,
	}
	// collect request resource
	selected := gc.run.requestInProgress
	failpoint.Inject("triggerUpdate", func() {
		selected = true
	})
	counter := gc.run.requestUnitTokens
	switch selectTyp {
	case periodicReport:
		selected = selected || gc.shouldReportConsumption()
		failpoint.Inject("triggerPeriodicReport", func(val failpoint.Value) {
			selected = gc.name == val.(string)
		})
		fallthrough
	case lowToken:
		if counter.limiter.IsLowTokens() {
			selected = true
		}
		failpoint.Inject("triggerLowRUReport", func(val failpoint.Value) {
			if selectTyp == lowToken {
				selected = gc.name == val.(string)
			}
		})
	}
	request := &rmpb.RequestUnitItem{
		Type:  rmpb.RequestUnitType_RU,
		Value: gc.calcRequest(counter),
	}

	req.Request = &rmpb.TokenBucketRequest_RuItems{
		RuItems: &rmpb.TokenBucketRequest_RequestRU{
			RequestRU: []*rmpb.RequestUnitItem{request},
		},
	}
	if !selected {
		return nil
	}
	req.ConsumptionSinceLastRequest = updateDeltaConsumption(gc.run.lastRequestConsumption, gc.run.consumption)
	gc.run.lastRequestTime = time.Now()
	gc.run.requestInProgress = true
	return req
}

func (gc *groupCostController) getMeta() *rmpb.ResourceGroup {
	gc.metaLock.RLock()
	defer gc.metaLock.RUnlock()
	return gc.meta
}

func (gc *groupCostController) modifyMeta(newMeta *rmpb.ResourceGroup) {
	gc.metaLock.Lock()
	defer gc.metaLock.Unlock()
	gc.meta = newMeta
}

func (gc *groupCostController) calcRequest(counter *tokenCounter) float64 {
	// `needTokensAmplification` is used to properly amplify a need. The reason is that in the current implementation,
	// the token returned from the server determines the average consumption speed.
	// Therefore, when the fillrate of resource group increases, `needTokensAmplification` can enable the client to obtain more tokens.
	value := counter.avgRUPerSec * gc.run.targetPeriod.Seconds() * needTokensAmplification
	value -= counter.limiter.AvailableTokens(gc.run.now)
	if value < 0 {
		value = 0
	}
	return value
}

func (gc *groupCostController) acquireTokens(ctx context.Context, delta *rmpb.Consumption, waitDuration *time.Duration, allowDebt bool) (time.Duration, error) {
	gc.metrics.runningKVRequestCounter.Inc()
	defer gc.metrics.runningKVRequestCounter.Dec()
	var (
		err error
		d   time.Duration
	)
retryLoop:
	for range gc.mainCfg.WaitRetryTimes {
		counter := gc.run.requestUnitTokens
		reconfiguredCh := counter.limiter.GetReconfiguredCh()
		now := time.Now()
		var res *Reservation
		if v := getRUValueFromConsumption(delta); v > 0 {
			// record the consume token histogram if enable controller debug mode.
			if enableControllerTraceLog.Load() {
				gc.metrics.consumeTokenHistogram.Observe(v)
			}
			// allow debt for small request or not in throttled. remove tokens directly.
			if allowDebt {
				counter.limiter.RemoveTokens(now, v)
				break retryLoop
			}
			res = counter.limiter.Reserve(ctx, gc.mainCfg.LTBMaxWaitDuration, now, v)
		}
		if d, err = WaitReservations(ctx, now, []*Reservation{res}); err == nil || errs.ErrClientResourceGroupThrottled.NotEqual(err) {
			break retryLoop
		}
		gc.metrics.requestRetryCounter.Inc()
		waitStart := time.Now()
		waitTimer := time.NewTimer(gc.mainCfg.WaitRetryInterval)
		select {
		case <-ctx.Done():
			if !waitTimer.Stop() {
				select {
				case <-waitTimer.C:
				default:
				}
			}
			*waitDuration += time.Since(waitStart)
			return d, ctx.Err()
		case <-reconfiguredCh:
			if !waitTimer.Stop() {
				select {
				case <-waitTimer.C:
				default:
				}
			}
		case <-waitTimer.C:
		}
		*waitDuration += time.Since(waitStart)
	}
	return d, err
}

func (gc *groupCostController) onRequestWaitImpl(
	ctx context.Context, info RequestInfo,
) (delta, penalty *rmpb.Consumption, waitDuration time.Duration, priority uint32, err error) {
	delta = &rmpb.Consumption{}
	for _, calc := range gc.calculators {
		calc.BeforeKVRequest(delta, info)
	}

	gc.mu.Lock()
	add(gc.mu.consumption, delta)
	gc.mu.Unlock()

	if !gc.burstable.Load() {
		d, err := gc.acquireTokens(ctx, delta, &waitDuration, false)
		if err != nil {
			if errs.ErrClientResourceGroupThrottled.Equal(err) {
				gc.metrics.failedRequestCounterWithThrottled.Inc()
				gc.metrics.failedLimitReserveDuration.Observe(d.Seconds())
			} else {
				gc.metrics.failedRequestCounterWithOthers.Inc()
			}
			gc.mu.Lock()
			sub(gc.mu.consumption, delta)
			gc.mu.Unlock()
			failpoint.Inject("triggerUpdate", func() {
				gc.lowRUNotifyChan <- notifyMsg{}
			})
			return nil, nil, waitDuration, 0, err
		}
		gc.metrics.successfulRequestDuration.Observe(d.Seconds())
		waitDuration += d
	}

	gc.mu.Lock()
	// Calculate the penalty of the store
	penalty = &rmpb.Consumption{}
	if storeCounter, exist := gc.mu.storeCounter[info.StoreID()]; exist {
		*penalty = *gc.mu.globalCounter
		sub(penalty, storeCounter)
	} else {
		gc.mu.storeCounter[info.StoreID()] = &rmpb.Consumption{}
	}
	// More accurately, it should be reset when the request succeed. But it would cause all concurrent requests piggyback large delta which inflates penalty.
	// So here resets it directly as failure is rare.
	*gc.mu.storeCounter[info.StoreID()] = *gc.mu.globalCounter
	gc.mu.Unlock()

	return delta, penalty, waitDuration, gc.getMeta().GetPriority(), nil
}

func (gc *groupCostController) onResponseImpl(
	req RequestInfo, resp ResponseInfo,
) (*rmpb.Consumption, error) {
	delta := &rmpb.Consumption{}
	for _, calc := range gc.calculators {
		calc.AfterKVRequest(delta, req, resp)
	}
	if !gc.burstable.Load() {
		counter := gc.run.requestUnitTokens
		if v := getRUValueFromConsumption(delta); v > 0 {
			counter.limiter.RemoveTokens(time.Now(), v)
		}
	}

	gc.mu.Lock()
	// Record the consumption of the request
	add(gc.mu.consumption, delta)
	// Record the consumption of the request by store
	count := &rmpb.Consumption{}
	*count = *delta
	// As the penalty is only counted when the request is completed, so here needs to calculate the write cost which is added in `BeforeKVRequest`
	for _, calc := range gc.calculators {
		calc.BeforeKVRequest(count, req)
	}
	add(gc.mu.storeCounter[req.StoreID()], count)
	add(gc.mu.globalCounter, count)
	gc.mu.Unlock()

	return delta, nil
}

func (gc *groupCostController) onResponseWaitImpl(
	ctx context.Context, req RequestInfo, resp ResponseInfo,
) (*rmpb.Consumption, time.Duration, error) {
	delta := &rmpb.Consumption{}
	for _, calc := range gc.calculators {
		calc.AfterKVRequest(delta, req, resp)
	}
	var waitDuration time.Duration
	if !gc.burstable.Load() {
		allowDebt := delta.ReadBytes+delta.WriteBytes < bigRequestThreshold || !gc.isThrottled.Load()
		d, err := gc.acquireTokens(ctx, delta, &waitDuration, allowDebt)
		if err != nil {
			if errs.ErrClientResourceGroupThrottled.Equal(err) {
				gc.metrics.failedRequestCounterWithThrottled.Inc()
				gc.metrics.failedLimitReserveDuration.Observe(d.Seconds())
			} else {
				gc.metrics.failedRequestCounterWithOthers.Inc()
			}
			return nil, waitDuration, err
		}
		gc.metrics.successfulRequestDuration.Observe(d.Seconds())
		waitDuration += d
	}

	gc.mu.Lock()
	// Record the consumption of the request
	add(gc.mu.consumption, delta)
	// Record the consumption of the request by store
	count := &rmpb.Consumption{}
	*count = *delta
	// As the penalty is only counted when the request is completed, so here needs to calculate the write cost which is added in `BeforeKVRequest`
	for _, calc := range gc.calculators {
		calc.BeforeKVRequest(count, req)
	}
	add(gc.mu.storeCounter[req.StoreID()], count)
	add(gc.mu.globalCounter, count)
	gc.mu.Unlock()

	return delta, waitDuration, nil
}

func (gc *groupCostController) addRUConsumption(consumption *rmpb.Consumption) {
	gc.mu.Lock()
	add(gc.mu.consumption, consumption)
	gc.mu.Unlock()
}

// GetActiveResourceGroup is used to get active resource group.
// This is used for test only.
func (c *ResourceGroupsController) GetActiveResourceGroup(resourceGroupName string) *rmpb.ResourceGroup {
	gc, ok := c.loadGroupController(resourceGroupName)
	if !ok || gc.tombstone.Load() {
		return nil
	}
	return gc.getMeta()
}

// This is used for test only.
func (gc *groupCostController) getKVCalculator() *KVCalculator {
	for _, calc := range gc.calculators {
		if kvCalc, ok := calc.(*KVCalculator); ok {
			return kvCalc
		}
	}
	return nil
}
