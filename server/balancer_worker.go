// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

type balancerWorker struct {
	sync.RWMutex

	wg      sync.WaitGroup
	cluster *clusterInfo

	// should we extract to another structure, so
	// Balancer can use it?
	balanceOperators map[uint64]*balanceOperator

	balancers []Balancer
	cfg       *BalanceConfig

	regionCache      *expireRegionCache
	historyOperators *lruCache
	events           *fifoCache

	quit chan struct{}
}

func newBalancerWorker(cluster *clusterInfo, cfg *BalanceConfig) *balancerWorker {
	bw := &balancerWorker{
		cfg:              cfg,
		cluster:          cluster,
		balanceOperators: make(map[uint64]*balanceOperator),
		regionCache:      newExpireRegionCache(time.Duration(cfg.BalanceInterval)*time.Second, 4*time.Duration(cfg.BalanceInterval)*time.Second),
		historyOperators: newLRUCache(100),
		events:           newFifoCache(10000),
		quit:             make(chan struct{}),
	}

	bw.balancers = append(bw.balancers, newLeaderBalancer(cfg))
	bw.balancers = append(bw.balancers, newCapacityBalancer(cfg))

	return bw
}

func (bw *balancerWorker) run() {
	bw.wg.Add(1)
	go bw.workBalancer()
}

func (bw *balancerWorker) workBalancer() {
	defer bw.wg.Done()

	timer := time.NewTimer(time.Duration(bw.cfg.BalanceInterval) * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-bw.quit:
			return
		case <-timer.C:
			err := bw.doBalance()
			if err != nil {
				log.Warnf("do balance failed - %v", errors.ErrorStack(err))
			}

			timer.Reset(time.Duration(bw.cfg.BalanceInterval) * time.Second)
		}
	}
}

func (bw *balancerWorker) stop() {
	close(bw.quit)
	bw.wg.Wait()

	bw.Lock()
	defer bw.Unlock()

	bw.balanceOperators = map[uint64]*balanceOperator{}
}

func (bw *balancerWorker) addBalanceOperator(regionID uint64, op *balanceOperator) bool {
	bw.Lock()
	defer bw.Unlock()

	oldOp, ok := bw.balanceOperators[regionID]
	if ok {
		if !oldOp.Start.IsZero() {
			// Old operator is still in progress, don't replace it.
			return false
		}
		if op.Type != adminOP && op.Type <= oldOp.Type {
			// New operator is not an admin operator, and its priority
			// is not higher than the old one.
			return false
		}
	}

	// adminOP and replicaOP should not care about cache.
	if op.Type == balanceOP {
		// If the region is set balanced some time before, we can't set
		// it again in a time interval.
		_, ok := bw.regionCache.get(regionID)
		if ok {
			return false
		}
	}

	// TODO: should we check allowBalance again here?

	collectBalancerCounterMetrics(op)

	bw.balanceOperators[regionID] = op
	bw.historyOperators.add(regionID, op)

	return true
}

func (bw *balancerWorker) removeBalanceOperator(regionID uint64) {
	bw.Lock()
	defer bw.Unlock()

	// Log balancer information.
	op, ok := bw.balanceOperators[regionID]
	if !ok {
		return
	}

	delete(bw.balanceOperators, regionID)

	op.End = time.Now()
	bw.historyOperators.add(regionID, op)
}

func (bw *balancerWorker) addRegionCache(regionID uint64) {
	bw.regionCache.set(regionID, nil)
}

func (bw *balancerWorker) removeRegionCache(regionID uint64) {
	bw.regionCache.delete(regionID)
}

func (bw *balancerWorker) getBalanceOperator(regionID uint64) *balanceOperator {
	bw.RLock()
	defer bw.RUnlock()

	return bw.balanceOperators[regionID]
}

func (bw *balancerWorker) getBalanceOperators() map[uint64]Operator {
	bw.RLock()
	defer bw.RUnlock()

	balanceOperators := make(map[uint64]Operator, len(bw.balanceOperators))
	for key, value := range bw.balanceOperators {
		balanceOperators[key] = value
	}

	return balanceOperators
}

func (bw *balancerWorker) getHistoryOperators() []Operator {
	bw.RLock()
	defer bw.RUnlock()

	elems := bw.historyOperators.elems()
	operators := make([]Operator, 0, len(elems))
	for _, elem := range elems {
		operators = append(operators, elem.value.(Operator))
	}

	return operators
}

// allowBalance indicates that whether we can add more balance operator or not.
func (bw *balancerWorker) allowBalance() bool {
	bw.RLock()
	balanceCount := uint64(len(bw.balanceOperators))
	bw.RUnlock()

	// TODO: We should introduce more strategies to control
	// how many balance tasks at same time.
	if balanceCount >= bw.cfg.MaxBalanceCount {
		return false
	}

	return true
}

func (bw *balancerWorker) doBalance() error {
	collectBalancerGaugeMetrics(bw.getBalanceOperators())

	balanceCount := uint64(0)
	for i := uint64(0); i < bw.cfg.MaxBalanceRetryPerLoop; i++ {
		if balanceCount >= bw.cfg.MaxBalanceCountPerLoop {
			return nil
		}

		if !bw.allowBalance() {
			return nil
		}

		var (
			bop     *balanceOperator
			maxDiff float64
		)

		// Find the balance operator candidates.
		for _, balancer := range bw.balancers {
			diff, op, err := balancer.Balance(bw.cluster)
			if err != nil {
				return errors.Trace(err)
			}
			if maxDiff < diff {
				bop = op
			}
		}
		if bop == nil {
			continue
		}

		regionID := bop.getRegionID()
		if bw.addBalanceOperator(regionID, bop) {
			bw.addRegionCache(regionID)
			balanceCount++
		}
	}

	return nil
}

func (bw *balancerWorker) checkReplicas(region *regionInfo) error {
	bc := newReplicaBalancer(region, bw.cfg)
	_, op, err := bc.Balance(bw.cluster)
	if err != nil {
		return errors.Trace(err)
	}
	if op != nil {
		bw.addBalanceOperator(region.GetId(), op)
	}
	return nil
}

func collectOperatorMetrics(bop *balanceOperator) map[string]uint64 {
	metrics := make(map[string]uint64)
	prefix := ""
	switch bop.Type {
	case adminOP:
		prefix = "admin_"
	case replicaOP:
		prefix = "replica_"
	case balanceOP:
		prefix = "balance_"
	}
	for _, op := range bop.Ops {
		if _, ok := op.(*onceOperator); ok {
			op = op.(*onceOperator).Op
		}
		switch o := op.(type) {
		case *changePeerOperator:
			metrics[prefix+o.Name]++
		case *transferLeaderOperator:
			metrics[prefix+o.Name]++
		}
	}
	return metrics
}

func collectBalancerCounterMetrics(bop *balanceOperator) {
	metrics := collectOperatorMetrics(bop)
	for label, value := range metrics {
		balancerCounter.WithLabelValues(label).Add(float64(value))
	}
}

func collectBalancerGaugeMetrics(ops map[uint64]Operator) {
	metrics := make(map[string]uint64)
	for _, op := range ops {
		if bop, ok := op.(*balanceOperator); ok {
			values := collectOperatorMetrics(bop)
			for label, value := range values {
				metrics[label] += value
			}
		}
	}
	for label, value := range metrics {
		balancerGauge.WithLabelValues(label).Set(float64(value))
	}
}
