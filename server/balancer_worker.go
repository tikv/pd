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

const (
	defaultBalanceInterval = 30 * time.Second
	// We can allow BalanceCount regions to do balance at same time.
	defaultBalanceCount = 16

	maxRetryBalanceNumber = 10
)

type balancerWorker struct {
	sync.RWMutex

	wg       sync.WaitGroup
	interval time.Duration
	cluster  *clusterInfo

	// should we extract to another structure, so
	// Balancer can use it?
	balanceOperators map[uint64]*balanceOperator
	balanceCount     int

	balancer Balancer

	regionCache      *expireRegionCache
	historyOperators *lruCache

	eventCh chan LogEvent

	quit chan struct{}
}

func newBalancerWorker(cluster *clusterInfo, balancer Balancer, interval time.Duration) *balancerWorker {
	bw := &balancerWorker{
		interval:         interval,
		cluster:          cluster,
		balanceOperators: make(map[uint64]*balanceOperator),
		balanceCount:     defaultBalanceCount,
		balancer:         balancer,
		regionCache:      newExpireRegionCache(interval, 2*interval),
		historyOperators: newLRUCache(100),
		eventCh:          make(chan LogEvent, 10000),
		quit:             make(chan struct{}),
	}

	return bw
}

func (bw *balancerWorker) run() {
	bw.wg.Add(1)
	go bw.workBalancer()
}

func (bw *balancerWorker) workBalancer() {
	defer bw.wg.Done()

	ticker := time.NewTicker(bw.interval)
	defer ticker.Stop()

	for {
		select {
		case <-bw.quit:
			return
		case <-ticker.C:
			err := bw.doBalance()
			if err != nil {
				log.Warnf("do balance failed - %v", errors.ErrorStack(err))
			}
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

	_, ok := bw.balanceOperators[regionID]
	if ok {
		return false
	}

	// If the region is set balanced some time before, we can't set
	// it again in a time interval.
	_, ok = bw.regionCache.get(regionID)
	if ok {
		return false
	}

	// TODO: should we check allowBalance again here?

	op.Start = time.Now()
	bw.balanceOperators[regionID] = op

	bw.historyOperators.add(regionID, op)

	bw.postEvent(op)

	return true
}

func (bw *balancerWorker) removeBalanceOperator(regionID uint64) {
	bw.Lock()
	defer bw.Unlock()

	// Log balancer information.
	op, ok := bw.balanceOperators[regionID]
	if ok {
		op.End = time.Now()
		log.Infof("balancer operator finished - %s", op)
	} else {
		log.Errorf("balancer operator is empty to remove - %d", regionID)
	}

	delete(bw.balanceOperators, regionID)

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
	balanceCount := len(bw.balanceOperators)
	bw.RUnlock()

	// TODO: We should introduce more strategies to control
	// how many balance tasks at same time.
	if balanceCount >= bw.balanceCount {
		log.Infof("%d exceed max balance count %d, can't do balance", balanceCount, bw.balanceCount)
		return false
	}

	return true
}

func (bw *balancerWorker) doBalance() error {
	stats := bw.cluster.stats

	for i := 0; i < maxRetryBalanceNumber; i++ {
		if !bw.allowBalance() {
			return nil
		}

		stats.Increment("balance.select")
		// TODO: support select balance count in balancer.
		balanceOperator, err := bw.balancer.Balance(bw.cluster)
		if err != nil {
			stats.Increment("balance.select.fail")
			return errors.Trace(err)
		}
		if balanceOperator == nil {
			stats.Increment("balance.select.none")
			continue
		}

		regionID := balanceOperator.getRegionID()
		if bw.addBalanceOperator(regionID, balanceOperator) {
			bw.addRegionCache(regionID)
			stats.Increment("balance.select.success")
			return nil
		}

		stats.Increment("balance.select.fail")

		// Here mean the selected region has an operator already, we may retry to
		// select another region for balance.
	}

	log.Info("find no proper region for balance, retry later")
	return nil
}
