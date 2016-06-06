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
	defaultBalanceInterval = 60 * time.Second
	// We can allow BalanceCount regions to do balance at same time.
	defaultBalanceCount = 16

	maxRetryBalanceNumber = 10
)

type balanceWorker struct {
	sync.RWMutex

	wg       sync.WaitGroup
	interval time.Duration
	cluster  *ClusterInfo

	// should we extract to another structure, so
	// Balancer can use it?
	balanceOperators map[uint64]*BalanceOperator
	balanceCount     int

	balancer Balancer

	quit chan struct{}
}

func newBalanceWorker(cluster *ClusterInfo, balancer Balancer) *balanceWorker {
	bw := &balanceWorker{
		interval:         defaultBalanceInterval,
		cluster:          cluster,
		balanceOperators: make(map[uint64]*BalanceOperator),
		balanceCount:     defaultBalanceCount,
		balancer:         balancer,
		quit:             make(chan struct{}),
	}

	bw.wg.Add(1)
	go bw.run()
	return bw
}

func (bw *balanceWorker) run() error {
	defer bw.wg.Done()

	ticker := time.NewTicker(bw.interval)
	defer ticker.Stop()

	for {
		select {
		case <-bw.quit:
			return nil
		case <-ticker.C:
			err := bw.doBalance()
			if err != nil {
				log.Warnf("do balance failed - %v", errors.ErrorStack(err))
			}
		}
	}
}

func (bw *balanceWorker) stop() {
	close(bw.quit)
	bw.wg.Wait()

	bw.Lock()
	defer bw.Unlock()

	bw.balanceOperators = map[uint64]*BalanceOperator{}
}

func (bw *balanceWorker) addBalanceOperator(regionID uint64, op *BalanceOperator) bool {
	bw.Lock()
	defer bw.Unlock()

	_, ok := bw.balanceOperators[regionID]
	if ok {
		return false
	}

	// TODO: should we check allowBalance again here?

	bw.balanceOperators[regionID] = op
	return true
}

func (bw *balanceWorker) removeBalanceOperator(regionID uint64) {
	bw.Lock()
	defer bw.Unlock()

	delete(bw.balanceOperators, regionID)
}

func (bw *balanceWorker) getBalanceOperator(regionID uint64) *BalanceOperator {
	bw.RLock()
	defer bw.RUnlock()

	return bw.balanceOperators[regionID]
}

// allowBalance indicates that whether we can add more balance operator or not.
func (bw *balanceWorker) allowBalance() bool {
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

func (bw *balanceWorker) doBalance() error {
	for i := 0; i < maxRetryBalanceNumber; i++ {
		if !bw.allowBalance() {
			return nil
		}

		// TODO: support select balance count in balancer.
		balanceOperator, err := bw.balancer.Balance(bw.cluster)
		if err != nil {
			return errors.Trace(err)
		}

		if bw.addBalanceOperator(balanceOperator.GetRegionID(), balanceOperator) {
			return nil
		}

		// Here mean the selected region has an operator already, we may retry to
		// select another region for balance.
	}

	log.Info("find no proper region for balance, retry later")
	return nil
}
