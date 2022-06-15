// Copyright 2022 TiKV Project Authors.
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

package diagnosis

import (
	"context"

	"github.com/tikv/pd/pkg/syncutil"
)

type DiagnosisController struct {
	mu syncutil.RWMutex
	//enable    bool
	ctx       context.Context
	ctxCancel context.CancelFunc
	//cache        *cache.TTLUint64
	storeReaders  map[uint64]*DiagnosisAnalyzer
	historyRecord map[uint64]*DiagnosisAnalyzer
	// currentReader  *DiagnosisStoreRecoder
	currentStep   int
	currentSource uint64
	currentRegion uint64
	currentTarget uint64
}

func NewDiagnosisController(ctx context.Context) *DiagnosisController {
	ctx, ctxCancel := context.WithCancel(ctx)
	return &DiagnosisController{
		//enable:    false,
		ctx:       ctx,
		ctxCancel: ctxCancel,
		//cache:        cache.NewIDTTL(ctx, time.Minute, 5*time.Minute),
		storeReaders:  make(map[uint64]*DiagnosisAnalyzer),
		historyRecord: make(map[uint64]*DiagnosisAnalyzer),
	}
}

func (c *DiagnosisController) InitSchedule() {
	c.currentStep = 0
}

func (c *DiagnosisController) CleanUpSchedule(success bool) {
	if c.currentSource != 0 {
		if record, ok := c.storeReaders[c.currentSource]; ok {
			record.schedulable = success
			c.historyRecord[c.currentSource] = record
			delete(c.storeReaders, c.currentSource)
		}
	}
	c.currentSource = 0
}

func (c *DiagnosisController) DiagnoseStore(storeID uint64) {
	stepRecorders := make([]DiagnoseStepRecoder, 4)
	for i := 0; i < 4; i++ {
		stepRecorders[i] = NewDiagnosisRecoder()
	}
	c.storeReaders[storeID] = &DiagnosisAnalyzer{
		stepRecorders: stepRecorders,
	}
}

func (c *DiagnosisController) GetDiagnosisAnalyzer(storeID uint64) *DiagnosisAnalyzer {
	return c.historyRecord[storeID]
}

func (c *DiagnosisController) SetObject(objectID uint64) {
	switch c.currentStep {
	case 1:
		c.currentSource = objectID
	case 2:
		c.currentRegion = objectID
	case 3:
		c.currentTarget = objectID
	}
}

func (c *DiagnosisController) NextStep() {
	c.currentStep++
}

func (c *DiagnosisController) LastStep() {
	c.currentStep--
}

func (c *DiagnosisController) Diagnose(objectID uint64, reason string) {
	if !c.ShouldDiagnose(objectID) {
		return
	}
	reader := c.storeReaders[c.currentSource]
	reader.GenerateStoreRecord(c.currentStep, objectID, reason)
	if c.currentStep == 0 {
		c.CleanUpSchedule(false)
	}
}

// func (c *DiagnosisController) SelectSourceStores(stores []*core.StoreInfo, filtxers []filter.Filter, opt *config.PersistOptions) []*core.StoreInfo {
// 	return nil
// }

func (c *DiagnosisController) ShouldDiagnose(objectID uint64) bool {
	if c.currentStep == 0 {
		c.currentSource = objectID
	}
	// if !c.enable {
	// 	return false
	// }
	//return c.cache.Exists(storeID)
	_, ok := c.storeReaders[c.currentSource]
	return ok
}

type DiagnosisAnalyzer struct {
	stepRecorders []DiagnoseStepRecoder
	// schedulable is true when scheduler can create operator for specific store
	schedulable bool
}

func (a *DiagnosisAnalyzer) GenerateStoreRecord(step int, objectID uint64, reason string) {
	a.stepRecorders[step].Add(reason, objectID)
}

func (a *DiagnosisAnalyzer) GetReasonRecord() []DiagnoseStepRecoder {
	return a.stepRecorders
}

func (a *DiagnosisAnalyzer) Schedulable() bool {
	return a.schedulable
}

type DiagnoseStepRecoder interface {
	Add(reason string, id uint64)
	GetMostReason() *ReasonRecorder
	GetAllReasons() map[string]*ReasonRecorder
}

type DiagnosisRecoder struct {
	reasonCounter map[string]*ReasonRecorder
	count         int
}

func NewDiagnosisRecoder() *DiagnosisRecoder {
	return &DiagnosisRecoder{
		reasonCounter: make(map[string]*ReasonRecorder),
	}
}

func (r *DiagnosisRecoder) Add(reason string, id uint64) {
	if _, ok := r.reasonCounter[reason]; !ok {
		r.reasonCounter[reason] = &ReasonRecorder{Reason: reason}
	}
	reader := r.reasonCounter[reason]
	reader.Count++
	reader.SampleId = id
	r.count++
}

func (r *DiagnosisRecoder) GetMostReason() (most *ReasonRecorder) {
	for _, recorder := range r.reasonCounter {
		if most == nil || most.Count < recorder.Count {
			most = recorder
		}
	}
	return
}

func (r *DiagnosisRecoder) GetAllReasons() map[string]*ReasonRecorder {
	return r.reasonCounter
}

type ReasonRecorder struct {
	Reason   string
	Count    int
	SampleId uint64
}
