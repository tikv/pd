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
	"time"

	"github.com/tikv/pd/pkg/cache"
)

type DiagnosisController struct {
	enable       bool
	ctx          context.Context
	ctxCancel    context.CancelFunc
	cache        *cache.TTLUint64
	storeReaders map[uint64]*DiagnosisAnalyzer
	// currentReader  *DiagnosisStoreRecoder
	// currentStoreId uint64
}

func NewDiagnosisController(ctx context.Context) *DiagnosisController {
	ctx, ctxCancel := context.WithCancel(ctx)
	return &DiagnosisController{
		enable:       false,
		ctx:          ctx,
		ctxCancel:    ctxCancel,
		cache:        cache.NewIDTTL(ctx, time.Minute, 5*time.Minute),
		storeReaders: make(map[uint64]*DiagnosisAnalyzer),
	}
}

func (c *DiagnosisController) Diagnose(objectID uint64, step int, reason string) {
	if !c.ShouldDiagnose(objectID) {
		return
	}

}

// func (c *DiagnosisController) SelectSourceStores(stores []*core.StoreInfo, filters []filter.Filter, opt *config.PersistOptions) []*core.StoreInfo {
// 	return nil
// }

func (c *DiagnosisController) ShouldDiagnose(storeID uint64) bool {
	if !c.enable {
		return false
	}
	return c.cache.Exists(storeID)
}

type DiagnoseRecoder interface {
	Add(reason string, id uint64)
	GetMostReason() *ReasonReader
	GetAllReasons() map[string]*ReasonReader
}

type ReasonReader struct {
	ratio    float32
	sampleId uint64
}

type DiagnosisAnalyzer struct {
	stepRecoder []*DiagnosisStoreRecoder
}

type DiagnosisStoreRecoder struct {
	reasonCounter  map[string]int
	reasonSampleId map[string]uint64
}

type DiagnosisEventTracker struct {
	controller *DiagnosisController
	step       int
}
