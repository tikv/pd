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

package diagnostic

import (
	"time"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/schedulers"
)

// Manager is used to manage the diagnostic result of schedulers for now.
type Manager struct {
	config              config.SchedulerConfigProvider
	schedulerController *schedulers.Controller
}

// NewManager creates a new Manager.
func NewManager(schedulerController *schedulers.Controller, config config.SchedulerConfigProvider) *Manager {
	return &Manager{
		config:              config,
		schedulerController: schedulerController,
	}
}

// GetDiagnosticResult gets the diagnostic result of the scheduler.
func (d *Manager) GetDiagnosticResult(name string) (*schedulers.DiagnosticResult, error) {
	if !d.config.IsDiagnosticAllowed() {
		return nil, errs.ErrDiagnosticDisabled
	}

	scheduler := d.schedulerController.GetScheduler(name)
	if scheduler == nil {
		ts := uint64(time.Now().Unix())
		res := &schedulers.DiagnosticResult{Name: name, Timestamp: ts, Status: schedulers.Disabled}
		return res, nil
	}
	isDisabled := d.config.IsSchedulerDisabled(scheduler.Scheduler.GetType())
	if isDisabled {
		ts := uint64(time.Now().Unix())
		res := &schedulers.DiagnosticResult{Name: name, Timestamp: ts, Status: schedulers.Disabled}
		return res, nil
	}

	recorder := d.getSchedulerRecorder(name)
	if recorder == nil {
		return nil, errs.ErrSchedulerUndiagnosable.FastGenByArgs(name)
	}
	result := recorder.GetLastResult()
	if result == nil {
		return nil, errs.ErrNoDiagnosticResult.FastGenByArgs(name)
	}
	return result, nil
}

func (d *Manager) getSchedulerRecorder(name string) *schedulers.DiagnosticRecorder {
	return d.schedulerController.GetScheduler(name).GetDiagnosticRecorder()
}
