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

// Package endpoint provides storage operations for maintenance tasks.
// This implementation follows the design specified in RFC-0118:
// https://github.com/tikv/rfcs/blob/master/text/0118-pd-maintenance-endpoints.md
//
// The storage layer provides atomic operations to ensure only one maintenance
// task can be active at a time, preventing Raft quorum loss during TiKV maintenance.

package endpoint

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
	"go.uber.org/zap"
)

// MaintenanceLockName is the name used for the maintenance lock key.
// This global lock ensures only one maintenance task can be active at a time.
const MaintenanceLockName = "maintenance_lock"

// MaintenanceTask represents a maintenance task stored in etcd.
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
type MaintenanceTask struct {
	Type           string `json:"type"`            // The type of maintenance task
	ID             string `json:"id"`              // Unique identifier for the task
	StartTimestamp int64  `json:"start_timestamp"` // Unix timestamp when the task started
	Description    string `json:"description"`     // Optional description of the task
}

// MaintenanceStorage defines the interface for maintenance task storage operations.
type MaintenanceStorage interface {
	// LoadMaintenanceTask loads a maintenance task by type from storage.
	LoadMaintenanceTask(taskType string) (*MaintenanceTask, error)
	// LoadAllMaintenanceTasks loads all maintenance tasks from storage.
	LoadAllMaintenanceTasks(f func(k, v string)) error
	// TryStartMaintenanceTaskAtomic tries to start a maintenance task atomically.
	TryStartMaintenanceTaskAtomic(ctx context.Context, task *MaintenanceTask) (bool, *MaintenanceTask, error)
	// TryDeleteMaintenanceTaskAtomic tries to delete a maintenance task atomically.
	TryDeleteMaintenanceTaskAtomic(ctx context.Context, taskType, taskID string) (bool, *MaintenanceTask, error)
}

var _ MaintenanceStorage = (*StorageEndpoint)(nil)

// LoadMaintenanceTask loads a maintenance task by type from storage.
func (se *StorageEndpoint) LoadMaintenanceTask(taskType string) (*MaintenanceTask, error) {
	key := keypath.MaintenanceTaskPath(taskType)
	val, err := se.Load(key)
	if err != nil {
		return nil, err
	}
	if val == "" {
		return nil, nil
	}
	var task MaintenanceTask
	if err := json.Unmarshal([]byte(val), &task); err != nil {
		return nil, err
	}
	return &task, nil
}

// LoadAllMaintenanceTasks loads all maintenance tasks from storage.
func (se *StorageEndpoint) LoadAllMaintenanceTasks(f func(k, v string)) error {
	return se.loadRangeByPrefix(keypath.MaintenanceTaskPathPrefix(), f)
}

// TryStartMaintenanceTaskAtomic tries to start a maintenance task atomically.
// Returns (true, nil) if success, (false, existingTask, nil) if conflict, (false, nil, err) if error.
func (se *StorageEndpoint) TryStartMaintenanceTaskAtomic(_ context.Context, task *MaintenanceTask) (bool, *MaintenanceTask, error) {
	logger := logutil.BgLogger().With(zap.String("component", "maintenance"), zap.String("op", "TryStartMaintenanceTaskAtomic"))
	logger.Debug("Attempting to start maintenance task", zap.String("type", task.Type), zap.String("id", task.ID))
	// Use a single atomic transaction with proper etcd pattern
	rawTxn := se.CreateRawTxn()

	// Maintenance lock key to enforce single task type constraint
	lockKey := keypath.MaintenanceTaskPath(MaintenanceLockName)
	// Task key for storing the actual task data
	taskKey := keypath.MaintenanceTaskPath(task.Type)

	// Condition: check if no maintenance task is running (lock doesn't exist)
	cond := kv.RawTxnCondition{
		Key:     lockKey,
		CmpType: kv.RawTxnCmpNotExists,
	}

	// Then: put both the lock and the task
	val, err := json.Marshal(task)
	if err != nil {
		logger.Error("Failed to marshal maintenance task", zap.Error(err), zap.Any("task", task))
		return false, nil, err
	}

	// Put the maintenance lock with "task_type/task_id"
	lockOp := kv.RawTxnOp{
		Key:    lockKey,
		OpType: kv.RawTxnOpPut,
		Value:  task.Type + "/" + task.ID, // Store "task_type/task_id" as the lock value
	}

	// Put the actual task
	taskOp := kv.RawTxnOp{
		Key:    taskKey,
		OpType: kv.RawTxnOpPut,
		Value:  string(val),
	}

	// Else: get all tasks to see what's currently running
	getAllOp := kv.RawTxnOp{
		Key:    keypath.MaintenanceTaskPathPrefix(),          // Start key
		OpType: kv.RawTxnOpGetRange,                          // Get range of keys
		EndKey: keypath.MaintenanceTaskPathPrefix() + "\xff", // End key (prefix + max byte)
		Limit:  100,                                          // Reasonable limit
	}

	// Execute the transaction: if no maintenance running, put lock and task; else get all tasks
	resp, err := rawTxn.If(cond).Then(lockOp, taskOp).Else(getAllOp).Commit()
	if err != nil {
		logger.Error("Transaction commit failed", zap.Error(err))
		return false, nil, err
	}

	// Check if the transaction succeeded (lock and task were put)
	if resp.Succeeded {
		logger.Info("Maintenance task started successfully", zap.String("type", task.Type), zap.String("id", task.ID))
		return true, nil, nil
	}

	// Transaction failed, meaning there was an existing lock
	// Get all tasks and find the one that's currently running
	logger.Debug("Transaction failed, checking for existing tasks")
	if len(resp.Responses) > 0 && len(resp.Responses[0].KeyValuePairs) > 0 {
		var existingTask *MaintenanceTask
		taskCount := 0

		for _, kv := range resp.Responses[0].KeyValuePairs {
			// Skip the lock key itself
			if kv.Key == lockKey {
				continue
			}

			// Parse the task
			var task MaintenanceTask
			if err := json.Unmarshal([]byte(kv.Value), &task); err == nil {
				existingTask = &task
				taskCount++
			}
		}

		// Assert that there should be only one task
		if taskCount > 1 {
			// This shouldn't happen - multiple tasks running simultaneously
			logger.Error("Multiple maintenance tasks running simultaneously", zap.Int("count", taskCount))
			return false, nil, fmt.Errorf("multiple maintenance tasks running simultaneously: found %d tasks", taskCount)
		}

		// Return the single task that's running
		if existingTask != nil {
			logger.Info("Found existing maintenance task", zap.Any("existingTask", existingTask))
			return false, existingTask, nil
		}
	}
	logger.Debug("No existing maintenance task found")
	return false, nil, nil
}

// TryDeleteMaintenanceTaskAtomic tries to delete a maintenance task atomically.
// Returns (true, nil) if success, (false, existingTask, nil) if conflict, (false, nil, err) if error.
func (se *StorageEndpoint) TryDeleteMaintenanceTaskAtomic(_ context.Context, taskType, taskID string) (bool, *MaintenanceTask, error) {
	logger := logutil.BgLogger().With(zap.String("component", "maintenance"), zap.String("op", "TryDeleteMaintenanceTaskAtomic"))
	logger.Debug("Attempting to delete maintenance task", zap.String("type", taskType), zap.String("id", taskID))
	// Use a single atomic transaction
	rawTxn := se.CreateRawTxn()

	lockKey := keypath.MaintenanceTaskPath(MaintenanceLockName)
	taskKey := keypath.MaintenanceTaskPath(taskType)

	// Condition: check if the lock value matches "task_type/task_id"
	expectedLockValue := taskType + "/" + taskID
	cond := kv.RawTxnCondition{
		Key:     lockKey,
		CmpType: kv.RawTxnCmpEqual,
		Value:   expectedLockValue,
	}

	// Then: delete both the lock and the task
	lockDeleteOp := kv.RawTxnOp{
		Key:    lockKey,
		OpType: kv.RawTxnOpDelete,
	}

	taskDeleteOp := kv.RawTxnOp{
		Key:    taskKey,
		OpType: kv.RawTxnOpDelete,
	}

	// Else: get all tasks to check what's currently running
	getAllOp := kv.RawTxnOp{
		Key:    keypath.MaintenanceTaskPathPrefix(),          // Start key
		OpType: kv.RawTxnOpGetRange,                          // Get range of keys
		EndKey: keypath.MaintenanceTaskPathPrefix() + "\xff", // End key (prefix + max byte)
		Limit:  100,                                          // Reasonable limit
	}

	// Execute the transaction: if lock matches "task_type/task_id", delete both; else get all tasks
	resp, err := rawTxn.If(cond).Then(lockDeleteOp, taskDeleteOp).Else(getAllOp).Commit()
	if err != nil {
		logger.Error("Transaction commit failed", zap.Error(err))
		return false, nil, err
	}

	// Check if the transaction succeeded (deletes were executed)
	if resp.Succeeded {
		logger.Info("Maintenance task deleted successfully", zap.String("type", taskType), zap.String("id", taskID))
		return true, nil, nil
	}

	// Transaction failed, meaning we got all tasks
	// Check if there's a task running for the requested type
	logger.Debug("Transaction failed, checking for existing tasks")
	if len(resp.Responses) > 0 && len(resp.Responses[0].KeyValuePairs) > 0 {
		for _, kv := range resp.Responses[0].KeyValuePairs {
			// Skip the lock key itself
			if kv.Key == lockKey {
				continue
			}

			// Parse the task
			var existingTask MaintenanceTask
			if err := json.Unmarshal([]byte(kv.Value), &existingTask); err == nil {
				// Check if this is the task type we're looking for
				if existingTask.Type == taskType {
					// Found a task for this type, return it as conflict
					logger.Info("Found existing maintenance task for type", zap.Any("existingTask", existingTask))
					return false, &existingTask, nil
				}
			}
		}
	}
	logger.Debug("No maintenance task found for type", zap.String("type", taskType))
	return false, nil, nil
}
