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

// Package handlers implements the maintenance endpoints for PD.
// This implementation follows the design specified in RFC-0118:
// https://github.com/tikv/rfcs/blob/master/text/0118-pd-maintenance-endpoints.md
//
// The maintenance endpoints provide a way to serialize TiKV maintenance operations
// to prevent Raft quorum loss by ensuring only one maintenance task is active at a time.

package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/tikv/pd/pkg/maintenance"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/middlewares"
)

// RegisterMaintenance register maintenance related handlers to router paths.
func RegisterMaintenance(r *gin.RouterGroup) {
	router := r.Group("maintenance")
	router.PUT("/:task_type/:task_id", StartMaintenanceTask)
	router.GET("", GetMaintenanceTask)
	router.GET("/:task_type", GetMaintenanceTaskByType)
	router.DELETE("/:task_type/:task_id", DeleteMaintenanceTask)
}

// StartMaintenanceTask starts a maintenance task.
// @Tags     maintenance
// @Summary  Start a maintenance task.
// @Param    task_type  path  string  true   "The type of maintenance task"
// @Param    task_id    path  string  true   "Unique identifier for the task"
// @Accept   plain
// @Produce  json
// @Success  200  {string}  string  "Maintenance task started successfully."
// @Failure  409  {object}  map[string]interface{}  "Another maintenance task is already running."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /maintenance/{task_type}/{task_id} [put]
func StartMaintenanceTask(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	taskType := c.Param("task_type")
	taskID := c.Param("task_id")

	// Read optional description from request body
	description := ""
	if c.Request.Body != nil {
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
			return
		}
		description = string(body)
	}

	task := &endpoint.MaintenanceTask{
		Type:           taskType,
		ID:             taskID,
		StartTimestamp: time.Now().Unix(),
		Description:    description,
	}

	ok, existingTask, err := svr.GetStorage().TryStartMaintenanceTaskAtomic(c.Request.Context(), task)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	if !ok {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{
			"error":         "Another maintenance task is already running",
			"existing_task": existingTask,
		})
		return
	}

	// Update metrics
	maintenance.SetMaintenanceTaskInfo(taskType, taskID, 1)

	c.JSON(http.StatusOK, "Maintenance task started successfully.")
}

// GetMaintenanceTask gets information about the ongoing maintenance task.
// @Tags     maintenance
// @Summary  Get information about the ongoing maintenance task.
// @Produce  json
// @Success  200  {array}  endpoint.MaintenanceTask
// @Failure  404  {string}  string  "No maintenance task is running."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /maintenance [get]
func GetMaintenanceTask(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	var tasks []*endpoint.MaintenanceTask

	err := svr.GetStorage().LoadAllMaintenanceTasks(func(k, v string) {
		// Skip the lock key
		if k == keypath.MaintenanceTaskPath("__maintenance_lock__") {
			return
		}
		task := &endpoint.MaintenanceTask{} // Heap allocation
		if err := json.Unmarshal([]byte(v), task); err == nil {
			tasks = append(tasks, task)
		}
	})

	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	if len(tasks) == 0 {
		c.AbortWithStatusJSON(http.StatusNotFound, "No maintenance task is running")
		return
	}

	// Return all tasks to help with debugging
	c.JSON(http.StatusOK, tasks)
}

// GetMaintenanceTaskByType gets information about the ongoing maintenance task for a specific type.
// @Tags     maintenance
// @Summary  Get information about the ongoing maintenance task for a specific type.
// @Param    task_type  path  string  true  "The type of maintenance task"
// @Produce  json
// @Success  200  {object}  endpoint.MaintenanceTask
// @Failure  404  {string}  string  "No maintenance task is running for this type."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /maintenance/{task_type} [get]
func GetMaintenanceTaskByType(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	taskType := c.Param("task_type")

	task, err := svr.GetStorage().LoadMaintenanceTask(taskType)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	// If no task found for this type, return 404
	if task == nil {
		c.AbortWithStatusJSON(http.StatusNotFound, "No maintenance task is running for this type")
		return
	}

	c.JSON(http.StatusOK, task)
}

// DeleteMaintenanceTask deletes a maintenance task.
// @Tags     maintenance
// @Summary  Delete a maintenance task.
// @Param    task_type  path  string  true  "The type of maintenance task"
// @Param    task_id    path  string  true  "Unique identifier for the task"
// @Produce  json
// @Success  200  {string}  string  "Maintenance task deleted successfully."
// @Failure  404  {string}  string  "No maintenance task is running for this type."
// @Failure  409  {object}  map[string]interface{}  "Task ID does not match the current task."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /maintenance/{task_type}/{task_id} [delete]
func DeleteMaintenanceTask(c *gin.Context) {
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	taskType := c.Param("task_type")
	taskID := c.Param("task_id")

	ok, existingTask, err := svr.GetStorage().TryDeleteMaintenanceTaskAtomic(c.Request.Context(), taskType, taskID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	if !ok {
		if existingTask == nil {
			c.AbortWithStatusJSON(http.StatusNotFound, "No maintenance task is running for this type")
			return
		}
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{
			"error":         "Task ID does not match the current task",
			"existing_task": existingTask,
		})
		return
	}

	// Update metrics
	maintenance.SetMaintenanceTaskInfo(taskType, taskID, 0)

	c.JSON(http.StatusOK, "Maintenance task deleted successfully.")
}
