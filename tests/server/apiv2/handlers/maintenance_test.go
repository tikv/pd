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

package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/tikv/pd/tests"
)

type maintenanceTestSuite struct {
	suite.Suite
	env *tests.SchedulingTestEnvironment
}

func TestMaintenanceTestSuite(t *testing.T) {
	suite.Run(t, new(maintenanceTestSuite))
}

func (suite *maintenanceTestSuite) SetupSuite() {
	suite.env = tests.NewSchedulingTestEnvironment(suite.T())
}

func (suite *maintenanceTestSuite) TearDownSuite() {
	suite.env.Cleanup()
}

func (suite *maintenanceTestSuite) TestMaintenanceAPI() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/maintenance", leader.GetAddr())

		taskType := "task_tikv"
		taskID := "task_123"
		desc := "Upgrade rolling restart for TiKV store-1"

		// 1. Start a maintenance task
		putURL := fmt.Sprintf("%s/%s/%s", baseURL, taskType, taskID)
		resp, err := http.NewRequest(http.MethodPut, putURL, bytes.NewBufferString(desc))
		re.NoError(err)
		resp.Header.Set("Content-Type", "text/plain")
		res, err := client.Do(resp)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
		body, _ := io.ReadAll(res.Body)
		re.Contains(string(body), "started successfully")

		// 2. Try to start another task of the same type (should fail)
		sameTypeTaskID := "task_456"
		sameTypeDesc := "Another TiKV upgrade task"
		sameTypePutURL := fmt.Sprintf("%s/%s/%s", baseURL, taskType, sameTypeTaskID)
		resp2, err := http.NewRequest(http.MethodPut, sameTypePutURL, bytes.NewBufferString(sameTypeDesc))
		re.NoError(err)
		resp2.Header.Set("Content-Type", "text/plain")
		res2, err := client.Do(resp2)
		re.NoError(err)
		defer res2.Body.Close()
		re.Equal(http.StatusConflict, res2.StatusCode)

		// Verify the error response contains details about the existing task
		body, err = io.ReadAll(res2.Body)
		re.NoError(err)

		type conflictErrorResp struct {
			Error        string      `json:"error"`
			ExistingTask interface{} `json:"existing_task"`
		}
		var errorResp conflictErrorResp
		err = json.Unmarshal(body, &errorResp)
		re.NoError(err)
		re.Equal("Another maintenance task is already running", errorResp.Error)
		re.NotNil(errorResp.ExistingTask)

		// 3. Try to start another task of different type (should also fail due to constraint)
		otherTaskType := "tidb_upgrade"
		otherTaskID := "task_789"
		otherDescription := "Upgrade TiDB server-1"

		otherPutURL := fmt.Sprintf("%s/%s/%s", baseURL, otherTaskType, otherTaskID)
		resp3, err := http.NewRequest(http.MethodPut, otherPutURL, bytes.NewBufferString(otherDescription))
		re.NoError(err)
		resp3.Header.Set("Content-Type", "text/plain")
		res3, err := client.Do(resp3)
		re.NoError(err)
		defer res3.Body.Close()
		re.Equal(http.StatusConflict, res3.StatusCode)

		// 4. Get all maintenance tasks
		res, err = client.Get(baseURL)
		re.NoError(err)
		defer res.Body.Close()
		if res.StatusCode == http.StatusOK {
			var tasks []struct {
				Type           string `json:"type"`
				ID             string `json:"id"`
				StartTimestamp int64  `json:"start_timestamp"`
				Description    string `json:"description"`
			}
			json.NewDecoder(res.Body).Decode(&tasks)
			re.Len(tasks, 1) // Should only have one task
			task := tasks[0]
			re.Equal(taskType, task.Type)
			re.Equal(taskID, task.ID)
			re.Equal(desc, task.Description)
			re.Less(time.Now().Unix()-task.StartTimestamp, int64(60))
		} else {
			suite.T().Fatalf("expected 200, got %d", res.StatusCode)
		}

		// 5. Get maintenance task by type
		getTypeURL := fmt.Sprintf("%s/%s", baseURL, taskType)
		res, err = client.Get(getTypeURL)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
		var taskByType struct {
			Type           string `json:"type"`
			ID             string `json:"id"`
			StartTimestamp int64  `json:"start_timestamp"`
			Description    string `json:"description"`
		}
		json.NewDecoder(res.Body).Decode(&taskByType)
		re.Equal(taskType, taskByType.Type)
		re.Equal(taskID, taskByType.ID)
		re.Equal(desc, taskByType.Description)

		// 6. End maintenance task with wrong ID (should conflict)
		wrongID := "wrong_id"
		deleteURL := fmt.Sprintf("%s/%s/%s", baseURL, taskType, wrongID)
		req, err := http.NewRequest(http.MethodDelete, deleteURL, nil)
		re.NoError(err)
		res, err = client.Do(req)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusConflict, res.StatusCode)

		// Verify the error response contains details about the existing task
		body, err = io.ReadAll(res.Body)
		re.NoError(err)

		var deleteErrorResp conflictErrorResp
		err = json.Unmarshal(body, &deleteErrorResp)
		re.NoError(err)
		re.Equal("Task ID does not match the current task", deleteErrorResp.Error)
		re.NotNil(deleteErrorResp.ExistingTask)

		// 7. End maintenance task with correct ID
		deleteURL = fmt.Sprintf("%s/%s/%s", baseURL, taskType, taskID)
		req, err = http.NewRequest(http.MethodDelete, deleteURL, nil)
		re.NoError(err)
		res, err = client.Do(req)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)
		body, _ = io.ReadAll(res.Body)
		re.Contains(string(body), "ended successfully")

		// 8. Get maintenance task after deletion (should 404)
		res, err = client.Get(baseURL)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusNotFound, res.StatusCode)

		// 9. Now we should be able to start the same type task that was rejected before
		resp4, err := http.NewRequest(http.MethodPut, sameTypePutURL, bytes.NewBufferString(sameTypeDesc))
		re.NoError(err)
		resp4.Header.Set("Content-Type", "text/plain")
		res4, err := client.Do(resp4)
		re.NoError(err)
		defer res4.Body.Close()
		re.Equal(http.StatusOK, res4.StatusCode)

		// 10. Clean up the second task
		req, err = http.NewRequest(http.MethodDelete, sameTypePutURL, nil)
		re.NoError(err)
		res, err = client.Do(req)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)

		// 11. Test GET /maintenance/{task_type} when no task exists for that type (should 404)
		getTypeURL = fmt.Sprintf("%s/nonexistent_type", baseURL)
		res, err = client.Get(getTypeURL)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusNotFound, res.StatusCode)

		// 12. Test DELETE /maintenance/{task_type}/{task_id} when no task exists for that type (should 404)
		deleteURL = fmt.Sprintf("%s/nonexistent_type/task_123", baseURL)
		req, err = http.NewRequest(http.MethodDelete, deleteURL, nil)
		re.NoError(err)
		res, err = client.Do(req)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusNotFound, res.StatusCode)
	})
}

func (suite *maintenanceTestSuite) TestMaintenanceAPIAtomicOperations() {
	suite.env.RunTest(func(cluster *tests.TestCluster) {
		re := suite.Require()
		leader := cluster.GetLeaderServer()
		client := tests.TestDialClient
		baseURL := fmt.Sprintf("%s/pd/api/v2/maintenance", leader.GetAddr())

		taskType := "concurrent_test"
		numConcurrentRequests := 10
		var wg sync.WaitGroup
		successCount := 0
		conflictCount := 0
		var mu sync.Mutex

		// Test concurrent start requests
		suite.T().Log("Testing concurrent start requests...")
		for i := range make([]struct{}, numConcurrentRequests) {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				taskID := fmt.Sprintf("task_%d", index)
				desc := fmt.Sprintf("Concurrent task %d", index)
				putURL := fmt.Sprintf("%s/%s/%s", baseURL, taskType, taskID)

				resp, err := http.NewRequest(http.MethodPut, putURL, bytes.NewBufferString(desc))
				if err != nil {
					suite.T().Logf("Failed to create request: %v", err)
					return
				}
				resp.Header.Set("Content-Type", "text/plain")

				res, err := client.Do(resp)
				if err != nil {
					suite.T().Logf("Failed to send request: %v", err)
					return
				}
				defer res.Body.Close()

				mu.Lock()
				switch res.StatusCode {
				case http.StatusOK:
					successCount++
					suite.T().Logf("Task %d started successfully", index)
				case http.StatusConflict:
					conflictCount++
					suite.T().Logf("Task %d got conflict", index)
				default:
					suite.T().Logf("Task %d got unexpected status: %d", index, res.StatusCode)
				}
				mu.Unlock()
			}(i)
		}
		wg.Wait()

		// Verify atomic behavior: only one task should succeed
		re.Equal(1, successCount, "Only one task should succeed")
		re.Equal(numConcurrentRequests-1, conflictCount, "All other tasks should get conflict")

		// Get the current task to find which one succeeded
		res, err := client.Get(baseURL)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusOK, res.StatusCode)

		var tasks []struct {
			Type           string `json:"type"`
			ID             string `json:"id"`
			StartTimestamp int64  `json:"start_timestamp"`
			Description    string `json:"description"`
		}
		json.NewDecoder(res.Body).Decode(&tasks)
		re.Len(tasks, 1, "Should only have one task")
		successfulTaskID := tasks[0].ID

		// Test concurrent end requests with wrong IDs
		suite.T().Log("Testing concurrent end requests with wrong IDs...")
		wrongEndCount := 0
		for i := range make([]struct{}, numConcurrentRequests) {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				wrongTaskID := fmt.Sprintf("wrong_task_%d", index)
				deleteURL := fmt.Sprintf("%s/%s/%s", baseURL, taskType, wrongTaskID)

				req, err := http.NewRequest(http.MethodDelete, deleteURL, nil)
				if err != nil {
					suite.T().Logf("Failed to create delete request: %v", err)
					return
				}

				res, err := client.Do(req)
				if err != nil {
					suite.T().Logf("Failed to send delete request: %v", err)
					return
				}
				defer res.Body.Close()

				mu.Lock()
				if res.StatusCode == http.StatusConflict {
					wrongEndCount++
				}
				mu.Unlock()
			}(i)
		}
		wg.Wait()

		re.Equal(numConcurrentRequests, wrongEndCount, "All wrong end requests should get conflict")

		// Test concurrent end requests with correct ID
		suite.T().Log("Testing concurrent end requests with correct ID...")
		correctEndCount := 0
		for i := range make([]struct{}, numConcurrentRequests) {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				deleteURL := fmt.Sprintf("%s/%s/%s", baseURL, taskType, successfulTaskID)

				req, err := http.NewRequest(http.MethodDelete, deleteURL, nil)
				if err != nil {
					suite.T().Logf("Failed to create delete request: %v", err)
					return
				}

				res, err := client.Do(req)
				if err != nil {
					suite.T().Logf("Failed to send delete request: %v", err)
					return
				}
				defer res.Body.Close()

				mu.Lock()
				switch res.StatusCode {
				case http.StatusOK:
					correctEndCount++
					suite.T().Logf("Task ended successfully on attempt %d", index)
				case http.StatusNotFound:
					suite.T().Logf("Task already ended on attempt %d", index)
				default:
					suite.T().Logf("Unexpected status on attempt %d: %d", index, res.StatusCode)
				}
				mu.Unlock()
			}(i)
		}
		wg.Wait()

		// Verify that exactly one end request succeeded
		re.Equal(1, correctEndCount, "Only one end request should succeed")

		// Verify no tasks are running
		res, err = client.Get(baseURL)
		re.NoError(err)
		defer res.Body.Close()
		re.Equal(http.StatusNotFound, res.StatusCode, "No tasks should be running after deletion")
	})
}
