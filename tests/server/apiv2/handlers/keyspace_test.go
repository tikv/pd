// Copyright 2023 TiKV Project Authors.
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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"

	"github.com/tikv/pd/pkg/keyspace"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

func TestAuditJSONReadErrorCompatibility(t *testing.T) {
	for _, redaction := range []logutil.RedactInfoLogType{logutil.RedactInfoLogOFF, logutil.RedactInfoLogMarker} {
		t.Run(fmt.Sprintf("redaction=%d", redaction), func(t *testing.T) {
			re := require.New(t)
			cluster, err := tests.NewTestCluster(t.Context(), 1, func(cfg *config.Config, _ string) {
				cfg.Security.RedactInfoLog = redaction
			})
			re.NoError(err)
			defer cluster.Destroy()
			re.NoError(cluster.RunInitialServers())
			re.NotEmpty(cluster.WaitLeader())
			leader := cluster.GetLeaderServer()
			re.NoError(leader.BootstrapCluster())
			address, err := url.Parse(leader.GetAddr())
			re.NoError(err)
			for _, enabled := range []bool{false, true} {
				re.NoError(leader.GetServer().SetAuditConfig(config.AuditConfig{EnableAudit: enabled}))
				for _, padding := range []int{0, 16 << 10} {
					t.Run(fmt.Sprintf("audit=%t/padding=%d", enabled, padding), func(t *testing.T) {
						re := require.New(t)
						payload := `{"name":"` + strings.Repeat("x", padding)
						conn, err := net.Dial("tcp", address.Host)
						re.NoError(err)
						defer conn.Close()
						re.NoError(conn.SetDeadline(time.Now().Add(10 * time.Second)))
						_, err = fmt.Fprintf(conn,
							"POST /pd/api/v2/keyspaces HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s",
							address.Host, len(payload)+20, payload)
						re.NoError(err)
						re.NoError(conn.(*net.TCPConn).CloseWrite())
						response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodPost})
						re.NoError(err)
						data, err := io.ReadAll(response.Body)
						re.NoError(err)
						re.NoError(response.Body.Close())
						re.Equal(http.StatusBadRequest, response.StatusCode)
						re.Contains(string(data), "[PD:gin:ErrBindJSON]unexpected EOF")
					})
				}
			}
		})
	}
}

func TestAuditFollowerWithoutConfigRead(t *testing.T) {
	re := require.New(t)
	cluster, err := tests.NewTestCluster(t.Context(), 3)
	re.NoError(err)
	defer cluster.Destroy()
	re.NoError(cluster.RunInitialServers())
	leaderName := cluster.WaitLeader()
	re.NotEmpty(leaderName)
	leader := cluster.GetLeaderServer()
	re.NoError(leader.BootstrapCluster())
	follower := cluster.GetServer(cluster.GetFollower())
	re.NotNil(follower)
	logFile := testutil.InitTempFileLogger(t, "info")
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("audit=%t", enabled), func(t *testing.T) {
			re := require.New(t)
			re.NoError(leader.GetServer().SetAuditConfig(config.AuditConfig{EnableAudit: enabled}))
			follower.GetServer().GetServiceMiddlewarePersistOptions().SetAuditConfig(&config.AuditConfig{EnableAudit: enabled})
			before, err := os.ReadFile(logFile)
			re.NoError(err)
			// A storage lookup would exceed its deadline. Auditing must use the
			// local switch so forwarding does not depend on that lookup.
			const slowGet = "github.com/tikv/pd/pkg/utils/etcdutil/SlowEtcdKVGet"
			delaySeconds := int(etcdutil.DefaultRequestTimeout/time.Second) + 1
			re.NoError(failpoint.Enable(slowGet, fmt.Sprintf("return(%d)", delaySeconds)))
			t.Cleanup(func() { re.NoError(failpoint.Disable(slowGet)) })
			re.NoError(testutil.CheckPostJSON(tests.TestDialClient, leader.GetAddr()+"/pd/api/v2/keyspaces", []byte("{"),
				testutil.Status(re, http.StatusBadRequest), testutil.StringContain(re, "[PD:gin:ErrBindJSON]")))
			response, err := tests.TestDialClient.Post(follower.GetAddr()+"/pd/api/v2/keyspaces", "application/json", strings.NewReader("{"))
			re.NoError(err)
			data, err := io.ReadAll(response.Body)
			re.NoError(err)
			re.NoError(response.Body.Close())
			after, err := os.ReadFile(logFile)
			re.NoError(err)
			entries := string(after[len(before):])
			re.NotContains(entries, keypath.ServiceMiddlewarePath())
			expected := 0
			if enabled {
				expected = 2 // The leader audits the direct and forwarded requests.
			}
			re.Equal(expected, strings.Count(entries, "ServiceLabel:CreateKeyspace,"))
			re.True(leader.GetServer().GetMember().IsServing())
			re.Equal(leaderName, cluster.GetLeader())
			re.Equal(http.StatusBadRequest, response.StatusCode)
			re.Contains(string(data), "[PD:gin:ErrBindJSON]")
		})
	}
}

type keyspaceTestSuite struct {
	suite.Suite
	cleanup func()
	cluster *tests.TestCluster
	server  *tests.TestServer
}

func TestKeyspaceTestSuite(t *testing.T) {
	suite.Run(t, new(keyspaceTestSuite))
}

func (suite *keyspaceTestSuite) SetupTest() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(context.Background())
	suite.cleanup = cancel
	cluster, err := tests.NewTestCluster(ctx, 1)
	suite.cluster = cluster
	re.NoError(err)
	re.NoError(cluster.RunInitialServers())
	re.NotEmpty(cluster.WaitLeader())
	suite.server = cluster.GetLeaderServer()
	re.NoError(suite.server.BootstrapCluster())
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion", "return(true)"))
}

func (suite *keyspaceTestSuite) TearDownTest() {
	re := suite.Require()
	suite.cleanup()
	suite.cluster.Destroy()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/keyspace/skipSplitRegion"))
}

func (suite *keyspaceTestSuite) TestCreateLoadKeyspace() {
	re := suite.Require()
	keyspaces := mustMakeTestKeyspaces(re, suite.server, 10)
	for _, created := range keyspaces {
		loaded := mustLoadKeyspaces(re, suite.server, created.Name)
		re.Equal(created, loaded)
	}
	bootstrapKeyspace := mustLoadKeyspaces(re, suite.server, keyspace.GetBootstrapKeyspaceName())
	re.Equal(keyspace.GetBootstrapKeyspaceName(), bootstrapKeyspace.Name)
	re.Equal(keyspacepb.KeyspaceState_ENABLED, bootstrapKeyspace.State)
}

func (suite *keyspaceTestSuite) TestHTTPAuditSwitch() {
	re := suite.Require()
	logFile := testutil.InitTempFileLogger(suite.T(), "info")
	configURL := suite.server.GetAddr() + "/pd/api/v1/service-middleware/config"
	snapshot := func() map[string]float64 {
		families, err := prometheus.DefaultGatherer.Gather()
		re.NoError(err)
		counts := make(map[string]float64)
		for _, family := range families {
			if family.GetName() != "pd_service_audit_requests_total" && family.GetName() != "pd_service_audit_handling_seconds" {
				continue
			}
			for _, metric := range family.GetMetric() {
				var service string
				for _, label := range metric.GetLabel() {
					if label.GetName() == "service" {
						service = label.GetValue()
					}
				}
				counts[family.GetName()+":"+service] += metric.GetCounter().GetValue() + float64(metric.GetHistogram().GetSampleCount())
			}
		}
		return counts
	}
	for i, enabled := range []bool{false, true, false} {
		body := fmt.Appendf(nil, `{"audit.enable-audit":"%t"}`, enabled)
		re.NoError(testutil.CheckPostJSON(tests.TestDialClient, configURL, body, testutil.StatusOK(re)))
		before := snapshot()
		request := &handlers.CreateKeyspaceParams{Name: fmt.Sprintf("audit-keyspace-%d", i)}
		MustCreateKeyspace(re, suite.server, request)
		re.Equal(request.Name, mustLoadKeyspaces(re, suite.server, request.Name).Name)
		re.NoError(testutil.CheckGetJSON(tests.TestDialClient, suite.server.GetAddr()+"/pd/api/v1/members", nil, testutil.StatusOK(re)))
		microserviceStatus := http.StatusNotFound
		if suite.server.GetServer().IsKeyspaceGroupEnabled() {
			microserviceStatus = http.StatusOK
		}
		re.NoError(testutil.CheckGetJSON(tests.TestDialClient, suite.server.GetAddr()+"/pd/api/v2/ms/members/tso", nil, testutil.Status(re, microserviceStatus)))
		data, err := os.ReadFile(logFile)
		re.NoError(err)
		expected := 0
		if i > 0 {
			expected = 1
		}
		re.Equal(expected, strings.Count(string(data), "ServiceLabel:CreateKeyspace,"))
		re.NotContains(string(data), "ServiceLabel:LoadKeyspace,")
		after := snapshot()
		increment := float64(0)
		if enabled {
			increment = 1
		}
		for _, family := range []string{"pd_service_audit_requests_total", "pd_service_audit_handling_seconds"} {
			for _, service := range []string{"CreateKeyspace", "LoadKeyspace", "GetMembers", "GetMicroserviceMembers"} {
				key := family + ":" + service
				re.Equal(increment, after[key]-before[key], key)
			}
		}
	}
}

func (suite *keyspaceTestSuite) TestCreateLoadKeyspaceByID() {
	re := suite.Require()
	keyspaces := mustMakeTestKeyspacesByIDs(re, suite.server, 10)
	for _, created := range keyspaces {
		loaded := mustLoadKeyspaces(re, suite.server, created.Name)
		re.Equal(created, loaded)
	}
	bootstrapKeyspace := mustLoadKeyspaces(re, suite.server, keyspace.GetBootstrapKeyspaceName())
	re.Equal(keyspace.GetBootstrapKeyspaceName(), bootstrapKeyspace.Name)
	re.Equal(keyspacepb.KeyspaceState_ENABLED, bootstrapKeyspace.State)
}

func (suite *keyspaceTestSuite) TestUpdateKeyspaceConfig() {
	re := suite.Require()
	keyspaces := mustMakeTestKeyspaces(re, suite.server, 10)
	for _, created := range keyspaces {
		config1val := "300"
		updateRequest := &handlers.UpdateConfigParams{
			Config: map[string]*string{
				"config1": &config1val,
				"config2": nil,
			},
		}
		updated := mustUpdateKeyspaceConfig(re, suite.server, created.Name, updateRequest)
		checkUpdateRequest(re, updateRequest, created.Config, updated.Config)
	}
}

func (suite *keyspaceTestSuite) TestUpdateKeyspaceConfigPreconditions() {
	re := suite.Require()
	created := MustCreateKeyspace(re, suite.server, &handlers.CreateKeyspaceParams{
		Name:   "test_keyspace_cas",
		Config: map[string]string{},
	})
	re.NotNil(created)

	currentKey := "current_file_id"
	nextKey := "next_file_id"

	next := "1000"
	status, body, meta := tryUpdateKeyspaceConfig(re, suite.server, created.Name, &handlers.UpdateConfigParams{
		Config: map[string]*string{
			nextKey: &next,
		},
		Preconditions: map[string]*string{
			nextKey: nil,
		},
	})
	re.Equal(http.StatusOK, status, body)
	re.Equal(next, meta.Config[nextKey])

	otherNext := "2000"
	status, body, meta = tryUpdateKeyspaceConfig(re, suite.server, created.Name, &handlers.UpdateConfigParams{
		Config: map[string]*string{
			nextKey: &otherNext,
		},
		Preconditions: map[string]*string{
			nextKey: nil,
		},
	})
	re.Equal(http.StatusConflict, status)
	re.Nil(meta)
	re.Contains(body, "precondition failed")

	status, _, meta = tryUpdateKeyspaceConfig(re, suite.server, created.Name, &handlers.UpdateConfigParams{
		Config: map[string]*string{
			currentKey: &next,
		},
		Preconditions: map[string]*string{
			nextKey: &next,
		},
	})
	re.Equal(http.StatusOK, status)
	re.Equal(next, meta.Config[currentKey])
	re.Equal(next, meta.Config[nextKey])

	wrongExpected := "999"
	status, body, meta = tryUpdateKeyspaceConfig(re, suite.server, created.Name, &handlers.UpdateConfigParams{
		Config: map[string]*string{
			nextKey: nil,
		},
		Preconditions: map[string]*string{
			nextKey: &wrongExpected,
		},
	})
	re.Equal(http.StatusConflict, status)
	re.Nil(meta)
	re.Contains(body, "expected")

	status, _, meta = tryUpdateKeyspaceConfig(re, suite.server, created.Name, &handlers.UpdateConfigParams{
		Config: map[string]*string{
			nextKey: nil,
		},
		Preconditions: map[string]*string{
			nextKey: &next,
		},
	})
	re.Equal(http.StatusOK, status)
	re.Equal(next, meta.Config[currentKey])
	re.NotContains(meta.GetConfig(), nextKey)
}

func (suite *keyspaceTestSuite) TestUpdateKeyspaceConfigPreconditionsConcurrentSetNextIfAbsent() {
	re := suite.Require()
	created := MustCreateKeyspace(re, suite.server, &handlers.CreateKeyspaceParams{
		Name:   "test_keyspace_cas2",
		Config: map[string]string{},
	})
	re.NotNil(created)

	nextKey := "next_file_id"
	start := make(chan struct{})
	results := make(chan int, 2)

	go func() {
		<-start
		next := "1000"
		status, body, _ := tryUpdateKeyspaceConfig(re, suite.server, created.Name, &handlers.UpdateConfigParams{
			Config: map[string]*string{
				nextKey: &next,
			},
			Preconditions: map[string]*string{
				nextKey: nil,
			},
		})
		if status != http.StatusOK && status != http.StatusConflict {
			re.FailNow("unexpected status", "status=%d body=%s", status, body)
		}
		results <- status
	}()
	go func() {
		<-start
		next := "2000"
		status, body, _ := tryUpdateKeyspaceConfig(re, suite.server, created.Name, &handlers.UpdateConfigParams{
			Config: map[string]*string{
				nextKey: &next,
			},
			Preconditions: map[string]*string{
				nextKey: nil,
			},
		})
		if status != http.StatusOK && status != http.StatusConflict {
			re.FailNow("unexpected status", "status=%d body=%s", status, body)
		}
		results <- status
	}()

	close(start)

	s1 := <-results
	s2 := <-results
	re.ElementsMatch([]int{http.StatusOK, http.StatusConflict}, []int{s1, s2})
}

func (suite *keyspaceTestSuite) TestUpdateKeyspaceState() {
	re := suite.Require()
	keyspaces := mustMakeTestKeyspaces(re, suite.server, 10)
	for _, created := range keyspaces {
		// Should NOT allow archiving ENABLED keyspace.
		success, _ := sendUpdateStateRequest(re, suite.server, created.Name, &handlers.UpdateStateParam{State: "archived"})
		re.False(success)
		// Disabling an ENABLED keyspace is allowed.
		success, disabled := sendUpdateStateRequest(re, suite.server, created.Name, &handlers.UpdateStateParam{State: "disabled"})
		re.True(success)
		re.Equal(keyspacepb.KeyspaceState_DISABLED, disabled.State)
		// Disabling an already DISABLED keyspace should not result in any change.
		success, disabledAgain := sendUpdateStateRequest(re, suite.server, created.Name, &handlers.UpdateStateParam{State: "disabled"})
		re.True(success)
		re.Equal(disabled, disabledAgain)
		// Tombstone a DISABLED keyspace should not be allowed.
		success, _ = sendUpdateStateRequest(re, suite.server, created.Name, &handlers.UpdateStateParam{State: "tombstone"})
		re.False(success)
		// Archiving a DISABLED keyspace should be allowed.
		success, archived := sendUpdateStateRequest(re, suite.server, created.Name, &handlers.UpdateStateParam{State: "archived"})
		re.True(success)
		re.Equal(keyspacepb.KeyspaceState_ARCHIVED, archived.State)
		// Enabling an ARCHIVED keyspace is not allowed.
		success, _ = sendUpdateStateRequest(re, suite.server, created.Name, &handlers.UpdateStateParam{State: "enabled"})
		re.False(success)
		// Tombstone an ARCHIVED keyspace is allowed.
		success, tombstone := sendUpdateStateRequest(re, suite.server, created.Name, &handlers.UpdateStateParam{State: "tombstone"})
		re.True(success)
		re.Equal(keyspacepb.KeyspaceState_TOMBSTONE, tombstone.State)
	}
	// Changing default keyspace's state is NOT allowed.
	success, _ := sendUpdateStateRequest(re, suite.server, constant.DefaultKeyspaceName, &handlers.UpdateStateParam{State: "disabled"})
	re.False(success)
}

func (suite *keyspaceTestSuite) TestLoadRangeKeyspace() {
	re := suite.Require()
	keyspaces := mustMakeTestKeyspaces(re, suite.server, 50)
	loadResponse := sendLoadRangeRequest(re, suite.server, "", "")
	re.Empty(loadResponse.NextPageToken) // Load response should contain no more pages.
	// Load response should contain all created keyspace and a bootstrap keyspace.
	re.Len(loadResponse.Keyspaces, len(keyspaces)+1)

	// Find the bootstrap keyspace in the response
	var bootstrapKeyspace *handlers.KeyspaceMeta
	var userKeyspaces []*handlers.KeyspaceMeta
	for _, ks := range loadResponse.Keyspaces {
		if ks.Name == keyspace.GetBootstrapKeyspaceName() {
			bootstrapKeyspace = ks
		} else {
			userKeyspaces = append(userKeyspaces, ks)
		}
	}

	// Verify bootstrap keyspace exists and is enabled
	re.NotNil(bootstrapKeyspace)
	re.Equal(keyspace.GetBootstrapKeyspaceName(), bootstrapKeyspace.Name)
	re.Equal(keyspacepb.KeyspaceState_ENABLED, bootstrapKeyspace.State)

	// Verify we have the correct number of user keyspaces
	re.Len(userKeyspaces, len(keyspaces))
}

func mustMakeTestKeyspaces(re *require.Assertions, server *tests.TestServer, count int) []*keyspacepb.KeyspaceMeta {
	testConfig := map[string]string{
		"config1": "100",
		"config2": "200",
	}
	resultMeta := make([]*keyspacepb.KeyspaceMeta, count)
	base := uint64(time.Now().UnixNano())
	for i := range count {
		createRequest := &handlers.CreateKeyspaceParams{
			Name:   fmt.Sprintf("ks_%x_%02d", base&0xffffffff, i),
			Config: testConfig,
		}
		resultMeta[i] = MustCreateKeyspace(re, server, createRequest)
	}
	return resultMeta
}

func mustMakeTestKeyspacesByIDs(re *require.Assertions, server *tests.TestServer, count int) []*keyspacepb.KeyspaceMeta {
	testConfig := map[string]string{
		"config1": "100",
		"config2": "200",
	}
	resultMeta := make([]*keyspacepb.KeyspaceMeta, count)
	for i := range count {
		name := strconv.Itoa(i + 1)
		id := uint32(i + 1)
		createRequest := &handlers.CreateKeyspaceByIDParams{
			ID:     &id,
			Name:   name,
			Config: testConfig,
		}
		resultMeta[i] = MustCreateKeyspaceByID(re, server, createRequest)
	}
	return resultMeta
}

// checkUpdateRequest verifies a keyspace meta matches a update request.
func checkUpdateRequest(re *require.Assertions, request *handlers.UpdateConfigParams, oldConfig, newConfig map[string]string) {
	expected := map[string]string{}
	maps.Copy(expected, oldConfig)
	for k, v := range request.Config {
		if v == nil {
			delete(expected, k)
		} else {
			expected[k] = *v
		}
	}
	re.Equal(expected, newConfig)
}

// TestKeyspaceErrorMessage verifies that BindJSON errors return clear error messages.
func (suite *keyspaceTestSuite) TestKeyspaceErrorMessage() {
	re := suite.Require()

	// Test CreateKeyspace with invalid JSON
	resp, err := tests.TestDialClient.Post(
		suite.server.GetAddr()+keyspacesPrefix,
		"application/json",
		bytes.NewBufferString("{invalid json}"),
	)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusBadRequest, resp.StatusCode)

	var errorMsg string
	re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
	re.NotEmpty(errorMsg, "Error message should not be empty")
	re.Contains(errorMsg, "invalid", "Error message should indicate invalid input")

	// Test UpdateKeyspaceConfig with invalid JSON
	httpReq, err := http.NewRequest(
		http.MethodPatch,
		suite.server.GetAddr()+keyspacesPrefix+"/test/config",
		bytes.NewBufferString("{invalid json}"),
	)
	re.NoError(err)
	resp, err = tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusBadRequest, resp.StatusCode)

	errorMsg = ""
	re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
	re.NotEmpty(errorMsg, "Error message should not be empty")
	re.Contains(errorMsg, "invalid", "Error message should indicate invalid input")

	// Test UpdateKeyspaceState with invalid JSON
	httpReq, err = http.NewRequest(
		http.MethodPut,
		suite.server.GetAddr()+keyspacesPrefix+"/test/state",
		bytes.NewBufferString("{invalid json}"),
	)
	re.NoError(err)
	resp, err = tests.TestDialClient.Do(httpReq)
	re.NoError(err)
	defer resp.Body.Close()
	re.Equal(http.StatusBadRequest, resp.StatusCode)

	errorMsg = ""
	re.NoError(json.NewDecoder(resp.Body).Decode(&errorMsg))
	re.NotEmpty(errorMsg, "Error message should not be empty")
	re.Contains(errorMsg, "invalid", "Error message should indicate invalid input")
}
