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

package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/goleak"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/keyspace/constant"
	mcsserver "github.com/tikv/pd/pkg/mcs/server"
	"github.com/tikv/pd/pkg/metering"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testutil.WaitForEtcdConnections(m), testutil.LeakOptions...)
}

type mockConfigProvider struct{ bs.Server }

func (*mockConfigProvider) GetControllerConfig() *ControllerConfig { return &ControllerConfig{} }

func (*mockConfigProvider) GetMeteringWriter() *metering.Writer { return nil }

func (*mockConfigProvider) GetResourceGroupWriteRole() ResourceGroupWriteRole {
	return ResourceGroupWriteRoleLegacyAll
}

func (*mockConfigProvider) AddStartCallback(...func()) {}

func (*mockConfigProvider) AddServiceReadyCallback(...func(context.Context) error) {}

type mockRoleConfigProvider struct {
	bs.Server
	role ResourceGroupWriteRole
}

func (*mockRoleConfigProvider) GetControllerConfig() *ControllerConfig { return &ControllerConfig{} }

func (*mockRoleConfigProvider) GetMeteringWriter() *metering.Writer { return nil }

func (m *mockRoleConfigProvider) GetResourceGroupWriteRole() ResourceGroupWriteRole { return m.role }

func (*mockRoleConfigProvider) AddStartCallback(...func()) {}

func (*mockRoleConfigProvider) AddServiceReadyCallback(...func(context.Context) error) {}

type testBasicServer struct{}

func (*testBasicServer) Name() string { return "test-rm" }

func (*testBasicServer) GetAddr() string { return "" }

func (*testBasicServer) Context() context.Context { return context.Background() }

func (*testBasicServer) Run() error { return nil }

func (*testBasicServer) Close() {}

func (*testBasicServer) GetServingUrls() []string { return nil }

func (*testBasicServer) GetClient() *clientv3.Client { return nil }

func (*testBasicServer) GetHTTPClient() *http.Client { return nil }

func (*testBasicServer) AddStartCallback(...func()) {}

func (*testBasicServer) IsServing() bool { return true }

func (*testBasicServer) AddServiceReadyCallback(...func(context.Context) error) {}

type fakeMetadataLoopWatcher struct {
	startWatchLoopFn func()
	waitLoadFn       func() error
}

func (w *fakeMetadataLoopWatcher) StartWatchLoop() {
	if w.startWatchLoopFn != nil {
		w.startWatchLoopFn()
	}
}

func (w *fakeMetadataLoopWatcher) WaitLoad() error {
	if w.waitLoadFn != nil {
		return w.waitLoadFn()
	}
	return nil
}

type failingControllerConfigStorage struct {
	storage.Storage
	err error
}

type cancelControllerConfigStorage struct {
	endpoint.ResourceGroupStorage
	endpoint.KeyspaceStorage
	cancel context.CancelFunc
}

type keyspaceLookupStorage struct {
	endpoint.ResourceGroupStorage
	endpoint.KeyspaceStorage
	runInTxn func(context.Context, func(kv.Txn) error) error
}

func (s keyspaceLookupStorage) RunInTxn(ctx context.Context, fn func(kv.Txn) error) error {
	return s.runInTxn(ctx, fn)
}

type tokenBucketTestStream struct {
	grpc.ServerStream
	ctx       context.Context
	request   *rmpb.TokenBucketsRequest
	responses []*rmpb.TokenBucketsResponse
}

func (s *tokenBucketTestStream) Context() context.Context { return s.ctx }

func (s *tokenBucketTestStream) Recv() (*rmpb.TokenBucketsRequest, error) {
	if s.request == nil {
		return nil, io.EOF
	}
	request := s.request
	s.request = nil
	return request, nil
}

func (s *tokenBucketTestStream) Send(response *rmpb.TokenBucketsResponse) error {
	s.responses = append(s.responses, response)
	return nil
}

func (s cancelControllerConfigStorage) LoadControllerConfig() (string, error) {
	s.cancel()
	return s.ResourceGroupStorage.LoadControllerConfig()
}

func (s failingControllerConfigStorage) SaveControllerConfig(any) error {
	return s.err
}

type testMetadataLoopWatcherFactory func(
	ctx context.Context,
	wg *sync.WaitGroup,
	client *clientv3.Client,
	name, key string,
	preEventsFn func([]*clientv3.Event) error,
	putFn, deleteFn func(*mvccpb.KeyValue) error,
	postEventsFn func([]*clientv3.Event) error,
	isWithPrefix bool,
) metadataLoopWatcher

func prepareManager() *Manager {
	storage := storage.NewStorageWithMemoryBackend()
	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = storage
	return m
}

func prepareMetadataWatcherManager() *Manager {
	m := prepareManager()
	m.enableMetadataWatcher = true
	m.srv = &testBasicServer{}
	return m
}

func withMetadataLoopWatcherFactory(t *testing.T, factory testMetadataLoopWatcherFactory) {
	t.Helper()
	originalFactory := newMetadataLoopWatcher
	newMetadataLoopWatcher = factory
	t.Cleanup(func() {
		newMetadataLoopWatcher = originalFactory
	})
}

func TestManagerMetadataWatcherLifecycle(t *testing.T) {
	t.Run("enables_metadata_watcher_for_rm_service_server", func(t *testing.T) {
		re := require.New(t)
		m := NewManager[*Server](&Server{
			BaseServer: &mcsserver.BaseServer{},
			cfg:        &Config{},
		})
		re.True(m.enableMetadataWatcher)
	})

	t.Run("cancels_metadata_watcher_context_on_init_error", func(t *testing.T) {
		re := require.New(t)
		m := prepareMetadataWatcherManager()

		parentCtx := t.Context()

		watcherErr := errors.New("watcher load failed")
		var capturedCtx context.Context
		withMetadataLoopWatcherFactory(t, func(
			ctx context.Context,
			_ *sync.WaitGroup,
			_ *clientv3.Client,
			_, _ string,
			_ func([]*clientv3.Event) error,
			_, _ func(*mvccpb.KeyValue) error,
			_ func([]*clientv3.Event) error,
			_ bool,
		) metadataLoopWatcher {
			capturedCtx = ctx
			return &fakeMetadataLoopWatcher{
				waitLoadFn: func() error { return watcherErr },
			}
		})

		err := m.Init(parentCtx)
		re.ErrorIs(err, watcherErr)
		re.NotNil(m.cancel)
		re.NotNil(capturedCtx)
		re.NotEqual(parentCtx.Done(), capturedCtx.Done())
		re.NoError(parentCtx.Err())
		re.ErrorIs(capturedCtx.Err(), context.Canceled)
	})

	t.Run("cancels_metadata_watcher_context_on_close", func(t *testing.T) {
		re := require.New(t)
		m := prepareMetadataWatcherManager()

		parentCtx := t.Context()

		var capturedCtx context.Context
		withMetadataLoopWatcherFactory(t, func(
			ctx context.Context,
			_ *sync.WaitGroup,
			_ *clientv3.Client,
			_, _ string,
			_ func([]*clientv3.Event) error,
			_, _ func(*mvccpb.KeyValue) error,
			_ func([]*clientv3.Event) error,
			_ bool,
		) metadataLoopWatcher {
			capturedCtx = ctx
			return &fakeMetadataLoopWatcher{}
		})

		re.NoError(m.Init(parentCtx))
		re.NotNil(capturedCtx)
		re.NotEqual(parentCtx.Done(), capturedCtx.Done())
		re.NoError(capturedCtx.Err())

		m.Close()

		re.ErrorIs(capturedCtx.Err(), context.Canceled)
	})
}

func TestManagerInitDoesNotAcceptConsumptionAfterContextCancellation(t *testing.T) {
	re := require.New(t)
	m := prepareManager()
	m.srv = &testBasicServer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Cleanup(m.Close)
	m.storage = cancelControllerConfigStorage{
		ResourceGroupStorage: m.storage,
		KeyspaceStorage:      m.storage,
		cancel:               cancel,
	}

	re.ErrorIs(m.Init(ctx), context.Canceled)
	m.consumptionMu.RLock()
	re.False(m.acceptingConsumption)
	re.Nil(m.consumptionTermDone)
	m.consumptionMu.RUnlock()
	re.Error(m.dispatchConsumption(&rmpb.TokenBucketRequest{}))
}

func TestLoadKeyspaceResourceGroupsRejectsMismatchedPayloadName(t *testing.T) {
	re := require.New(t)

	memStorage := storage.NewStorageWithMemoryBackend()
	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = memStorage

	group := &rmpb.ResourceGroup{
		Name:       "payload-group",
		Mode:       rmpb.GroupMode_RUMode,
		KeyspaceId: &rmpb.KeyspaceIDValue{Keyspace: &rmpb.KeyspaceIDValue_Value{Value: 42}},
	}
	rawGroup, err := proto.Marshal(group)
	re.NoError(err)
	re.NoError(memStorage.Save(keypath.KeyspaceResourceGroupSettingPath(42, "key-group"), string(rawGroup)))

	re.NoError(m.loadKeyspaceResourceGroups())
	krgm := m.getKeyspaceResourceGroupManager(42)
	re.NotNil(krgm)
	re.Nil(krgm.getResourceGroup("payload-group", false))
	re.Nil(krgm.getResourceGroup("key-group", false))
	re.NotNil(krgm.getResourceGroup(DefaultResourceGroupName, false))
}

func TestManagerControllerConfigSnapshots(t *testing.T) {
	t.Run("returns_snapshot", func(t *testing.T) {
		re := require.New(t)

		m := prepareManager()
		m.controllerConfig = &ControllerConfig{
			RequestUnit: RequestUnitConfig{
				ReadBaseCost: 0.5,
			},
		}

		snapshot := m.GetControllerConfig()
		snapshot.RequestUnit.ReadBaseCost = 1.5

		re.InDelta(0.5, m.controllerConfig.RequestUnit.ReadBaseCost, 0.00001)
	})

	t.Run("publishes_new_snapshot_after_successful_update", func(t *testing.T) {
		re := require.New(t)

		m := prepareManager()
		m.controllerConfig = &ControllerConfig{
			RequestUnit: RequestUnitConfig{
				ReadBaseCost: 0.5,
			},
		}

		previous := m.controllerConfig
		re.NoError(m.UpdateControllerConfigItem("request-unit.read-base-cost", 1.5))
		re.NotSame(previous, m.controllerConfig)
		re.InDelta(0.5, previous.RequestUnit.ReadBaseCost, 0.00001)
		re.InDelta(1.5, m.controllerConfig.RequestUnit.ReadBaseCost, 0.00001)
	})

	t.Run("does_not_publish_unsaved_snapshot", func(t *testing.T) {
		re := require.New(t)

		expectedErr := errors.New("save controller config failed")
		m := prepareManager()
		m.storage = failingControllerConfigStorage{
			Storage: storage.NewStorageWithMemoryBackend(),
			err:     expectedErr,
		}
		m.controllerConfig = &ControllerConfig{
			RequestUnit: RequestUnitConfig{
				ReadBaseCost: 0.5,
			},
		}

		previous := m.controllerConfig
		err := m.UpdateControllerConfigItem("request-unit.read-base-cost", 1.5)
		re.ErrorIs(err, expectedErr)
		re.Same(previous, m.controllerConfig)
		re.InDelta(0.5, m.controllerConfig.RequestUnit.ReadBaseCost, 0.00001)
	})
}

func TestPushMetricsConfig(t *testing.T) {
	re := require.New(t)

	re.Equal(pushMetricsConfig{}, getPushMetricsConfig(nil))
	re.Equal(pushMetricsConfig{}, getPushMetricsConfig(&ControllerConfig{
		PushMetricsAddress: "127.0.0.1:9091",
	}))
	re.Equal(pushMetricsConfig{
		address:  "127.0.0.1:9091",
		interval: time.Second,
	}, getPushMetricsConfig(&ControllerConfig{
		PushMetricsAddress: "127.0.0.1:9091",
		PushMetricsInterval: typeutil.Duration{
			Duration: time.Second,
		},
	}))
}

func TestSyncPushMetricsTicker(t *testing.T) {
	re := require.New(t)

	current := pushMetricsConfig{}
	ticker := current.syncPushMetricsTicker(pushMetricsConfig{
		address:  "127.0.0.1:9091",
		interval: time.Second,
	}, nil)
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()
	re.NotNil(ticker)
	re.NotNil(ticker.C)
	re.NotEqual(pushMetricsConfig{address: "127.0.0.1:9090", interval: time.Second}, current)

	sameTicker := current.syncPushMetricsTicker(current, ticker)
	re.Same(ticker, sameTicker)
	re.Equal(pushMetricsConfig{address: "127.0.0.1:9091", interval: time.Second}, current)

	ticker = current.syncPushMetricsTicker(pushMetricsConfig{}, ticker)
	re.Nil(ticker)
	re.Equal(pushMetricsConfig{}, current)
}

func TestInitManager(t *testing.T) {
	re := require.New(t)
	m := prepareManager()

	re.Empty(m.getKeyspaceResourceGroupManagers())
	ctx := t.Context()
	err := m.Init(ctx)
	re.NoError(err)
	// There should only be one null keyspace resource group manager.
	keyspaceID := uint32(1)
	krgm := m.getKeyspaceResourceGroupManager(constant.NullKeyspaceID)
	re.NotNil(krgm)
	re.Equal(constant.NullKeyspaceID, krgm.keyspaceID)
	re.Equal(DefaultResourceGroupName, krgm.getMutableResourceGroup(DefaultResourceGroupName).Name)
	// Add a new keyspace resource group manager.
	group := &rmpb.ResourceGroup{
		Name:       "test_group",
		Mode:       rmpb.GroupMode_RUMode,
		Priority:   5,
		KeyspaceId: &rmpb.KeyspaceIDValue{Keyspace: &rmpb.KeyspaceIDValue_Value{Value: keyspaceID}},
	}
	err = m.AddResourceGroup(group)
	re.NoError(err)
	// Adding a new keyspace resource group should create a new keyspace resource group manager.
	krgm = m.getKeyspaceResourceGroupManager(1)
	re.NotNil(krgm)
	re.Equal(group.KeyspaceId.GetValue(), krgm.keyspaceID)
	re.Equal(group.Name, krgm.getMutableResourceGroup(group.Name).Name)
	// A default resource group should be created for the keyspace as well.
	defaultGroup := krgm.getMutableResourceGroup(DefaultResourceGroupName)
	re.Equal(DefaultResourceGroupName, defaultGroup.Name)
	// Modify the default resource group settings.
	defaultGroup.RUSettings.RU.Settings.FillRate = 100
	defaultGroupPb := defaultGroup.IntoProtoResourceGroup(keyspaceID)
	err = m.ModifyResourceGroup(defaultGroupPb)
	re.NoError(err)
	// Rebuild the manager based on the same storage.
	storage := m.storage
	m = NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = storage
	err = m.Init(ctx)
	re.NoError(err)
	re.Len(m.getKeyspaceResourceGroupManagers(), 2)
	// Get the default resource group.
	rg, err := m.GetResourceGroup(1, DefaultResourceGroupName, true)
	re.NoError(err)
	re.NotNil(rg)
	// Verify the default resource group settings are updated. This is to ensure the default resource group
	// can be loaded from the storage correctly rather than created as a new one.
	re.Equal(defaultGroup.getFillRate(), rg.getFillRate())
}

func TestBackgroundMetricsFlush(t *testing.T) {
	re := require.New(t)
	m := prepareManager()

	ctx := t.Context()
	err := m.Init(ctx)
	re.NoError(err)
	// Test without keyspace ID
	checkBackgroundMetricsFlush(ctx, re, m, nil)
	// Test with keyspace ID
	checkBackgroundMetricsFlush(ctx, re, m, &rmpb.KeyspaceIDValue{Keyspace: &rmpb.KeyspaceIDValue_Value{Value: 1}})
}

func checkBackgroundMetricsFlush(ctx context.Context, re *require.Assertions, manager *Manager, keyspaceIDValue *rmpb.KeyspaceIDValue) {
	// Prepare the keyspace name for later lookup.
	prepareKeyspaceName(ctx, re, manager, keyspaceIDValue, "test_keyspace")
	// Add a resource group.
	group := &rmpb.ResourceGroup{
		Name:     "test_group",
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: 200,
				},
			},
		},
		KeyspaceId: keyspaceIDValue,
	}
	err := manager.AddResourceGroup(group)
	re.NoError(err)

	// Send consumption to the dispatcher.
	req := &rmpb.TokenBucketRequest{
		ResourceGroupName: group.GetName(),
		ConsumptionSinceLastRequest: &rmpb.Consumption{
			RRU: 10.0,
			WRU: 20.0,
		},
		KeyspaceId: keyspaceIDValue,
	}
	err = manager.dispatchConsumption(req)
	re.NoError(err)

	keyspaceID := ExtractKeyspaceID(req.GetKeyspaceId())
	// Verify consumption was added to the resource group.
	testutil.Eventually(re, func() bool {
		updatedGroup, err := manager.GetResourceGroup(keyspaceID, req.GetResourceGroupName(), true)
		re.NoError(err)
		re.NotNil(updatedGroup)
		return updatedGroup.RUConsumption.RRU == req.ConsumptionSinceLastRequest.RRU &&
			updatedGroup.RUConsumption.WRU == req.ConsumptionSinceLastRequest.WRU
	})
}

func TestDispatchConsumptionIncludesOnlyConsumption(t *testing.T) {
	re := require.New(t)
	m := newManagerBase(&ControllerConfig{}, ResourceGroupWriteRoleLegacyAll)
	req := &rmpb.TokenBucketRequest{
		ResourceGroupName: "test_group",
		ConsumptionSinceLastRequest: &rmpb.Consumption{
			RRU:        12,
			WRU:        8,
			WriteBytes: 1024,
		},
		KeyspaceId: &rmpb.KeyspaceIDValue{Keyspace: &rmpb.KeyspaceIDValue_Value{Value: 42}},
	}

	err := m.dispatchConsumption(req)
	re.NoError(err)

	item := <-m.consumptionDispatcher
	re.Equal(uint32(42), item.keyspaceID)
	re.Empty(item.keyspaceName)
	re.Equal(req.GetResourceGroupName(), item.resourceGroupName)
	re.Equal(req.GetConsumptionSinceLastRequest(), item.Consumption)
}

func TestAcquireTokenBucketsDoesNotReadKeyspaceMetadata(t *testing.T) {
	re := require.New(t)
	m := prepareManager()
	m.srv = &testBasicServer{}
	ctx := t.Context()
	m.acceptingConsumption = true
	m.consumptionTermDone = ctx.Done()

	lookupCalls := 0
	m.storage = keyspaceLookupStorage{
		ResourceGroupStorage: m.storage,
		KeyspaceStorage:      m.storage,
		runInTxn: func(context.Context, func(kv.Txn) error) error {
			lookupCalls++
			return errors.New("keyspace metadata unavailable")
		},
	}
	requests := make([]*rmpb.TokenBucketRequest, 0, 2)
	for _, keyspaceID := range []uint32{42, constant.NullKeyspaceID} {
		requests = append(requests, &rmpb.TokenBucketRequest{
			KeyspaceId:                  &rmpb.KeyspaceIDValue{Keyspace: &rmpb.KeyspaceIDValue_Value{Value: keyspaceID}},
			ResourceGroupName:           DefaultResourceGroupName,
			ConsumptionSinceLastRequest: &rmpb.Consumption{RRU: 1},
			Request: &rmpb.TokenBucketRequest_RuItems{RuItems: &rmpb.TokenBucketRequest_RequestRU{
				RequestRU: []*rmpb.RequestUnitItem{{Type: rmpb.RequestUnitType_RU, Value: 10}},
			}},
		})
	}
	stream := &tokenBucketTestStream{
		ctx: ctx,
		request: &rmpb.TokenBucketsRequest{
			Requests:              requests,
			TargetRequestPeriodMs: 1000,
			ClientUniqueId:        1,
		},
	}
	service := &Service{ctx: ctx, manager: m}
	re.NoError(service.AcquireTokenBuckets(stream))
	re.Zero(lookupCalls)
	re.Len(stream.responses, 1)
	re.Len(stream.responses[0].Responses, len(requests))
	for i, request := range requests {
		response := stream.responses[0].Responses[i]
		re.Equal(request.KeyspaceId, response.KeyspaceId)
		re.NotEmpty(response.GrantedRUTokens)
		item := <-m.consumptionDispatcher
		re.Equal(ExtractKeyspaceID(request.KeyspaceId), item.keyspaceID)
		re.Same(request.ConsumptionSinceLastRequest, item.Consumption)
	}
}

// Put a keyspace meta into the storage.
func prepareKeyspaceName(ctx context.Context, re *require.Assertions, manager *Manager, keyspaceIDValue *rmpb.KeyspaceIDValue, keyspaceName string) {
	keyspaceMeta := &keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: ExtractKeyspaceID(keyspaceIDValue)},
		Name:     keyspaceName,
	}
	err := manager.storage.RunInTxn(ctx, func(txn kv.Txn) error {
		err := manager.storage.SaveKeyspaceMeta(txn, keyspaceMeta)
		if err != nil {
			return err
		}
		return manager.storage.SaveKeyspaceID(txn, keyspaceMeta.GetId(), keyspaceMeta.Name)
	})
	re.NoError(err)
}

func TestAddAndModifyResourceGroup(t *testing.T) {
	re := require.New(t)

	storage := storage.NewStorageWithMemoryBackend()
	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = storage

	ctx := t.Context()
	err := m.Init(ctx)
	re.NoError(err)

	// Test without keyspace ID
	checkAddAndModifyResourceGroup(re, m, nil)
	// Test with keyspace ID
	checkAddAndModifyResourceGroup(re, m, &rmpb.KeyspaceIDValue{Keyspace: &rmpb.KeyspaceIDValue_Value{Value: 1}})
}

func checkAddAndModifyResourceGroup(re *require.Assertions, manager *Manager, keyspaceIDValue *rmpb.KeyspaceIDValue) {
	group := &rmpb.ResourceGroup{
		Name:     "test_group",
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: 200,
				},
			},
		},
		KeyspaceId: keyspaceIDValue,
	}
	err := manager.AddResourceGroup(group)
	re.NoError(err)

	group.Priority = 10
	group.RUSettings.RU.Settings.BurstLimit = 300
	err = manager.ModifyResourceGroup(group)
	re.NoError(err)

	keyspaceID := ExtractKeyspaceID(keyspaceIDValue)
	testutil.Eventually(re, func() bool {
		rg, err := manager.GetResourceGroup(keyspaceID, group.Name, true)
		re.NoError(err)
		re.NotNil(rg)
		return rg.Priority == group.Priority &&
			rg.RUSettings.RU.getBurstLimitSetting() == group.RUSettings.RU.Settings.BurstLimit
	})
}

func TestCleanUpMetrics(t *testing.T) {
	re := require.New(t)
	m := prepareManager()

	ctx := t.Context()
	// Put a keyspace meta.
	keyspaceID := uint32(1)
	prepareKeyspaceName(ctx, re, m, &rmpb.KeyspaceIDValue{Keyspace: &rmpb.KeyspaceIDValue_Value{Value: keyspaceID}}, "test_keyspace")
	// Insert two consumption records manually.
	insertRecords := func() {
		m.metrics.consumptionRecordMap[consumptionRecordKey{
			keyspaceID: keyspaceID,
			groupName:  "test_group_1",
			ruType:     defaultTypeLabel,
		}] = time.Now().Add(-metricsCleanupTimeout * 2)
		m.metrics.consumptionRecordMap[consumptionRecordKey{
			keyspaceID: keyspaceID,
			groupName:  "test_group_2",
			ruType:     defaultTypeLabel,
		}] = time.Now().Add(-metricsCleanupTimeout / 2)
	}
	insertRecords()
	m.cleanUpMetrics(ctx)

	re.Len(m.metrics.consumptionRecordMap, 1)
	re.Contains(m.metrics.consumptionRecordMap, consumptionRecordKey{
		keyspaceID: keyspaceID,
		groupName:  "test_group_2",
		ruType:     defaultTypeLabel,
	})
	keyspaceName, err := m.getKeyspaceNameByID(ctx, keyspaceID)
	re.NoError(err)
	re.Equal("test_keyspace", keyspaceName)
}

func TestManagerInitResetsPreviousMetrics(t *testing.T) {
	re := require.New(t)
	m := prepareManager()
	m.srv = &testBasicServer{}
	t.Cleanup(m.Close)

	const (
		keyspaceName = "reinit-metrics-keyspace"
		groupName    = "reinit-metrics-group"
		keyspaceID   = uint32(511)
	)
	m.metrics.recordConsumption(&consumptionItem{
		keyspaceID:        keyspaceID,
		keyspaceName:      keyspaceName,
		resourceGroupName: groupName,
		Consumption:       &rmpb.Consumption{RRU: 1},
	}, &ControllerConfig{}, time.Now())
	m.metrics.setServiceLimitMetric(keyspaceName, 100)

	ctx := t.Context()
	re.NoError(m.Init(ctx))

	re.Empty(m.metrics.consumptionRecordMap)
	re.Empty(m.metrics.counterMetricsMap)
	re.Empty(m.metrics.maxPerSecTrackerMap)
	re.NotContains(m.metrics.serviceLimitNameSet, keyspaceName)
	re.Zero(promtestutil.ToFloat64(readRequestUnitCost.WithLabelValues(groupName, groupName, defaultTypeLabel, keyspaceName)))
	re.Zero(promtestutil.ToFloat64(serviceLimit.WithLabelValues(keyspaceName)))
	t.Cleanup(func() {
		deleteLabelValues(keyspaceName, groupName, defaultTypeLabel)
		serviceLimit.DeleteLabelValues(keyspaceName)
	})
}

func TestMetricsResetOnStepDown(t *testing.T) {
	re := require.New(t)
	m := prepareManager()
	t.Cleanup(m.Close)
	m.srv = &testBasicServer{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		keyspaceName = "step-down-keyspace"
		groupName    = "step-down-group"
		keyspaceID   = uint32(507)
	)
	keyspaceIDValue := &rmpb.KeyspaceIDValue{Keyspace: &rmpb.KeyspaceIDValue_Value{Value: keyspaceID}}
	prepareKeyspaceName(ctx, re, m, keyspaceIDValue, keyspaceName)
	m.updateKeyspaceNameLookup(keyspaceID, keyspaceName)
	t.Cleanup(func() {
		deleteLabelValues(keyspaceName, groupName, defaultTypeLabel)
		deleteLabelValues(keyspaceName, DefaultResourceGroupName, defaultTypeLabel)
		serviceLimit.DeleteLabelValues(keyspaceName)
	})

	re.NoError(m.Init(ctx))
	re.NoError(m.AddResourceGroup(&rmpb.ResourceGroup{
		Name:       groupName,
		Mode:       rmpb.GroupMode_RUMode,
		KeyspaceId: keyspaceIDValue,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 100, BurstLimit: 200}},
		},
	}))
	re.NoError(m.SetKeyspaceServiceLimit(keyspaceID, 100))
	re.NoError(m.dispatchConsumption(&rmpb.TokenBucketRequest{
		KeyspaceId:                  keyspaceIDValue,
		ResourceGroupName:           groupName,
		ConsumptionSinceLastRequest: &rmpb.Consumption{RRU: 1},
	}))

	// Wait for both consumption and the periodic gauges in the live term.
	// Prometheus children are safe to inspect while the worker updates them.
	testutil.Eventually(re, func() bool {
		return promtestutil.ToFloat64(readRequestUnitCost.WithLabelValues(groupName, groupName, defaultTypeLabel, keyspaceName)) == 1 &&
			promtestutil.ToFloat64(serviceLimit.WithLabelValues(keyspaceName)) == 100 &&
			promtestutil.ToFloat64(resourceGroupConfigGauge.WithLabelValues(groupName, ruPerSecLabel, keyspaceName)) > 0
	})

	cancel()
	m.Close()

	re.Nil(m.cancel)
	re.Error(m.dispatchConsumption(&rmpb.TokenBucketRequest{}))
	re.Empty(m.metrics.consumptionRecordMap)
	re.Empty(m.metrics.counterMetricsMap)
	re.Empty(m.metrics.gaugeMetricsMap)
	re.Empty(m.metrics.maxPerSecTrackerMap)
	re.Empty(m.metrics.serviceLimitNameSet)

	registry := prometheus.NewRegistry()
	registry.MustRegister(readRequestUnitCost, writeRequestUnitCost, availableRUCounter,
		resourceGroupConfigGauge, serviceLimit, readRequestUnitMaxPerSecCost, writeRequestUnitMaxPerSecCost)
	families, err := registry.Gather()
	re.NoError(err)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == keyspaceNameLabel {
					re.NotEqual(keyspaceName, label.GetValue(), "stale metric series: %s", family.GetName())
				}
			}
		}
	}
}

func TestBackgroundMetricsFlushDrainsPendingConsumptionOnStepDown(t *testing.T) {
	re := require.New(t)
	m := prepareManager()
	m.srv = &testBasicServer{}

	const (
		keyspaceName = "step-down-drain-keyspace"
		groupName    = "step-down-drain-group"
		keyspaceID   = uint32(510)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	prepareKeyspaceName(ctx, re, m, &rmpb.KeyspaceIDValue{Keyspace: &rmpb.KeyspaceIDValue_Value{Value: keyspaceID}}, keyspaceName)
	m.consumptionMu.Lock()
	m.consumptionTermDone = ctx.Done()
	m.acceptingConsumption = true
	m.consumptionMu.Unlock()
	// Fill the queue before any worker has resolved this keyspace's name.
	re.Empty(m.keyspaceNameLookup)
	for range defaultConsumptionChanSize {
		re.NoError(m.dispatchConsumption(&rmpb.TokenBucketRequest{
			KeyspaceId:                  &rmpb.KeyspaceIDValue{Keyspace: &rmpb.KeyspaceIDValue_Value{Value: keyspaceID}},
			ResourceGroupName:           groupName,
			ConsumptionSinceLastRequest: &rmpb.Consumption{RRU: 1},
		}))
	}
	cancel()
	m.wg.Add(1)
	go m.backgroundMetricsFlush(ctx)
	m.wg.Wait()

	re.Empty(m.consumptionDispatcher)
	re.Equal(float64(defaultConsumptionChanSize), m.ruCollector.keyspaceRUMetering[keyspaceName].oltpRU)
	re.Empty(m.metrics.consumptionRecordMap)
	re.Empty(m.metrics.counterMetricsMap)
	m.consumptionMu.RLock()
	re.False(m.acceptingConsumption)
	m.consumptionMu.RUnlock()
	t.Cleanup(func() {
		deleteLabelValues(keyspaceName, groupName, defaultTypeLabel)
	})
}

func TestDrainPendingConsumptionContinuesWithCachedReportsAfterCancellation(t *testing.T) {
	re := require.New(t)
	m := prepareManager()
	const (
		keyspaceID   = uint32(512)
		keyspaceName = "cached-drain-keyspace"
	)
	m.updateKeyspaceNameLookup(keyspaceID, keyspaceName)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lookupCalls := 0
	m.storage = keyspaceLookupStorage{
		ResourceGroupStorage: m.storage,
		KeyspaceStorage:      m.storage,
		runInTxn: func(ctx context.Context, _ func(kv.Txn) error) error {
			lookupCalls++
			cancel()
			<-ctx.Done()
			return ctx.Err()
		},
	}
	for i := range defaultConsumptionChanSize - 2 {
		m.consumptionDispatcher <- &consumptionItem{
			keyspaceID:  uint32(i) + keyspaceID + 1,
			Consumption: &rmpb.Consumption{RRU: 1},
		}
	}
	m.consumptionDispatcher <- &consumptionItem{keyspaceID: keyspaceID, Consumption: &rmpb.Consumption{RRU: 2}}
	m.consumptionDispatcher <- &consumptionItem{keyspaceID: constant.NullKeyspaceID, Consumption: &rmpb.Consumption{RRU: 3}}

	m.drainPendingConsumption(ctx, nil)

	re.ErrorIs(ctx.Err(), context.Canceled)
	re.Equal(1, lookupCalls)
	re.Empty(m.consumptionDispatcher)
	re.Len(m.ruCollector.keyspaceRUMetering, 2)
	re.Equal(2.0, m.ruCollector.keyspaceRUMetering[keyspaceName].oltpRU)
	re.Equal(3.0, m.ruCollector.keyspaceRUMetering[""].oltpRU)
	re.Empty(m.metrics.consumptionRecordMap)
	re.Empty(m.metrics.counterMetricsMap)
}

func TestBackgroundMetricsFlushRetriesInterruptedConsumptionOnStepDown(t *testing.T) {
	re := require.New(t)
	m := prepareManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const keyspaceID = uint32(513)
	const keyspaceName = "interrupted-drain-keyspace"
	prepareKeyspaceName(ctx, re, m, &rmpb.KeyspaceIDValue{Keyspace: &rmpb.KeyspaceIDValue_Value{Value: keyspaceID}}, keyspaceName)
	baseStorage := m.storage
	lookupCalls := 0
	var drainDeadline time.Time
	var hasDrainDeadline bool
	m.storage = keyspaceLookupStorage{
		ResourceGroupStorage: baseStorage,
		KeyspaceStorage:      baseStorage,
		runInTxn: func(lookupCtx context.Context, fn func(kv.Txn) error) error {
			lookupCalls++
			if lookupCalls == 1 {
				cancel()
				return lookupCtx.Err()
			}
			drainDeadline, hasDrainDeadline = lookupCtx.Deadline()
			return baseStorage.RunInTxn(lookupCtx, fn)
		},
	}
	m.consumptionDispatcher <- &consumptionItem{keyspaceID: keyspaceID, Consumption: &rmpb.Consumption{RRU: 1}}
	m.wg.Add(1)
	go m.backgroundMetricsFlush(ctx)
	m.wg.Wait()

	re.Equal(2, lookupCalls)
	re.True(hasDrainDeadline)
	re.Positive(time.Until(drainDeadline))
	re.LessOrEqual(time.Until(drainDeadline), etcdutil.DefaultRequestTimeout)
	re.Empty(m.consumptionDispatcher)
	re.Equal(1.0, m.ruCollector.keyspaceRUMetering[keyspaceName].oltpRU)
	re.Empty(m.metrics.consumptionRecordMap)
	re.Empty(m.metrics.counterMetricsMap)
}

func TestCleanUpMetricsKeepsGroupStateForActiveRUType(t *testing.T) {
	re := require.New(t)
	m := newManagerBase(&ControllerConfig{}, ResourceGroupWriteRoleLegacyAll)

	const (
		keyspaceName = "active-ru-cleanup-keyspace"
		groupName    = "active-ru-cleanup-group"
		keyspaceID   = uint32(509)
	)
	m.updateKeyspaceNameLookup(keyspaceID, keyspaceName)
	m.metrics.recordConsumption(&consumptionItem{
		keyspaceID:        keyspaceID,
		keyspaceName:      keyspaceName,
		resourceGroupName: groupName,
		Consumption:       &rmpb.Consumption{RRU: 2, ReadCrossAzTrafficBytes: 3},
	}, &ControllerConfig{}, time.Now().Add(-metricsCleanupTimeout*2))
	m.metrics.recordConsumption(&consumptionItem{
		keyspaceID:        keyspaceID,
		keyspaceName:      keyspaceName,
		resourceGroupName: groupName,
		isBackground:      true,
		Consumption:       &rmpb.Consumption{RRU: 5, ReadCrossAzTrafficBytes: 7},
	}, &ControllerConfig{}, time.Now())
	m.metrics.getGaugeMetrics(keyspaceID, keyspaceName, groupName).availableRUCounter.Set(1)
	tracker := m.metrics.maxPerSecTrackerMap[trackerKey{keyspaceID, groupName}]
	m.ruCollector.Collect(&consumptionItem{
		keyspaceName: keyspaceName,
		Consumption:  &rmpb.Consumption{RRU: 11},
	})

	m.cleanUpMetrics(context.Background())

	re.NotContains(m.metrics.consumptionRecordMap, consumptionRecordKey{keyspaceID, groupName, defaultTypeLabel})
	re.Contains(m.metrics.consumptionRecordMap, consumptionRecordKey{keyspaceID, groupName, backgroundTypeLabel})
	re.Same(tracker, m.metrics.maxPerSecTrackerMap[trackerKey{keyspaceID, groupName}])
	re.Equal(float64(10), promtestutil.ToFloat64(crossAZTrafficCost.WithLabelValues(groupName, "read", keyspaceName)))
	re.Equal(float64(1), promtestutil.ToFloat64(availableRUCounter.WithLabelValues(groupName, groupName, keyspaceName)))
	re.Equal(11.0, m.ruCollector.keyspaceRUMetering[keyspaceName].oltpRU)
	t.Cleanup(func() {
		deleteLabelValues(keyspaceName, groupName, defaultTypeLabel)
		deleteLabelValues(keyspaceName, groupName, backgroundTypeLabel)
	})
}

func TestKeyspaceServiceLimit(t *testing.T) {
	re := require.New(t)

	storage := storage.NewStorageWithMemoryBackend()
	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = storage

	ctx := t.Context()
	err := m.Init(ctx)
	re.NoError(err)
	// Test the default service limit is 0.0.
	limiter := m.GetKeyspaceServiceLimiter(constant.NullKeyspaceID)
	re.NotNil(limiter)
	re.Equal(0.0, limiter.ServiceLimit)
	re.Equal(0.0, limiter.AvailableTokens)
	group := &rmpb.ResourceGroup{
		Name:     "test_group",
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: 200,
				},
			},
		},
		KeyspaceId: &rmpb.KeyspaceIDValue{Keyspace: &rmpb.KeyspaceIDValue_Value{Value: 1}},
	}
	// Test the limiter of the non-existing keyspace is nil.
	limiter = m.GetKeyspaceServiceLimiter(group.KeyspaceId.GetValue())
	re.Nil(limiter)
	// Test the limiter of the newly created keyspace is 0.0.
	err = m.AddResourceGroup(group)
	re.NoError(err)
	limiter = m.GetKeyspaceServiceLimiter(1)
	re.Equal(0.0, limiter.ServiceLimit)
	re.Equal(0.0, limiter.AvailableTokens)
	// Test set the service limit of the keyspace.
	re.NoError(m.SetKeyspaceServiceLimit(1, 100.0))
	limiter = m.GetKeyspaceServiceLimiter(1)
	re.Equal(100.0, limiter.ServiceLimit)
	re.Equal(0.0, limiter.AvailableTokens) // When setting from 0 to positive, available tokens remain 0
	// Test set the service limit of the non-existing keyspace.
	limiter = m.GetKeyspaceServiceLimiter(2)
	re.Nil(limiter)
	re.NoError(m.SetKeyspaceServiceLimit(2, 100.0))
	limiter = m.GetKeyspaceServiceLimiter(2)
	re.Equal(100.0, limiter.ServiceLimit)
	re.Equal(0.0, limiter.AvailableTokens)
	// Ensure the keyspace resource group manager is initialized correctly.
	krgm := m.getKeyspaceResourceGroupManager(2)
	re.NotNil(krgm)
	re.Equal(uint32(2), krgm.keyspaceID)
	re.Equal(DefaultResourceGroupName, krgm.getMutableResourceGroup(DefaultResourceGroupName).Name)
}

func TestKeyspaceNameLookup(t *testing.T) {
	re := require.New(t)
	m := prepareManager()

	ctx := t.Context()
	err := m.Init(ctx)
	re.NoError(err)
	// Get the null keyspace ID by an empty name.
	idValue, err := m.GetKeyspaceIDByName(ctx, "")
	re.NoError(err)
	re.NotNil(idValue)
	re.Equal(constant.NullKeyspaceID, idValue.GetValue())
	// Get the non-existing keyspace ID by name.
	idValue, err = m.GetKeyspaceIDByName(ctx, "non-existing-keyspace")
	re.Error(err)
	re.Nil(idValue)
	// Get the null keyspace name.
	name, err := m.getKeyspaceNameByID(ctx, constant.NullKeyspaceID)
	re.NoError(err)
	re.Empty(name)
	// Get the non-existing keyspace name.
	name, err = m.getKeyspaceNameByID(ctx, 1)
	re.Error(err)
	re.Empty(name)
	// Get the keyspace ID by name first, then get the keyspace name by ID.
	prepareKeyspaceName(ctx, re, m, &rmpb.KeyspaceIDValue{Keyspace: &rmpb.KeyspaceIDValue_Value{Value: 1}}, "test_keyspace")
	idValue, err = m.GetKeyspaceIDByName(ctx, "test_keyspace")
	re.NoError(err)
	re.NotNil(idValue)
	re.Equal(uint32(1), idValue.GetValue())
	name, err = m.getKeyspaceNameByID(ctx, 1)
	re.NoError(err)
	re.Equal("test_keyspace", name)
	// Get the keyspace name by ID first, then get the keyspace ID by name.
	prepareKeyspaceName(ctx, re, m, &rmpb.KeyspaceIDValue{Keyspace: &rmpb.KeyspaceIDValue_Value{Value: 2}}, "test_keyspace_2")
	name, err = m.getKeyspaceNameByID(ctx, 2)
	re.NoError(err)
	re.Equal("test_keyspace_2", name)
	idValue, err = m.GetKeyspaceIDByName(ctx, "test_keyspace_2")
	re.NoError(err)
	re.NotNil(idValue)
	re.Equal(uint32(2), idValue.GetValue())
}

func TestResourceGroupPersistence(t *testing.T) {
	re := require.New(t)
	m := prepareManager()

	// Prepare the resource group and service limit.
	group := &rmpb.ResourceGroup{
		Name:       "test_group",
		Mode:       rmpb.GroupMode_RUMode,
		Priority:   5,
		KeyspaceId: &rmpb.KeyspaceIDValue{Keyspace: &rmpb.KeyspaceIDValue_Value{Value: 1}},
	}
	err := m.AddResourceGroup(group)
	re.NoError(err)
	keyspaceID := ExtractKeyspaceID(group.KeyspaceId)
	re.NoError(m.SetKeyspaceServiceLimit(keyspaceID, 100.0))

	// Use the same storage to rebuild a manager.
	storage := m.storage
	m = NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = storage
	// Initialize the manager.
	ctx := t.Context()
	err = m.Init(ctx)
	re.NoError(err)
	// Check the resource group is loaded from the storage.
	rg, err := m.GetResourceGroup(keyspaceID, group.Name, true)
	re.NoError(err)
	re.NotNil(rg)
	re.Equal(group.Name, rg.Name)
	re.Equal(group.Mode, rg.Mode)
	re.Equal(group.Priority, rg.Priority)
	// Check the service limit is loaded from the storage.
	limiter := m.GetKeyspaceServiceLimiter(keyspaceID)
	re.NotNil(limiter)
	re.Equal(100.0, limiter.ServiceLimit)
	// Null keyspace ID should have a default zero service limit.
	limiter = m.GetKeyspaceServiceLimiter(constant.NullKeyspaceID)
	re.NotNil(limiter)
	re.Equal(0.0, limiter.ServiceLimit)
	// Non-existing keyspace should have a nil limiter.
	limiter = m.GetKeyspaceServiceLimiter(2)
	re.Nil(limiter)
}

func TestManagerMetadataWriteRoleGates(t *testing.T) {
	re := require.New(t)
	m := NewManager[*mockRoleConfigProvider](&mockRoleConfigProvider{
		role: ResourceGroupWriteRoleRMTokenOnly,
	})
	m.storage = storage.NewStorageWithMemoryBackend()
	ctx := t.Context()
	re.NoError(m.Init(ctx))
	re.Equal(ResourceGroupWriteRoleRMTokenOnly, m.GetWriteRole())

	group := &rmpb.ResourceGroup{
		Name: "test_group",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 100}},
		},
	}
	re.ErrorIs(m.AddResourceGroup(group), errMetadataWriteDisabled)
	re.ErrorIs(m.ModifyResourceGroup(group), errMetadataWriteDisabled)
	re.ErrorIs(m.DeleteResourceGroup(constant.NullKeyspaceID, group.Name), errMetadataWriteDisabled)
	re.ErrorIs(m.UpdateControllerConfigItem("request-unit.read-base-cost", 1.0), errMetadataWriteDisabled)
	re.ErrorIs(m.SetKeyspaceServiceLimit(constant.NullKeyspaceID, 1.0), errMetadataWriteDisabled)
}

func TestKeyspaceResourceGroupManagerWriteRoleGates(t *testing.T) {
	re := require.New(t)
	memStorage := storage.NewStorageWithMemoryBackend()
	group := &rmpb.ResourceGroup{
		Name: "test_group",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 100}},
		},
	}

	tokenOnlyKRGM := newKeyspaceResourceGroupManager(1, memStorage, ResourceGroupWriteRoleRMTokenOnly)
	re.NoError(tokenOnlyKRGM.addResourceGroup(group))
	modifiedGroup := &rmpb.ResourceGroup{
		Name: "test_group",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 200}},
		},
	}
	re.ErrorIs(tokenOnlyKRGM.modifyResourceGroup(modifiedGroup), errMetadataWriteDisabled)
	re.Equal(float64(100), tokenOnlyKRGM.getResourceGroup(group.GetName(), false).getFillRate())
	re.ErrorIs(tokenOnlyKRGM.deleteResourceGroup(group.GetName()), errMetadataWriteDisabled)
	re.NotNil(tokenOnlyKRGM.getResourceGroup(group.GetName(), false))

	var tokenOnlySettingsCount int
	re.NoError(memStorage.LoadResourceGroupSettings(func(keyspaceID uint32, name, _ string) {
		if keyspaceID == tokenOnlyKRGM.keyspaceID && name == group.GetName() {
			tokenOnlySettingsCount++
		}
	}))
	var tokenOnlyStatesCount int
	re.NoError(memStorage.LoadResourceGroupStates(func(keyspaceID uint32, name, _ string) {
		if keyspaceID == tokenOnlyKRGM.keyspaceID && name == group.GetName() {
			tokenOnlyStatesCount++
		}
	}))
	re.Equal(0, tokenOnlySettingsCount)
	re.Equal(1, tokenOnlyStatesCount)

	metaOnlyKRGM := newKeyspaceResourceGroupManager(2, memStorage, ResourceGroupWriteRolePDMetaOnly)
	re.NoError(metaOnlyKRGM.addResourceGroup(group))
	metaOnlyKRGM.persistResourceGroupRunningState()

	var metaOnlySettingsCount int
	re.NoError(memStorage.LoadResourceGroupSettings(func(keyspaceID uint32, name, _ string) {
		if keyspaceID == metaOnlyKRGM.keyspaceID && name == group.GetName() {
			metaOnlySettingsCount++
		}
	}))
	var metaOnlyStatesCount int
	re.NoError(memStorage.LoadResourceGroupStates(func(keyspaceID uint32, name, _ string) {
		if keyspaceID == metaOnlyKRGM.keyspaceID && name == group.GetName() {
			metaOnlyStatesCount++
		}
	}))
	re.Equal(1, metaOnlySettingsCount)
	re.Equal(0, metaOnlyStatesCount)
}
