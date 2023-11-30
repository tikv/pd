// Copyright 2016 TiKV Project Authors.
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

package pd

import (
	"context"
	"fmt"
	"math/rand"
	"runtime/trace"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/grpcutil"
	"github.com/tikv/pd/client/tlsutil"
	"github.com/tikv/pd/client/tsoutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

const (
	// defaultKeyspaceID is the default key space id.
	// Valid keyspace id range is [0, 0xFFFFFF](uint24max, or 16777215)
	// ​0 is reserved for default keyspace with the name "DEFAULT", It's initialized
	// when PD bootstrap and reserved for users who haven't been assigned keyspace.
	defaultKeyspaceID = uint32(0)
	maxKeyspaceID     = uint32(0xFFFFFF)
	// nullKeyspaceID is used for api v1 or legacy path where is keyspace agnostic.
	nullKeyspaceID = uint32(0xFFFFFFFF)
	// defaultKeySpaceGroupID is the default key space group id.
	// We also reserved 0 for the keyspace group for the same purpose.
	defaultKeySpaceGroupID = uint32(0)
	defaultKeyspaceName    = "DEFAULT"
)

// Region contains information of a region's meta and its peers.
type Region struct {
	Meta         *metapb.Region
	Leader       *metapb.Peer
	DownPeers    []*metapb.Peer
	PendingPeers []*metapb.Peer
	Buckets      *metapb.Buckets
}

// GlobalConfigItem standard format of KV pair in GlobalConfig client
type GlobalConfigItem struct {
	EventType pdpb.EventType
	Name      string
	Value     string
	PayLoad   []byte
}

// Client is a PD (Placement Driver) RPC client.
// It should not be used after calling Close().
type Client interface {
	// GetClusterID gets the cluster ID from PD.
	GetClusterID(ctx context.Context) uint64
	// GetAllMembers gets the members Info from PD
	GetAllMembers(ctx context.Context) ([]*pdpb.Member, error)
	// GetLeaderAddr returns current leader's address. It returns "" before
	// syncing leader from server.
	GetLeaderAddr() string
	// GetRegion gets a region and its leader Peer from PD by key.
	// The region may expire after split. Caller is responsible for caching and
	// taking care of region change.
	// Also, it may return nil if PD finds no Region for the key temporarily,
	// client should retry later.
	GetRegion(ctx context.Context, key []byte, opts ...GetRegionOption) (*Region, error)
	// GetRegionFromMember gets a region from certain members.
	GetRegionFromMember(ctx context.Context, key []byte, memberURLs []string, opts ...GetRegionOption) (*Region, error)
	// GetPrevRegion gets the previous region and its leader Peer of the region where the key is located.
	GetPrevRegion(ctx context.Context, key []byte, opts ...GetRegionOption) (*Region, error)
	// GetRegionByID gets a region and its leader Peer from PD by id.
	GetRegionByID(ctx context.Context, regionID uint64, opts ...GetRegionOption) (*Region, error)
	// ScanRegions gets a list of regions, starts from the region that contains key.
	// Limit limits the maximum number of regions returned.
	// If a region has no leader, corresponding leader will be placed by a peer
	// with empty value (PeerID is 0).
	ScanRegions(ctx context.Context, key, endKey []byte, limit int, opts ...GetRegionOption) ([]*Region, error)
	// GetStore gets a store from PD by store id.
	// The store may expire later. Caller is responsible for caching and taking care
	// of store change.
	GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error)
	// GetAllStores gets all stores from pd.
	// The store may expire later. Caller is responsible for caching and taking care
	// of store change.
	GetAllStores(ctx context.Context, opts ...GetStoreOption) ([]*metapb.Store, error)
	// UpdateGCSafePoint TiKV will check it and do GC themselves if necessary.
	// If the given safePoint is less than the current one, it will not be updated.
	// Returns the new safePoint after updating.
	UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error)
	// UpdateServiceGCSafePoint updates the safepoint for specific service and
	// returns the minimum safepoint across all services, this value is used to
	// determine the safepoint for multiple services, it does not trigger a GC
	// job. Use UpdateGCSafePoint to trigger the GC job if needed.
	UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error)
	// ScatterRegion scatters the specified region. Should use it for a batch of regions,
	// and the distribution of these regions will be dispersed.
	// NOTICE: This method is the old version of ScatterRegions, you should use the later one as your first choice.
	ScatterRegion(ctx context.Context, regionID uint64) error
	// ScatterRegions scatters the specified regions. Should use it for a batch of regions,
	// and the distribution of these regions will be dispersed.
	ScatterRegions(ctx context.Context, regionsID []uint64, opts ...RegionsOption) (*pdpb.ScatterRegionResponse, error)
	// SplitRegions split regions by given split keys
	SplitRegions(ctx context.Context, splitKeys [][]byte, opts ...RegionsOption) (*pdpb.SplitRegionsResponse, error)
	// SplitAndScatterRegions split regions by given split keys and scatter new regions
	SplitAndScatterRegions(ctx context.Context, splitKeys [][]byte, opts ...RegionsOption) (*pdpb.SplitAndScatterRegionsResponse, error)
	// GetOperator gets the status of operator of the specified region.
	GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error)

	// LoadGlobalConfig gets the global config from etcd
	LoadGlobalConfig(ctx context.Context, names []string, configPath string) ([]GlobalConfigItem, int64, error)
	// StoreGlobalConfig set the config from etcd
	StoreGlobalConfig(ctx context.Context, configPath string, items []GlobalConfigItem) error
	// WatchGlobalConfig returns a stream with all global config and updates
	WatchGlobalConfig(ctx context.Context, configPath string, revision int64) (chan []GlobalConfigItem, error)
	// UpdateOption updates the client option.
	UpdateOption(option DynamicOption, value interface{}) error

	// GetExternalTimestamp returns external timestamp
	GetExternalTimestamp(ctx context.Context) (uint64, error)
	// SetExternalTimestamp sets external timestamp
	SetExternalTimestamp(ctx context.Context, timestamp uint64) error

	// TSOClient is the TSO client.
	TSOClient
	// MetaStorageClient is the meta storage client.
	MetaStorageClient
	// KeyspaceClient manages keyspace metadata.
	KeyspaceClient
	// GCClient manages gcSafePointV2 and serviceSafePointV2
	GCClient
	// ResourceManagerClient manages resource group metadata and token assignment.
	ResourceManagerClient
	// Close closes the client.
	Close()
}

// GetStoreOp represents available options when getting stores.
type GetStoreOp struct {
	excludeTombstone bool
}

// GetStoreOption configures GetStoreOp.
type GetStoreOption func(*GetStoreOp)

// WithExcludeTombstone excludes tombstone stores from the result.
func WithExcludeTombstone() GetStoreOption {
	return func(op *GetStoreOp) { op.excludeTombstone = true }
}

// RegionsOp represents available options when operate regions
type RegionsOp struct {
	group          string
	retryLimit     uint64
	skipStoreLimit bool
}

// RegionsOption configures RegionsOp
type RegionsOption func(op *RegionsOp)

// WithGroup specify the group during Scatter/Split Regions
func WithGroup(group string) RegionsOption {
	return func(op *RegionsOp) { op.group = group }
}

// WithRetry specify the retry limit during Scatter/Split Regions
func WithRetry(retry uint64) RegionsOption {
	return func(op *RegionsOp) { op.retryLimit = retry }
}

// WithSkipStoreLimit specify if skip the store limit check during Scatter/Split Regions
func WithSkipStoreLimit() RegionsOption {
	return func(op *RegionsOp) { op.skipStoreLimit = true }
}

// GetRegionOp represents available options when getting regions.
type GetRegionOp struct {
	needBuckets         bool
	allowFollowerHandle bool
}

// GetRegionOption configures GetRegionOp.
type GetRegionOption func(op *GetRegionOp)

// WithBuckets means getting region and its buckets.
func WithBuckets() GetRegionOption {
	return func(op *GetRegionOp) { op.needBuckets = true }
}

// WithAllowFollowerHandle means that client can send request to follower and let it handle this request.
func WithAllowFollowerHandle() GetRegionOption {
	return func(op *GetRegionOp) { op.allowFollowerHandle = true }
}

// LeaderHealthCheckInterval might be changed in the unit to shorten the testing time.
var LeaderHealthCheckInterval = time.Second

var (
	// errUnmatchedClusterID is returned when found a PD with a different cluster ID.
	errUnmatchedClusterID = errors.New("[pd] unmatched cluster id")
	// errFailInitClusterID is returned when failed to load clusterID from all supplied PD addresses.
	errFailInitClusterID = errors.New("[pd] failed to get cluster id")
	// errClosing is returned when request is canceled when client is closing.
	errClosing = errors.New("[pd] closing")
	// errTSOLength is returned when the number of response timestamps is inconsistent with request.
	errTSOLength = errors.New("[pd] tso length in rpc response is incorrect")
	// errInvalidRespHeader is returned when the response doesn't contain service mode info unexpectedly.
	errNoServiceModeReturned = errors.New("[pd] no service mode returned")
)

// ClientOption configures client.
type ClientOption func(c *client)

// WithGRPCDialOptions configures the client with gRPC dial options.
func WithGRPCDialOptions(opts ...grpc.DialOption) ClientOption {
	return func(c *client) {
		c.option.gRPCDialOptions = append(c.option.gRPCDialOptions, opts...)
	}
}

// WithCustomTimeoutOption configures the client with timeout option.
func WithCustomTimeoutOption(timeout time.Duration) ClientOption {
	return func(c *client) {
		c.option.timeout = timeout
	}
}

// WithForwardingOption configures the client with forwarding option.
func WithForwardingOption(enableForwarding bool) ClientOption {
	return func(c *client) {
		c.option.enableForwarding = enableForwarding
	}
}

// WithMaxErrorRetry configures the client max retry times when connect meets error.
func WithMaxErrorRetry(count int) ClientOption {
	return func(c *client) {
		c.option.maxRetryTimes = count
	}
}

// WithMetricsLabels configures the client with metrics labels.
func WithMetricsLabels(labels prometheus.Labels) ClientOption {
	return func(c *client) {
		c.option.metricsLabels = labels
	}
}

// WithInitMetricsOption configures the client with metrics labels.
func WithInitMetricsOption(initMetrics bool) ClientOption {
	return func(c *client) {
		c.option.initMetrics = initMetrics
	}
}

var _ Client = (*client)(nil)

// serviceModeKeeper is for service mode switching.
type serviceModeKeeper struct {
	// RMutex here is for the future usage that there might be multiple goroutines
	// triggering service mode switching concurrently.
	sync.RWMutex
	serviceMode     pdpb.ServiceMode
	tsoClient       *tsoClient
	tsoSvcDiscovery ServiceDiscovery
}

func (k *serviceModeKeeper) close() {
	k.Lock()
	defer k.Unlock()
	switch k.serviceMode {
	case pdpb.ServiceMode_API_SVC_MODE:
		k.tsoSvcDiscovery.Close()
		fallthrough
	case pdpb.ServiceMode_PD_SVC_MODE:
		if k.tsoClient != nil {
			k.tsoClient.Close()
		}
	case pdpb.ServiceMode_UNKNOWN_SVC_MODE:
	}
}

type client struct {
	keyspaceID      uint32
	svrUrls         []string
	pdSvcDiscovery  ServiceDiscovery
	tokenDispatcher *tokenDispatcher

	// For service mode switching.
	serviceModeKeeper

	// For internal usage.
	updateTokenConnectionCh chan struct{}
	leaderNetworkFailure    int32

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	tlsCfg *tlsutil.TLSConfig
	option *option
}

// SecurityOption records options about tls
type SecurityOption struct {
	CAPath   string
	CertPath string
	KeyPath  string

	SSLCABytes   []byte
	SSLCertBytes []byte
	SSLKEYBytes  []byte
}

// NewClient creates a PD client.
func NewClient(
	svrAddrs []string, security SecurityOption, opts ...ClientOption,
) (Client, error) {
	return NewClientWithContext(context.Background(), svrAddrs, security, opts...)
}

// NewClientWithContext creates a PD client with context. This API uses the default keyspace id 0.
func NewClientWithContext(
	ctx context.Context, svrAddrs []string,
	security SecurityOption, opts ...ClientOption,
) (Client, error) {
	return createClientWithKeyspace(ctx, nullKeyspaceID, svrAddrs, security, opts...)
}

// NewClientWithKeyspace creates a client with context and the specified keyspace id.
// And now, it's only for test purpose.
func NewClientWithKeyspace(
	ctx context.Context, keyspaceID uint32, svrAddrs []string,
	security SecurityOption, opts ...ClientOption,
) (Client, error) {
	if keyspaceID < defaultKeyspaceID || keyspaceID > maxKeyspaceID {
		return nil, errors.Errorf("invalid keyspace id %d. It must be in the range of [%d, %d]",
			keyspaceID, defaultKeyspaceID, maxKeyspaceID)
	}
	return createClientWithKeyspace(ctx, keyspaceID, svrAddrs, security, opts...)
}

// createClientWithKeyspace creates a client with context and the specified keyspace id.
func createClientWithKeyspace(
	ctx context.Context, keyspaceID uint32, svrAddrs []string,
	security SecurityOption, opts ...ClientOption,
) (Client, error) {
	tlsCfg := &tlsutil.TLSConfig{
		CAPath:   security.CAPath,
		CertPath: security.CertPath,
		KeyPath:  security.KeyPath,

		SSLCABytes:   security.SSLCABytes,
		SSLCertBytes: security.SSLCertBytes,
		SSLKEYBytes:  security.SSLKEYBytes,
	}

	clientCtx, clientCancel := context.WithCancel(ctx)
	c := &client{
		updateTokenConnectionCh: make(chan struct{}, 1),
		ctx:                     clientCtx,
		cancel:                  clientCancel,
		keyspaceID:              keyspaceID,
		svrUrls:                 addrsToUrls(svrAddrs),
		tlsCfg:                  tlsCfg,
		option:                  newOption(),
	}

	// Inject the client options.
	for _, opt := range opts {
		opt(c)
	}

	c.pdSvcDiscovery = newPDServiceDiscovery(
		clientCtx, clientCancel, &c.wg, c.setServiceMode,
		nil, keyspaceID, c.svrUrls, c.tlsCfg, c.option)
	if err := c.setup(); err != nil {
		c.cancel()
		return nil, err
	}

	return c, nil
}

// APIVersion is the API version the server and the client is using.
// See more details in https://github.com/tikv/rfcs/blob/master/text/0069-api-v2.md#kvproto
type APIVersion int

// The API versions the client supports.
// As for V1TTL, client won't use it and we just remove it.
const (
	V1 APIVersion = iota
	_
	V2
)

// APIContext is the context for API version.
type APIContext interface {
	GetAPIVersion() (apiVersion APIVersion)
	GetKeyspaceName() (keyspaceName string)
}

type apiContextV1 struct{}

// NewAPIContextV1 creates a API context for V1.
func NewAPIContextV1() APIContext {
	return &apiContextV1{}
}

// GetAPIVersion returns the API version.
func (apiCtx *apiContextV1) GetAPIVersion() (version APIVersion) {
	return V1
}

// GetKeyspaceName returns the keyspace name.
func (apiCtx *apiContextV1) GetKeyspaceName() (keyspaceName string) {
	return ""
}

type apiContextV2 struct {
	keyspaceName string
}

// NewAPIContextV2 creates a API context with the specified keyspace name for V2.
func NewAPIContextV2(keyspaceName string) APIContext {
	if len(keyspaceName) == 0 {
		keyspaceName = defaultKeyspaceName
	}
	return &apiContextV2{keyspaceName: keyspaceName}
}

// GetAPIVersion returns the API version.
func (apiCtx *apiContextV2) GetAPIVersion() (version APIVersion) {
	return V2
}

// GetKeyspaceName returns the keyspace name.
func (apiCtx *apiContextV2) GetKeyspaceName() (keyspaceName string) {
	return apiCtx.keyspaceName
}

// NewClientWithAPIContext creates a client according to the API context.
func NewClientWithAPIContext(
	ctx context.Context, apiCtx APIContext, svrAddrs []string,
	security SecurityOption, opts ...ClientOption,
) (Client, error) {
	apiVersion, keyspaceName := apiCtx.GetAPIVersion(), apiCtx.GetKeyspaceName()
	switch apiVersion {
	case V1:
		return NewClientWithContext(ctx, svrAddrs, security, opts...)
	case V2:
		return newClientWithKeyspaceName(ctx, keyspaceName, svrAddrs, security, opts...)
	default:
		return nil, errors.Errorf("[pd] invalid API version %d", apiVersion)
	}
}

// newClientWithKeyspaceName creates a client with context and the specified keyspace name.
func newClientWithKeyspaceName(
	ctx context.Context, keyspaceName string, svrAddrs []string,
	security SecurityOption, opts ...ClientOption,
) (Client, error) {
	tlsCfg := &tlsutil.TLSConfig{
		CAPath:   security.CAPath,
		CertPath: security.CertPath,
		KeyPath:  security.KeyPath,

		SSLCABytes:   security.SSLCABytes,
		SSLCertBytes: security.SSLCertBytes,
		SSLKEYBytes:  security.SSLKEYBytes,
	}

	clientCtx, clientCancel := context.WithCancel(ctx)
	c := &client{
		updateTokenConnectionCh: make(chan struct{}, 1),
		ctx:                     clientCtx,
		cancel:                  clientCancel,
		svrUrls:                 addrsToUrls(svrAddrs),
		tlsCfg:                  tlsCfg,
		option:                  newOption(),
	}

	// Inject the client options.
	for _, opt := range opts {
		opt(c)
	}

	updateKeyspaceIDCb := func() error {
		if err := c.initRetry(c.loadKeyspaceMeta, keyspaceName); err != nil {
			return err
		}
		// c.keyspaceID is the source of truth for keyspace id.
		c.pdSvcDiscovery.(*pdServiceDiscovery).SetKeyspaceID(c.keyspaceID)
		return nil
	}

	// Create a PD service discovery with null keyspace id, then query the real id wth the keyspace name,
	// finally update the keyspace id to the PD service discovery for the following interactions.
	c.pdSvcDiscovery = newPDServiceDiscovery(
		clientCtx, clientCancel, &c.wg, c.setServiceMode, updateKeyspaceIDCb, nullKeyspaceID, c.svrUrls, c.tlsCfg, c.option)
	if err := c.setup(); err != nil {
		c.cancel()
		return nil, err
	}
	log.Info("[pd] create pd client with endpoints and keyspace",
		zap.Strings("pd-address", svrAddrs),
		zap.String("keyspace-name", keyspaceName),
		zap.Uint32("keyspace-id", c.keyspaceID))
	return c, nil
}

func (c *client) initRetry(f func(s string) error, str string) error {
	var err error
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for i := 0; i < c.option.maxRetryTimes; i++ {
		if err = f(str); err == nil {
			return nil
		}
		select {
		case <-c.ctx.Done():
			return err
		case <-ticker.C:
		}
	}
	return errors.WithStack(err)
}

func (c *client) loadKeyspaceMeta(keyspace string) error {
	keyspaceMeta, err := c.LoadKeyspace(context.TODO(), keyspace)
	if err != nil {
		return err
	}
	c.keyspaceID = keyspaceMeta.GetId()
	return nil
}

func (c *client) setup() error {
	// Init the metrics.
	if c.option.initMetrics {
		initAndRegisterMetrics(c.option.metricsLabels)
	}

	// Init the client base.
	if err := c.pdSvcDiscovery.Init(); err != nil {
		return err
	}

	// Register callbacks
	c.pdSvcDiscovery.AddServingAddrSwitchedCallback(c.scheduleUpdateTokenConnection)

	// Create dispatchers
	c.createTokenDispatcher()

	// Start the daemons.
	c.wg.Add(1)
	go c.leaderCheckLoop()
	return nil
}

func (c *client) Close() {
	c.cancel()
	c.wg.Wait()

	c.serviceModeKeeper.close()
	c.pdSvcDiscovery.Close()

	if c.tokenDispatcher != nil {
		tokenErr := errors.WithStack(errClosing)
		c.tokenDispatcher.tokenBatchController.revokePendingTokenRequest(tokenErr)
		c.tokenDispatcher.dispatcherCancel()
	}
}

func (c *client) setServiceMode(newMode pdpb.ServiceMode) {
	c.Lock()
	defer c.Unlock()

	if newMode == c.serviceMode {
		return
	}
	log.Info("[pd] changing service mode",
		zap.String("old-mode", c.serviceMode.String()),
		zap.String("new-mode", newMode.String()))
	// Re-create a new TSO client.
	var (
		newTSOCli          *tsoClient
		newTSOSvcDiscovery ServiceDiscovery
	)
	switch newMode {
	case pdpb.ServiceMode_PD_SVC_MODE:
		newTSOCli = newTSOClient(c.ctx, c.option,
			c.pdSvcDiscovery, &pdTSOStreamBuilderFactory{})
	case pdpb.ServiceMode_API_SVC_MODE:
		newTSOSvcDiscovery = newTSOServiceDiscovery(
			c.ctx, MetaStorageClient(c), c.pdSvcDiscovery,
			c.GetClusterID(c.ctx), c.keyspaceID, c.tlsCfg, c.option)
		// At this point, the keyspace group isn't known yet. Starts from the default keyspace group,
		// and will be updated later.
		newTSOCli = newTSOClient(c.ctx, c.option,
			newTSOSvcDiscovery, &tsoTSOStreamBuilderFactory{})
		if err := newTSOSvcDiscovery.Init(); err != nil {
			log.Error("[pd] failed to initialize tso service discovery. keep the current service mode",
				zap.Strings("svr-urls", c.svrUrls),
				zap.String("current-mode", c.serviceMode.String()),
				zap.Error(err))
			return
		}
	case pdpb.ServiceMode_UNKNOWN_SVC_MODE:
		log.Warn("[pd] intend to switch to unknown service mode, just return")
		return
	}
	newTSOCli.Setup()
	// Replace the old TSO client.
	oldTSOClient := c.tsoClient
	c.tsoClient = newTSOCli
	oldTSOClient.Close()
	// Replace the old TSO service discovery if needed.
	oldTSOSvcDiscovery := c.tsoSvcDiscovery
	// If newTSOSvcDiscovery is nil, that's expected, as it means we are switching to PD service mode and
	// no tso microservice discovery is needed.
	c.tsoSvcDiscovery = newTSOSvcDiscovery
	// Close the old TSO service discovery safely after both the old client and service discovery are replaced.
	if oldTSOSvcDiscovery != nil {
		// We are switching from API service mode to PD service mode, so delete the old tso microservice discovery.
		oldTSOSvcDiscovery.Close()
	}
	oldMode := c.serviceMode
	c.serviceMode = newMode
	log.Info("[pd] service mode changed",
		zap.String("old-mode", oldMode.String()),
		zap.String("new-mode", newMode.String()))
}

func (c *client) getTSOClient() *tsoClient {
	c.RLock()
	defer c.RUnlock()
	return c.tsoClient
}

func (c *client) getServiceMode() pdpb.ServiceMode {
	c.RLock()
	defer c.RUnlock()
	return c.serviceMode
}

func (c *client) scheduleUpdateTokenConnection() {
	select {
	case c.updateTokenConnectionCh <- struct{}{}:
	default:
	}
}

// GetClusterID returns the ClusterID.
func (c *client) GetClusterID(context.Context) uint64 {
	return c.pdSvcDiscovery.GetClusterID()
}

// GetLeaderAddr returns the leader address.
func (c *client) GetLeaderAddr() string {
	return c.pdSvcDiscovery.GetServingAddr()
}

// GetServiceDiscovery returns the client-side service discovery object
func (c *client) GetServiceDiscovery() ServiceDiscovery {
	return c.pdSvcDiscovery
}

// UpdateOption updates the client option.
func (c *client) UpdateOption(option DynamicOption, value interface{}) error {
	switch option {
	case MaxTSOBatchWaitInterval:
		interval, ok := value.(time.Duration)
		if !ok {
			return errors.New("[pd] invalid value type for MaxTSOBatchWaitInterval option, it should be time.Duration")
		}
		if err := c.option.setMaxTSOBatchWaitInterval(interval); err != nil {
			return err
		}
	case EnableTSOFollowerProxy:
		enable, ok := value.(bool)
		if !ok {
			return errors.New("[pd] invalid value type for EnableTSOFollowerProxy option, it should be bool")
		}
		c.option.setEnableTSOFollowerProxy(enable)
	case EnableFollowerHandle:
		enable, ok := value.(bool)
		if !ok {
			return errors.New("[pd] invalid value type for EnableFollowerHandle option, it should be bool")
		}
		c.option.setEnableFollowerHandle(enable)
	default:
		return errors.New("[pd] unsupported client option")
	}
	return nil
}

func (c *client) leaderCheckLoop() {
	defer c.wg.Done()

	leaderCheckLoopCtx, leaderCheckLoopCancel := context.WithCancel(c.ctx)
	defer leaderCheckLoopCancel()

	ticker := time.NewTicker(LeaderHealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.checkLeaderHealth(leaderCheckLoopCtx)
		}
	}
}

func (c *client) checkLeaderHealth(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	defer cancel()
	if client := c.pdSvcDiscovery.GetServingEndpointClientConn(); client != nil {
		healthCli := healthpb.NewHealthClient(client)
		resp, err := healthCli.Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
		failpoint.Inject("unreachableNetwork1", func() {
			resp = nil
			err = status.New(codes.Unavailable, "unavailable").Err()
		})
		rpcErr, ok := status.FromError(err)
		if (ok && isNetworkError(rpcErr.Code())) || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			atomic.StoreInt32(&(c.leaderNetworkFailure), int32(1))
		} else {
			atomic.StoreInt32(&(c.leaderNetworkFailure), int32(0))
		}
	} else {
		atomic.StoreInt32(&(c.leaderNetworkFailure), int32(1))
	}
}

func (c *client) GetAllMembers(ctx context.Context) ([]*pdpb.Member, error) {
	start := time.Now()
	defer func() { cmdDurationGetAllMembers.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &pdpb.GetMembersRequest{Header: c.requestHeader()}
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		cancel()
		return nil, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.GetMembers(ctx, req)
	cancel()
	if err = c.respForErr(cmdFailDurationGetAllMembers, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}
	return resp.GetMembers(), nil
}

// leaderClient gets the client of current PD leader.
func (c *client) leaderClient() pdpb.PDClient {
	if client := c.pdSvcDiscovery.GetServingEndpointClientConn(); client != nil {
		return pdpb.NewPDClient(client)
	}
	return nil
}

// backupClientConn gets a grpc client connection of the current reachable and healthy
// backup service endpoints randomly. Backup service endpoints are followers in a
// quorum-based cluster or secondaries in a primary/secondary configured cluster.
func (c *client) backupClientConn() (*grpc.ClientConn, string) {
	addrs := c.pdSvcDiscovery.GetBackupAddrs()
	if len(addrs) < 1 {
		return nil, ""
	}
	var (
		cc  *grpc.ClientConn
		err error
	)
	for i := 0; i < len(addrs); i++ {
		addr := addrs[rand.Intn(len(addrs))]
		if cc, err = c.pdSvcDiscovery.GetOrCreateGRPCConn(addr); err != nil {
			continue
		}
		healthCtx, healthCancel := context.WithTimeout(c.ctx, c.option.timeout)
		resp, err := healthpb.NewHealthClient(cc).Check(healthCtx, &healthpb.HealthCheckRequest{Service: ""})
		healthCancel()
		if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			return cc, addr
		}
	}
	return nil, ""
}

func (c *client) getClientAndContext(ctx context.Context) (pdpb.PDClient, context.Context) {
	if c.option.enableForwarding && atomic.LoadInt32(&c.leaderNetworkFailure) == 1 {
		backupClientConn, addr := c.backupClientConn()
		if backupClientConn != nil {
			log.Debug("[pd] use follower client", zap.String("addr", addr))
			return pdpb.NewPDClient(backupClientConn), grpcutil.BuildForwardContext(ctx, c.GetLeaderAddr())
		}
	}
	return c.leaderClient(), ctx
}

func (c *client) GetTSAsync(ctx context.Context) TSFuture {
	return c.GetLocalTSAsync(ctx, globalDCLocation)
}

func (c *client) GetLocalTSAsync(ctx context.Context, dcLocation string) TSFuture {
	defer trace.StartRegion(ctx, "GetLocalTSAsync").End()
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("GetLocalTSAsync", opentracing.ChildOf(span.Context()))
		ctx = opentracing.ContextWithSpan(ctx, span)
	}

	req := tsoReqPool.Get().(*tsoRequest)
	req.requestCtx = ctx
	req.clientCtx = c.ctx
	tsoClient := c.getTSOClient()
	req.start = time.Now()
	req.dcLocation = dcLocation

	if tsoClient == nil {
		req.done <- errs.ErrClientGetTSO.FastGenByArgs("tso client is nil")
		return req
	}

	if err := tsoClient.dispatchRequest(dcLocation, req); err != nil {
		// Wait for a while and try again
		time.Sleep(50 * time.Millisecond)
		if err = tsoClient.dispatchRequest(dcLocation, req); err != nil {
			req.done <- err
		}
	}
	return req
}

func (c *client) GetTS(ctx context.Context) (physical int64, logical int64, err error) {
	resp := c.GetTSAsync(ctx)
	return resp.Wait()
}

func (c *client) GetLocalTS(ctx context.Context, dcLocation string) (physical int64, logical int64, err error) {
	resp := c.GetLocalTSAsync(ctx, dcLocation)
	return resp.Wait()
}

func (c *client) GetMinTS(ctx context.Context) (physical int64, logical int64, err error) {
	// Handle compatibility issue in case of PD/API server doesn't support GetMinTS API.
	serviceMode := c.getServiceMode()
	switch serviceMode {
	case pdpb.ServiceMode_UNKNOWN_SVC_MODE:
		return 0, 0, errs.ErrClientGetMinTSO.FastGenByArgs("unknown service mode")
	case pdpb.ServiceMode_PD_SVC_MODE:
		// If the service mode is switched to API during GetTS() call, which happens during migration,
		// returning the default timeline should be fine.
		return c.GetTS(ctx)
	case pdpb.ServiceMode_API_SVC_MODE:
	default:
		return 0, 0, errs.ErrClientGetMinTSO.FastGenByArgs("undefined service mode")
	}
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	// Call GetMinTS API to get the minimal TS from the API leader.
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		cancel()
		return 0, 0, errs.ErrClientGetProtoClient
	}

	resp, err := protoClient.GetMinTS(ctx, &pdpb.GetMinTSRequest{
		Header: c.requestHeader(),
	})
	cancel()
	if err != nil {
		if strings.Contains(err.Error(), "Unimplemented") {
			// If the method is not supported, we fallback to GetTS.
			return c.GetTS(ctx)
		}
		return 0, 0, errs.ErrClientGetMinTSO.Wrap(err).GenWithStackByCause()
	}
	if resp == nil {
		attachErr := errors.Errorf("error:%s", "no min ts info collected")
		return 0, 0, errs.ErrClientGetMinTSO.Wrap(attachErr).GenWithStackByCause()
	}
	if resp.GetHeader().GetError() != nil {
		attachErr := errors.Errorf("error:%s s", resp.GetHeader().GetError().String())
		return 0, 0, errs.ErrClientGetMinTSO.Wrap(attachErr).GenWithStackByCause()
	}

	minTS := resp.GetTimestamp()
	return minTS.Physical, tsoutil.AddLogical(minTS.Logical, 0, minTS.SuffixBits), nil
}

func handleRegionResponse(res *pdpb.GetRegionResponse) *Region {
	if res.Region == nil {
		return nil
	}

	r := &Region{
		Meta:         res.Region,
		Leader:       res.Leader,
		PendingPeers: res.PendingPeers,
		Buckets:      res.Buckets,
	}
	for _, s := range res.DownPeers {
		r.DownPeers = append(r.DownPeers, s.Peer)
	}
	return r
}

func isNetworkError(code codes.Code) bool {
	return code == codes.Unavailable || code == codes.DeadlineExceeded
}

func (c *client) GetRegionFromMember(ctx context.Context, key []byte, memberURLs []string, opts ...GetRegionOption) (*Region, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.GetRegionFromMember", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationGetRegion.Observe(time.Since(start).Seconds()) }()

	var resp *pdpb.GetRegionResponse
	for _, url := range memberURLs {
		conn, err := c.pdSvcDiscovery.GetOrCreateGRPCConn(url)
		if err != nil {
			log.Error("[pd] can't get grpc connection", zap.String("member-URL", url), errs.ZapError(err))
			continue
		}
		cc := pdpb.NewPDClient(conn)
		resp, err = cc.GetRegion(ctx, &pdpb.GetRegionRequest{
			Header:    c.requestHeader(),
			RegionKey: key,
		})
		if err != nil || resp.GetHeader().GetError() != nil {
			log.Error("[pd] can't get region info", zap.String("member-URL", url), errs.ZapError(err))
			continue
		}
		if resp != nil {
			break
		}
	}

	if resp == nil {
		cmdFailDurationGetRegion.Observe(time.Since(start).Seconds())
		c.pdSvcDiscovery.ScheduleCheckMemberChanged()
		errorMsg := fmt.Sprintf("[pd] can't get region info from member URLs: %+v", memberURLs)
		return nil, errors.WithStack(errors.New(errorMsg))
	}
	return handleRegionResponse(resp), nil
}

func (c *client) GetRegion(ctx context.Context, key []byte, opts ...GetRegionOption) (*Region, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.GetRegion", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationGetRegion.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)

	options := &GetRegionOp{}
	for _, opt := range opts {
		opt(options)
	}
	req := &pdpb.GetRegionRequest{
		Header:      c.requestHeader(),
		RegionKey:   key,
		NeedBuckets: options.needBuckets,
	}
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		cancel()
		return nil, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.GetRegion(ctx, req)
	cancel()

	if err = c.respForErr(cmdFailDurationGetRegion, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}
	return handleRegionResponse(resp), nil
}

func (c *client) GetPrevRegion(ctx context.Context, key []byte, opts ...GetRegionOption) (*Region, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.GetPrevRegion", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationGetPrevRegion.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)

	options := &GetRegionOp{}
	for _, opt := range opts {
		opt(options)
	}
	req := &pdpb.GetRegionRequest{
		Header:      c.requestHeader(),
		RegionKey:   key,
		NeedBuckets: options.needBuckets,
	}
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		cancel()
		return nil, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.GetPrevRegion(ctx, req)
	cancel()

	if err = c.respForErr(cmdFailDurationGetPrevRegion, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}
	return handleRegionResponse(resp), nil
}

func (c *client) GetRegionByID(ctx context.Context, regionID uint64, opts ...GetRegionOption) (*Region, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.GetRegionByID", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationGetRegionByID.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)

	options := &GetRegionOp{}
	for _, opt := range opts {
		opt(options)
	}
	req := &pdpb.GetRegionByIDRequest{
		Header:      c.requestHeader(),
		RegionId:    regionID,
		NeedBuckets: options.needBuckets,
	}
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		cancel()
		return nil, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.GetRegionByID(ctx, req)
	cancel()

	if err = c.respForErr(cmdFailedDurationGetRegionByID, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}
	return handleRegionResponse(resp), nil
}

func (c *client) ScanRegions(ctx context.Context, key, endKey []byte, limit int, opts ...GetRegionOption) ([]*Region, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.ScanRegions", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationScanRegions.Observe(time.Since(start).Seconds()) }()

	var cancel context.CancelFunc
	scanCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		scanCtx, cancel = context.WithTimeout(ctx, c.option.timeout)
		defer cancel()
	}
	req := &pdpb.ScanRegionsRequest{
		Header:   c.requestHeader(),
		StartKey: key,
		EndKey:   endKey,
		Limit:    int32(limit),
	}
	protoClient, scanCtx := c.getClientAndContext(scanCtx)
	if protoClient == nil {
		cancel()
		return nil, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.ScanRegions(scanCtx, req)

	if err = c.respForErr(cmdFailedDurationScanRegions, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}

	return handleRegionsResponse(resp), nil
}

func handleRegionsResponse(resp *pdpb.ScanRegionsResponse) []*Region {
	var regions []*Region
	if len(resp.GetRegions()) == 0 {
		// Make it compatible with old server.
		metas, leaders := resp.GetRegionMetas(), resp.GetLeaders()
		for i := range metas {
			r := &Region{Meta: metas[i]}
			if i < len(leaders) {
				r.Leader = leaders[i]
			}
			regions = append(regions, r)
		}
	} else {
		for _, r := range resp.GetRegions() {
			region := &Region{
				Meta:         r.Region,
				Leader:       r.Leader,
				PendingPeers: r.PendingPeers,
			}
			for _, p := range r.DownPeers {
				region.DownPeers = append(region.DownPeers, p.Peer)
			}
			regions = append(regions, region)
		}
	}
	return regions
}

func (c *client) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.GetStore", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationGetStore.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &pdpb.GetStoreRequest{
		Header:  c.requestHeader(),
		StoreId: storeID,
	}
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		cancel()
		return nil, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.GetStore(ctx, req)
	cancel()

	if err = c.respForErr(cmdFailedDurationGetStore, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}
	return handleStoreResponse(resp)
}

func handleStoreResponse(resp *pdpb.GetStoreResponse) (*metapb.Store, error) {
	store := resp.GetStore()
	if store == nil {
		return nil, errors.New("[pd] store field in rpc response not set")
	}
	if store.GetNodeState() == metapb.NodeState_Removed {
		return nil, nil
	}
	return store, nil
}

func (c *client) GetAllStores(ctx context.Context, opts ...GetStoreOption) ([]*metapb.Store, error) {
	// Applies options
	options := &GetStoreOp{}
	for _, opt := range opts {
		opt(options)
	}

	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.GetAllStores", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationGetAllStores.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &pdpb.GetAllStoresRequest{
		Header:                 c.requestHeader(),
		ExcludeTombstoneStores: options.excludeTombstone,
	}
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		cancel()
		return nil, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.GetAllStores(ctx, req)
	cancel()

	if err = c.respForErr(cmdFailedDurationGetAllStores, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}
	return resp.GetStores(), nil
}

func (c *client) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.UpdateGCSafePoint", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationUpdateGCSafePoint.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &pdpb.UpdateGCSafePointRequest{
		Header:    c.requestHeader(),
		SafePoint: safePoint,
	}
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		cancel()
		return 0, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.UpdateGCSafePoint(ctx, req)
	cancel()

	if err = c.respForErr(cmdFailedDurationUpdateGCSafePoint, start, err, resp.GetHeader()); err != nil {
		return 0, err
	}
	return resp.GetNewSafePoint(), nil
}

// UpdateServiceGCSafePoint updates the safepoint for specific service and
// returns the minimum safepoint across all services, this value is used to
// determine the safepoint for multiple services, it does not trigger a GC
// job. Use UpdateGCSafePoint to trigger the GC job if needed.
func (c *client) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.UpdateServiceGCSafePoint", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}

	start := time.Now()
	defer func() { cmdDurationUpdateServiceGCSafePoint.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &pdpb.UpdateServiceGCSafePointRequest{
		Header:    c.requestHeader(),
		ServiceId: []byte(serviceID),
		TTL:       ttl,
		SafePoint: safePoint,
	}
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		cancel()
		return 0, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.UpdateServiceGCSafePoint(ctx, req)
	cancel()

	if err = c.respForErr(cmdFailedDurationUpdateServiceGCSafePoint, start, err, resp.GetHeader()); err != nil {
		return 0, err
	}
	return resp.GetMinSafePoint(), nil
}

func (c *client) ScatterRegion(ctx context.Context, regionID uint64) error {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.ScatterRegion", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	return c.scatterRegionsWithGroup(ctx, regionID, "")
}

func (c *client) scatterRegionsWithGroup(ctx context.Context, regionID uint64, group string) error {
	start := time.Now()
	defer func() { cmdDurationScatterRegion.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &pdpb.ScatterRegionRequest{
		Header:   c.requestHeader(),
		RegionId: regionID,
		Group:    group,
	}
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		cancel()
		return errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.ScatterRegion(ctx, req)
	cancel()
	if err != nil {
		return err
	}
	if resp.Header.GetError() != nil {
		return errors.Errorf("scatter region %d failed: %s", regionID, resp.Header.GetError().String())
	}
	return nil
}

func (c *client) ScatterRegions(ctx context.Context, regionsID []uint64, opts ...RegionsOption) (*pdpb.ScatterRegionResponse, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.ScatterRegions", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	return c.scatterRegionsWithOptions(ctx, regionsID, opts...)
}

func (c *client) SplitAndScatterRegions(ctx context.Context, splitKeys [][]byte, opts ...RegionsOption) (*pdpb.SplitAndScatterRegionsResponse, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.SplitAndScatterRegions", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationSplitAndScatterRegions.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	defer cancel()
	options := &RegionsOp{}
	for _, opt := range opts {
		opt(options)
	}
	req := &pdpb.SplitAndScatterRegionsRequest{
		Header:     c.requestHeader(),
		SplitKeys:  splitKeys,
		Group:      options.group,
		RetryLimit: options.retryLimit,
	}

	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		cancel()
		return nil, errs.ErrClientGetProtoClient
	}
	return protoClient.SplitAndScatterRegions(ctx, req)
}

func (c *client) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.GetOperator", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationGetOperator.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	defer cancel()
	req := &pdpb.GetOperatorRequest{
		Header:   c.requestHeader(),
		RegionId: regionID,
	}
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		cancel()
		return nil, errs.ErrClientGetProtoClient
	}
	return protoClient.GetOperator(ctx, req)
}

// SplitRegions split regions by given split keys
func (c *client) SplitRegions(ctx context.Context, splitKeys [][]byte, opts ...RegionsOption) (*pdpb.SplitRegionsResponse, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		span = opentracing.StartSpan("pdclient.SplitRegions", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { cmdDurationSplitRegions.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	defer cancel()
	options := &RegionsOp{}
	for _, opt := range opts {
		opt(options)
	}
	req := &pdpb.SplitRegionsRequest{
		Header:     c.requestHeader(),
		SplitKeys:  splitKeys,
		RetryLimit: options.retryLimit,
	}
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		cancel()
		return nil, errs.ErrClientGetProtoClient
	}
	return protoClient.SplitRegions(ctx, req)
}

func (c *client) requestHeader() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: c.pdSvcDiscovery.GetClusterID(),
	}
}

func (c *client) scatterRegionsWithOptions(ctx context.Context, regionsID []uint64, opts ...RegionsOption) (*pdpb.ScatterRegionResponse, error) {
	start := time.Now()
	defer func() { cmdDurationScatterRegions.Observe(time.Since(start).Seconds()) }()
	options := &RegionsOp{}
	for _, opt := range opts {
		opt(options)
	}
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	req := &pdpb.ScatterRegionRequest{
		Header:         c.requestHeader(),
		Group:          options.group,
		RegionsId:      regionsID,
		RetryLimit:     options.retryLimit,
		SkipStoreLimit: options.skipStoreLimit,
	}

	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		cancel()
		return nil, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.ScatterRegion(ctx, req)
	cancel()

	if err != nil {
		return nil, err
	}
	if resp.Header.GetError() != nil {
		return nil, errors.Errorf("scatter regions %v failed: %s", regionsID, resp.Header.GetError().String())
	}
	return resp, nil
}

func addrsToUrls(addrs []string) []string {
	// Add default schema "http://" to addrs.
	urls := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if strings.Contains(addr, "://") {
			urls = append(urls, addr)
		} else {
			urls = append(urls, "http://"+addr)
		}
	}
	return urls
}

// IsLeaderChange will determine whether there is a leader change.
func IsLeaderChange(err error) bool {
	if err == errs.ErrClientTSOStreamClosed {
		return true
	}
	errMsg := err.Error()
	return strings.Contains(errMsg, errs.NotLeaderErr) ||
		strings.Contains(errMsg, errs.MismatchLeaderErr) ||
		strings.Contains(errMsg, errs.NotServedErr)
}

func trimHTTPPrefix(str string) string {
	str = strings.TrimPrefix(str, "http://")
	str = strings.TrimPrefix(str, "https://")
	return str
}

func (c *client) LoadGlobalConfig(ctx context.Context, names []string, configPath string) ([]GlobalConfigItem, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	defer cancel()
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		return nil, 0, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.LoadGlobalConfig(ctx, &pdpb.LoadGlobalConfigRequest{Names: names, ConfigPath: configPath})
	if err != nil {
		return nil, 0, err
	}

	res := make([]GlobalConfigItem, len(resp.GetItems()))
	for i, item := range resp.GetItems() {
		cfg := GlobalConfigItem{Name: item.GetName(), EventType: item.GetKind(), PayLoad: item.GetPayload()}
		if item.GetValue() == "" {
			// We need to keep the Value field for CDC compatibility.
			// But if you not use `Names`, will only have `Payload` field.
			cfg.Value = string(item.GetPayload())
		} else {
			cfg.Value = item.GetValue()
		}
		res[i] = cfg
	}
	return res, resp.GetRevision(), nil
}

func (c *client) StoreGlobalConfig(ctx context.Context, configPath string, items []GlobalConfigItem) error {
	resArr := make([]*pdpb.GlobalConfigItem, len(items))
	for i, it := range items {
		resArr[i] = &pdpb.GlobalConfigItem{Name: it.Name, Value: it.Value, Kind: it.EventType, Payload: it.PayLoad}
	}
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	defer cancel()
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		return errs.ErrClientGetProtoClient
	}
	_, err := protoClient.StoreGlobalConfig(ctx, &pdpb.StoreGlobalConfigRequest{Changes: resArr, ConfigPath: configPath})
	if err != nil {
		return err
	}
	return nil
}

func (c *client) WatchGlobalConfig(ctx context.Context, configPath string, revision int64) (chan []GlobalConfigItem, error) {
	// TODO: Add retry mechanism
	// register watch components there
	globalConfigWatcherCh := make(chan []GlobalConfigItem, 16)
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	defer cancel()
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		return nil, errs.ErrClientGetProtoClient
	}
	res, err := protoClient.WatchGlobalConfig(ctx, &pdpb.WatchGlobalConfigRequest{
		ConfigPath: configPath,
		Revision:   revision,
	})
	if err != nil {
		close(globalConfigWatcherCh)
		return nil, err
	}
	go func() {
		defer func() {
			close(globalConfigWatcherCh)
			if r := recover(); r != nil {
				log.Error("[pd] panic in client `WatchGlobalConfig`", zap.Any("error", r))
				return
			}
		}()
		for {
			m, err := res.Recv()
			if err != nil {
				return
			}
			arr := make([]GlobalConfigItem, len(m.Changes))
			for j, i := range m.Changes {
				// We need to keep the Value field for CDC compatibility.
				// But if you not use `Names`, will only have `Payload` field.
				if i.GetValue() == "" {
					arr[j] = GlobalConfigItem{i.GetKind(), i.GetName(), string(i.GetPayload()), i.GetPayload()}
				} else {
					arr[j] = GlobalConfigItem{i.GetKind(), i.GetName(), i.GetValue(), i.GetPayload()}
				}
			}
			select {
			case <-ctx.Done():
				return
			case globalConfigWatcherCh <- arr:
			}
		}
	}()
	return globalConfigWatcherCh, err
}

func (c *client) GetExternalTimestamp(ctx context.Context) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	defer cancel()
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		return 0, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.GetExternalTimestamp(ctx, &pdpb.GetExternalTimestampRequest{
		Header: c.requestHeader(),
	})
	if err != nil {
		return 0, err
	}
	resErr := resp.GetHeader().GetError()
	if resErr != nil {
		return 0, errors.Errorf("[pd]" + resErr.Message)
	}
	return resp.GetTimestamp(), nil
}

func (c *client) SetExternalTimestamp(ctx context.Context, timestamp uint64) error {
	ctx, cancel := context.WithTimeout(ctx, c.option.timeout)
	defer cancel()
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		return errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.SetExternalTimestamp(ctx, &pdpb.SetExternalTimestampRequest{
		Header:    c.requestHeader(),
		Timestamp: timestamp,
	})
	if err != nil {
		return err
	}
	resErr := resp.GetHeader().GetError()
	if resErr != nil {
		return errors.Errorf("[pd]" + resErr.Message)
	}
	return nil
}

func (c *client) respForErr(observer prometheus.Observer, start time.Time, err error, header *pdpb.ResponseHeader) error {
	if err != nil || header.GetError() != nil {
		observer.Observe(time.Since(start).Seconds())
		if err != nil {
			c.pdSvcDiscovery.ScheduleCheckMemberChanged()
			return errors.WithStack(err)
		}
		return errors.WithStack(errors.New(header.GetError().String()))
	}
	return nil
}

// GetTSOAllocators returns {dc-location -> TSO allocator leader URL} connection map
// For test only.
func (c *client) GetTSOAllocators() *sync.Map {
	tsoClient := c.getTSOClient()
	if tsoClient == nil {
		return nil
	}
	return tsoClient.GetTSOAllocators()
}
