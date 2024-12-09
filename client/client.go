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
	"runtime/trace"
	"strings"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/client/clients/metastorage"
	"github.com/tikv/pd/client/clients/router"
	"github.com/tikv/pd/client/clients/tso"
	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/metrics"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/client/pkg/utils/tlsutil"
	sd "github.com/tikv/pd/client/servicediscovery"
	"go.uber.org/zap"
)

// GlobalConfigItem standard format of KV pair in GlobalConfig client
type GlobalConfigItem struct {
	EventType pdpb.EventType
	Name      string
	Value     string
	PayLoad   []byte
}

// RPCClient is a PD (Placement Driver) RPC and related mcs client which can only call RPC.
type RPCClient interface {
	// GetAllMembers gets the members Info from PD
	GetAllMembers(ctx context.Context) ([]*pdpb.Member, error)
	// GetStore gets a store from PD by store id.
	// The store may expire later. Caller is responsible for caching and taking care
	// of store change.
	GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error)
	// GetAllStores gets all stores from pd.
	// The store may expire later. Caller is responsible for caching and taking care
	// of store change.
	GetAllStores(ctx context.Context, opts ...opt.GetStoreOption) ([]*metapb.Store, error)
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
	ScatterRegions(ctx context.Context, regionsID []uint64, opts ...opt.RegionsOption) (*pdpb.ScatterRegionResponse, error)
	// SplitRegions split regions by given split keys
	SplitRegions(ctx context.Context, splitKeys [][]byte, opts ...opt.RegionsOption) (*pdpb.SplitRegionsResponse, error)
	// SplitAndScatterRegions split regions by given split keys and scatter new regions
	SplitAndScatterRegions(ctx context.Context, splitKeys [][]byte, opts ...opt.RegionsOption) (*pdpb.SplitAndScatterRegionsResponse, error)
	// GetOperator gets the status of operator of the specified region.
	GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error)

	// LoadGlobalConfig gets the global config from etcd
	LoadGlobalConfig(ctx context.Context, names []string, configPath string) ([]GlobalConfigItem, int64, error)
	// StoreGlobalConfig set the config from etcd
	StoreGlobalConfig(ctx context.Context, configPath string, items []GlobalConfigItem) error
	// WatchGlobalConfig returns a stream with all global config and updates
	WatchGlobalConfig(ctx context.Context, configPath string, revision int64) (chan []GlobalConfigItem, error)

	// GetExternalTimestamp returns external timestamp
	GetExternalTimestamp(ctx context.Context) (uint64, error)
	// SetExternalTimestamp sets external timestamp
	SetExternalTimestamp(ctx context.Context, timestamp uint64) error

	// WithCallerComponent returns a new RPCClient with the specified caller
	// component. Caller component refers to the specific part or module within
	// the process. You can set the component in two ways:
	//   * Define it manually, like `caller.Component("DDL")`.
	//   * Use the provided helper function, `caller.GetComponent(upperLayer)`.
	//     The upperLayer parameter specifies the depth of the caller stack,
	//     where 0 means the current function. Adjust the upperLayer value based
	//     on your needs.
	WithCallerComponent(callerComponent caller.Component) RPCClient

	router.Client
	tso.Client
	metastorage.Client
	// KeyspaceClient manages keyspace metadata.
	KeyspaceClient
	// GCClient manages gcSafePointV2 and serviceSafePointV2
	GCClient
	// ResourceManagerClient manages resource group metadata and token assignment.
	ResourceManagerClient
}

// Client is a PD (Placement Driver) RPC client.
// It should not be used after calling Close().
type Client interface {
	RPCClient

	// GetClusterID gets the cluster ID from PD.
	GetClusterID(ctx context.Context) uint64
	// GetLeaderURL returns current leader's URL. It returns "" before
	// syncing leader from server.
	GetLeaderURL() string
	// GetServiceDiscovery returns ServiceDiscovery
	GetServiceDiscovery() sd.ServiceDiscovery

	// UpdateOption updates the client option.
	UpdateOption(option opt.DynamicOption, value any) error

	// Close closes the client.
	Close()
}

var _ Client = (*client)(nil)

// serviceModeKeeper is for service mode switching.
type serviceModeKeeper struct {
	// RMutex here is for the future usage that there might be multiple goroutines
	// triggering service mode switching concurrently.
	sync.RWMutex
	serviceMode     pdpb.ServiceMode
	tsoClient       *tso.Cli
	tsoSvcDiscovery sd.ServiceDiscovery
}

func (k *serviceModeKeeper) close() {
	k.Lock()
	defer k.Unlock()
	switch k.serviceMode {
	case pdpb.ServiceMode_API_SVC_MODE:
		k.tsoSvcDiscovery.Close()
		fallthrough
	case pdpb.ServiceMode_PD_SVC_MODE:
		k.tsoClient.Close()
	case pdpb.ServiceMode_UNKNOWN_SVC_MODE:
	}
}

type client struct {
	// Caller component refers to the components within the process.
	callerComponent caller.Component

	inner *innerClient
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
	callerComponent caller.Component,
	svrAddrs []string, security SecurityOption, opts ...opt.ClientOption,
) (Client, error) {
	return NewClientWithContext(context.Background(), callerComponent,
		svrAddrs, security, opts...)
}

// NewClientWithContext creates a PD client with context. This API uses the default keyspace id 0.
func NewClientWithContext(
	ctx context.Context,
	callerComponent caller.Component,
	svrAddrs []string,
	security SecurityOption, opts ...opt.ClientOption,
) (Client, error) {
	return createClientWithKeyspace(ctx, callerComponent,
		constants.NullKeyspaceID, svrAddrs, security, opts...)
}

// NewClientWithKeyspace creates a client with context and the specified keyspace id.
// And now, it's only for test purpose.
func NewClientWithKeyspace(
	ctx context.Context,
	callerComponent caller.Component,
	keyspaceID uint32, svrAddrs []string,
	security SecurityOption, opts ...opt.ClientOption,
) (Client, error) {
	if keyspaceID < constants.DefaultKeyspaceID || keyspaceID > constants.MaxKeyspaceID {
		return nil, errors.Errorf("invalid keyspace id %d. It must be in the range of [%d, %d]",
			keyspaceID, constants.DefaultKeyspaceID, constants.MaxKeyspaceID)
	}
	return createClientWithKeyspace(ctx, callerComponent, keyspaceID,
		svrAddrs, security, opts...)
}

// createClientWithKeyspace creates a client with context and the specified keyspace id.
func createClientWithKeyspace(
	ctx context.Context,
	callerComponent caller.Component,
	keyspaceID uint32, svrAddrs []string,
	security SecurityOption, opts ...opt.ClientOption,
) (Client, error) {
	tlsCfg, err := tlsutil.TLSConfig{
		CAPath:   security.CAPath,
		CertPath: security.CertPath,
		KeyPath:  security.KeyPath,

		SSLCABytes:   security.SSLCABytes,
		SSLCertBytes: security.SSLCertBytes,
		SSLKEYBytes:  security.SSLKEYBytes,
	}.ToTLSConfig()
	if err != nil {
		return nil, err
	}

	clientCtx, clientCancel := context.WithCancel(ctx)
	c := &client{
		callerComponent: adjustCallerComponent(callerComponent),
		inner: &innerClient{
			keyspaceID:              keyspaceID,
			svrUrls:                 svrAddrs,
			updateTokenConnectionCh: make(chan struct{}, 1),
			ctx:                     clientCtx,
			cancel:                  clientCancel,
			tlsCfg:                  tlsCfg,
			option:                  opt.NewOption(),
		},
	}

	// Inject the client options.
	for _, opt := range opts {
		opt(c.inner.option)
	}

	return c, c.inner.init(nil)
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
func (*apiContextV1) GetAPIVersion() (version APIVersion) {
	return V1
}

// GetKeyspaceName returns the keyspace name.
func (*apiContextV1) GetKeyspaceName() (keyspaceName string) {
	return ""
}

type apiContextV2 struct {
	keyspaceName string
}

// NewAPIContextV2 creates a API context with the specified keyspace name for V2.
func NewAPIContextV2(keyspaceName string) APIContext {
	if len(keyspaceName) == 0 {
		keyspaceName = constants.DefaultKeyspaceName
	}
	return &apiContextV2{keyspaceName: keyspaceName}
}

// GetAPIVersion returns the API version.
func (*apiContextV2) GetAPIVersion() (version APIVersion) {
	return V2
}

// GetKeyspaceName returns the keyspace name.
func (apiCtx *apiContextV2) GetKeyspaceName() (keyspaceName string) {
	return apiCtx.keyspaceName
}

// NewClientWithAPIContext creates a client according to the API context.
func NewClientWithAPIContext(
	ctx context.Context, apiCtx APIContext,
	callerComponent caller.Component,
	svrAddrs []string,
	security SecurityOption, opts ...opt.ClientOption,
) (Client, error) {
	apiVersion, keyspaceName := apiCtx.GetAPIVersion(), apiCtx.GetKeyspaceName()
	switch apiVersion {
	case V1:
		return NewClientWithContext(ctx, callerComponent, svrAddrs,
			security, opts...)
	case V2:
		return newClientWithKeyspaceName(ctx, callerComponent,
			keyspaceName, svrAddrs, security, opts...)
	default:
		return nil, errors.Errorf("[pd] invalid API version %d", apiVersion)
	}
}

// newClientWithKeyspaceName creates a client with context and the specified keyspace name.
func newClientWithKeyspaceName(
	ctx context.Context,
	callerComponent caller.Component,
	keyspaceName string, svrAddrs []string,
	security SecurityOption, opts ...opt.ClientOption,
) (Client, error) {
	tlsCfg, err := tlsutil.TLSConfig{
		CAPath:   security.CAPath,
		CertPath: security.CertPath,
		KeyPath:  security.KeyPath,

		SSLCABytes:   security.SSLCABytes,
		SSLCertBytes: security.SSLCertBytes,
		SSLKEYBytes:  security.SSLKEYBytes,
	}.ToTLSConfig()
	if err != nil {
		return nil, err
	}
	clientCtx, clientCancel := context.WithCancel(ctx)
	c := &client{
		callerComponent: adjustCallerComponent(callerComponent),
		inner: &innerClient{
			// Create a PD service discovery with null keyspace id, then query the real id with the keyspace name,
			// finally update the keyspace id to the PD service discovery for the following interactions.
			keyspaceID:              constants.NullKeyspaceID,
			updateTokenConnectionCh: make(chan struct{}, 1),
			ctx:                     clientCtx,
			cancel:                  clientCancel,
			svrUrls:                 svrAddrs,
			tlsCfg:                  tlsCfg,
			option:                  opt.NewOption(),
		},
	}

	// Inject the client options.
	for _, opt := range opts {
		opt(c.inner.option)
	}

	updateKeyspaceIDFunc := func() error {
		keyspaceMeta, err := c.LoadKeyspace(clientCtx, keyspaceName)
		if err != nil {
			return err
		}
		c.inner.keyspaceID = keyspaceMeta.GetId()
		// c.keyspaceID is the source of truth for keyspace id.
		c.inner.pdSvcDiscovery.SetKeyspaceID(c.inner.keyspaceID)
		return nil
	}

	if err := c.inner.init(updateKeyspaceIDFunc); err != nil {
		return nil, err
	}
	log.Info("[pd] create pd client with endpoints and keyspace",
		zap.Strings("pd-address", svrAddrs),
		zap.String("keyspace-name", keyspaceName),
		zap.Uint32("keyspace-id", c.inner.keyspaceID))
	return c, nil
}

// Close closes the client.
func (c *client) Close() {
	c.inner.close()
}

// ResetTSOClient resets the TSO client, only for test.
func (c *client) ResetTSOClient() {
	c.inner.Lock()
	defer c.inner.Unlock()
	c.inner.resetTSOClientLocked(c.inner.serviceMode)
}

// GetClusterID returns the ClusterID.
func (c *client) GetClusterID(context.Context) uint64 {
	return c.inner.pdSvcDiscovery.GetClusterID()
}

// GetLeaderURL returns the leader URL.
func (c *client) GetLeaderURL() string {
	return c.inner.pdSvcDiscovery.GetServingURL()
}

// GetServiceDiscovery returns the client-side service discovery object
func (c *client) GetServiceDiscovery() sd.ServiceDiscovery {
	return c.inner.pdSvcDiscovery
}

// UpdateOption updates the client option.
func (c *client) UpdateOption(option opt.DynamicOption, value any) error {
	switch option {
	case opt.MaxTSOBatchWaitInterval:
		interval, ok := value.(time.Duration)
		if !ok {
			return errors.New("[pd] invalid value type for MaxTSOBatchWaitInterval option, it should be time.Duration")
		}
		if err := c.inner.option.SetMaxTSOBatchWaitInterval(interval); err != nil {
			return err
		}
	case opt.EnableTSOFollowerProxy:
		if c.inner.getServiceMode() != pdpb.ServiceMode_PD_SVC_MODE {
			return errors.New("[pd] tso follower proxy is only supported in PD service mode")
		}
		enable, ok := value.(bool)
		if !ok {
			return errors.New("[pd] invalid value type for EnableTSOFollowerProxy option, it should be bool")
		}
		c.inner.option.SetEnableTSOFollowerProxy(enable)
	case opt.EnableFollowerHandle:
		enable, ok := value.(bool)
		if !ok {
			return errors.New("[pd] invalid value type for EnableFollowerHandle option, it should be bool")
		}
		c.inner.option.SetEnableFollowerHandle(enable)
	case opt.TSOClientRPCConcurrency:
		value, ok := value.(int)
		if !ok {
			return errors.New("[pd] invalid value type for TSOClientRPCConcurrency option, it should be int")
		}
		c.inner.option.SetTSOClientRPCConcurrency(value)
	default:
		return errors.New("[pd] unsupported client option")
	}
	return nil
}

// GetAllMembers gets the members Info from PD.
func (c *client) GetAllMembers(ctx context.Context) ([]*pdpb.Member, error) {
	start := time.Now()
	defer func() { metrics.CmdDurationGetAllMembers.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
	defer cancel()
	req := &pdpb.GetMembersRequest{Header: c.requestHeader()}
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		return nil, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.GetMembers(ctx, req)
	if err = c.respForErr(metrics.CmdFailedDurationGetAllMembers, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}
	return resp.GetMembers(), nil
}

// getClientAndContext returns the leader pd client and the original context. If leader is unhealthy, it returns
// follower pd client and the context which holds forward information.
func (c *client) getClientAndContext(ctx context.Context) (pdpb.PDClient, context.Context) {
	serviceClient := c.inner.pdSvcDiscovery.GetServiceClient()
	if serviceClient == nil || serviceClient.GetClientConn() == nil {
		return nil, ctx
	}
	return pdpb.NewPDClient(serviceClient.GetClientConn()), serviceClient.BuildGRPCTargetContext(ctx, true)
}

// GetTSAsync implements the TSOClient interface.
func (c *client) GetTSAsync(ctx context.Context) tso.TSFuture {
	defer trace.StartRegion(ctx, "pdclient.GetTSAsync").End()
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.GetTSAsync", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	return c.inner.dispatchTSORequestWithRetry(ctx)
}

// GetLocalTSAsync implements the TSOClient interface.
//
// Deprecated: Local TSO will be completely removed in the future. Currently, regardless of the
// parameters passed in, this method will default to returning the global TSO.
func (c *client) GetLocalTSAsync(ctx context.Context, _ string) tso.TSFuture {
	return c.GetTSAsync(ctx)
}

// GetTS implements the TSOClient interface.
func (c *client) GetTS(ctx context.Context) (physical int64, logical int64, err error) {
	resp := c.GetTSAsync(ctx)
	return resp.Wait()
}

// GetLocalTS implements the TSOClient interface.
//
// Deprecated: Local TSO will be completely removed in the future. Currently, regardless of the
// parameters passed in, this method will default to returning the global TSO.
func (c *client) GetLocalTS(ctx context.Context, _ string) (physical int64, logical int64, err error) {
	return c.GetTS(ctx)
}

// GetMinTS implements the TSOClient interface.
func (c *client) GetMinTS(ctx context.Context) (physical int64, logical int64, err error) {
	// Handle compatibility issue in case of PD/API server doesn't support GetMinTS API.
	serviceMode := c.inner.getServiceMode()
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
	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
	defer cancel()
	// Call GetMinTS API to get the minimal TS from the API leader.
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		return 0, 0, errs.ErrClientGetProtoClient
	}

	resp, err := protoClient.GetMinTS(ctx, &pdpb.GetMinTSRequest{
		Header: c.requestHeader(),
	})
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
	return minTS.Physical, minTS.Logical, nil
}

func handleRegionResponse(res *pdpb.GetRegionResponse) *router.Region {
	if res.Region == nil {
		return nil
	}

	r := &router.Region{
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

// GetRegionFromMember implements the RPCClient interface.
func (c *client) GetRegionFromMember(ctx context.Context, key []byte, memberURLs []string, _ ...opt.GetRegionOption) (*router.Region, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.GetRegionFromMember", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationGetRegion.Observe(time.Since(start).Seconds()) }()

	var resp *pdpb.GetRegionResponse
	for _, url := range memberURLs {
		conn, err := c.inner.pdSvcDiscovery.GetOrCreateGRPCConn(url)
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
		metrics.CmdFailedDurationGetRegion.Observe(time.Since(start).Seconds())
		c.inner.pdSvcDiscovery.ScheduleCheckMemberChanged()
		errorMsg := fmt.Sprintf("[pd] can't get region info from member URLs: %+v", memberURLs)
		return nil, errors.WithStack(errors.New(errorMsg))
	}
	return handleRegionResponse(resp), nil
}

// GetRegion implements the RPCClient interface.
func (c *client) GetRegion(ctx context.Context, key []byte, opts ...opt.GetRegionOption) (*router.Region, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.GetRegion", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationGetRegion.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
	defer cancel()

	options := &opt.GetRegionOp{}
	for _, opt := range opts {
		opt(options)
	}
	req := &pdpb.GetRegionRequest{
		Header:      c.requestHeader(),
		RegionKey:   key,
		NeedBuckets: options.NeedBuckets,
	}
	serviceClient, cctx := c.inner.getRegionAPIClientAndContext(ctx,
		options.AllowFollowerHandle && c.inner.option.GetEnableFollowerHandle())
	if serviceClient == nil {
		return nil, errs.ErrClientGetProtoClient
	}
	resp, err := pdpb.NewPDClient(serviceClient.GetClientConn()).GetRegion(cctx, req)
	if serviceClient.NeedRetry(resp.GetHeader().GetError(), err) {
		protoClient, cctx := c.getClientAndContext(ctx)
		if protoClient == nil {
			return nil, errs.ErrClientGetProtoClient
		}
		resp, err = protoClient.GetRegion(cctx, req)
	}

	if err = c.respForErr(metrics.CmdFailedDurationGetRegion, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}
	return handleRegionResponse(resp), nil
}

// GetPrevRegion implements the RPCClient interface.
func (c *client) GetPrevRegion(ctx context.Context, key []byte, opts ...opt.GetRegionOption) (*router.Region, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.GetPrevRegion", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationGetPrevRegion.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
	defer cancel()

	options := &opt.GetRegionOp{}
	for _, opt := range opts {
		opt(options)
	}
	req := &pdpb.GetRegionRequest{
		Header:      c.requestHeader(),
		RegionKey:   key,
		NeedBuckets: options.NeedBuckets,
	}
	serviceClient, cctx := c.inner.getRegionAPIClientAndContext(ctx,
		options.AllowFollowerHandle && c.inner.option.GetEnableFollowerHandle())
	if serviceClient == nil {
		return nil, errs.ErrClientGetProtoClient
	}
	resp, err := pdpb.NewPDClient(serviceClient.GetClientConn()).GetPrevRegion(cctx, req)
	if serviceClient.NeedRetry(resp.GetHeader().GetError(), err) {
		protoClient, cctx := c.getClientAndContext(ctx)
		if protoClient == nil {
			return nil, errs.ErrClientGetProtoClient
		}
		resp, err = protoClient.GetPrevRegion(cctx, req)
	}

	if err = c.respForErr(metrics.CmdFailedDurationGetPrevRegion, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}
	return handleRegionResponse(resp), nil
}

// GetRegionByID implements the RPCClient interface.
func (c *client) GetRegionByID(ctx context.Context, regionID uint64, opts ...opt.GetRegionOption) (*router.Region, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.GetRegionByID", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationGetRegionByID.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
	defer cancel()

	options := &opt.GetRegionOp{}
	for _, opt := range opts {
		opt(options)
	}
	req := &pdpb.GetRegionByIDRequest{
		Header:      c.requestHeader(),
		RegionId:    regionID,
		NeedBuckets: options.NeedBuckets,
	}
	serviceClient, cctx := c.inner.getRegionAPIClientAndContext(ctx,
		options.AllowFollowerHandle && c.inner.option.GetEnableFollowerHandle())
	if serviceClient == nil {
		return nil, errs.ErrClientGetProtoClient
	}
	resp, err := pdpb.NewPDClient(serviceClient.GetClientConn()).GetRegionByID(cctx, req)
	if serviceClient.NeedRetry(resp.GetHeader().GetError(), err) {
		protoClient, cctx := c.getClientAndContext(ctx)
		if protoClient == nil {
			return nil, errs.ErrClientGetProtoClient
		}
		resp, err = protoClient.GetRegionByID(cctx, req)
	}

	if err = c.respForErr(metrics.CmdFailedDurationGetRegionByID, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}
	return handleRegionResponse(resp), nil
}

// ScanRegions implements the RPCClient interface.
func (c *client) ScanRegions(ctx context.Context, key, endKey []byte, limit int, opts ...opt.GetRegionOption) ([]*router.Region, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.ScanRegions", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationScanRegions.Observe(time.Since(start).Seconds()) }()

	var cancel context.CancelFunc
	scanCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		scanCtx, cancel = context.WithTimeout(ctx, c.inner.option.Timeout)
		defer cancel()
	}
	options := &opt.GetRegionOp{}
	for _, opt := range opts {
		opt(options)
	}
	req := &pdpb.ScanRegionsRequest{
		Header:   c.requestHeader(),
		StartKey: key,
		EndKey:   endKey,
		Limit:    int32(limit),
	}
	serviceClient, cctx := c.inner.getRegionAPIClientAndContext(scanCtx,
		options.AllowFollowerHandle && c.inner.option.GetEnableFollowerHandle())
	if serviceClient == nil {
		return nil, errs.ErrClientGetProtoClient
	}
	//nolint:staticcheck
	resp, err := pdpb.NewPDClient(serviceClient.GetClientConn()).ScanRegions(cctx, req)
	failpoint.Inject("responseNil", func() {
		resp = nil
	})
	if serviceClient.NeedRetry(resp.GetHeader().GetError(), err) {
		protoClient, cctx := c.getClientAndContext(scanCtx)
		if protoClient == nil {
			return nil, errs.ErrClientGetProtoClient
		}
		//nolint:staticcheck
		resp, err = protoClient.ScanRegions(cctx, req)
	}

	if err = c.respForErr(metrics.CmdFailedDurationScanRegions, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}

	return handleRegionsResponse(resp), nil
}

// BatchScanRegions implements the RPCClient interface.
func (c *client) BatchScanRegions(ctx context.Context, ranges []router.KeyRange, limit int, opts ...opt.GetRegionOption) ([]*router.Region, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.BatchScanRegions", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationBatchScanRegions.Observe(time.Since(start).Seconds()) }()

	var cancel context.CancelFunc
	scanCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		scanCtx, cancel = context.WithTimeout(ctx, c.inner.option.Timeout)
		defer cancel()
	}
	options := &opt.GetRegionOp{}
	for _, opt := range opts {
		opt(options)
	}
	pbRanges := make([]*pdpb.KeyRange, 0, len(ranges))
	for _, r := range ranges {
		pbRanges = append(pbRanges, &pdpb.KeyRange{StartKey: r.StartKey, EndKey: r.EndKey})
	}
	req := &pdpb.BatchScanRegionsRequest{
		Header:             c.requestHeader(),
		NeedBuckets:        options.NeedBuckets,
		Ranges:             pbRanges,
		Limit:              int32(limit),
		ContainAllKeyRange: options.OutputMustContainAllKeyRange,
	}
	serviceClient, cctx := c.inner.getRegionAPIClientAndContext(scanCtx,
		options.AllowFollowerHandle && c.inner.option.GetEnableFollowerHandle())
	if serviceClient == nil {
		return nil, errs.ErrClientGetProtoClient
	}
	resp, err := pdpb.NewPDClient(serviceClient.GetClientConn()).BatchScanRegions(cctx, req)
	failpoint.Inject("responseNil", func() {
		resp = nil
	})
	if serviceClient.NeedRetry(resp.GetHeader().GetError(), err) {
		protoClient, cctx := c.getClientAndContext(scanCtx)
		if protoClient == nil {
			return nil, errs.ErrClientGetProtoClient
		}
		resp, err = protoClient.BatchScanRegions(cctx, req)
	}

	if err = c.respForErr(metrics.CmdFailedDurationBatchScanRegions, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}

	return handleBatchRegionsResponse(resp), nil
}

func handleBatchRegionsResponse(resp *pdpb.BatchScanRegionsResponse) []*router.Region {
	regions := make([]*router.Region, 0, len(resp.GetRegions()))
	for _, r := range resp.GetRegions() {
		region := &router.Region{
			Meta:         r.Region,
			Leader:       r.Leader,
			PendingPeers: r.PendingPeers,
			Buckets:      r.Buckets,
		}
		for _, p := range r.DownPeers {
			region.DownPeers = append(region.DownPeers, p.Peer)
		}
		regions = append(regions, region)
	}
	return regions
}

func handleRegionsResponse(resp *pdpb.ScanRegionsResponse) []*router.Region {
	var regions []*router.Region
	if len(resp.GetRegions()) == 0 {
		// Make it compatible with old server.
		metas, leaders := resp.GetRegionMetas(), resp.GetLeaders()
		for i := range metas {
			r := &router.Region{Meta: metas[i]}
			if i < len(leaders) {
				r.Leader = leaders[i]
			}
			regions = append(regions, r)
		}
	} else {
		for _, r := range resp.GetRegions() {
			region := &router.Region{
				Meta:         r.Region,
				Leader:       r.Leader,
				PendingPeers: r.PendingPeers,
				Buckets:      r.Buckets,
			}
			for _, p := range r.DownPeers {
				region.DownPeers = append(region.DownPeers, p.Peer)
			}
			regions = append(regions, region)
		}
	}
	return regions
}

// GetStore implements the RPCClient interface.
func (c *client) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.GetStore", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationGetStore.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
	defer cancel()
	req := &pdpb.GetStoreRequest{
		Header:  c.requestHeader(),
		StoreId: storeID,
	}
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		return nil, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.GetStore(ctx, req)

	if err = c.respForErr(metrics.CmdFailedDurationGetStore, start, err, resp.GetHeader()); err != nil {
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

// GetAllStores implements the RPCClient interface.
func (c *client) GetAllStores(ctx context.Context, opts ...opt.GetStoreOption) ([]*metapb.Store, error) {
	// Applies options
	options := &opt.GetStoreOp{}
	for _, opt := range opts {
		opt(options)
	}

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.GetAllStores", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationGetAllStores.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
	defer cancel()
	req := &pdpb.GetAllStoresRequest{
		Header:                 c.requestHeader(),
		ExcludeTombstoneStores: options.ExcludeTombstone,
	}
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		return nil, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.GetAllStores(ctx, req)

	if err = c.respForErr(metrics.CmdFailedDurationGetAllStores, start, err, resp.GetHeader()); err != nil {
		return nil, err
	}
	return resp.GetStores(), nil
}

// UpdateGCSafePoint implements the RPCClient interface.
func (c *client) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.UpdateGCSafePoint", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationUpdateGCSafePoint.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
	defer cancel()
	req := &pdpb.UpdateGCSafePointRequest{
		Header:    c.requestHeader(),
		SafePoint: safePoint,
	}
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		return 0, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.UpdateGCSafePoint(ctx, req)

	if err = c.respForErr(metrics.CmdFailedDurationUpdateGCSafePoint, start, err, resp.GetHeader()); err != nil {
		return 0, err
	}
	return resp.GetNewSafePoint(), nil
}

// UpdateServiceGCSafePoint updates the safepoint for specific service and
// returns the minimum safepoint across all services, this value is used to
// determine the safepoint for multiple services, it does not trigger a GC
// job. Use UpdateGCSafePoint to trigger the GC job if needed.
func (c *client) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.UpdateServiceGCSafePoint", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}

	start := time.Now()
	defer func() { metrics.CmdDurationUpdateServiceGCSafePoint.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
	defer cancel()
	req := &pdpb.UpdateServiceGCSafePointRequest{
		Header:    c.requestHeader(),
		ServiceId: []byte(serviceID),
		TTL:       ttl,
		SafePoint: safePoint,
	}
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		return 0, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.UpdateServiceGCSafePoint(ctx, req)

	if err = c.respForErr(metrics.CmdFailedDurationUpdateServiceGCSafePoint, start, err, resp.GetHeader()); err != nil {
		return 0, err
	}
	return resp.GetMinSafePoint(), nil
}

// ScatterRegion implements the RPCClient interface.
func (c *client) ScatterRegion(ctx context.Context, regionID uint64) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.ScatterRegion", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	return c.scatterRegionsWithGroup(ctx, regionID, "")
}

func (c *client) scatterRegionsWithGroup(ctx context.Context, regionID uint64, group string) error {
	start := time.Now()
	defer func() { metrics.CmdDurationScatterRegion.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
	defer cancel()
	req := &pdpb.ScatterRegionRequest{
		Header:   c.requestHeader(),
		RegionId: regionID,
		Group:    group,
	}
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		return errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.ScatterRegion(ctx, req)
	if err != nil {
		return err
	}
	if resp.GetHeader().GetError() != nil {
		return errors.Errorf("scatter region %d failed: %s", regionID, resp.GetHeader().GetError().String())
	}
	return nil
}

// ScatterRegions implements the RPCClient interface.
func (c *client) ScatterRegions(ctx context.Context, regionsID []uint64, opts ...opt.RegionsOption) (*pdpb.ScatterRegionResponse, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.ScatterRegions", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	return c.scatterRegionsWithOptions(ctx, regionsID, opts...)
}

// SplitAndScatterRegions implements the RPCClient interface.
func (c *client) SplitAndScatterRegions(ctx context.Context, splitKeys [][]byte, opts ...opt.RegionsOption) (*pdpb.SplitAndScatterRegionsResponse, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.SplitAndScatterRegions", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationSplitAndScatterRegions.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
	defer cancel()
	options := &opt.RegionsOp{}
	for _, opt := range opts {
		opt(options)
	}
	req := &pdpb.SplitAndScatterRegionsRequest{
		Header:     c.requestHeader(),
		SplitKeys:  splitKeys,
		Group:      options.Group,
		RetryLimit: options.RetryLimit,
	}

	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		return nil, errs.ErrClientGetProtoClient
	}
	return protoClient.SplitAndScatterRegions(ctx, req)
}

// GetOperator implements the RPCClient interface.
func (c *client) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.GetOperator", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationGetOperator.Observe(time.Since(start).Seconds()) }()

	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
	defer cancel()
	req := &pdpb.GetOperatorRequest{
		Header:   c.requestHeader(),
		RegionId: regionID,
	}
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		return nil, errs.ErrClientGetProtoClient
	}
	return protoClient.GetOperator(ctx, req)
}

// SplitRegions split regions by given split keys
func (c *client) SplitRegions(ctx context.Context, splitKeys [][]byte, opts ...opt.RegionsOption) (*pdpb.SplitRegionsResponse, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan("pdclient.SplitRegions", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}
	start := time.Now()
	defer func() { metrics.CmdDurationSplitRegions.Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
	defer cancel()
	options := &opt.RegionsOp{}
	for _, opt := range opts {
		opt(options)
	}
	req := &pdpb.SplitRegionsRequest{
		Header:     c.requestHeader(),
		SplitKeys:  splitKeys,
		RetryLimit: options.RetryLimit,
	}
	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		return nil, errs.ErrClientGetProtoClient
	}
	return protoClient.SplitRegions(ctx, req)
}

func (c *client) requestHeader() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId:       c.inner.pdSvcDiscovery.GetClusterID(),
		CallerId:        string(caller.GetCallerID()),
		CallerComponent: string(c.callerComponent),
	}
}

func (c *client) scatterRegionsWithOptions(ctx context.Context, regionsID []uint64, opts ...opt.RegionsOption) (*pdpb.ScatterRegionResponse, error) {
	start := time.Now()
	defer func() { metrics.CmdDurationScatterRegions.Observe(time.Since(start).Seconds()) }()
	options := &opt.RegionsOp{}
	for _, opt := range opts {
		opt(options)
	}
	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
	defer cancel()
	req := &pdpb.ScatterRegionRequest{
		Header:         c.requestHeader(),
		Group:          options.Group,
		RegionsId:      regionsID,
		RetryLimit:     options.RetryLimit,
		SkipStoreLimit: options.SkipStoreLimit,
	}

	protoClient, ctx := c.getClientAndContext(ctx)
	if protoClient == nil {
		return nil, errs.ErrClientGetProtoClient
	}
	resp, err := protoClient.ScatterRegion(ctx, req)

	if err != nil {
		return nil, err
	}
	if resp.GetHeader().GetError() != nil {
		return nil, errors.Errorf("scatter regions %v failed: %s", regionsID, resp.GetHeader().GetError().String())
	}
	return resp, nil
}

// LoadGlobalConfig implements the RPCClient interface.
func (c *client) LoadGlobalConfig(ctx context.Context, names []string, configPath string) ([]GlobalConfigItem, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
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

// StoreGlobalConfig implements the RPCClient interface.
func (c *client) StoreGlobalConfig(ctx context.Context, configPath string, items []GlobalConfigItem) error {
	resArr := make([]*pdpb.GlobalConfigItem, len(items))
	for i, it := range items {
		resArr[i] = &pdpb.GlobalConfigItem{Name: it.Name, Value: it.Value, Kind: it.EventType, Payload: it.PayLoad}
	}
	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
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

// WatchGlobalConfig implements the RPCClient interface.
func (c *client) WatchGlobalConfig(ctx context.Context, configPath string, revision int64) (chan []GlobalConfigItem, error) {
	// TODO: Add retry mechanism
	// register watch components there
	globalConfigWatcherCh := make(chan []GlobalConfigItem, 16)
	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
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

// GetExternalTimestamp implements the RPCClient interface.
func (c *client) GetExternalTimestamp(ctx context.Context) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
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
		return 0, errors.New("[pd]" + resErr.Message)
	}
	return resp.GetTimestamp(), nil
}

// SetExternalTimestamp implements the RPCClient interface.
func (c *client) SetExternalTimestamp(ctx context.Context, timestamp uint64) error {
	ctx, cancel := context.WithTimeout(ctx, c.inner.option.Timeout)
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
		return errors.New("[pd]" + resErr.Message)
	}
	return nil
}

func (c *client) respForErr(observer prometheus.Observer, start time.Time, err error, header *pdpb.ResponseHeader) error {
	if err != nil || header.GetError() != nil {
		observer.Observe(time.Since(start).Seconds())
		if err != nil {
			c.inner.pdSvcDiscovery.ScheduleCheckMemberChanged()
			return errors.WithStack(err)
		}
		return errors.WithStack(errors.New(header.GetError().String()))
	}
	return nil
}

// WithCallerComponent implements the RPCClient interface.
func (c *client) WithCallerComponent(callerComponent caller.Component) RPCClient {
	newClient := *c
	newClient.callerComponent = callerComponent
	return &newClient
}

// adjustCallerComponent returns the caller component if it is empty, it
// is the upper layer of the pd client.
func adjustCallerComponent(callerComponent caller.Component) caller.Component {
	callerComponent = caller.Component(strings.TrimSpace(string(callerComponent)))
	if len(callerComponent) != 0 {
		return callerComponent
	}
	for i := range 10 { // limit the loop to 10 iterations to avoid infinite loop
		callerComponent = caller.GetComponent(i)
		if !strings.Contains(string(callerComponent), "pd/client") {
			return callerComponent
		}
	}
	log.Warn("Unknown callerComponent", zap.String("callerComponent", string(callerComponent)))
	// If the callerComponent is still in pd/client, we set it to empty.
	return ""
}
