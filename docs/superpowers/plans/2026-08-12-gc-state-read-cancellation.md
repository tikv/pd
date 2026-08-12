# GC State Read Cancellation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make request cancellation stop in-flight GC state reads at etcd,
release the GC state manager lock, and reach callers through the existing
gRPC and HTTP APIs.

**Architecture:** Add opt-in context-aware capabilities beside the existing
storage interfaces, use them in a new context-aware GC transaction path, and
thread the execution context through the manager read chain. Keep legacy
storage and write paths unchanged. Map caller cancellation only at the API
boundary.

**Tech Stack:** Go 1.25, etcd v3 client, gRPC client interceptors, Gin,
Gorilla mux, Testify, PD failpoint-aware Make targets.

## Global constraints

This plan implements the approved
[design](../specs/2026-08-11-gc-state-read-cancellation-design.md).
Every task must preserve these constraints.

- Do not change protobuf definitions, client interfaces, routes, or JSON
  response schemas.
- Keep kv.Base, kv.RawTxnCapable, EtcdKVGet, CreateRawTxn, and
  RunInGCStateTransaction available.
- Add companion context-aware APIs. Do not retrofit cancellation into GC
  write transactions.
- Require both context-aware reads and context-aware raw transactions before
  the new GC transaction performs its first storage read.
- Return an explicit capability error. Do not fall back to contextless I/O.
- Implement the new capabilities only for the etcd backend. Memory and
  LevelDB remain valid in their existing production roles.
- Keep the manager read-lock model. Cancellation must release the current
  read lock; it must not redesign lock ownership or cache synchronization.
- Treat a successful transaction commit as the read linearization point. Do
  not add a context check after a successful commit.
- Return the caller's exact context.Canceled or context.DeadlineExceeded when
  the caller context ended.
- Preserve ErrEtcdKVGet and ErrEtcdTxnInternal when only the internal
  ten-second etcd operation timeout ended.
- Do not emit error-level logs for caller cancellation. Preserve existing
  slow-operation warnings and logs for non-context errors.
- Do not add metrics or runtime dependencies.
- Use deterministic gRPC client interceptors. Do not use SlowEtcdKVGet,
  sleeps, or timing assumptions to coordinate cancellation tests.
- Use timeouts only as test watchdogs.
- Run tests through failpoint-aware Make targets where the package imports
  failpoint.
- Preserve the unrelated untracked .superpowers, .wt, and
  docs/gc-refactor-handoff-summary.md paths.

## File map

The change follows one vertical request path. The responsibilities below keep
the boundaries explicit before implementation begins.

- Modify pkg/utils/etcdutil/testutil.go to provide a deterministic, one-shot
  unary gRPC blocker for embedded-etcd tests.
- Modify pkg/utils/etcdutil/etcdutil.go and etcdutil_test.go to add and test
  EtcdKVGetWithContext.
- Modify pkg/storage/kv/kv.go, etcd_kv.go, and kv_test.go to define the two
  opt-in capabilities and implement them for etcd.
- Modify pkg/storage/endpoint/endpoint.go, gc_states.go, and
  gc_states_test.go to add context-aware JSON reads, provider reads, and GC
  transactions.
- Modify pkg/gc/gc_state_manager.go and gc_state_manager_test.go to thread
  context through single, combined, and all-keyspace reads.
- Modify server/gc_service.go and add server/gc_service_test.go to propagate
  context and map context failures to standard gRPC status errors.
- Modify server/api/service_gc_safepoint.go and
  server/apiv2/handlers/safe_point.go to pass HTTP request contexts and avoid
  writing an error response after disconnect.
- Modify direct manager call sites in
  tests/server/api/service_gc_safepoint_test.go and
  tests/integrations/client/client_test.go for the new manager signature.
- Keep pkg/utils/syncutil/ordered_single_flight.go unchanged. Its execCtx
  contract is consumed by the manager, and its existing tests remain the
  cancellation contract for shared executions.

---

## Task 1: Add deterministic etcd read cancellation

This task establishes the test primitive and the lowest context-aware Range
operation. Its commit must leave the old EtcdKVGet behavior available.

### Files

This task touches only the shared etcd utility package.

- Modify: pkg/utils/etcdutil/testutil.go
- Modify: pkg/utils/etcdutil/etcdutil.go:163-187
- Modify: pkg/utils/etcdutil/etcdutil_test.go:95-125

### Interfaces

The test helper consumes a gRPC method and request predicate. The production
function consumes the caller context and produces the existing GetResponse.

~~~go
type UnaryRPCMatcher func(method string, request any) bool

type BlockingUnaryClientInterceptor struct {
    matcher UnaryRPCMatcher
}

func NewBlockingUnaryClientInterceptor(
    matcher UnaryRPCMatcher,
) *BlockingUnaryClientInterceptor

func (b *BlockingUnaryClientInterceptor) Enable()
func (b *BlockingUnaryClientInterceptor) Entered() <-chan struct{}
func (b *BlockingUnaryClientInterceptor) ContextDone() <-chan struct{}
func (b *BlockingUnaryClientInterceptor) Release()

func (b *BlockingUnaryClientInterceptor) UnaryClientInterceptor(
    ctx context.Context,
    method string,
    request, reply any,
    connection *grpc.ClientConn,
    invoker grpc.UnaryInvoker,
    options ...grpc.CallOption,
) error

func EtcdKVGetWithContext(
    ctx context.Context,
    client *clientv3.Client,
    key string,
    options ...clientv3.OpOption,
) (*clientv3.GetResponse, error)
~~~

### Steps

Each checkbox is one small implementation or verification action.

- [ ] Add TestEtcdKVGetWithContextCancellation before adding either new
  symbol. Match only the exact Range key so client startup and health traffic
  continue normally.

~~~go
func TestEtcdKVGetWithContextCancellation(t *testing.T) {
    re := require.New(t)
    const key = "test/context"

    blocker := NewBlockingUnaryClientInterceptor(
        func(method string, request any) bool {
            rangeRequest, ok := request.(*etcdserverpb.RangeRequest)
            return method == "/etcdserverpb.KV/Range" &&
                ok &&
                string(rangeRequest.Key) == key &&
                len(rangeRequest.RangeEnd) == 0
        },
    )
    option := &TestEtcdClusterOptions{
        ClientCfgModifier: func(config *clientv3.Config) {
            config.DialOptions = append(
                config.DialOptions,
                grpc.WithChainUnaryInterceptor(
                    blocker.UnaryClientInterceptor,
                ),
            )
        },
    }
    _, client, clean := NewTestEtcdCluster(t, 1, option)
    defer clean()
    defer blocker.Release()

    ctx, cancel := context.WithCancel(context.Background())
    result := make(chan error, 1)
    blocker.Enable()
    go func() {
        _, err := EtcdKVGetWithContext(ctx, client, key)
        result <- err
    }()

    select {
    case <-blocker.Entered():
    case <-time.After(5 * time.Second):
        re.FailNow("the etcd Range request was not intercepted")
    }
    cancel()
    select {
    case <-blocker.ContextDone():
    case <-time.After(5 * time.Second):
        re.FailNow("the intercepted Range context was not canceled")
    }
    select {
    case err := <-result:
        re.Equal(context.Canceled, err)
    case <-time.After(5 * time.Second):
        re.FailNow("EtcdKVGetWithContext did not return")
    }
}
~~~

- [ ] Add TestEtcdKVGetWithContextPreCanceled. Cancel the context before the
  call and assert the returned error equals context.Canceled.

~~~go
func TestEtcdKVGetWithContextPreCanceled(t *testing.T) {
    re := require.New(t)
    _, client, clean := NewTestEtcdCluster(t, 1, nil)
    defer clean()

    ctx, cancel := context.WithCancel(context.Background())
    cancel()
    response, err := EtcdKVGetWithContext(ctx, client, "unused")
    re.Nil(response)
    re.Equal(context.Canceled, err)
}
~~~

- [ ] Add TestEtcdKVGetWithContextExpiredDeadline. Create a deadline in the
  past and assert the response is nil and the returned error equals
  context.DeadlineExceeded.

~~~go
func TestEtcdKVGetWithContextExpiredDeadline(t *testing.T) {
    re := require.New(t)
    _, client, clean := NewTestEtcdCluster(t, 1, nil)
    defer clean()

    ctx, cancel := context.WithDeadline(
        context.Background(),
        time.Now().Add(-time.Second),
    )
    defer cancel()
    response, err := EtcdKVGetWithContext(ctx, client, "unused")
    re.Nil(response)
    re.Equal(context.DeadlineExceeded, err)
}
~~~

- [ ] Run the focused tests and confirm they fail to compile because
  NewBlockingUnaryClientInterceptor and EtcdKVGetWithContext do not exist.

~~~sh
make gotest \
  GOTEST_ARGS='./pkg/utils/etcdutil -run TestEtcdKVGetWithContext -count=1'
~~~

- [ ] Add the one-shot blocker to testutil.go. Enable arms exactly one
  matching RPC. CompareAndSwap disarms it before blocking so later storage
  work cannot be trapped by a completed test stage.

~~~go
type UnaryRPCMatcher func(method string, request any) bool

type BlockingUnaryClientInterceptor struct {
    matcher UnaryRPCMatcher
    enabled atomic.Bool

    entered     chan struct{}
    contextDone chan struct{}
    release     chan struct{}

    enteredOnce     sync.Once
    contextDoneOnce sync.Once
    releaseOnce     sync.Once
}

func NewBlockingUnaryClientInterceptor(
    matcher UnaryRPCMatcher,
) *BlockingUnaryClientInterceptor {
    return &BlockingUnaryClientInterceptor{
        matcher:     matcher,
        entered:     make(chan struct{}),
        contextDone: make(chan struct{}),
        release:     make(chan struct{}),
    }
}

func (b *BlockingUnaryClientInterceptor) Enable() {
    b.enabled.Store(true)
}

func (b *BlockingUnaryClientInterceptor) Entered() <-chan struct{} {
    return b.entered
}

func (b *BlockingUnaryClientInterceptor) ContextDone() <-chan struct{} {
    return b.contextDone
}

func (b *BlockingUnaryClientInterceptor) Release() {
    b.releaseOnce.Do(func() {
        close(b.release)
    })
}

func (b *BlockingUnaryClientInterceptor) UnaryClientInterceptor(
    ctx context.Context,
    method string,
    request, reply any,
    connection *grpc.ClientConn,
    invoker grpc.UnaryInvoker,
    options ...grpc.CallOption,
) error {
    if !b.enabled.Load() || !b.matcher(method, request) ||
        !b.enabled.CompareAndSwap(true, false) {
        return invoker(
            ctx,
            method,
            request,
            reply,
            connection,
            options...,
        )
    }

    b.enteredOnce.Do(func() {
        close(b.entered)
    })
    select {
    case <-ctx.Done():
        b.contextDoneOnce.Do(func() {
            close(b.contextDone)
        })
        return ctx.Err()
    case <-b.release:
        return invoker(
            ctx,
            method,
            request,
            reply,
            connection,
            options...,
        )
    }
}
~~~

- [ ] Add GoDoc for UnaryRPCMatcher, BlockingUnaryClientInterceptor, its
  constructor, and all exported methods. State that an instance blocks at
  most one matched call and Release is safe to call more than once.

- [ ] Refactor EtcdKVGet to delegate to EtcdKVGetWithContext with
  client.Ctx, then implement parent-aware timeout and error classification.

~~~go
func EtcdKVGet(
    client *clientv3.Client,
    key string,
    options ...clientv3.OpOption,
) (*clientv3.GetResponse, error) {
    return EtcdKVGetWithContext(
        client.Ctx(),
        client,
        key,
        options...,
    )
}

func EtcdKVGetWithContext(
    ctx context.Context,
    client *clientv3.Client,
    key string,
    options ...clientv3.OpOption,
) (*clientv3.GetResponse, error) {
    if err := ctx.Err(); err != nil {
        return nil, err
    }

    operationCtx, cancel := context.WithTimeout(
        ctx,
        DefaultRequestTimeout,
    )
    defer cancel()

    start := time.Now()
    failpoint.Inject("SlowEtcdKVGet", func(value failpoint.Value) {
        seconds := value.(int)
        time.Sleep(time.Duration(seconds) * time.Second)
    })
    response, err := clientv3.NewKV(client).Get(
        operationCtx,
        key,
        options...,
    )
    if cost := time.Since(start); cost > DefaultSlowRequestTime {
        log.Warn(
            "kv gets too slow",
            zap.String("request-key", key),
            zap.Duration("cost", cost),
            errs.ZapError(err),
        )
    }

    if err != nil {
        if contextErr := ctx.Err(); contextErr != nil {
            return response, contextErr
        }
        wrapped := errs.ErrEtcdKVGet.Wrap(err).GenWithStackByCause()
        log.Error(
            "load from etcd meet error",
            zap.String("key", key),
            errs.ZapError(wrapped),
        )
        return response, wrapped
    }
    return response, nil
}
~~~

- [ ] Run gofmt on all three touched files.

~~~sh
gofmt -w \
  pkg/utils/etcdutil/testutil.go \
  pkg/utils/etcdutil/etcdutil.go \
  pkg/utils/etcdutil/etcdutil_test.go
~~~

- [ ] Run the new tests and the legacy EtcdKVGet regression test.

~~~sh
make gotest \
  GOTEST_ARGS='./pkg/utils/etcdutil -run TestEtcdKVGet -count=1'
~~~

- [ ] Inspect the diff and confirm only caller-context errors bypass
  ErrEtcdKVGet and error-level logging. Confirm slow warnings still execute.

~~~sh
git diff --check
git diff -- \
  pkg/utils/etcdutil/testutil.go \
  pkg/utils/etcdutil/etcdutil.go \
  pkg/utils/etcdutil/etcdutil_test.go
~~~

- [ ] Stage and commit this task.

~~~sh
git add \
  pkg/utils/etcdutil/testutil.go \
  pkg/utils/etcdutil/etcdutil.go \
  pkg/utils/etcdutil/etcdutil_test.go
git diff --cached --check
git commit -s -m "utils/etcdutil: add context-aware get"
~~~

## Task 2: Add opt-in context capabilities to the etcd backend

This task exposes context-aware point reads, range reads, and raw
transactions without expanding kv.Base or changing non-etcd backends.

### Files

The capability interfaces and their sole implementation live in the KV
package.

- Modify: pkg/storage/kv/kv.go:95-111
- Modify: pkg/storage/kv/etcd_kv.go:52-158
- Modify: pkg/storage/kv/kv_test.go:25-120

### Interfaces

The endpoint layer will consume both interfaces. The etcd backend will
produce both interfaces.

~~~go
type ContextReader interface {
    LoadWithContext(
        ctx context.Context,
        key string,
    ) (string, error)
    LoadRangeWithContext(
        ctx context.Context,
        key, endKey string,
        limit int,
    ) (
        keys []string,
        values []string,
        err error,
    )
}

type RawTxnCapableWithContext interface {
    CreateRawTxnWithContext(ctx context.Context) RawTxn
}
~~~

### Steps

The tests first prove that the provided context reaches both etcd RPC forms.

- [ ] Add compile-time interface assertions and
  TestEtcdLoadRangeWithContextCancellation to kv_test.go. Match the exact
  Range key and end key.

~~~go
var _ ContextReader = (*etcdKVBase)(nil)
var _ RawTxnCapableWithContext = (*etcdKVBase)(nil)

func TestEtcdLoadRangeWithContextCancellation(t *testing.T) {
    re := require.New(t)
    const (
        startKey = "context/range/start"
        endKey   = "context/range/end"
    )
    blocker := etcdutil.NewBlockingUnaryClientInterceptor(
        func(method string, request any) bool {
            rangeRequest, ok := request.(*etcdserverpb.RangeRequest)
            return method == "/etcdserverpb.KV/Range" &&
                ok &&
                string(rangeRequest.Key) == startKey &&
                string(rangeRequest.RangeEnd) == endKey
        },
    )
    client, clean := newEtcdClientWithBlocker(t, blocker)
    defer clean()
    defer blocker.Release()

    base := NewEtcdKVBase(client)
    ctx, cancel := context.WithCancel(context.Background())
    result := make(chan error, 1)
    blocker.Enable()
    go func() {
        _, _, err := base.LoadRangeWithContext(
            ctx,
            startKey,
            endKey,
            10,
        )
        result <- err
    }()

    waitForBlockerEntry(re, blocker)
    cancel()
    waitForBlockerContextDone(re, blocker)
    re.Equal(context.Canceled, waitForError(re, result))
}
~~~

- [ ] Add TestEtcdRawTxnWithContextCancellation. Match a Txn whose comparison
  contains the exact key.

~~~go
func TestEtcdRawTxnWithContextCancellation(t *testing.T) {
    re := require.New(t)
    const key = "context/txn"
    blocker := etcdutil.NewBlockingUnaryClientInterceptor(
        func(method string, request any) bool {
            txnRequest, ok := request.(*etcdserverpb.TxnRequest)
            return method == "/etcdserverpb.KV/Txn" &&
                ok &&
                len(txnRequest.Compare) == 1 &&
                string(txnRequest.Compare[0].Key) == key
        },
    )
    client, clean := newEtcdClientWithBlocker(t, blocker)
    defer clean()
    defer blocker.Release()

    base := NewEtcdKVBase(client)
    ctx, cancel := context.WithCancel(context.Background())
    result := make(chan error, 1)
    blocker.Enable()
    go func() {
        _, err := base.CreateRawTxnWithContext(ctx).
            If(RawTxnCondition{
                Key:     key,
                CmpType: RawTxnCmpNotExists,
            }).
            Then(RawTxnOp{
                Key:    key,
                OpType: RawTxnOpGet,
            }).
            Commit()
        result <- err
    }()

    waitForBlockerEntry(re, blocker)
    cancel()
    waitForBlockerContextDone(re, blocker)
    re.ErrorIs(waitForError(re, result), context.Canceled)
}
~~~

- [ ] Add local kv_test.go helpers named newEtcdClientWithBlocker,
  waitForBlockerEntry, waitForBlockerContextDone, and waitForError. Each wait
  uses a select with a five-second watchdog and contains no sleep.

- [ ] Run the focused tests and confirm the missing interfaces and methods
  cause compilation failure.

~~~sh
make gotest \
  GOTEST_ARGS='./pkg/storage/kv -run TestEtcd.*Context -count=1'
~~~

- [ ] Add ContextReader and RawTxnCapableWithContext immediately beside
  RawTxnCapable in kv.go. Do not embed either interface in Base.

- [ ] Make Load and LoadRange delegate to their context-aware companions with
  the etcd client context. Move the current range-option construction into
  LoadRangeWithContext.

~~~go
func (kv *etcdKVBase) Load(key string) (string, error) {
    return kv.LoadWithContext(kv.client.Ctx(), key)
}

func (kv *etcdKVBase) LoadWithContext(
    ctx context.Context,
    key string,
) (string, error) {
    response, err := etcdutil.EtcdKVGetWithContext(
        ctx,
        kv.client,
        key,
    )
    if err != nil {
        return "", err
    }
    if count := len(response.Kvs); count == 0 {
        return "", nil
    } else if count > 1 {
        return "", errs.ErrEtcdKVGetResponse.
            GenWithStackByArgs(response.Kvs)
    }
    return string(response.Kvs[0].Value), nil
}

func (kv *etcdKVBase) LoadRange(
    key, endKey string,
    limit int,
) ([]string, []string, error) {
    return kv.LoadRangeWithContext(
        kv.client.Ctx(),
        key,
        endKey,
        limit,
    )
}

func (kv *etcdKVBase) LoadRangeWithContext(
    ctx context.Context,
    key, endKey string,
    limit int,
) (keys, values []string, err error) {
    var options []clientv3.OpOption
    if endKey == "\x00" {
        options = append(options, clientv3.WithPrefix())
    } else {
        options = append(options, clientv3.WithRange(endKey))
    }
    options = append(options, clientv3.WithLimit(int64(limit)))

    response, err := etcdutil.EtcdKVGetWithContext(
        ctx,
        kv.client,
        key,
        options...,
    )
    if err != nil {
        return nil, nil, err
    }
    keys = make([]string, 0, len(response.Kvs))
    values = make([]string, 0, len(response.Kvs))
    for _, item := range response.Kvs {
        keys = append(keys, string(item.Key))
        values = append(values, string(item.Value))
    }
    return keys, values, nil
}
~~~

- [ ] Make CreateRawTxn delegate to CreateRawTxnWithContext and construct the
  new wrapper with NewSlowLogTxnWithContext.

~~~go
func (kv *etcdKVBase) CreateRawTxn() RawTxn {
    return kv.CreateRawTxnWithContext(kv.client.Ctx())
}

func (kv *etcdKVBase) CreateRawTxnWithContext(
    ctx context.Context,
) RawTxn {
    return &rawTxnWrapper{
        inner: NewSlowLogTxnWithContext(ctx, kv.client),
    }
}
~~~

- [ ] Run gofmt on the three files.

~~~sh
gofmt -w \
  pkg/storage/kv/kv.go \
  pkg/storage/kv/etcd_kv.go \
  pkg/storage/kv/kv_test.go
~~~

- [ ] Run the context tests and the existing etcd, memory, and LevelDB tests.
  The latter two prove that expanding opt-in capabilities did not expand
  Base.

~~~sh
make gotest \
  GOTEST_ARGS='./pkg/storage/kv -run "Test(Etcd|LevelDB|MemKV)" -count=1'
~~~

- [ ] Inspect the diff and confirm MemoryKV and LevelDBKV have no new methods.

~~~sh
git diff --check
git diff -- \
  pkg/storage/kv/kv.go \
  pkg/storage/kv/etcd_kv.go \
  pkg/storage/kv/kv_test.go
~~~

- [ ] Stage and commit this task.

~~~sh
git add \
  pkg/storage/kv/kv.go \
  pkg/storage/kv/etcd_kv.go \
  pkg/storage/kv/kv_test.go
git diff --cached --check
git commit -s -m "storage/kv: add context-aware etcd operations"
~~~

## Task 3: Add context-aware GC provider transactions

This task adds the endpoint companions consumed by GC reads. The legacy GC
transaction remains the only path used by writes.

### Files

The endpoint helpers and GC provider methods are changed together so every
new public companion has a storage implementation and test coverage.

- Modify: pkg/storage/endpoint/endpoint.go:45-100
- Modify: pkg/storage/endpoint/gc_states.go:277-500
- Modify: pkg/storage/endpoint/gc_states_test.go:25-265

### Interfaces

The manager will consume these five exact provider methods.

~~~go
func (p GCStateProvider) LoadTxnSafePointWithContext(
    ctx context.Context,
    keyspaceID uint32,
) (uint64, error)

func (p GCStateProvider) LoadGCSafePointWithContext(
    ctx context.Context,
    keyspaceID uint32,
) (uint64, error)

func (p GCStateProvider) LoadAllGCBarriersWithContext(
    ctx context.Context,
    keyspaceID uint32,
) ([]*GCBarrier, error)

func (p GCStateProvider) LoadAllGlobalGCBarriersWithContext(
    ctx context.Context,
) ([]*GlobalGCBarrier, error)

func (p GCStateProvider) RunInGCStateTransactionWithContext(
    ctx context.Context,
    fn func(batch *GCStateWriteBatch) error,
) error
~~~

### Steps

The first tests pin capability rejection and all three I/O stages.

- [ ] Refactor the test constructor without changing existing call sites.
  newEtcdStorageEndpoint delegates to
  newEtcdStorageEndpointWithOptions with a nil option.

~~~go
func newEtcdStorageEndpoint(
    t *testing.T,
) (*StorageEndpoint, func()) {
    return newEtcdStorageEndpointWithOptions(t, nil)
}

func newEtcdStorageEndpointWithOptions(
    t *testing.T,
    option *etcdutil.TestEtcdClusterOptions,
) (*StorageEndpoint, func()) {
    _, client, clean := etcdutil.NewTestEtcdCluster(t, 1, option)
    base := kv.NewEtcdKVBase(client)
    return NewStorageEndpoint(base, nil), clean
}
~~~

- [ ] Add TestGCStateContextTransactionRejectsMissingCapabilities. Cover a
  plain MemoryKV and a contextReaderOnlyBase. Assert the second backend's
  LoadWithContext method is never called, proving both capabilities are
  checked before the revision read.

~~~go
type contextReaderOnlyBase struct {
    kv.Base
    loadCalled atomic.Bool
}

func (b *contextReaderOnlyBase) LoadWithContext(
    context.Context,
    string,
) (string, error) {
    b.loadCalled.Store(true)
    return "", nil
}

func (b *contextReaderOnlyBase) LoadRangeWithContext(
    context.Context,
    string,
    string,
    int,
) ([]string, []string, error) {
    b.loadCalled.Store(true)
    return nil, nil, nil
}

func TestGCStateContextTransactionRejectsMissingCapabilities(
    t *testing.T,
) {
    re := require.New(t)
    memoryEndpoint := NewStorageEndpoint(kv.NewMemoryKV(), nil)
    err := memoryEndpoint.GetGCStateProvider().
        RunInGCStateTransactionWithContext(
            context.Background(),
            func(*GCStateWriteBatch) error {
                return nil
            },
        )
    re.ErrorContains(err, "context-aware reads")

    readerOnly := &contextReaderOnlyBase{
        Base: kv.NewMemoryKV(),
    }
    readerEndpoint := NewStorageEndpoint(readerOnly, nil)
    err = readerEndpoint.GetGCStateProvider().
        RunInGCStateTransactionWithContext(
            context.Background(),
            func(*GCStateWriteBatch) error {
                return nil
            },
        )
    re.ErrorContains(err, "context-aware raw transactions")
    re.False(readerOnly.loadCalled.Load())
}
~~~

- [ ] Add a table-driven TestGCStateContextTransactionCancellation with one
  fresh embedded-etcd client per subtest. Use these exact match conditions:
  initial-revision matches a Range at GCStateRevisionPath; data-read matches
  a Range at TxnSafePointPath for the null keyspace; final-transaction
  matches a Txn whose first comparison key is GCStateRevisionPath.

~~~go
testCases := []struct {
    name    string
    matcher etcdutil.UnaryRPCMatcher
    body    func(
        context.Context,
        GCStateProvider,
        *GCStateWriteBatch,
    ) error
}{
    {
        name: "initial-revision",
        matcher: matchRangeKey(
            keypath.GCStateRevisionPath(),
        ),
        body: func(
            context.Context,
            GCStateProvider,
            *GCStateWriteBatch,
        ) error {
            return nil
        },
    },
    {
        name: "data-read",
        matcher: matchRangeKey(
            keypath.TxnSafePointPath(
                constant.NullKeyspaceID,
            ),
        ),
        body: func(
            ctx context.Context,
            provider GCStateProvider,
            _ *GCStateWriteBatch,
        ) error {
            _, err := provider.LoadTxnSafePointWithContext(
                ctx,
                constant.NullKeyspaceID,
            )
            return err
        },
    },
    {
        name: "final-transaction",
        matcher: matchTxnComparisonKey(
            keypath.GCStateRevisionPath(),
        ),
        body: func(
            context.Context,
            GCStateProvider,
            *GCStateWriteBatch,
        ) error {
            return nil
        },
    },
}
~~~

- [ ] In each cancellation subtest, arm the blocker, call
  RunInGCStateTransactionWithContext in a goroutine, wait for Entered, cancel
  the parent, wait for ContextDone, and assert the returned error equals
  context.Canceled. Use five-second watchdogs for every wait.

- [ ] Add TestGCStateContextReads. Seed a transaction safe point, GC safe
  point, local barrier, and global barrier with legacy write APIs. Read them
  through all four WithContext methods and assert their complete values.

- [ ] Run the new endpoint tests and confirm they fail to compile because the
  context-aware endpoint and provider methods are absent.

~~~sh
make gotest \
  GOTEST_ARGS="./pkg/storage/endpoint \
  -run 'TestGCStateContext' -count=1"
~~~

- [ ] Add private StorageEndpoint capability accessors. These methods perform
  type assertions only; they do not perform storage I/O.

~~~go
func (se *StorageEndpoint) contextReader() (
    kv.ContextReader,
    error,
) {
    reader, ok := se.Base.(kv.ContextReader)
    if !ok {
        return nil, errors.New(
            "storage endpoint does not support context-aware reads",
        )
    }
    return reader, nil
}

func (se *StorageEndpoint) rawTxnCapableWithContext() (
    kv.RawTxnCapableWithContext,
    error,
) {
    capable, ok := se.Base.(kv.RawTxnCapableWithContext)
    if !ok {
        return nil, errors.New(
            "storage endpoint does not support " +
                "context-aware raw transactions",
        )
    }
    return capable, nil
}
~~~

- [ ] Add loadWithContext, loadRangeWithContext, loadJSONWithContext, and
  loadJSONByPrefixWithContext. Each helper uses ContextReader and returns its
  explicit capability error without a contextless fallback.

~~~go
func (se *StorageEndpoint) loadWithContext(
    ctx context.Context,
    key string,
) (string, error) {
    reader, err := se.contextReader()
    if err != nil {
        return "", err
    }
    return reader.LoadWithContext(ctx, key)
}

func loadJSONWithContext[T any](
    ctx context.Context,
    storage *StorageEndpoint,
    key string,
) (T, error) {
    value, err := storage.loadWithContext(ctx, key)
    if err != nil {
        var empty T
        return empty, err
    }
    if value == "" {
        var empty T
        return empty, nil
    }
    var data T
    if err = json.Unmarshal([]byte(value), &data); err != nil {
        return data, errs.ErrJSONUnmarshal.
            Wrap(err).
            GenWithStackByArgs()
    }
    return data, nil
}
~~~

- [ ] Implement LoadTxnSafePointWithContext and
  LoadGCSafePointWithContext with the same parsing and null-keyspace data
  formats as their legacy counterparts. Use storage.loadWithContext for
  scalar reads and return its context error unchanged.

- [ ] Implement LoadAllGCBarriersWithContext with
  loadJSONByPrefixWithContext. Convert every ServiceSafePoint through
  gcBarrierFromServiceSafePoint and preserve the empty-result behavior.

- [ ] Implement LoadAllGlobalGCBarriersWithContext with
  loadJSONByPrefixWithContext at GlobalGCBarrierPrefix. Keep
  LoadAllGlobalGCBarriers unchanged for standalone legacy callers.

- [ ] Implement RunInGCStateTransactionWithContext. Resolve both capabilities
  before the first revision read, use the context-aware reader for that read,
  and use the context-aware raw transaction for commit.

~~~go
func (p GCStateProvider) RunInGCStateTransactionWithContext(
    ctx context.Context,
    fn func(batch *GCStateWriteBatch) error,
) error {
    if err := ctx.Err(); err != nil {
        return err
    }
    reader, err := p.storage.contextReader()
    if err != nil {
        return err
    }
    txnCapable, err := p.storage.rawTxnCapableWithContext()
    if err != nil {
        return err
    }

    revisionKey := keypath.GCStateRevisionPath()
    currentRevision, err := reader.LoadWithContext(ctx, revisionKey)
    if err != nil {
        if contextErr := ctx.Err(); contextErr != nil {
            return contextErr
        }
        return errors.AddStack(err)
    }

    condition, nextRevision, err :=
        prepareGCStateRevision(currentRevision, revisionKey)
    if err != nil {
        return err
    }

    batch := GCStateWriteBatch{}
    if err = fn(&batch); err != nil {
        if contextErr := ctx.Err(); contextErr != nil {
            return contextErr
        }
        return errors.AddStack(err)
    }
    operations := appendGCStateRevision(
        batch.ops,
        revisionKey,
        nextRevision,
    )

    result, err := txnCapable.CreateRawTxnWithContext(ctx).
        If(condition).
        Then(operations...).
        Commit()
    if err != nil {
        if contextErr := ctx.Err(); contextErr != nil {
            return contextErr
        }
        return errs.ErrEtcdTxnInternal.
            Wrap(err).
            GenWithStackByArgs()
    }
    if !result.Succeeded {
        return errs.ErrEtcdTxnConflict.GenWithStackByArgs()
    }
    if len(operations) != len(result.Responses) {
        return errors.Errorf(
            "unexpected number of results: %d != %d",
            len(operations),
            len(result.Responses),
        )
    }
    return nil
}
~~~

- [ ] Extract prepareGCStateRevision and appendGCStateRevision, and make both
  transaction methods use them. Keep the legacy method on contextless Load
  and CreateRawTxn.

~~~go
func prepareGCStateRevision(
    currentRevision, revisionKey string,
) (kv.RawTxnCondition, string, error) {
    condition := kv.RawTxnCondition{
        Key:     revisionKey,
        CmpType: kv.RawTxnCmpNotExists,
    }
    var currentRevisionValue uint64
    if currentRevision != "" {
        condition.CmpType = kv.RawTxnCmpEqual
        condition.Value = currentRevision
        parsed, err := strconv.ParseUint(
            currentRevision,
            10,
            64,
        )
        if err != nil {
            return kv.RawTxnCondition{}, "", errors.AddStack(err)
        }
        currentRevisionValue = parsed
    }
    nextRevision := strconv.FormatUint(
        currentRevisionValue+1,
        10,
    )
    return condition, nextRevision, nil
}

func appendGCStateRevision(
    operations []kv.RawTxnOp,
    revisionKey, nextRevision string,
) []kv.RawTxnOp {
    if len(operations) == 0 {
        return operations
    }
    return append(operations, kv.RawTxnOp{
        Key:    revisionKey,
        OpType: kv.RawTxnOpPut,
        Value:  nextRevision,
    })
}
~~~

- [ ] In both transaction methods, preserve ErrEtcdTxnConflict and the result
  count check. Keep the context-aware method's exact context-error branch
  before ErrEtcdTxnInternal wrapping.

- [ ] Run gofmt on the endpoint files.

~~~sh
gofmt -w \
  pkg/storage/endpoint/endpoint.go \
  pkg/storage/endpoint/gc_states.go \
  pkg/storage/endpoint/gc_states_test.go
~~~

- [ ] Run context tests and the existing transaction and barrier suites.

~~~sh
make gotest \
  GOTEST_ARGS="./pkg/storage/endpoint \
  -run 'Test(GCStateContext|GCStateTransactionACID|.*GCBarrier)' \
  -count=1"
~~~

- [ ] Inspect the diff. Confirm RunInGCStateTransaction still uses legacy
  Load and CreateRawTxn, while the new method has no fallback.

~~~sh
git diff --check
git diff -- \
  pkg/storage/endpoint/endpoint.go \
  pkg/storage/endpoint/gc_states.go \
  pkg/storage/endpoint/gc_states_test.go
~~~

- [ ] Stage and commit this task.

~~~sh
git add \
  pkg/storage/endpoint/endpoint.go \
  pkg/storage/endpoint/gc_states.go \
  pkg/storage/endpoint/gc_states_test.go
git diff --cached --check
git commit -s -m "storage/endpoint: add context-aware GC transactions"
~~~

## Task 4: Propagate context through every manager GC state read

This task connects request and singleflight execution contexts to storage.
It also proves lock release, cache behavior, and combined-result atomicity.

### Files

Changing the public manager signatures requires mechanical updates to every
direct call site in the same commit so the repository remains buildable.

- Modify: pkg/gc/gc_state_manager.go:796-1104
- Modify: pkg/gc/gc_state_manager_test.go
- Modify: server/gc_service.go:690-785
- Modify: server/api/service_gc_safepoint.go:60-68
- Modify: server/apiv2/handlers/safe_point.go:50-70
- Modify: tests/server/api/service_gc_safepoint_test.go:124
- Modify: tests/integrations/client/client_test.go:2182

### Interfaces

The public manager methods now consume a context directly. The all-keyspace
method retains its existing signature.

~~~go
func (m *GCStateManager) GetGCState(
    ctx context.Context,
    keyspaceID uint32,
    excludeGCBarriers bool,
) (GCState, error)

func (m *GCStateManager) GetGCStateWithGlobalGCBarriers(
    ctx context.Context,
    keyspaceID uint32,
    excludeGCBarriers bool,
) (
    GCState,
    []*endpoint.GlobalGCBarrier,
    error,
)
~~~

### Steps

The manager tests use the shared interceptor to prove storage observes
cancellation rather than merely observing an early caller return.

- [ ] Add TestGetGCStateRejectsPreCanceledContext. Seed the cache, pass an
  already canceled context, and assert a zero GCState plus the exact context
  error. This pins the public-entry check before cache access.

- [ ] Add TestGetGCStateCancellationReleasesManagerLock. Build a manager with
  a blocker matching the null keyspace transaction-safe-point Range, start a
  slow-path read, cancel after Entered, and wait for ContextDone.

~~~go
type gcStateReadResult struct {
    state GCState
    err   error
}

ctx, cancel := context.WithCancel(context.Background())
result := make(chan gcStateReadResult, 1)
blocker.Enable()
go func() {
    state, err := manager.GetGCState(
        ctx,
        constant.NullKeyspaceID,
        true,
    )
    result <- gcStateReadResult{state: state, err: err}
}()

waitForBlockerEntry(re, blocker)
cancel()
waitForBlockerContextDone(re, blocker)
readResult := waitForGCStateReadResult(re, result)
re.Equal(context.Canceled, readResult.err)
re.Equal(GCState{}, readResult.state)
_, cached := manager.gcStateCache.load(
    constant.NullKeyspaceID,
)
re.False(cached)
~~~

- [ ] Extend that test by starting AdvanceTxnSafePoint after the canceled read
  returns. Require it to complete within five seconds. Because the blocker is
  one-shot, completion proves the manager RLock was released.

~~~go
writeResult := make(chan error, 1)
go func() {
    _, err := manager.AdvanceTxnSafePoint(
        constant.NullKeyspaceID,
        1,
        time.Now(),
    )
    writeResult <- err
}()
select {
case err := <-writeResult:
    re.NoError(err)
case <-time.After(5 * time.Second):
    re.FailNow("GC state cancellation did not release the manager lock")
}
~~~

- [ ] Add TestGetGCStateChecksContextAfterManagerLock. Use
  getGCStateBeforeSlowPath to hold the read before RLock, acquire m.mu.Lock in
  the test, release the failpoint, cancel while the read waits for RLock, then
  unlock. Assert the exact context error and no cache entry.

- [ ] Add
  TestGetGCStateWithGlobalGCBarriersCancellationReturnsNoPartialResult.
  Match the global-barrier prefix Range, cancel it, and assert GCState is zero,
  the barrier slice is nil, and no cache entry was written.

- [ ] Add table-driven
  TestGetAllKeyspacesGCStatesPropagatesExecutionContext with two cases. The
  null-keyspace case matches its transaction-safe-point Range. The
  per-keyspace case matches keyspace 2. In both cases, cancel the only waiter
  and require blocker.ContextDone to close.

~~~go
testCases := []struct {
    name       string
    keyspaceID uint32
}{
    {
        name:       "null-keyspace",
        keyspaceID: constant.NullKeyspaceID,
    },
    {
        name:       "per-keyspace",
        keyspaceID: 2,
    },
}
~~~

- [ ] In the per-keyspace case, assert that the earlier null-keyspace cache
  entry may remain, while keyspace 2 has no partial cache entry. Start one
  later background GetAllKeyspacesGCStates call and require completion within
  five seconds to prove the canceled execution released the singleflight
  token.

- [ ] Update existing manager tests to pass context.Background to direct
  GetGCState and GetGCStateWithGlobalGCBarriers calls.

- [ ] Run the new manager tests and confirm they fail to compile against the
  old method signatures and contextless implementation.

~~~sh
make gotest \
  GOTEST_ARGS="./pkg/gc \
  -run 'TestGCStateManager/(TestGetGCState.*Context|\
TestGetAllKeyspacesGCStatesPropagatesExecutionContext)' \
  -count=1"
~~~

- [ ] Add context to getGCStateImpl, getGCStateImplSlow, and
  getGCStateInTransaction. Check ctx.Err at getGCStateImpl entry and
  immediately after acquiring m.mu.RLock.

~~~go
func (m *GCStateManager) getGCStateImpl(
    ctx context.Context,
    keyspaceID uint32,
    excludeGCBarriers bool,
) (GCState, error) {
    if err := ctx.Err(); err != nil {
        return GCState{}, err
    }
    if excludeGCBarriers && m.nodeIsLeader() {
        if cached, ok := m.gcStateCache.load(keyspaceID); ok {
            failpoint.InjectCall("getGCStateCacheAccess", "hit")
            gcStateCacheAccessHitCounter.Inc()
            return GCState{
                KeyspaceID:      keyspaceID,
                IsKeyspaceLevel:
                    keyspaceID != constant.NullKeyspaceID,
                TxnSafePoint: cached.TxnSafePoint,
                GCSafePoint:  cached.GCSafePoint,
            }, nil
        }
    }
    return m.getGCStateImplSlow(
        ctx,
        keyspaceID,
        excludeGCBarriers,
    )
}

func (m *GCStateManager) getGCStateImplSlow(
    ctx context.Context,
    keyspaceID uint32,
    excludeGCBarriers bool,
) (GCState, error) {
    failpoint.InjectCall("getGCStateBeforeSlowPath")
    m.mu.RLock()
    defer m.mu.RUnlock()
    if err := ctx.Err(); err != nil {
        return GCState{}, err
    }

    var result GCState
    err := m.gcMetaStorage.
        RunInGCStateTransactionWithContext(
            ctx,
            func(batch *endpoint.GCStateWriteBatch) error {
                var readErr error
                result, readErr = m.getGCStateInTransaction(
                    ctx,
                    keyspaceID,
                    excludeGCBarriers,
                    batch,
                )
                return readErr
            },
        )
    if err != nil {
        return GCState{}, err
    }

    if excludeGCBarriers {
        m.gcStateCache.store(
            keyspaceID,
            gcStateCacheEntry{
                TxnSafePoint: result.TxnSafePoint,
                GCSafePoint:  result.GCSafePoint,
            },
        )
    }
    return result, nil
}
~~~

- [ ] In getGCStateInTransaction, call the three WithContext provider reads
  with the same ctx. Keep gc_worker barrier filtering unchanged.

~~~go
result.TxnSafePoint, err =
    m.gcMetaStorage.LoadTxnSafePointWithContext(
        ctx,
        keyspaceID,
    )
result.GCSafePoint, err =
    m.gcMetaStorage.LoadGCSafePointWithContext(
        ctx,
        keyspaceID,
    )
result.GCBarriers, err =
    m.gcMetaStorage.LoadAllGCBarriersWithContext(
        ctx,
        keyspaceID,
    )
~~~

- [ ] Change GetGCState to accept ctx, reject an ended context before
  redirectKeyspace, and pass ctx to getGCStateImpl. Emit the existing error
  log only when ctx.Err is nil.

~~~go
if err != nil && ctx.Err() == nil {
    log.Error(
        "failed to get GC state",
        zap.Uint32("keyspace-id", keyspaceID),
        zap.String("keyspace-name", keyspaceName),
        zap.Bool("exclude-gc-barriers", excludeGCBarriers),
        zap.Error(err),
    )
}
~~~

- [ ] Change GetGCStateWithGlobalGCBarriers to accept ctx. Check ctx at public
  entry and after RLock, call RunInGCStateTransactionWithContext, pass ctx to
  getGCStateInTransaction, and call
  LoadAllGlobalGCBarriersWithContext. Return zero and nil for every failure.

- [ ] Keep the combined-read cache store after a successful transaction. Do
  not inspect ctx after commit. Guard its existing error log with
  ctx.Err() == nil.

- [ ] Pass iterateAllKeyspacesGCStates ctx to both getGCStateImpl calls: the
  null-keyspace read and each selected keyspace read. This ctx is already the
  OrderedSingleFlight body's execCtx at both call sites.

~~~go
nullState, err := m.getGCStateImpl(
    ctx,
    constant.NullKeyspaceID,
    excludeGCBarriers,
)

gcState, err := m.getGCStateImpl(
    ctx,
    keyspaceMeta.GetId(),
    excludeGCBarriers,
)
~~~

- [ ] Update server/gc_service.go to pass its ctx to both manager single-read
  methods. Update the two HTTP handlers to pass r.Context and
  c.Request.Context. Do not change API error mapping in this task.

- [ ] Update the two non-manager-test direct callers with
  context.Background. Use rg to prove no old arity remains.

~~~sh
rg -n \
  'GetGCState\(|GetGCStateWithGlobalGCBarriers\(' \
  --glob '*.go'
~~~

- [ ] Run gofmt on every touched Go file.

~~~sh
gofmt -w \
  pkg/gc/gc_state_manager.go \
  pkg/gc/gc_state_manager_test.go \
  server/gc_service.go \
  server/api/service_gc_safepoint.go \
  server/apiv2/handlers/safe_point.go \
  tests/server/api/service_gc_safepoint_test.go \
  tests/integrations/client/client_test.go
~~~

- [ ] Run all manager tests, including existing cache, conflict, and ordered
  singleflight cases.

~~~sh
make gotest GOTEST_ARGS='./pkg/gc -count=1'
~~~

- [ ] Run the existing OrderedSingleFlight cancellation tests without
  changing their implementation.

~~~sh
make gotest \
  GOTEST_ARGS="./pkg/utils/syncutil \
  -run TestOrderedSingleFlightCancellation -count=1"
~~~

- [ ] Compile and run direct server and integration call-site packages.

~~~sh
make gotest \
  GOTEST_ARGS="./server ./tests/server/api \
  ./tests/server/apiv2/handlers ./tests/integrations/client \
  -run '^$' -count=1"
~~~

- [ ] Inspect the diff and confirm the standalone manager
  LoadAllGlobalGCBarriers method and all GC write methods remain
  contextless.

~~~sh
git diff --check
git diff -- \
  pkg/gc/gc_state_manager.go \
  pkg/gc/gc_state_manager_test.go \
  server/gc_service.go \
  server/api/service_gc_safepoint.go \
  server/apiv2/handlers/safe_point.go
~~~

- [ ] Stage and commit this task.

~~~sh
git add \
  pkg/gc/gc_state_manager.go \
  pkg/gc/gc_state_manager_test.go \
  server/gc_service.go \
  server/api/service_gc_safepoint.go \
  server/apiv2/handlers/safe_point.go \
  tests/server/api/service_gc_safepoint_test.go \
  tests/integrations/client/client_test.go
git diff --cached --check
git commit -s -m "gc: cancel GC state reads"
~~~

## Task 5: Preserve cancellation semantics at API boundaries

This task makes transport behavior explicit after the storage and manager
chain can return caller context errors.

### Files

The gRPC service gains a focused unit test. The HTTP handlers keep their
existing response formats for active requests.

- Modify: server/gc_service.go:690-790
- Create: server/gc_service_test.go
- Modify: server/api/service_gc_safepoint.go:60-70
- Modify: server/apiv2/handlers/safe_point.go:50-72
- Verify: tests/server/gc/gc_test.go
- Verify: tests/server/api/service_gc_safepoint_test.go
- Verify: tests/server/apiv2/handlers/safe_point_test.go

### Interfaces

The new helper is package-private and converts only caller-context errors.

~~~go
func contextErrorToGRPCStatus(err error) error
~~~

### Steps

The unit test pins transport codes before the handlers are changed.

- [ ] Create server/gc_service_test.go with a table-driven test for exact and
  wrapped context errors, plus a non-context error.

~~~go
func TestContextErrorToGRPCStatus(t *testing.T) {
    testCases := []struct {
        name string
        err  error
        code codes.Code
    }{
        {
            name: "canceled",
            err:  context.Canceled,
            code: codes.Canceled,
        },
        {
            name: "wrapped-canceled",
            err: fmt.Errorf(
                "read GC state: %w",
                context.Canceled,
            ),
            code: codes.Canceled,
        },
        {
            name: "deadline",
            err:  context.DeadlineExceeded,
            code: codes.DeadlineExceeded,
        },
        {
            name: "wrapped-deadline",
            err: fmt.Errorf(
                "read GC state: %w",
                context.DeadlineExceeded,
            ),
            code: codes.DeadlineExceeded,
        },
        {
            name: "storage-error",
            err:  errors.New("storage failed"),
            code: codes.OK,
        },
    }

    for _, testCase := range testCases {
        t.Run(testCase.name, func(t *testing.T) {
            statusErr := contextErrorToGRPCStatus(testCase.err)
            if testCase.code == codes.OK {
                require.NoError(t, statusErr)
                return
            }
            require.Equal(t, testCase.code, status.Code(statusErr))
        })
    }
}
~~~

- [ ] Run the new unit test and confirm it fails because the helper does not
  exist.

~~~sh
make gotest \
  GOTEST_ARGS='./server -run TestContextErrorToGRPCStatus -count=1'
~~~

- [ ] Implement contextErrorToGRPCStatus with standard-library errors.Is and
  status.FromContextError. Return nil for every non-context error.

~~~go
func contextErrorToGRPCStatus(err error) error {
    if errors.Is(err, context.Canceled) ||
        errors.Is(err, context.DeadlineExceeded) {
        return status.FromContextError(err).Err()
    }
    return nil
}
~~~

- [ ] In GrpcServer.GetGCState, return nil plus the status error when the
  helper recognizes cancellation. Keep the existing response-header path for
  every other manager error.

~~~go
if err != nil {
    if statusErr := contextErrorToGRPCStatus(err); statusErr != nil {
        return nil, statusErr
    }
    return &pdpb.GetGCStateResponse{
        Header: grpcutil.WrapErrorToHeader(
            pdpb.ErrorType_UNKNOWN,
            err.Error(),
        ),
    }, nil
}
~~~

- [ ] Apply the same branch to
  GrpcServer.GetAllKeyspacesGCStates. Do not change the later standalone
  LoadAllGlobalGCBarriers call.

- [ ] In serviceGCSafepointHandler.GetGCSafePoint, retain the request
  parameter and capture requestCtx before the manager call. If the manager
  returns an error and requestCtx.Err is non-nil, return without calling
  render.JSON. Otherwise retain the current HTTP 500 response.

~~~go
func (h *serviceGCSafepointHandler) GetGCSafePoint(
    writer http.ResponseWriter,
    request *http.Request,
) {
    requestCtx := request.Context()
    manager := h.svr.GetGCStateManager()
    gcState, err := manager.GetGCState(
        requestCtx,
        constant.NullKeyspaceID,
        false,
    )
    if err != nil {
        if requestCtx.Err() != nil {
            return
        }
        h.rd.JSON(
            writer,
            http.StatusInternalServerError,
            err.Error(),
        )
        return
    }
~~~

- [ ] In the Gin LoadGCSafePoint handler, capture
  c.Request.Context before the manager call. On manager error, return without
  AbortWithStatusJSON when that context ended; retain HTTP 500 otherwise.

~~~go
requestCtx := c.Request.Context()
gcState, err := manager.GetGCState(
    requestCtx,
    uint32(keyspaceID),
    true,
)
if err != nil {
    if requestCtx.Err() != nil {
        return
    }
    c.AbortWithStatusJSON(
        http.StatusInternalServerError,
        err.Error(),
    )
    return
}
~~~

- [ ] Run gofmt on the four changed or created service files.

~~~sh
gofmt -w \
  server/gc_service.go \
  server/gc_service_test.go \
  server/api/service_gc_safepoint.go \
  server/apiv2/handlers/safe_point.go
~~~

- [ ] Run the gRPC status unit test and existing GC API regression tests.

~~~sh
make gotest \
  GOTEST_ARGS="./server ./tests/server/gc \
  -run 'Test(ContextErrorToGRPCStatus|GetGCState|GCState)' \
  -count=1"
~~~

- [ ] Run both HTTP safe-point regression suites. Their success responses and
  JSON payloads must remain unchanged.

~~~sh
make gotest \
  GOTEST_ARGS="./tests/server/api \
  ./tests/server/apiv2/handlers \
  -run 'Test(ServiceGCSafepointTestSuite|SafePointTestSuite)' \
  -count=1"
~~~

- [ ] Inspect the diff and confirm context errors use transport status while
  non-context errors still use the existing gRPC header or HTTP 500.

~~~sh
git diff --check
git diff -- \
  server/gc_service.go \
  server/gc_service_test.go \
  server/api/service_gc_safepoint.go \
  server/apiv2/handlers/safe_point.go
~~~

- [ ] Stage and commit this task.

~~~sh
git add \
  server/gc_service.go \
  server/gc_service_test.go \
  server/api/service_gc_safepoint.go \
  server/apiv2/handlers/safe_point.go
git diff --cached --check
git commit -s -m "server: preserve GC read cancellation status"
~~~

## Task 6: Verify the complete cancellation chain

This task does not add planned behavior. It validates the complete branch,
repository hygiene, and the scope exclusions before handoff.

### Files

Verification covers every file changed by Tasks 1 through 5 and confirms no
generated or unrelated file entered the diff.

- Verify: all files listed in the file map
- Verify: go.mod and go.sum remain unchanged
- Verify: pkg/storage/kv/leveldb_kv.go remains unchanged
- Verify: pkg/storage/kv/memory_kv.go remains unchanged
- Verify: pkg/utils/syncutil/ordered_single_flight.go remains unchanged

### Steps

These commands are ordered from focused behavior to repository-wide checks.

- [ ] Ensure failpoint-generated code is absent before verification.

~~~sh
make failpoint-disable
git status --short
~~~

- [ ] Run all focused packages in one failpoint-aware test invocation.

~~~sh
make gotest \
  GOTEST_ARGS="./pkg/utils/etcdutil ./pkg/storage/kv \
  ./pkg/storage/endpoint ./pkg/gc ./pkg/utils/syncutil \
  ./server ./tests/server/gc ./tests/server/api \
  ./tests/server/apiv2/handlers -count=1"
~~~

- [ ] Run the repository basic test target.

~~~sh
make basic-test
~~~

- [ ] Run module, formatting, static-analysis, and generated-error-document
  checks.

~~~sh
make check
~~~

- [ ] Confirm the modules are clean and no new dependency was introduced.

~~~sh
git diff --exit-code -- go.mod go.sum
~~~

- [ ] Scan every GC state manager call and every new context API to confirm
  signatures and call direction.

~~~sh
rg -n \
  "GetGCState\(|GetGCStateWithGlobalGCBarriers\(|\
RunInGCStateTransactionWithContext|Load.*WithContext|\
CreateRawTxnWithContext|EtcdKVGetWithContext" \
  --glob '*.go'
~~~

- [ ] Confirm the contextless APIs remain present for compatibility.

~~~sh
rg -n \
  "^func .*EtcdKVGet\(|^func .*CreateRawTxn\(|\
^func .*RunInGCStateTransaction\(" \
  pkg/utils/etcdutil \
  pkg/storage/kv \
  pkg/storage/endpoint
~~~

- [ ] Confirm no fallback calls contextless Load, LoadRange, or CreateRawTxn
  from RunInGCStateTransactionWithContext.

~~~sh
sed -n \
  '/RunInGCStateTransactionWithContext/,/^}/p' \
  pkg/storage/endpoint/gc_states.go
~~~

- [ ] Review the full branch diff and run whitespace validation.

~~~sh
git diff --check upstream/master...HEAD
git diff --stat upstream/master...HEAD
git diff upstream/master...HEAD
~~~

- [ ] Confirm only this plan and the known unrelated paths remain untracked.
  Confirm no failpoint artifacts, coverage files, or test binaries were
  created.

~~~sh
git status --short --branch
~~~

- [ ] If verification required a code correction, stage only the affected
  implementation files and create a signed, package-scoped fix commit. If no
  correction was needed, leave the five task commits unchanged.

## Next steps

After every checkbox passes, use the branch-finishing workflow to choose
between opening a pull request, merging locally, or keeping the branch for
additional review. The pull request must reference issue 11131 and describe
the compatibility boundaries, deterministic cancellation tests, and the
absence of fallback for unsupported storage backends.
