# GC 缓存批量加载与预热 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 issue #11311 的独立方案，降低并发冷读重复和 leader 切换后的首次加载开销。

**Architecture:** storage 提供一次事务读取多个 safe-point pair 的能力。Metadata 索引按固定 revision 并行发现初始候选；manager 用 generation、flight 和读锁协调前台加载与四并发后台预热。实现不包含 WatchGCStates RPC。

**Tech Stack:** Go 1.26+、etcd/client v3.5.15、现有 PD cache/RawTxn、testify、failpoint。

**Spec:** [中文设计](../../analysis/gc-cold-loads-11311.md)，已经完成 review 并获得用户确认。

## Global Constraints

- 所有 sub-agent 顺序执行；一个任务实现、测试和 review 完成后，才能派发下一任务。Sub-agent 不得自行委派。
- 工作区为 `/home/wenxuan/dev/pingcap/pd/.wt/issue-11311-gc-cache-warmup-impl`，分支为 `issue-11311-gc-cache-warmup-impl`，基础提交为 `3917c36a337bfa3dcd8df7baba83e33865710a35`。
- 元数据索引及主动预热仅 NextGen；通用批读和前台去重不依赖索引或 Watch API。
- Metadata 首页最多 256 条，与可选 H 同一 Txn，顶层 Header.Revision 得到 R；其余扫描固定 R。
- Metadata 任务按 4096 ID 滚动派发，4 个 worker；候选上界 b >= H 时当前任务扩展至 prefixEnd；H 不限制成员范围。
- GC 每批最多 60 个实际未缓存且由本批取得 flight 的 scope，最多 120 个 Get，一个事务；4 个后台 worker，包括 null singleton 与重试。
- 共用 fill 入口只做一次缓存检查，预热不增加缓存预筛选；过滤后从现有候选补齐，不等待未来页。
- Null scope 优先独立预热，与 metadata 并行，使用四个后台执行名额之一。
- 每批 manager RLock 覆盖读取至发布，批间释放；不持 flight 锁等待 manager，不持 manager 等待其他 flight、队列或执行容量。
- 预热仅针对初始任务；后台以 flight 终态记录完成，后续失效不重做；实时 metadata 变化不扩张目标。
- 缺失 key 与已存在空字符串沿用零值；非空解码错误不伪造零值，损坏 scope 不阻塞健康 scope。
- Active generation 控制缓存资格；reset 前禁读，取消 retiring generation 后再等 manager 写锁；旧清理和 flight 不能修改新任期。
- 保留 barrier-inclusive、global-barrier 联合校验、follower/legacy 读取与 OrderedSingleFlight 语义。
- 使用相同模型层级和 reasoning effort；不得擅自切换模型。遵守 AGENTS.md、failpoint 启停、DCO 和提交格式；不修改依赖版本。
- 不运行性能实验，不新增相关计划；不引入公开配置项、协议变化或 Watch 广播。

## Review Focus

以下跨模块风险必须有对应任务的测试证据：

- 已存在但值为空的 safe point 在新旧 reader 中均返回零：Task 1。
- H 缺失仍取得可信 R；高位 SYSTEM 与显式 ID 不因 H 或第一页裁剪被遗漏：Task 2。
- Cache miss 与旧 flight 结束交错仍不产生重复读；foreground 与后台共享容量规则不同：Tasks 3、4。
- 旧任期延迟清理、重置中快读和取消时等锁不污染新缓存或死锁：Task 3。
- Flight 成功后缓存先失效、后台后醒来时仍只预热一次；队列溢出和 metadata 重建不引入增量目标：Task 4。

## 验证环境与执行规则

使用 `GOTOOLCHAIN=auto` 选择满足 go.mod 的已安装工具链。优先复用根仓库的 `.tools/bin`，通过 make 的 `GO_TOOLS_BIN_PATH` 参数指定，不修改工具版本。测试命令中的目标包范围限制编译和执行范围；最终仍执行受影响包的 basic-test、race/NextGen 回归及 make check。

每次 Go 测试使用 `make gotest` 或其他自动启停 failpoint 的 make 目标。不要在 failpoints 启用时编辑代码、运行 git 或非测试命令；测试异常退出后先恢复。任务报告保存完整命令、退出码、RED/GREEN 摘要和实际测试输出位置。发现环境问题时向主 agent 报告证据，由主 agent 处理，不降低 Go 版本或绕开检查。

每个任务从主 agent 记录的 BASE 开始，完成后提交本任务代码并报告 SHA。修改文档和计划由主 agent 负责。不得 push、创建 PR 或修改其他 worktree。接口调整必须报告给主 agent，更新后续任务后继续；不默默改变已经确认的语义。

### Task 1: 可取消的 safe-point 批读

**Files:**

- Modify: `pkg/storage/kv/kv.go`、`pkg/storage/kv/etcd_kv.go`、`pkg/storage/endpoint/endpoint.go`、`pkg/storage/endpoint/gc_states.go`。
- Create: `pkg/storage/endpoint/gc_state_batch.go`、`pkg/storage/endpoint/gc_state_batch_test.go`。
- Test: `pkg/storage/kv/kv_test.go` 及现有 endpoint 测试。

**Interfaces:**

- 新增可选能力 `kv.RawTxnWithContextCapable`：`CreateRawTxnWithContext(context.Context) kv.RawTxn`；保留 `RawTxnCapable.CreateRawTxn()`。
- 新增 endpoint 内部 `createRawTxnWithContext(ctx context.Context) (kv.RawTxn, error)`；明确不支持能力时返回错误，不能静默丢弃取消。
- 新增 `endpoint.GCSafePointReadResult`，字段为 `KeyspaceID uint32`、`TxnSafePoint uint64`、`GCSafePoint uint64`、`Err error`。
- 新增 `GCStateProvider.LoadGCSafePointPairs(ctx context.Context, keyspaceIDs []uint32) ([]GCSafePointReadResult, error)`。返回值按输入顺序；批级读取/响应错误由外层 error 表示，局部解码错误由对应结果 Err 表示。
- 暴露 `MaxGCSafePointBatchSize = etcdutil.MaxEtcdTxnOps / 2`，供 manager 使用。

- [ ] **Step 1: 编写失败测试。** `TestLoadGCSafePointPairs` 覆盖 null/独立 scope、缺失和已存在空值、混合三种编码、部分非空畸形值；`TestLoadGCSafePointPairsBatchBounds` 覆盖空输入零 RPC、60 scope 一次事务、超限及重复 ID 拒绝；context 测试验证已取消及阻塞读取后取消能够返回。
- [ ] **Step 2: 运行 RED。** 使用 `GOTOOLCHAIN=auto make gotest GO_TOOLS_BIN_PATH=/home/wenxuan/dev/pingcap/pd/.tools/bin GOTEST_ARGS='./pkg/storage/kv ./pkg/storage/endpoint -run "TestLoadGCSafePointPairs|TestRawTxnWithContext" -count=1'`，记录缺少能力或行为导致的失败。
- [ ] **Step 3: 实现接口。** etcd context 原始事务复用已有 `NewSlowLogTxnWithContext`；一次事务包含两个精确 Get/scope。复用或提取旧 reader 的纯解码帮助函数，保留空字符串零值；校验结果条数、key 映射及读取结果形状。
- [ ] **Step 4: GREEN 与包回归。** 运行新测试，再运行 kv/endpoint 两个包全部测试。用存储 RPC 观测证实一批一个 Txn、没有额外 GC revision Get 或比较；测试必须检查实际响应语义而不是只镜像组装代码。
- [ ] **Step 5: 自查、格式化与提交。** 确认无依赖变化、failpoints 已关闭，以 `storage: batch GC safe point reads` 类似主题签名提交，并报告接口及验证证据。

### Task 2: 抽取并并行加载 enabled-keyspace 索引

**Files:**

- Create: `pkg/gc/enabled_keyspace_cache.go`、`pkg/gc/enabled_keyspace_cache_test.go`、可按扫描职责拆出 `pkg/gc/enabled_keyspace_loader.go`。
- Source: `/home/wenxuan/dev/pingcap/pd/.wt/wfxr/watch-gc-states/pkg/gc/enabled_keyspace_cache.go` 与测试，仅抽取该能力。

**Interfaces:**

- 保留 `enabledKeyspace`、`appliedRevision()`、`waitReady(ctx)`、`snapshotAtLeast(ctx, revision)` 的行为。
- 构造函数为 `newEnabledKeyspaceCache(client *clientv3.Client, prefix string) *enabledKeyspaceCache`；将 context 传入 `run(ctx context.Context, hooks enabledKeyspaceLoadHooks)`、load/watch 辅助方法，避免新增持有 context 的 struct 字段。
- `enabledKeyspaceLoadHooks` 有 `onPage func([]enabledKeyspace)` 和 `onInitialSnapshot func([]enabledKeyspace)`。只在首次成功快照前的页面及首次完整发布时调用，运行于 index mutex 外；调用方保证不等待 GC I/O。
- 页和快照回调获得不可变值副本；不依赖后续复用的可变 map/slice。后续 reload 不再触发初始回调。

- [ ] **Step 1: 抽取基线并添加失败测试。** 先保留源索引分页/watch/progress/compaction 测试；新增首页 Txn、H 缺失/畸形、More=false、4096 边界、高位 ID、固定 R、四并发及取消测试。现有 Get 拦截点需适配首页改为 Txn。
- [ ] **Step 2: 运行 RED。** `GOTOOLCHAIN=auto make gotest GO_TOOLS_BIN_PATH=/home/wenxuan/dev/pingcap/pd/.tools/bin GOTEST_ARGS='./pkg/gc -run "TestEnabledKeyspace" -count=1'`，记录新增约束未满足的证据。
- [ ] **Step 3: 实现首页与扫描。** 一个 Txn 同时读取最多 256 个 metadata 和 allocator key，从 Header 得到 R；保持原始续读 cursor。滚动派发4096-ID任务，b>=H扩当前区间至prefixEnd；H不可用顺序扫描。生产者和四 worker 共享可取消尝试生命周期，所有任务成功再发布。
- [ ] **Step 4: 实现初始通知。** 各页完整解码后通知，初始完整快照固化通知一次；始终维护 all-ENABLED index。读取、channel 等待及回调不持 metadata mutex。保留 watch 的 pending/progress 规则。
- [ ] **Step 5: GREEN 与索引回归。** 运行全部 `TestEnabledKeyspace*`，覆盖请求路由落后 follower 且 H 缺失、写入发生在 R 之后、满 channel 取消、慢任务继续派工。确认无 WatchGCStates 类型或协议依赖。
- [ ] **Step 6: 自查、格式化与提交。** 签名提交 `gc: load the enabled keyspace index concurrently` 类似主题，报告 callback、构造函数及运行接口供后续使用。

### Task 3: 共享冷读协调与任期隔离

**Files:**

- Modify: `pkg/gc/gc_state_manager.go`、`pkg/gc/gc_state_manager_test.go`、`server/cluster/cluster.go`。
- Create: `pkg/gc/gc_state_loader.go`、`pkg/gc/gc_state_loader_test.go`；生命周期复杂时可独立为 `pkg/gc/gc_state_lifecycle.go`。

**Interfaces:**

- 消费 Task 1 的 `LoadGCSafePointPairs` 与批次容量。
- `OnNodeBecomesLeader() func()` 返回 generation-bound 幂等清理函数；cluster 保存返回值。缓存资格由 active generation 决定。
- 为 Task 4 提供两阶段内部协议：`prepareGCStateLoadBatch` 在同一 goroutine 取得 manager RLock、检查缓存/flight 并从非阻塞候选源取得最多60个读取权；`executeGCStateLoadBatch` 执行并完成发布、flight终结和RUnlock。具体 batch/result 类型由本任务定义并在报告中列明；主 agent 在 Task 4 派发前据此固定其消费接口。
- Batch 结果必须分别表达已完成 scope、失败 scope 和正在由其他 flight 加载的 scope；flight 的终态成功通知不依赖缓存仍存在。
- 新前台入口通过上述协议加载 singleton，加入已有 flight 时在释放 manager 等锁后等待。Follower/legacy/barrier-inclusive 保留原分支。

- [ ] **Step 1: 写去重与生命周期失败测试。** 同一scope并发冷读只做一次事务；不同scope可重叠；foreground加入flight；取消、读失败唤醒；cache-miss/flight-retire交错；旧cleanup延迟和重复调用；reset暂停时禁用缓存快读；旧flight完成不能删除新flight。
- [ ] **Step 2: 运行 RED。** `GOTOOLCHAIN=auto make gotest GO_TOOLS_BIN_PATH=/home/wenxuan/dev/pingcap/pd/.tools/bin GOTEST_ARGS='./pkg/gc -run "TestGCState.*(Load|Leadership)|TestGCStateManager/(TestGetGCState|TestGetAllKeyspaces)" -count=1'`，记录真实行为失败。
- [ ] **Step 3: 实现 generation 与取消。** 参考 #11264 的 active generation 和 reset gating，移除 Watch 专有内容；先按 generation 禁用并取消，再等 manager 写锁清理；任务和上下文的生命周期不能因单个等待者退出而终止其他等待者需要的加载。
- [ ] **Step 4: 实现两阶段 fill。** lock顺序为 manager→flight→shard；一次缓存检查与claim同一临界区，完成也用flight锁。I/O前释放flight锁，manager读锁至发布才释放，完成不依赖后台assembly。维持快读、cache指标/既有hook可用性及外层API语义。
- [ ] **Step 5: GREEN 与原有GC回归。** 运行 loader/lifecycle新测试及完整pkg/gc测试，覆盖global barriers联合校验、follower直接读、OrderedSingleFlight和失败写入失效。按需补race验证真实交错，避免仅验证mock调用。
- [ ] **Step 6: 自查、提交与接口报告。** 签名提交，详细报告prepare/execute、generation、flight终态接口和锁所有权，供Task4使用。

### Task 4: 初始批量预热与 NextGen 集成

**Files:**

- Create: `pkg/gc/gc_state_warmup.go`、`pkg/gc/gc_state_warmup_test.go`。
- Modify: `pkg/gc/gc_state_manager.go` 及生命周期文件、`server/server.go`，必要的GC与server/cluster集成测试。
- 不修改 WatchGCStates 协议、client 或广播实现。

**Interfaces:**

- 消费 Task 2 的 `newEnabledKeyspaceCache(client *clientv3.Client, prefix string)` 与 `run(ctx context.Context, hooks enabledKeyspaceLoadHooks)`。`onPage func([]enabledKeyspace)` 可由扫描 worker 并发调用、初始失败重试时可重复；`onInitialSnapshot func([]enabledKeyspace)` 在首次完整发布后调用一次。两者获得值副本，后续 reload 不再调用；回调不能等待 GC I/O、assembly 锁或 manager 锁。索引 ready 不代表初始回调已处理完毕。
- 消费 Task 3 的 `prepareGCStateLoadBatch(ctx context.Context, generation *gcStateGeneration, next func() (uint32, bool)) (*gcStateLoadBatch, error)` 与 `executeGCStateLoadBatch(ctx context.Context, batch *gcStateLoadBatch) gcStateLoadResult`。`next` 非阻塞消费当前候选；prepare 出错时不持锁，非空 batch 保留 manager RLock，空 batch 已释放。每个成功返回的 batch 在同一 goroutine 恰好执行一次 execute，assembly 在 execute 前释放，execute 返回后才处理等待或调度。
- `gcStateLoadResult` 的字段为 `completed []uint32`、`failed map[uint32]error`、`joined map[uint32]*gcStateLoadFlight`。execute 返回后读取完整结果；`flight.done` 关闭后才可读取不可变 `flight.err`，也可用 `flight.wait(ctx)`。后台 nil err 直接记完成，不检查缓存是否仍存在，不为每个 joined scope 无界创建等待 goroutine。
- `gcStateGeneration` 已提供唯一身份、`done` 退休信号和执行批次取消登记；`activeGeneration` 为缓存资格门禁。Task 4 补充 metadata/warmup 生命周期，context 保留在运行函数中，先取消再等 manager 写锁，等待 worker 退出时不持其需要的锁。详细所有权协议见 [loader 注释](../../../pkg/gc/gc_state_loader.go)。
- 增加或抽取 `GCStateManager.SetEtcdClient(client *clientv3.Client)`，在首个generation启动前由NextGen分支配置；非NextGen不启用metadata/预热。
- `gcStateWarmup` 拥有有界页提示队列、初始目标/完成状态、共享assembly及四worker调度；运行context由生命周期传入，generation取消会终止所有owned work。

- [ ] **Step 1: 写预热失败测试。** null优先且占四容量之一；metadata后页暂停时前页可预热；四batch重叠且不超过四；过滤后120候选60miss一Txn；256missing五Txn；尾批不等新页；foreground不等后台执行容量。
- [ ] **Step 2: 写恢复与范围测试。** 满队列遗漏由初始快照补齐；局部损坏/传输失败退避且健康scope继续；后台join成功后缓存先失效仍记completed；后续新增/启用/mode change/reload不预热，原pending可恢复；取消满队列和writer排队无死锁。
- [ ] **Step 3: 运行 RED。** `GOTOOLCHAIN=auto make gotest GO_TOOLS_BIN_PATH=/home/wenxuan/dev/pingcap/pd/.tools/bin GOTEST_ARGS='./pkg/gc -run "TestGCStateWarmup" -count=1'`，保留失败证据。
- [ ] **Step 4: 实现并衔接。** 初始null独立singleton与metadata并行；页提示不阻塞索引；初始完整目标固定，completed独立于cache。先取得后台容量再assembly/prepare，I/O前释放assembly，execute同goroutine释放manager锁；重试复用同一路径。
- [ ] **Step 5: NextGen wiring和退出。** 服务初始化只在NextGen传入etcd client；generation启动两个pool并管理取消。成功或不再需要的初始目标完成后退出GCpool并释放临时集合，metadata继续watch。
- [ ] **Step 6: GREEN与集成验证。** 运行新测试、完整GC/存储受影响包测试，普通与NextGen/deadlock/race组合；验证server/cluster callback构建和生命周期。失败时修正实际实现并重测受影响范围。
- [ ] **Step 7: 自查与提交。** 签名提交完整集成，报告测试命令、结果、资源退出、剩余风险；不得以未完成的todo或禁用测试交付。

## 最终集成门槛

所有任务完成并经各自review后，主 agent 组织一次覆盖整个分支的独立review，并处理真实发现。运行受影响包的 `make basic-test`、race及NextGen回归，执行 `make check`；若环境或基线导致失败，保留完整证据、隔离原因并尽可能修复，不将未运行的检查报告为通过。

最终核对 spec 第13节验收矩阵，确认无依赖漂移、无failpoint生成物、无无关改动。保持独立分支及可review提交，按用户授权的独立PR流程继续处理。
