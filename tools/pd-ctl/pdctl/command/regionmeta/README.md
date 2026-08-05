# PD region meta consistency check

`pd-ctl region meta-consistency` 检查同一 PD 集群中 Leader 与各 Follower 本地缓存的 region meta 是否一致。命令直接访问每个 PD 实例，以小批量、同批跨实例并发、单实例串行和全局限速方式扫描，输出一个 JSON 报告。

本命令使用 `/members`、`/regions/count`、`/regions/key` 和 `/region/id/{id}` API，并依赖 Follower 本地读取 Header。兼容性测试覆盖当前 master 和 [`v8.5.4-20260625-8b1b130`](https://github.com/tikv/pd/releases/tag/v8.5.4-20260625-8b1b130)；用于其他版本前，应确认这些接口的行为兼容。

> 全量检查会增加 PD 的 HTTP 处理、JSON 序列化、网络流量和短时 Region tree 读锁开销。默认参数用于限制这些开销；如果生产要求严格零扰动，不应执行本命令。

## 快速执行

建议从同 VPC 的独立运维机执行，并为每轮检查使用新的输出文件：

```bash
report="region-meta-report-$(date -u +%Y%m%dT%H%M%SZ).json"
set +e
pd-ctl -u http://10.0.0.1:2379 region meta-consistency \
  --output "$report"
rc=$?
set -e

printf 'exit_code=%s report=%s\n' "$rc" "$report"
test ! -s "$report" || jq '{status, summary, confirmation, nodes}' "$report"
```

一个 URL 仅作为 seed。命令通过 `/pd/api/v1/members` 发现成员，并直连每个成员公布的 `client_urls`；不要使用负载均衡地址代替成员直连地址。

| 退出码 | 状态 | 含义 |
| ---: | --- | --- |
| `0` | `consistent` | 本轮没有保留差异。 |
| `1` | `inconsistent` | 检查成功，并确认至少一个稳定差异。 |
| `2` | `incomplete` 或无新报告 | 证据不足或执行失败；检查 stderr，不要沿用旧报告。 |
| `130` | 无新报告 | 用户通过 Ctrl-C 中断。 |

退出码 `1` 表示发现不一致，不是命令执行失败。

## 执行条件

- 集群至少有两个 PD 成员；执行机能直连所有成员公布的 `client_urls`。
- Follower Region Syncer 正常，能够处理携带 `PD-Allow-Follower-Handle: true` 的本地读取请求。
- 在业务低峰执行，PD CPU、内存、业务延迟和现有告警均有明确余量。
- 扫描期间不安排 PD 重启、扩缩容或主动 Leader 切换。
- `--work-dir` 与输出目录已存在、可写且空间充足；报告按内部诊断数据保护。

推荐独立检查机至少为 `1 vCPU / 512 MiB RAM / 3 GiB 可用磁盘 / 100 Mbps`；建议配置为 `2 vCPU / 1 GiB RAM / 5 GiB 可用磁盘`。

## 检查内容

| 类别 | 比较字段 |
| --- | --- |
| Region | Region ID |
| Key Range | `start_key`、`end_key` |
| Epoch | `conf_ver`、`version` |
| Peers | `id`、`store_id`、`role`、`is_witness` |
| Region Leader Peer | `id`、`store_id`、`role`、`is_witness` |

流量、大小、`pending_peers`、`down_peers`、Buckets 等心跳统计字段不参与比较；Peer 数组顺序也不参与比较。报告中的 `reference` 只是扫描开始时的 PD Leader 身份，不代表该节点的数据必然正确。`leader_peer` 表示 Region Leader Peer，与 PD Leader 是两个概念。

## 工作流程与结论边界

1. 读取成员、PD Leader 和 cluster ID，为每个 PD 实例建立直接 HTTP 连接。
2. 同时读取各实例 Region 数，通过 `/pd/api/v1/regions/key` 按 Key Range 分页；同一批请求尽量同时开始。
3. 每个实例最多有一个在途请求。所有实例共享请求预算：一组包含 N 个请求时消耗 N 份预算，因此默认长期总速率不超过 20 requests/s。
4. 请求携带 `PD-Allow-Follower-Handle: true`、`PD-Redirector: pd-ctl-region-meta-consistency` 和 `X-Caller-ID: pd-ctl`，直接读取目标实例的本地 Region cache。
5. 相同数据比较后立即释放；只有差异写入有界临时 JSONL，并按 Region ID 外部归并排序。
6. 对 Region ID 最小的前 `--confirm-limit` 个差异等待 1 秒后并发复查。结束时再次核对 PD Leader、cluster ID 和成员身份。

只有某节点的扫描前数量、实际扫描数量、扫描后数量相等，本轮扫描才会被接受。默认不自动重扫，避免在 Region 持续变化时放大负载。

多个独立 HTTP 请求不能组成分布式原子快照：

- `consistent` 表示本轮观察没有留下差异，不等价于线性一致性证明。
- `inconsistent` 表示至少一个已复查差异保持相同，是较强的不一致证据。
- `incomplete` 表示证据不足，不能解释为一致。

## 参数

完整参数以 `pd-ctl region meta-consistency --help` 为准。

| 参数 | 默认值（含单位） | 说明 |
| --- | ---: | --- |
| `-u, --pd` | `http://127.0.0.1:2379`（URL） | 一个 seed URL；不能包含用户名、密码、Path、Query 或 Fragment。 |
| `--batch-size` | `128 Region/请求` | 每次最多读取的 Region 数，范围 `1..1024 Region/请求`。 |
| `--interval` | `50ms/请求` | 每份全局 HTTP 请求预算的间隔；必须使用 Go duration，如 `100ms`、`1s`。三实例同批请求的最小组间隔为 `150ms`。 |
| `--timeout` | `10s/请求` | 单个 HTTP 请求的超时。 |
| `--max-runtime` | `4h/整轮` | 整轮检查的 wall-clock 硬上限。 |
| `--retries` | `0 次/请求` | 单请求额外重试次数，范围 `0..10 次`。 |
| `--scan-retries` | `0 次/整轮` | Region 数变化时的整集群重扫次数，范围 `0..3 次`。 |
| `--confirm-limit` | `128 Region/整轮` | 二次确认上限，范围 `0..1024 Region`；`0` 禁用确认。 |
| `--work-dir` | 系统临时目录（路径） | 临时差异和 stdout 临时报告所在目录。 |
| `--max-temporary-disk-mib` | `1024 MiB` | 差异排序与归并临时数据硬上限。 |
| `--max-output-mib` | `1024 MiB` | 最终 JSON 报告硬上限。 |
| `--output` | `-`（stdout 或路径） | `-` 输出到 stdout；文件路径采用完整写入后原子替换。 |
| `--cacert` | 系统 CA（路径） | HTTPS CA bundle。 |
| `--cert`、`--key` | 未设置（路径） | mTLS 客户端证书与私钥，必须同时提供。 |
| `--authorization-file` | 未设置（路径） | 包含一行完整 Authorization Header 值的文件，只允许通过 HTTPS 发送。 |

`--interval 0` 只用于无业务隔离测试，不能用于生产。集群负载较高但仍允许受控诊断时，可进一步降低单次和长期负载：

```bash
pd-ctl -u http://10.0.0.1:2379 region meta-consistency \
  --batch-size 64 \
  --interval 100ms \
  --output region-meta-report.json
```

## 输出

报告为单行紧凑 JSON。关键字段如下：

| 字段 | 含义 |
| --- | --- |
| `status` | `consistent`、`inconsistent` 或 `incomplete`。 |
| `reference` | 扫描开始时的 PD Leader 名称、ID 和 URL。 |
| `settings` | 限速、并发、硬上限、HTTP 请求数、Response Body 字节数和临时磁盘峰值。 |
| `nodes` | 各 PD 的名称、地址、角色、Region 数、批次数、扫描次数和时间。 |
| `confirmation` | 差异复查范围、稳定/消失/变化的 Region ID 和未复查数量。 |
| `summary` | 不同 Region 总数和按字段计数。 |
| `differences` | 按 Region ID 排序的全部保留差异。 |

每个差异包含 `region_id`，并且只输出真正不同的字段：`missing_on`、`key_range`、`epoch`、`peers`、`leader_peer`。实例使用 `<PD member name>@<host:port>` 标识；Peer `role` 数值为 `0=Voter`、`1=Learner`、`2=IncomingVoter`、`3=DemotingVoter`。

一致结果的关键字段：

```json
{
  "status": "consistent",
  "summary": {"different_regions": 0, "by_field": {}},
  "confirmation": {"result": "not_needed", "final_differences": 0},
  "differences": []
}
```

不一致结果的关键字段：

```json
{
  "status": "inconsistent",
  "summary": {"different_regions": 2, "by_field": {"missing_on": 1, "epoch": 1}},
  "confirmation": {
    "result": "stable",
    "checked_regions": 2,
    "stable_regions": [42, 43],
    "final_differences": 2
  },
  "differences": [
    {
      "region_id": 42,
      "epoch": {
        "pd-0@10.0.0.1:2379": {"conf_ver": 3, "version": 8},
        "pd-1@10.0.0.2:2379": {"conf_ver": 3, "version": 7}
      }
    },
    {"region_id": 43, "missing_on": ["pd-1@10.0.0.2:2379"]}
  ]
}
```

常用查询：

```bash
jq '{status, summary, confirmation}' region-meta-report.json
jq -r '.differences[].region_id' region-meta-report.json
jq '.differences[] | select(has("missing_on") or has("key_range") or has("epoch"))' region-meta-report.json
jq '.differences[] | select(has("peers") or has("leader_peer"))' region-meta-report.json
```

## 资源与容量

命令每个 PD 最多保留一个当前响应，每个响应有 8 MiB 硬上限。差异排序使用固定 8 MiB 缓冲，二次确认只保留 `--confirm-limit` 个候选，报告逐条写入。因此内存主要由 PD 节点数、批量、当前响应和确认上限决定，不随 Region 总数线性增长。Region 数增加主要表现为请求数、累计网络流量和运行时间增加。

| 资源 | 最低配置 | 建议配置 | 说明 |
| --- | ---: | ---: | --- |
| CPU | 1 vCPU | 2 vCPU | 网络读取最多与 PD 实例数相同，比较与报告写入在主进程完成。 |
| 内存 | 512 MiB | 1 GiB | 覆盖三实例并发当前页、JSON 解码和固定排序缓冲。 |
| 可用磁盘 | 3 GiB | 5 GiB | 覆盖默认 1 GiB 临时上限、1 GiB 报告上限和文件系统余量。 |
| 网络 | 100 Mbps | 与 PD 同 VPC/可用区 | 必须直连所有 PD 地址。 |
| 任务窗口 | 4 h | 大于 4 h | 默认硬上限为 4 h。 |

若 `--work-dir` 与输出目录位于不同文件系统，两处都应分别按对应上限预留空间。

### 百万 Region 实测

以下容量数据使用 [`v8.5.4-20260625-8b1b130`](https://github.com/tikv/pd/releases/tag/v8.5.4-20260625-8b1b130) 的 PD API。测试环境为 AWS EC2 `r7i.4xlarge`（16 vCPU、123 GiB 可见内存、无 swap）和 Ubuntu 22.04.5。三个 PD、三个模拟 Store、每 Region 三个 Peer；每档使用全新数据目录，在心跳停止、三个节点 Region 数相等且 Region Syncer 索引追平后执行。`pd-ctl` 使用 Go 1.26.5 构建自 commit `ecbbc0cea2ee8507512a7384e1e015e349e911ab`，测试二进制 SHA-256 为 `248cba827138361ecaa6cf8456a0e90492dcd0bfe2edf2a1b4624643a0b95d05`。其他 PD 版本和数据模型应按等价环境重新测量。

| 每个 PD 的 Region 数 | HTTP 请求数 | Response Body | 默认限速时间下界 | `interval=0` Wall time | 检查器最大 RSS |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000,000 | 23,447 | 1.24 GiB | 19 min 32 s | 15.187 s | 42.25 MiB |
| 2,000,000 | 46,883 | 2.48 GiB | 39 min 04 s | 29.729 s | 42.25 MiB |
| 4,000,000 | 93,758 | 4.99 GiB | 1 h 18 min 08 s | 59.849 s | 43.84 MiB |
| 8,000,000 | 187,508 | 10.02 GiB | 2 h 36 min 15 s | 123.029 s | 43.02 MiB |

| 每个 PD 的 Region 数 | 三台 PD CPU 增量合计 | 单台 PD RSS 最大值（扫描前 → 扫描后） | 报告大小 |
| ---: | ---: | ---: | ---: |
| 1,000,000 | 21.08 s | 2.21 GiB → 2.67 GiB | 1,567 B |
| 2,000,000 | 32.91 s | 4.50 GiB → 5.41 GiB | 1,569 B |
| 4,000,000 | 63.80 s | 9.78 GiB → 11.20 GiB | 1,571 B |
| 8,000,000 | 196.51 s | 16.15 GiB → 18.30 GiB | 1,572 B |

四档均为 `consistent`，每个节点只扫描一次，没有请求重试或整轮重扫；`differences` 为空，临时差异磁盘为 `0`。`interval=0` 仅用于无业务隔离环境的极限测量，不能作为生产运行参数；生产时间由默认全局请求预算控制。8,000,000 Region 在默认预算下的三个 PD 合计平均 Body 流量约为 1.10 MiB/s。

检查器 RSS 保持稳定，是因为 `http_response_bytes` 是整轮累计值，而内存中只保留各节点当前页、固定排序缓冲和有限确认候选。PD 侧仍要为每次请求执行 Region tree 扫描和 JSON 序列化；无主动限速测试中，单个 PD 的最大 RSS 增量达到 3.76 GiB。因此优先从独立运维机执行；必须与 PD 同机时，除检查器资源外，还应至少保留一个空闲逻辑 CPU、6 GiB `MemAvailable` 和 3 GiB 非 PD 数据盘空间。对于本测试的 8,000,000 Region 数据模型，PD 主机至少使用 32 GiB 内存；真实 Key 长度、Peer 数、业务负载和 PD 配置不同，执行前必须按等价环境重新确认余量。

## 生产运行守则

- 使用生产默认参数；不要为了缩短时间而增大批量、取消限速或首先启用整轮重扫。
- 监控现有生产阈值下的 PD CPU、Go heap/GC、业务请求延迟，以及 `pd_region_syncer_status{type="sync_index"}` 和 `{type="last_index"}`。
- 任一现有 SLO、告警或资源阈值触发时立即 Ctrl-C，不临时放宽阈值。
- 报告包含 PD 地址、Region ID、Key Range、Peer ID 和 Store ID，不要发布到公开渠道。
- Authorization 文件仅限当前用户读取，通过 HTTPS 发送，并按组织安全规范保管。

每 100 个扫描批次会向 stderr 输出一次进度，不会污染 JSON stdout。

## 测试

核心测试覆盖一致、Region 缺失、Key Range、Epoch、Peers、Region Leader Peer、Role、Witness、uint64、瞬时差异、扫描期间数量增长与重试、成员变化、同批并发、全局请求预算、响应/磁盘/输出硬上限和外部排序：

```bash
make -C tools gotest \
  GOTEST_ARGS='./pd-ctl/pdctl/command/regionmeta -count=1'
```

三 PD 集成测试会灌入 Region 心跳，直接改写一个 Follower 的本地缓存，并验证命令能报告具体 Epoch 差异和 Region 缺失：

```bash
make -C tools gotest \
  GOTEST_ARGS='-tags=without_dashboard ./pd-ctl/tests/region -run TestRegionMetaConsistencyUsesFollowerLocalCache -count=1'
```
