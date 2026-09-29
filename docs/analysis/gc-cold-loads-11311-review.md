# GC 缓存批量加载与预热：设计评审记录

评审对象：[中文设计文档](gc-cold-loads-11311.md)。评审日期：2026 年 9 月 29 日。

结论：设计评审及修正后的复核均已完成，没有未解决的设计问题，可交用户确认。用户确认后，由主 agent 驱动 sub-agent 实现、集成、测试和代码 review。

## 评审范围与依据

主 agent 完成需求整理和自查，两位独立 sub-agent 分别从以下角度检查设计及其代码依据：

| Reviewer | 检查范围 | 复核结论 |
| --- | --- | --- |
| `review_gc_design_concurrency` | flight 去重、锁顺序、任期隔离、固定 revision 扫描、初始预热完成语义 | 两项问题已关闭，无新增阻塞问题 |
| `review_gc_design_scope` | 已确认需求覆盖、独立 PR 边界、现有行为兼容性、测试可执行性、中文表达 | 一项问题已关闭，未扩张需求 |
| 主 agent | 源码交叉核对、文档自查、修正整合及最终格式检查 | 已完成 |

源码基线为 master `3917c36a337bfa3dcd8df7baba83e33865710a35`；#11264 参考版本为 `69b3171250dd620e682a4d9058e725f95f6e1cc1`。另对照了已确认的讨论稿和现有 storage、GC manager、metadata cache 及测试设施。

首轮中文稿 SHA256 为 `75832b85182fab211bfb3d26a73443ff91689635b9f4e9ca0723017049e7524b`。两位 reviewer 对三项修正复核的版本 SHA256 为 `944b867d0f095ed32cbd7501aeb1823f9cbffd0802254d8a30a3ca9849bbb00d`。用户审阅期间的后续澄清及对应版本另记如下。

## 发现与处理

三项发现均已修正文档，并在验收条件中加入对应场景。

| 编号 | 级别 | 问题与场景 | 修正 | 状态 |
| --- | --- | --- | --- | --- |
| R1 | P1 | 原稿保留 leadership 计数的缓存门禁职责，却又按 generation 清理，可能在 reset 期间返回旧缓存，或使旧 teardown 的计数处理与清理分离 | §10 明确采用 #11264 的 active generation 门禁，reset 前禁用、重置完成后启用；清理先按任期禁用和取消，再等 manager 锁，并保证幂等及状态归属。§13 增加 reset 暂停和延迟/重复 teardown 场景 | 并发 reviewer 复核关闭 |
| R2 | P2 | 原稿仅允许缺失 key 返回零；现有三个 reader 对成功读取的空字符串也返回零，新批读会改变兼容性 | §7 保留缺失 key 与已存在空字符串的零值语义，非空值解码失败和读取错误仍返回错误。§13 增加空字符串对照用例 | 主审及范围 reviewer 核对，复核关闭 |
| R3 | P2 | 后台加入前台 flight，成功发布后被 writer 失效，再唤醒后台；若所有等待者统一重查 cache，后台可能重复预热已完成 scope | §8.3 区分前台重查当前缓存与后台消费对应 flight 的终态成功通知；后台记录完成后不因缓存失效重做。§13 增加该交错场景 | 并发 reviewer 复核关闭 |

R1 对照了 #11264 的 active generation 实现及 `TestGCStateLeadershipClearsCacheBeforeEnablingReads`。R2 对照了 [`loadGCSafePointForUnifiedGC`、`loadGCSafePointForKeyspaceLevelGC` 和 `LoadTxnSafePoint`](../../pkg/storage/endpoint/gc_states.go)。R3 是对已确认“一次初始预热、完成后按需加载”约束的落实。

## 复核结果与边界

两位 reviewer 均确认修正没有改变已确认的参数和范围：独立 PR、NextGen 索引及初始预热、metadata 首页与 H 同事务、固定 R、4096 ID 滚动任务、两个独立的四 worker pool、过滤后最多 60 个 scope 一批、无增量预热，以及 manager 读锁覆盖读取至发布。

本次 review 没有发现合法 metadata 下的区间覆盖遗漏、固定 R 快照错误或所规定锁顺序中的锁环。文档本地链接、空白、代码围栏和占位符检查通过。

这是设计评审，不是实现完成证明。本轮未修改产品代码、未运行 Go 测试，也未创建或发布 PR；实现后的功能、并发、部署兼容性测试及代码 review 仍需按设计执行。

## 用户确认与执行衔接

用户审阅时确认了 null scope 没有普通 metadata 记录、需要显式纳入预热，并最终选择启动时优先独立发起 singleton。当前 §4、§5、§8.2、§12 和 §13 明确：它与 metadata 并行，复用共用 fill 和 flight 去重，占四个后台名额之一，不等待 metadata，也不阻塞其他 scope；首次失败后的恢复回到普通批量重试。`review_gc_design_scope` 已复核通过。当前版本 SHA256 为 `5bf6a262e6dded054139df7f0bce75fb5721c4327213a33de198fb5287b03f89`。此前可合批的启动方案已由本次用户选择替代。

待用户确认的对象是中文设计正文，以及由主 agent 按第 11 节模块边界组织 sub-agent 实现的安排。确认前不派发实现任务。

确认后，主 agent 负责建立隔离工作区、明确接口与文件所有权、安排模块依赖和实现任务、整合结果、组织测试及代码 review。Sub-agent 使用与主 agent 相同的模型层级和 reasoning effort。若实现中发现需要改变已确认的功能范围或一致性契约，主 agent 先回报差异再推进；常规局部实现选择由主 agent 处理。
