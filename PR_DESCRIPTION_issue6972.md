# Issue #6972: Enable Non-Default Keyspace Group TSO to Read All Timestamp Keys

## 问题概述

目前，只有 PD leader TSO 和默认 keyspace group TSO 服务会在启动时读取所有前缀的 timestamp key 并获取最大的最新值。非默认 keyspace group TSO 服务在启动同步时没有相同的行为。

通过本修复，所有 keyspace group 的 TSO 服务（包括非默认的 keyspace group）在启动时都将读取所有 keyspace group 的 timestamp 并获取最大值，以确保 TSO 的单调性。

## 背景

在 keyspace group 分片场景下：
- 每个 keyspace group 维护自己的 timestamp，存储在 etcd 路径：`/ms/{cluster_id}/tso/{group_id}/gta/timestamp`
- 当非默认 keyspace group 重启时，如果只读取自己的 timestamp key，可能会遇到 TSO 回退的问题
- 这需要运维人员在启动/停止 TSO 节点时额外考虑元数据清理

## 解决方案

### 核心变更

1. **扩展 TSOStorage 接口**（`pkg/storage/endpoint/tso.go`）
   - 添加新方法：`LoadMaxTimestampAllGroups() (time.Time, error)`
   - 该方法读取所有 keyspace group 的 timestamp key 并返回最大值
   - 使用 etcd 范围查询读取前缀：`/ms/{cluster_id}/tso/` 下的所有数据
   - 解析每个 timestamp 并找到最大值
   - 处理错误情况（无效值、解析错误等）

2. **修改 TSO 同步逻辑**（`pkg/tso/tso.go`）
   - 在 `timestampOracle.syncTimestamp()` 中添加条件判断
   - 对于非默认 keyspace group（`keyspaceGroupID > 0` 且不是默认组 ID），调用 `LoadMaxTimestampAllGroups()`
   - 对于默认 keyspace group，使用原有的 `LoadTimestamp()` 行为
   - 确保 TSO 单调性

3. **添加辅助函数**（`pkg/utils/keypath/absolute_key_path.go`）
   - `MsTimestampPrefix()`: 返回微服务 TSO timestamp 键的前缀
   - `MsTimestampPrefixRangeEnd()`: 返回前缀范围的结束键
   - `ExtractKeyspaceGroupIDFromMsTimestamp()`: 从路径中提取 keyspace group ID

## 实现优势

1. **简化运维**：允许 TSO 节点更安全地启动/停止，无需考虑元数据清理
2. **保证单调性**：非默认 keyspace group 初始化时总是从最新的 TSO 开始
3. **保持向后兼容**：默认 keyspace group 行为不变
4. **最小化变更**：只需修改两个核心文件和添加辅助函数

## 测试验证

- **编译验证**：`make build` 成功
- **基本测试**：`make basic-test` 通过（除一个环境相关的 cgroup 测试）
- **代码审查**：遵循 PD 项目规范

## 影响范围

### 修改的文件
1. `pkg/storage/endpoint/tso.go` - 添加接口方法
2. `pkg/tso/tso.go` - 修改同步逻辑
3. `pkg/utils/keypath/absolute_key_path.go` - 添加辅助函数

### 行为变更
- **非默认 keyspace group**：启动时读取所有 keyspace group 的最大 timestamp
- **默认 keyspace group**：保持原有行为，只读取自己的 timestamp

## 关键细节

- `LoadMaxTimestampAllGroups()` 使用 etcd 范围查询读取所有 keyspace group 的 timestamp
- 使用前缀 `/ms/{cluster_id}/tso/` 和范围结束键 `/ms/{cluster_id}/tso_99999`
- 跳过空值或无效值，记录警告日志
- 返回的 timestamp 为所有有效 values 中的最大值
- 如果没有任何有效 timestamp，返回 `typeutil.ZeroTime`

## 相关设计考虑

这个修复借鉴了现有的 `mergingChecker` 的设计模式（`pkg/tso/keyspace_group_manager.go:1396-1414`），该模式已经是读取多个 keyspace group 的 timestamp 并找到最大值的实现。

## 向后兼容性

- 默认 keyspace group 的行为完全不变
- 现有的 API 接口保持不变
- 只添加了新的接口方法，不修改现有方法签名