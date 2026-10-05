<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# ChunkDB ownership 行为 gap 与实现设计

2026-10-06。本文是待实现设计；本次不修改 ChunkDB ownership 行为。
架构入口：[文档索引](doc/doc_index.md)。复杂度：High，关键在跨服务切换、
持久 fencing、Paxos 不确定结果和后台任务撤销，不能只修改一个 balance 函数。

## 1. 已核实的 gap

- 当前 ChunkDB 以 ChunkId hash 的 1024 个 slot 路由，分别维护 service map
  和 storage map。service owner 是 ChunkDB server，storage owner 是 KV group。
- 初始 slot 分配是 bootstrap 的固定分配，不等于持续动态 balance。
- `ChunkdbRangeMonitorDriver` 只接受 driver_version=2、OperatorOnly 和
  `fixed-slots-v1`，tick 只审计 map；server join、heartbeat 消失不改变 owner。
- 历史版本存在动态 range balance；`44a1e27f` 改为固定 hash slots 时移除了
  自动重分配行为。旧 balance 策略代码存在，不代表现在仍在运行。
- `RangeGuard` 使用固定 map；运行时 map 改变不会自动完成接管。
- monitor 测试明确要求 join / absent owner 都不重分配，因此这是当前合同，
  不是通过删除测试或直接接回旧 range 策略就能修复的单点缺陷。
- 同 ChunkId 的 mutex 同步 chunkinfo update 是预期行为，用户已确认保留。
  它既不是动态 owner 算法，也不能作为分布式 owner 交接的 fencing。

## 2. 目标与不变量

- KV-server 的 Group 0 domain monitor 根据可服务的 registry identity 分配
  service slots，自动补齐无 owner 的 slot、替换失效 owner，并持续均衡。
- 首期只移动 service owner，storage map 保持不变；服务切换不隐含物理数据迁移。
- 每个 slot 在一个发布 generation 中恰好一个 active owner；1024 个 slot 完整覆盖。
- 新 owner 激活前旧 owner 的写准入必须被持久 fence 截断；旧 RPC、旧 task、
  旧 cache 对象不能利用新 generation 写入。
- owner 分配不使用全局/slot 互斥锁串行化普通 chunk 写入。
  同 chunkinfo 的已有更新同步继续保留。
- 不确定写结果必须先查询 identity / 等待恢复 barrier 解决，不能换 owner 后盲重试。
- 同 instance_id 的进程重启也必须识别为新的 incarnation，不能沿用旧运行身份。

## 3. 各组件方案

### 3.1 Group 0 monitor

- 增加明确的动态 service-slots policy，保持固定 storage-slots policy。
  显式启用/迁移，不能让现有固定合同静默改变。
- 输入：可 fencing 的 server incarnation、健康/就绪状态、当前 assignment 与 generation。
  首期以 slot 数量均衡，稳定排序处理平局；配置 hysteresis、故障宽限与每 tick
  movement 上限，避免 heartbeat 抖动导致反复迁移。
- 用 Group 0 revision CAS 发布切换计划和 map head；多个 monitor / leader 不能
  同时发布同一个 slot 的竞争计划。审计异常停止修改并暴露诊断，不自动猜测修复。

### 3.2 Prepare → Fence → Publish → Activate

1. monitor 为 slot 生成包含旧/新 incarnation、generation、storage group 的计划。
2. 目标准备接管；旧 owner 停止领取新 task 和接受新 mutation。
3. storage KV group 原子切换持久 slot owner fence，等待已接收旧 generation 的
   mutation 确定并应用。旧 owner 不可达时也依赖 KV fencing，而不是它的确认。
4. Group 0 发布带 generation 的 service map；目标从已 fenced 的数据恢复 cache/task。
5. 目标确认就绪后激活。中间故障按已持久化阶段续做；不能回退到旧 generation。

这里需要可恢复的过渡状态。实际 wire/state schema 实现前必须明确发布中的 slot
如何返回 NotMyRange / Unavailable，不能把 Prepare 状态暴露为已可服务的 owner。

### 3.3 KV 写准入与 ChunkDB RPC

- RPC 和 storage mutation 带 slot generation / owner incarnation。
- KV 提供按 slot 的只读 owner identity 准入：普通 mutation 不改共享 fence key，
  owner 切换才关闭旧 generation、drain、持久化新值。
- DiskDB 本次机制只接受 DiskGroup fence namespace，不能未经协议设计直接复用
  prefix；ChunkDB 需要定义 slot fence key、storage group 范围及 map epoch 校验。
- chunkinfo 的记录 revision CAS 与 owner fence 同时满足；现有 per-chunk mutex
  只解决本进程内同 chunk 同步，保留。

### 3.4 Client / RangeGuard / Tasks

- client 和 RangeGuard 用不可变 service map 快照，完整验证 slot 覆盖后一次发布。
- NotMyRange 携带新 map generation / hint，client 有界刷新；已发 mutation 的
  unknown outcome 与明确未接收的 routing rejection 分开处理。
- task claim、续租和状态更新都带 owner generation。撤销旧 owner 后不能续租或
  提交完成；新 owner 根据持久 claim 和业务幂等性接管，不能只取消本地 future。
- 接管先恢复持久 chunkinfo/cache，不从旧 server 内存直接获得权威状态。

## 4. Scope / Module Structure

- `app/crowdb-kv-server/src/background/domain_monitor/chunkdb.rs`：动态 service monitor。
- `lib/crowdb-protocol/src/chunk_slot*`、`src/key*`：incarnation、epoch、计划与 fence。
- `lib/crowdb-kv/src/cluster/group_owner_fence.rs`：按明确定义的 slot fence 扩展准入。
- `lib/crowdb-chunkdb-client/src/binding/chunk_slots.rs`、`binding/range.rs`：快照刷新。
- `app/crowdb-chunkdb/src/range_guard.rs`、`main.rs`：切换订阅、准备、恢复与激活。
- ChunkDB RPC、task manager/store：携带 generation、撤销旧 claim、unknown outcome。
- 对应 integration tests 与 Console owner/readiness 展示。

配置扩展：动态 service policy、grace、hysteresis、movement budget、fencing capability。
Server wiring：Group 0 monitor 发布计划；ChunkDB 订阅并推进接管；data KV group
执行 fence；客户端订阅/刷新已发布 map。storage map 的持久归属不变。

## 5. Test Design

- 3 个 owner + 完整固定 map → 第 4 个 ready server 加入 → 有界迁移并最终均衡，
  storage map 不变，所有 slot 仍恰好覆盖一次。
- owner 离开/崩溃 → grace 之后接管 → 无空洞，旧 incarnation 写入和续租被拒绝。
- 已接收旧写未 apply → 并发交接 → fence 不提前生效，新 owner 恢复含该写的数据。
- Prepare / Fence / Publish / Activate 任一阶段 crash → 重启/新 monitor leader →
  根据持久阶段续做，不能激活两个 owner 或回退 generation。
- data KV leader 切换、RPC 取消、unknown outcome → 恢复旧槽位 → fencing 仍成立，
  同 request identity 不重复 mutation。
- stale client / map 部分页缺失 → 刷新或明确 Unavailable → 不发布残缺 map。
- 旧 task 被撤销、server 用相同 instance_id 重启 → 新 incarnation 接管 →
  旧 task 不能提交，新 task 正确恢复。
- heartbeat 抖动和 leader 重复 tick → hysteresis / CAS → movement 有界、计划幂等。
- 同 chunk 并发 chunkinfo update → 既有 mutex + record CAS → 行为保持。

## 6. 实施顺序

- 先确定 incarnation、map epoch、过渡状态和 wire contract。
- 实现并验证 data KV slot fencing 与 task claim generation。
- 接入 RangeGuard / client 动态快照及恢复，最后启用自动 monitor policy。
- 固定 policy 测试保留，新增动态 policy 测试；不把固定 map 的现有合同改成默认自动迁移。
