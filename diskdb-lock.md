<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# DiskDB 路径锁审计

2026-10-06。范围是 DiskDB 初始化、allocate/free、后台维护及 owner 交接。
架构入口：[DiskDB 设计](doc/design/diskdb/design-crowdb-diskdb.md)。

## 1. 结论与已修复项

- **DG ownership fence 的逐请求 CAS 应移除**：原实现每次元数据写入先远程读取
  fence，再把原值追加到 mutation batch，并以它的 revision 做 CAS。
  KV 的同 key CAS 准入独占到 apply 完成，因此同 DG 的不同 disk、不同 zone
  初始化也会互相返回 CasBusy。一个 disk 只有一个顺序初始化任务，竞争来自
  多个 disk 共用一个 DG fence，不是同一 disk 重复初始化。
- **已改为 KV-server 的并发 owner 准入**：请求携带 owner identity，KV leader
  从本地已应用状态验证它；业务 mutation 不写 fence，不额外远程 Get。
  每个 DG、每个 leader tenure 用原子计数记录已接收请求，普通请求可同时进行。
  这仍有共享原子计数的 cache-line 成本，不声称完全没有同步成本；没有逐请求
  独占 owner key、互斥等待或同 DG 单请求队列。
- **DG membership 的读 RwLock 已移除**：free 校验、NoSpace 路径、RPC 查询、
  usage aggregation 和后台遍历统一读取 `DiskMembership` 的不可变快照。
  `disks`、`by_id`、`allocating` 一次发布，避免分别发布造成路由不一致。
  原 RwLock 私有化，只用于低频 membership 修改和快照构建。
- **Scanner summary 的 RwLock 已移除**：用 `ArcSwapOption<ScanSummary>`
  发布不可变结果，状态查询不用读锁。
- **运行时 disk 状态返回已修复**：RPC 使用 `effective_status()`，不使用创建时
  保存的 `disk_value.status`；初始化失败后不会继续返回硬件记录中的 Up。

## 2. Owner 交接协议

- Group 0 的 `diskdb-ownership` monitor 负责选择 server、故障替换和数量均衡；
  DiskDB 不通过抢本地锁决定 owner。
- 目标 server 用 Group 0 owner revision 作为 generation，在绑定的 data KV group
  CAS 更新 `/diskdb/ownership-fence/<rack>/<node>/<dg>`，随后才重建 bitmap。
- 只有这次 ownership CAS 关闭该 DG 的 owner-write 准入。新请求直接返回 Busy，
  已接收请求并发完成并等待本地 apply；计数归零后才能持久化新 fence。
- claim 成功后，新 owner 从持久状态恢复；旧 identity 的后续写入被拒绝。
  后台任务保留启动时 group 对象的 generation，不能借新对象的身份继续写。
- 请求取消不能提前释放准入：proposal 任务独立运行并持有 guard 到结果确定。
  未确定的 proposal 在 guard 释放前标记该 tenure 未解决，跨 topology 保留；
  Paxos outcome unknown 时关闭 leader-read readiness；恢复 barrier 解决旧槽位前
  不接受新的 owner claim。leader tenure 分隔旧准入状态。
- 普通 Put/Delete/BatchWrite 不能更新保留的 fence key；ownership 更新必须走 CAS。
  owner-fenced batch 不能修改任何 fence key。业务记录的 revision CAS 仍保留，
  例如 tentative → committed 需要同时满足 owner identity 和记录 revision。
- 新协议带版本及旧协议不可满足的只读 precondition。旧 KV-server 拒绝请求，
  不会静默忽略 owner 校验；上线必须更新 KV-server 与 DiskDB。

主要实现：`group_owner_fence.rs`、`group_cas.rs`、`FBKvOwnerFence`、
`batch_write_owned`、`DdbKvClient::write_owned`。

## 3. 保留项及原因

- **bitmap 原子 CAS**：正常 allocate 依靠原子 bitmap 和快照，保留。
  用户已确认这类同步可以接受。
- **zone RwLock**：compaction 的内存 bit-clear 与 ghost scanner 的纠正需要隔离，
  防止两个维护动作重复清 bit / 调整 used count。正常 bitmap allocate 不取此锁。
  无空间触发同步 compaction 时，会进入这一维护路径并等待 zone 写锁；因此它
  仍是 NoSpace 慢路径上的等待点，不能把整个 allocate 路径说成完全无锁。
  scanner 的首次 try-read/try-write 只做忙碌探测并立即释放，最终纠正在短临界区。
  compaction 的持锁段不跨 KV 网络 await。单独删除锁会破坏维护操作间的隔离，
  本次保留；后续若改为统一 maintenance 原子状态，必须覆盖纠正和 compaction。
- **KeepAlive / Recovery map RwLock**：miss count、suspect 时间、恢复任务及恢复进度
  的控制面维护；普通 allocate/free 不访问这些 map。本次保留。
- **FreeBatcher drain election**：队列消费用原子状态选 drainer，不是 Mutex。
  实际 RPC free 走 `submit_direct`，没有经过跨 DG 单 drainer；不能把备用批量队列
  的串行消费当作正常 free 的锁瓶颈。
- **DiskIO BlockingEngine 队列 Mutex**：属于阻塞线程池引擎的任务队列，DiskIO
  启动优先尝试 io_uring，失败时回退 blocking。不能推断所有部署都在 io_uring。
  替换队列需要另行验证 wakeup、stop、backpressure 与任务 lifetime，本次不改。

## 4. 验证与剩余边界

- 8 个 disk 各顺序初始化 32 个 zone，真实三节点 KV 集群验证所有基线记录及 checksum。
- 并发 owner-write 不推进 fence revision；交接等待旧准入，取消 claim 调用方后仍完成；
  新 fence 生效后拒绝旧 owner；业务记录 CAS 只能成功一次。
- DG 快照并发发布一致性、移除后旧快照 lifetime、allocate/free、Scanner、usage。
- Disk RPC 覆盖 Init / Offline / Up 的有效状态。
- Capacity UI 覆盖跨 node 统计、自动选最少 DG 的普通 KV group、手工覆盖及重新打开。
- 本次没有重启用户的 9090 集群，已运行进程不会因源码修改自动获得修复。
  Catalog 未初始化是独立的 readiness 问题，不能靠把 disk 颜色改绿解决。

ChunkDB 动态 service ownership 的待实施合同见
[R221](doc/backlog/R221-chunkdb-dynamic-service-ownership.md)。

回归排查补充：修复 KV 测试 fixture 的进程内端口计数器，改用现有跨进程租约，
避免 closed-port 测试误连其它 test binary。排查期间出现过一次 native SIGSEGV，
没有 core；GDB 全量及后续普通全量重跑未复现，未确认其根因，不声称端口修复
已解决该崩溃。
