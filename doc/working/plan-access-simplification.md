<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Access Server Simplification Plan

Upstream: [Dataset SDK](../backlog/R220-dataset-fat-client-plan.md)、
[Access design](../design/access-server/design-crowdb-access-server.md)。

目标：减少 Access 请求的 RPC、持久化、复制、CPU 和共享竞争，保留确有业务用途的约束。
这是用户要求长期维护的清理计划；完成项转入验证记录和所属设计后，从待办中移除。

## 跟踪方式与当前状态

- `[ ]` 未完成，`[~]` 正在执行（最多一项），`[x]` 已验证；任务 ID 保持稳定。
- P1–P14 对应持久化审查；F1–F10 对应其他性能问题；V1–V4 是验收任务。
- D1–D3 的原则已由用户确认；已记录决定不等于实现完成。
  R1–R4 跟踪具体写入反例审查，D4–D5 仅在出现实际业务需求时讨论；不预设保留中间状态。
- 每项实现完成需记录：实际修改、正常/冲突/结果未知路径、验证命令和结果。
  静态发现、提出方案、代码完成、性能验收不能互相代替。
- 当前实施基线为 `489ae507`；旧性能审查来自 `878ee901`，
  涉及已删除 session 的结论已移除。下面的成本数字只描述注明的函数路径，需动态复核。
- 用户已恢复工作；本地交接已读入并按要求删除。交接时 46 个未完成 checkbox，
  包含条件决策、交叉审查和总体验收，不代表 46 个独立功能。

## 已确定的约束

- Dataset 读取无服务端 session：无持久化 reader、选中 ID 页、进度、续租、ack/close。
  continuation 固定 Dataset、Snapshot、Split revision、投影、分区和 shuffle；
  客户端处理成功后保存 next，重用原 token 可重放，不依赖固定 Access 实例。
- Snapshot 删除：持久化标记，拒绝新请求，默认等待五分钟后回收。
  重试/重启不重置期限；已准入流可继续，过久的流允许失败，不能截断后报成功。
  Access Server 不登记读者、不互相确认删除、不异步写释放记录。
- **非空 Iceberg namespace 删除必须失败**，已由用户决定，不再提出放宽选项。
- **用户决定（2026-10-24）**：允许 rename 在参与父目录中保存最小有界占位，用于严格非空
  删除和迟到 CAS 隔离；删除新 rename 的完整 journal、输入和响应。普通读取不登记。
- **用户决定（2026-10-24）**：完整 Snapshot/Split 结果即使没有赢 head CAS，也允许按 ID
  读取和引用。head CAS 仅切换 latest/stable/currentSplit，失败仍返回冲突，不覆盖后来者。
  未完成资源不开放；无正式引用的结果按期限清理，GC 隔离按 R4 实施。移除“曾发布”证明
  与请求账本，不能只放宽入口后遗留 CA/CR/SC/SRC/helper。
- definite CAS conflict 立即使用返回的 observed value 计算合法重试，无“三次后才更新”。
  返回值可能随后又变旧，因此仍需期限/重试上限；transport timeout 不等于 CAS 失败。
  明确 expected revision 的业务操作不能擅自改 base 重放。
- 新请求仍须鉴权并检查删除状态；同一请求内可复用已验证结果。
  跨请求缓存可变准入结果必须证明失效语义，不能只依靠 TTL。
- 已提交业务数据、共享 payload 存活引用、物理分配代际和校验仍须正确。
  每项持久化都要说明用途，但删除 session 不等于删除业务资源本身。
- **用户决定（2026-10-24）**：服务端不保存编辑、计算、上传的中间过程；客户端控制流程，
  失败后重新请求。允许 MPU，恢复粒度为完整 part，不支持 part 内断点续传。
  不用新的内存会话、固定实例路由或持久化 job 替代旧 session。
- 已完成 part、最终文件/Manifest 和正式引用属于数据结果；步骤号、处理偏移、工作页、
  输入/输出重放账本、pending journal 等不得仅为恢复请求过程而保存。
  确有其他用途的记录必须列出具体使用场景和最小方案，先 review，再保留或新增。
- 中途生成但未被正式引用的文件/part 交给 GC；删除过程状态之前必须验证 GC 能独立发现
  并回收它们，且不会误删待发布或已有 owner 的内容。不能假设现有 GC 已具备此能力。
- **用户决定（2026-10-24）**：未发布 MPU part 有明确可提交期限；过期拒绝新提交，
  客户端重传。GC 在期限后再等待安全期回收；已发布资源不能按该临时期限回收。
  期限必须随 part 结果返回，最终提交验证期限；本地时钟检查不能单独证明迟到发布安全。
- **用户澄清（2026-10-24）**：Access GC 只需通知 ChunkDB 的 DeleteChunkRange 接口。
  接口成功即完成本层责任，不等待物理释放、不轮询 reclaimed、不保存物理回收进度。
  ChunkDB 内部范围回收尚属单独工作，不作为本轮 Access 清理的实施内容。
- 幂等按操作效果判断：同目标同内容的普通覆盖可直接重试，不要求原响应重放、
  operation ledger 或多次查询来“证明幂等”。有反例按 R1–R4 单独 review；
  不把“最小提交回执 + 查询结果”设成所有写 API 的默认成本。
- 热路径不新增锁；不得用无界缓存、无界并发或降低校验来换吞吐。

## 已确认的设计决定与具体反例审查

### D1 — 客户端控制 Composition / Split，失败重新计算（已确认）

**旧流程及开销**：begin 写草稿 → edit 保存修订/回执 → evaluate 保存每页工作和中间行 →
CAS 推进进度 → prepare → publish。它让服务端能接着上次编辑/计算，却给每页增加 GET、
持久化和草稿竞争。这个恢复合同不再需要，不能仅降低检查点频率后继续保留。

**目标流程**：客户端保存规则/修改和调用顺序 → 提交独立完整、资源有界的请求 →
服务端计算并生成最终所需的内容 → 发布最终引用。失败由客户端重试，服务端不恢复过程。
大输入和分页由客户端组织，不要求一次塞进内存；输出文件可以先存在，再由最终发布引用。
这些文件不是服务端步骤回执或隐藏的工作状态；未引用内容由 GC 清理。

**实施边界**：删除 durable draft/edit/evaluation/work-progress 等过程记录与相应续算 API；
已发布 Snapshot/Split 成员继续固定，不能因为重试改变历史结果。迟到请求与 GC/发布竞争
需要单独证明安全；若认为必须保留某个封口/阶段记录，提交 R4 场景，不能默认保留。
影响 P2/P3/P12/P13，决策不再等待确认。

### D2 — 无服务端上传续传，MPU 按 part 由客户端控制（已确认）

**旧流程及开销**：upload descriptor → part intent → 物理写入 → part 结果/索引 →
complete → owner 引用。这里混合了实际数据结果与“服务端记住做到了哪一步”的状态，
还有重复 GET、共享索引页竞争和过程恢复记录。

**目标流程**：客户端决定分片与 part 标识 → 每次请求完整写一个 part → 返回完整 part 的
标识/长度/校验等结果 → 客户端收集结果并提交最终清单 → 发布最终资源。
某个 part 断了就重传该 part，不恢复其字节偏移；客户端无需服务端保存步骤/上传进度。
其他成功 part 可继续由客户端引用，换 Access 也不能依赖原实例的会话。
响应丢失时同一 part 的重写优先按相同目标/内容实现，不先加查询和去重账本。

**实施边界**：已完成 part 的数据和必要资源元数据可以存在；upload-wide pending、
逐步恢复 journal、part 内 checkpoint 不因 MPU 而自动获得保留理由。
S3 ListParts 等协议入口若需要索引，必须解释资源查询用途、最小记录与成本，不能据此
保留全部旧会话状态机。GC 负责失败的半成品和未被最终引用的完整 part。
影响 P4/P5/P6/P7/P13，跨重启续传不再是默认合同或待选建议。

### D3 — 按实际操作效果选择重试方式，不默认保存幂等账本（原则已确认）

用户认可普通覆盖写相同内容的逻辑幂等。此前“所有写操作至少保存提交证据、允许查询结果”
的统一建议撤回。先列目标对象、输入、并发写者、失败点和重复执行后果，再判断是否需要
条件写或其他约束；需要条件写不等于需要持久化过程/响应账本。
现有测试要求原错误/原响应重放，是旧实现合同，不能单凭测试把该行为保留下来。

#### R1 — 普通覆盖写：通常无需额外去重；并发语义要说清楚

**流程**：客户端向同一 key 写 A，响应丢失，再向同一 key 写 A。没有夹入其他修改时，
最终逻辑内容仍是 A。S3 `publish_versioned` 当前直接调用一次 `store.put`，
说明完成资源发布不必经过统一 RetryLedger。相同逻辑内容不要求物理地址/修订号不变；
重试产生的未引用物理副本交给 GC。

**边界例子**：A 已写入 → 另一个客户端写 B → A 的客户端重试 A，最终内容回到 A。
若 API 是无条件覆盖，这是新的有效覆盖，不需要为此引入账本；若 API 承诺不得覆盖
并发更新，就应由请求携带 expected version 做条件写。问题是 API 合同，不是同内容本身。
若每次覆盖还创建可枚举历史版本或其他业务副作用，则需要对该实际入口单独 review；
不要把尚未发现的副作用当作默认事实。

- [ ] **R1 逐路由确定覆盖合同**：普通 S3/Dataset part/文件发布分别记录是否无条件覆盖、
  是否有额外可见副作用。可重复覆盖的入口删除过程/响应去重和额外确认 GET；保持内容校验。
  Files: S3 `publication.rs`、Dataset upload/MPU、Iceberg file 入口。

#### R2 — expected-base 发布：CAS 是必要约束，历史成功查询未必必要

**具体流程**：请求固定 H0，希望发布 H1；CAS 已成功但回复丢失；另一个写者又把 H1
推进到 H2。原请求重试 H0→H1 时，CAS 返回 H2，直接覆盖 H1 会丢掉后续写者的更新。
这不同于无条件 PUT。当前 Iceberg `publish_direct_commit` 已用 CAS 返回值判断相同
candidate；观察到后续代际时可能返回 Uncertain，没有必要再盲目 GET 同一个 head。

**初步判断**：保留请求的 expected base/候选身份，重试同一条件写；observed 就是候选时
可以认定结果一致，observed 已变化时按真实条件失败/无法判断历史结果返回，不能暗中 rebase。
CAS 不再满足只证明“本次条件不成立”，不能谎称“第一次一定没成功”。
不为每次请求添加历史查询或回执；只有某个具体调用者必须识别被后续提交遮住的历史成功，
才提出该场景需要的最小方案及代价，交用户 review。

**资源可见性约束 review（2026-10-24，用户已选择完整结果按 ID 可用）**：旧实现把“内容已完整生成”与“曾经成功
切换 head”区分。Snapshot 的 `Published` 标记在 head CAS 之前写，不能单独证明成功；
`require_committed_snapshot`、历史读取、parent/stable 准入和 Split revision 历史依赖
`DS\0SC\0` / `DS\0SRC\0`。每次发布还写 `DS\0CA\0` attempt、`DS\0CR\0` response receipt；
同一成功证据写两份，head 被替换前还帮前一个写者补齐。CA/CR 是请求账本；SC/SRC 在当前
合同下承担资源可见性，不能把三者一起删后仍声称保留原合同。

- 场景：完整结果 S1 已经生成；客户端以 H0 发布 S1 的 CAS 输给 S2。现在 S1 既不可按 ID
  读取，也不可成为新 Snapshot 的 parent/stable。另一个场景是 S1 曾赢 head、回复丢失、
  又被 S2 替换；S1 必须可作历史资源。为区分两者，现实现保存上述成功证明。
- 已确认：完整不可变结果按资源 ID 即可读取/引用；head CAS 只决定 latest/stable 指针是否
  按指定 base 更新。未完成内容仍不可读；未引用结果按明确期限过期，过期请求失败并重做。
  这可删除“曾切换 head”证明、attempt/response 账本与相应 helper；GC/期限隔离仍须按 R4
  单独证明，不能仅凭本地超时回收。用户在 S0→S1/S2 竞争和回复丢失例子后明确选择此合同，
  不采用“必须曾发布才能读”的备选。

- [ ] **R2.2 删除发布过程账本**：移除新 CA/CR/SC/SRC 写入、覆盖 head 前的帮助与按 attempt
  历史回复查询；Composition/Split 的 Committing 恢复与 P2/P3 一起删除，客户端保留完整输入。
  旧记录只作为兼容解码/GC 输入，不让旧账本控制新请求。head 保留 expected base；同一
  candidate 的当前结果可判断相同，已被后续 head 遮住时不保证历史请求结果重放。

- [ ] **R2 审查条件发布的回复丢失窗口**：用 H0→H1→H2 测试区分条件失败、相同结果与历史未知；
  列出确实依赖历史精确结果的调用者，没有这种需求就移除相应 ledger/response replay。
  Files: Iceberg `commit/direct.rs`、Dataset `authority/publication/`、Split head、SDK。

#### R3 — rename / 严格非空删除：涉及多个资源，不是一个 key 的覆盖

**具体流程**：Iceberg rename A→B 当前涉及目标名称 reservation、表 head、目标 mapping、
源 mapping 和父 namespace admission。代码先改权威 head，再发布目标名、删除源名；
在任一边界宕机，重试不能只凭“A 不在了”就认定原操作成功，也不能误删重建后的 A。
并发创建子对象与删除父 namespace 的检查/执行之间，也不能只做 GET empty 后无条件删除。

**初步判断**：这是必须审查的多资源一致性场景，不是保留整个 journal 的结论。
先比较以单个权威记录确定最终状态、从资源事实重建派生 mapping、请求携带原资源代际、
以及已有存储原子能力，明确各方案能否保持非空删除失败和名称唯一。
若仍主张某个中间记录不可避免，必须展示具体失败时间线、无法从最终事实修复的原因和
最小持久化集合，先 review；不能用“恢复方便”作为理由。

- [ ] **R3 提交多资源操作的最小方案**：按 rename/create/drop 分别列失败点、实际读取可见性、
  新请求如何处理和无 journal 替代方案；有保留中间记录的需求时单独确认。
  Files: Iceberg `table/lifecycle/{operation,completion,reservation}.rs`、`namespace/`。

**rename 的具体持久化例外，用户已允许（2026-10-24）**：不能仅删除 journal 后依次改
三个 key。设父 namespace P 中已有 A，rename A→B 先预放 B mapping，再 CAS TableHead。
如果 P 的删除已经标记 Dropping，空目录探测先经过尚未可见的 B，随后 head 切到 B，
探测再经过已失效的 A，就可能误判为空；改名线程此时退出，P 被删，已有表丢失可达名称。
跨 namespace rename 的目标父目录被删也有相同问题。读取前后各 GET 一次父目录不能弥补
线程在 head CAS 后退出的窗口；增加等待时间也不能消除它。

最小候选方案是在参与 rename 的父 namespace 中保留有界准入占位，内容仅为稳定 TableId、
受影响的目标名称与预期 head 代际，持有期间删除返回可重试 busy。目标 mapping 仍通过
CAS 保证名称唯一；TableHead CAS 是名称切换点；mapping 只在与 head 的 ID/name epoch
一致时可见。完成后条件清除占位；进程退出后从 head/mapping 的最终事实完成名称发布，
或先推进 head fence 使迟到 CAS 失效，再取消占位。两边父目录按稳定 ID 顺序占用，
拿不到第二个时释放第一个并返回 busy，不能等待形成环。
这仍是服务端持久化协调状态，不能把它改名后声称“没有状态”。它只服务 rename 与严格
非空删除，不保存输入/输出 body、进度页或 HTTP 响应；预计每个参与父目录增加占用/清除
两次 CAS，替代当前完整 journal/payload 阶段，实际成本与竞争须回归计数。同父目录改名
存在共享 CAS 竞争，普通读取不登记也不争用该记录。不新增进程锁。

替代方案是实现底层跨 key 原子条件提交，或暂不提供 rename。当前 CatalogStore 只暴露
单 key CAS；Chunk-KV 的 batch_mutate 明确不提供跨 key/partition 原子性，不能冒充事务。
新建 child 可以在发布后校验父身份并把失败的新资源交给 GC；rename 移动的是既有表，
不能接受线程退出后把原表变成无名称孤儿。用户已确认允许最小占位，删除完整 journal。

#### R4 — 去掉过程状态后，GC 是否还能发现所有孤儿？这是实现前置检查

**具体流程**：物理文件已写出 → 尚未保存 part/最终引用时进程退出。若 GC 只扫描 upload
记录，就可能根本看不到这份数据。另一个窗口是文件已生成但尚未发布，GC 判断无 owner，
同时迟到请求又尝试发布；仅“当前无 owner”不自动意味着可以安全回收。
现有 `DatasetUploads::expire` 扫 `DS\0U\0`，并从 upload record 进入 abort/cleanup，
不能凭这个入口证明没有 upload record 的物理孤儿也能被清理。

**初步判断**：先证明底层分配/文件清单及引用信息足以发现孤儿，建立等待期和发布/回收边界，
并测试物理身份复用。扫描游标是本地优化，不恢复服务端业务步骤；必要的文件身份/时间等
资源事实与操作 checkpoint 分开审查。不能为补 GC 缺口直接把已否定的上传/计算 journal 加回来。

**已核实的缺口（2026-10-24）**：`ChunkdbClient::list_chunks` 能枚举 chunk；共享 writer
过期后在 `acknowledged_cursor` 封口，保留已经确认的字节。`FrameHeaderPrefix` 有大小和
写入时间，没有对象 owner；`reclaim_location` 只能回收调用者给定的物理范围。
Dataset expiry 依赖 upload/part 记录，Iceberg discovery/write sweep 依赖 file/intent 记录，
都不是“封口 chunk 范围减去全部存活引用”的独立扫描器。删除写前 intent 后，写出 payload
而尚未保存结果就宕机会产生不可发现的范围。tree 内部 orphan 回收不能代替用户 payload GC。

**初始核查（2026-10-24）**：生产 `chunkdb_rpc_service/mutations.rs::handle_delete_range`
当时返回 `Unimplemented`，没有更新 used-range/bitmap。R4.3 已补精确退休，
物理回收独立于本计划；`reclaim_location` 收到成功后返回 Accepted，表示通知已接受，
不表示容量已释放。底层独立工作是
[R95](../backlog/R95-chunkdb-chunk-range-delete.md)（范围资格与孤儿发现）及
[R92](../backlog/R92-chunkdb-in-chunk-gc.md)（混存 strip 的物理回收，当前仅占位设计）。
不能用“把 RPC 改为成功”或把共享 chunk 整块删除替代实现；R95 旧文档的 reader/session
约束需按本计划已确认的删除 grace、part 期限重新整理。R168 的逐对象 cleanup record
也不是本计划自动批准的新状态。涉及新持久化资源字段先列出用途、写入频率和替代方案。

**已确认的物理资源事实（2026-10-24，ChunkDB 独立工作）**：R95 若按范围逐步回收，需要在 chunk/strip
元数据中保留已经判定死亡的精确区间及布局代际。它表示“哪些物理字节已经不可再引用”，
不是 upload 的步骤、进度或 owner session；由后台 GC 按 chunk 合并写，前台上传不追加
cleanup/journal RPC。现有 `usage_bitmap` 描述的粒度不能直接用于任意字节范围，不能把
一个 frame 的删除向邻居取整。部分死亡 strip 暂时只记录死亡事实，释放整块分配或搬迁
存活部分后才增加实际可用容量；R92 负责后者及逻辑地址到新物理布局的映射。
Access 不等待这些步骤，不消费物理 reclaimed 字段。

失败流程：GC 先完成发布隔离与活引用检查 → 以 chunk revision 条件更新死亡范围/布局 →
物理释放。进程在二者间退出时，从资源事实重做释放；丢回复时重复操作不得伤及新代际。
如果不保留死亡范围，每次必须重新从全部最终引用计算；可以先只释放完全无引用的整 chunk，
但长期混存 chunk 的孤儿空间不会收敛，不能据此宣称 R4/R92 完成。用户已确认采用精确范围和布局代际的持久化资源状态，继续补齐范围回收；
不采用仅回收整 chunk 的替代方案，不再等待此项确认。

**已确认的期限与剩余工程证明**：未发布 part 过期后重传；不支持无限期稍后提交。
仅在 Access 开始请求时检查期限仍有竞态：检查通过 → 发布 RPC 延迟 → GC 回收 → 迟到发布。
必须在最终权威发布边界拒绝过期候选，或以资源代际/封口隔离迟到发布；等待若干分钟本身
不能证明不存在无限延迟请求。此处没有授权新增逐请求 journal，也不先删除唯一孤儿发现链路。

`DatasetAuthority::fence_reclaim` 已通过修改 head nonce 作废旧发布 CAS；
`commit_snapshot_attempt` 在读取 head 后检查 Snapshot 准入，再做 expected-head CAS。
这是可复用的隔离机制，但当前只针对指定 Snapshot，不能直接当作任意过期 part 的发布屏障。
后续须证明全体引用发布入口都经过该边界、扫描与新提交的顺序、跨 Dataset 共享引用的范围；
S3 普通对象是直接 PUT，不能未经设计就套用 Dataset head 机制或给每次 PUT 增加 CAS。

- [ ] **R4 验证无过程记录的孤儿发现**：注入文件生成前后、part 回复前后、最终引用前后宕机；
  证明无 owner 内容最终清掉、已发布/共享内容不误删。若缺底层发现能力，先补独立 GC，
  需要新增持久化信息时说明用途供 review。Files: Dataset expiry/reclaim、Iceberg file/GC、底层文件/分配枚举接口。
- [ ] **R4.1 建立有界物理范围发现**：枚举已封口用户数据 chunk，按完整 frame 边界建立候选，
  与最终 Manifest/文件引用求差；包含写成功但结果记录缺失、共享 chunk 混合存活和孤儿、
  writer 失效和物理身份复用。不得把整个 shared chunk 当作一个可删除文件。
  Files: chunk-client reclamation、chunkdb ListChunks、protocol frame、Dataset/Iceberg GC。
- [ ] **R4.2 实现提交期限和回收隔离**：part 结果携带期限，提交权威边界验证；覆盖期限前准入
  但期限后落地的 RPC、GC 与最终引用并发、成功发布后的长期存活。若需新增持久化资源事实，
  先列出最小字段和每请求 RPC 成本供 review。Files: Dataset publication/upload、Iceberg file、GC。
  完整但没有正式引用的 Snapshot/Split 也需独立过期与引用隔离；目前完整 Split 保留到 owner
  Snapshot 回收，不能把“放弃草稿不删完整结果”当作独立期限 GC 已完成。
- [ ] **R4.5 简化通知入口的前置查询**：当前接口要求 expected_modify_ts，reclaim_location
  因此先 QueryChunk，并兼容旧的 dedicated DeleteChunk 分支。与后续 ChunkDB 接口实现一起
  收敛为 chunk_id/offset/length 的单次 DeleteChunkRange 通知；布局、封口与物理回收判断归
  ChunkDB。删除此查询需要同步调整服务端的 revision 契约，另行实施。

**已移出 Access 实施范围**：死亡 strip 释放、混存搬迁、布局 grace、迟到 IO、后台物理扫描
属于 R92/R95 的 ChunkDB 工作。未提交的实验保存于 git stash
`e28ee7be`（`WIP whole-strip reclamation saved separately from access cleanup`），未并入本次修改。
已有精确退休接口不代表完整物理回收已实现；本计划不以它的完成作为 Access 通知成功条件。

### D4 — 全局硬配额，还是每台 Access 的资源准入？仅在发现活跃需求后讨论

**场景和流程**：旧 Iceberg multipart credit 在 catalog-wide 记录上预留 sessions/bytes，
写 pending journal，再创建 upload，结束时归还。不同上传会竞争同一个计数器；它实现的是
跨实例总量约束，而本地 semaphore/字节预算只保护一台机器。两者不能直接等价替换。
当前新上传前台路径未使用该全局 reserve/release；旧记录恢复仍可能依赖它。

**判断与建议**：先追踪入口和历史记录，删除确证无调用、无恢复责任的旧逻辑；
不因为看到了类名就把旧路径开销算进新上传。只有确有硬配额需求时才讨论全局协调方案。

- [ ] **D4 条件决策**：P6 若证明仍有跨实例硬配额需求，再确认是否允许按实例限制，
  接受实例数增加导致总上限增加；若无需求，记录无需产品决策及调查证据。

### D5 — GC 是否有完成时限和精确恢复进度要求？先测清理积压

**场景和流程**：Snapshot 标记后等待五分钟，只保证不早于期限回收；实际回收由后台扫描。
当前调度每 15 秒做 snapshot cleanup，再扫一页 upload（20 个 key，包含子记录）。
100,000 条稳定记录理想扫描周期已约 20.8 小时；单个大 upload 还可能串行清理大量 part。
持久化每页进度有写入成本，完全重扫则在重启后重复工作。

**判断与建议**：默认用有界、公平、可幂等重做的清理，允许本地游标和进度估计；
保留删除标记、物理身份和不能遗失的清理义务。重试删除必须验证代际，不能删到复用后的数据。
若实测重扫已影响容量或明确要求多久清完，再提出 expiry index 等资源发现方案及前台写放大供 review，不能自行恢复过程检查点。

- [ ] **D5 条件决策**：P13 测得积压后，如需保证清理完成时限或重启精确续扫，再确认具体目标；
  五分钟 grace 本身不意味着“五分钟内清完”。若无此需求，按可重复计算的实现处理。

## 无需重新讨论的工程判断

- P1 投影是可重建派生数据，当前失败就回退 canonical 内容；从 GET 中去掉同步持久化
  不改变表的权威数据合同。先验证冷读/热读成本，再选无缓存、本地有界缓存或发布时构建。
- confirmed CAS conflict 使用 observed value、已成功写入后的状态直传、请求内复用
  immutable descriptor、减少不必要复制，均可在现有合同下进行，不必逐项请求产品批准。
- namespace 非空删除、名称唯一性、共享 payload 引用、物理代际、已提交数据持久性、
  鉴权撤销边界不重新投票。若发现其实现复杂，先提出保持这些约束的更小方案。
- 尚未证明性能收益或未确认调用路径属于调查任务，不当作必须由用户回答的问题。

## 持久化清理任务

每项先记录正常/冲突/结果未知流程和必要不变量，再修改。D1/D2 和 D3 原则已经确定，
据此删除过程状态；任何需要保留中间状态的具体例外先提交 R1–R4 review。R4 是去掉旧过程记录的验收前置条件。

### P1 — Iceberg GET 派生投影

P1.1 已完成，见验证记录；当前 TableLoader 直接解析 canonical 内容，不读写持久化投影。

- [ ] **P1.2 验证剩余解析 CPU 成本**：当前选用无投影缓存；旧路径无论命中与否都读 canonical，
  额外访问持久化投影只节省部分解析。零投影 RPC 已有回归，尚未测混合大小的冷/热 CPU 和
  p99。与 F9 一起测量；只有解析成为瓶颈再比较本地有界缓存/发布时构建，明确上限和失效身份。
  Files: Iceberg `table/load.rs`、相关 table/projection 测试及真实负载。

### P2 — Dataset Composition（执行已确认 D1）

- [ ] **P2.1 分清输入、计算进度与候选内容**：逐项列出 draft/edit receipt/work page/row/candidate/
  cleanup seal 的用途、写入次数和调用者；按 D1 更新 API/SDK 合同，移除服务端续算接口。Files: Dataset `composition/{begin,edit,state,evaluation,evaluate_step,prepare,cleanup}.rs`。
- [ ] **P2.2 删除服务端编辑/计算过程**：删除 durable 编辑/进度阶段，客户端失败重新请求；
  保持发布前不可见、expected head 及源 Snapshot 被删时的明确错误。孤儿清理通过 R4 验证，例外封口记录先 review。Files: `composition/`、Access v2 composition、SDK 和恢复测试。

### P3 — Dataset Split（执行已确认 D1）

- [ ] **P3.1 分开临时评估和已发布成员**：盘点 edits/evaluation/overlap examples/work pages；
  保留历史 revision 和冻结成员，不能重算时偷偷改变既有 train/test。Files: Dataset `split/{lifecycle,state,work,evaluation,prepare,initial}.rs`。
- [ ] **P3.2 实现选定 Split 流程**：按 D1 删除辅助进度与检查点，客户端重试，保留 currentSplit
  发布 CAS、覆盖/重叠校验和旧 revision 读取。Files: `split/{publication,recovery,cleanup}.rs`、v2/SDK Split 和恢复测试。

### P4 — Dataset 上传、索引和 owners（执行已确认 D2）

- [ ] **P4.1 缩减 upload/part 状态与回读**：列出 create/intent/write/accept/index/complete/abort
  的必要资源事实与过程状态；删除后者，改成客户端控制完整 part/最终清单；
  成功结果直接返回，不为续传保存进度。需要保留 intent 的具体反例先按 R4 review。Files: Dataset `upload.rs`、`upload/{parts,index,ownership}.rs`。
- [ ] **P4.3 缩小 owner/part 争用范围**：测 128-part 索引页、增长的 retired owners 的争用和编码；
  比较更小记录，证明 claim-vs-reclaim 和迟到 claim 不破坏共享引用。Files: `upload/{index,ownership,retirement,reclaim}.rs`、相关并发/丢回复测试。

### P5 — S3 multipart（客户端控制 part；协议资源与过程状态分开审查）

- [ ] **P5.1 精简 MPU 持久化与重复装载**：先说明 UploadId/ListParts/Complete 所需资源事实，
  删除无用过程阶段。已完成正常 part 的 session/generation/pointer 直传，repository 成本
  从旧 `7G + 4C` 降至实测 `2G + 4C`；独立 identity 查询仍验证 listing index。
  剩余任务是客户端最终清单与 pending/accounting/generation 的必要性审查及删除，
  ListParts、完成数据和最终对象引用不能丢失。Files: S3 `metadata/multipart_repository{.rs,/parts.rs}`。

### P6 — Iceberg 旧 multipart / 全局 credits

**补充 review（2026-10-24）**：`reserve_for_tests`/`initialize_for_tests` 仅由兼容 fixture
调用，生产新上传没有全局硬配额入口。仍活跃的 `MultipartRecovery` 却在每页扫描前调用
`MultipartAdmission::load`（context 前后检查加 policy，共 3G），旧终态 upload 又通过
`release` 写 session snapshot、共享 pending 和 released receipt。其作用只是结算已停止
使用的计数器，不决定 part、已发布 FileId 或 GC 的物理引用资格。

移除方案：删生产 reserve/release/settle driver 和恢复中的 policy 步骤；已存在的 session
继续按自身期限、part/publish 资源事实处理，忽略 legacy credit。仅有 pending Reserve、
还没有 session 的旧记录不再凭 after payload 重新创建 upload：旧 reserve 的顺序是在
session 准入前，没有该 session 就没有合法 part 写入。pending Release 的 after 只修改
credit/revision，不改变已发布文件，故也无需回放。旧 policy、payload、credit 字段保留
只读格式识别，不将旧计数器归零冒充业务数据清理；其旧 key 的清理归 P13 的 legacy
metadata 扫描。没有活跃硬配额需求，不触发 D4。测试覆盖遗留/损坏 policy 对空页与已存在
上传零影响、过期与终态资源不丢失，恢复总读取由空页 5G 降至 2G，零 policy GET/写。

- [ ] **P6.2 删除确证无用的协调**：没有调用/恢复义务的代码可删；有旧记录则给出迁移/结算方案。
  只有仍有全局硬配额需求才触发 D4。Files: `file/multipart_admission.rs`、`file/multipart_credits/settle.rs` 及恢复测试。

### P7 — Iceberg 物理写入（执行 D2；必要安全例外先 review）

- [ ] **P7.1 区分安全事实与续传检查点**：逐一标明 ownership/generation、write intent、偏移、
  completed file、adoption receipt 防止的失败；可读 bytes 不等于已确认提交。Files: Iceberg `file/{writer,write_intent,injection}.rs` 和 writer 子模块。
- [ ] **P7.2 精简可重做阶段**：按 D2 删除未完成偏移/步骤恢复，复用请求已知 descriptor；
  保留复用存储拒绝能力；若认为须保留 intent/checkpoint，先给出具体 R4 反例和替代方案。用丢失写回复、旧实例迟到、重启/回收注入验证。Files: 同 P7.1 及 file recovery 测试。

### P8 — Iceberg RetryLedger（执行 D3；按 route 判定必要性）

HTTP RetryLedger 的新请求入口与执行代码已删除，见验证记录。旧格式仅供解码/GC；
内部业务 driver 的清理继续由 R2/R3/P9/P10 跟踪，不再优化已删除的账本路径。

### P9 — Iceberg namespace / table 多阶段操作

- [ ] **P9.1 直传阶段状态与上下文**：journal CAS 后复用 applied/observed，避免重新 load +
  ActiveRoot 检查；逐个标明必须重新检查的 clear/drop 边界。现有 D 层 namespace load
  静态成本为 `2 + 2D` GET。Files: Iceberg `namespace/`、`table/lifecycle/`、`commit/create/`、`catalog/repository.rs`。
- [ ] **P9.2 删除或替代阶段 journal**：画 create/drop/rename 的失败点与恢复流程；保持非空删除
  失败、名称唯一、代际隔离。按 R3 先审查无 journal 方案，确需中间状态再 review，不能用 eventual emptiness 简化。Files: 同 P9.1 及 namespace admission/drop/recovery 测试。

### P10 — Dataset 发布与显式资源控制

- [ ] **P10.1 区分发布事实与辅助回执**：区分 immutable Manifest/Snapshot/Split、latest/stable、retention
  与 attempt/receipt/visibility 的重复证明；最终发布 CAS 保持 expected base。Files: Dataset `authority.rs`、`authority/publication/`、`split/{head,publication,recovery}.rs`。
- [ ] **P10.2 精简额外标记**：依据 P10.1、D3 删除不必要恢复写；有必要回执反例先按 R2 review；retention API 查调用者和用途，
  不在无替代合同下删除用户 pin。不默认承诺 head 再次推进后仍可查询原操作的精确结果。Files: 同 P10.1、publication/retention 测试。

### P11 — S3 object / bucket 发布

- [ ] **P11.2 锁定普通对象最小路径**：普通 PUT/DELETE 已为无条件 mutation，不加 GET/CAS；
  单独验证 conditional 请求和 multipart 发布，记录包括 bucket resolution 的端到端成本。Files: S3 `publication.rs`、`metadata/namespace.rs`、对象/桶测试。

### P12 — Dataset inspection 索引

- [ ] **P12.1 比较 routed batch 与已完成的有界并发**：当前 F 个 schema field、N 个 sample、
  E 个非空 sample-field 仍有 `F + N + 2E` 行写和 3 个 summary/index 写。
  已完成 16 条/2 MiB 的并发准备，消除逐行串行等待；后续测 grouped RPC 能否进一步减少
  RTT 与消息数量，计入 batch 输入复制、分区分组和失败重试成本，不能把并发算成 RPC 数减少。
  Files: Dataset `inspection/{index,preparation}.rs`、`chunk_store.rs`、Chunk-KV batch 接口。
- [ ] **P12.2 精简索引准备/回收检查点**：保留服务必需的不可变索引，按 D1 删除辅助恢复进度，失败重做；
  未发布索引/候选纳入 R4 孤儿清理。显式 legacy preparation 仍是 mutation，读 miss 不自动持久化构建。Files: `inspection/{legacy,retirement,reclaim}.rs` 及索引恢复测试。

### P13 — GC / expiry / cleanup

- [ ] **P13.2 审查 cleanup 持久化**：将 claim/phase/cursor/tombstone 分为可重做进度与不可丢的
  义务/代际；即使重扫昂贵也先优化发现/预算；新增持久化信息必须 review，用途和额外前台写均写明。Files: Dataset `authority/access/`、Split/Composition cleanup、Iceberg `gc/`、S3 terminal cleanup。
- [ ] **P13.3 量化回收滞后**：稀疏过期、大 upload、多 cleaner、频繁重启、损坏记录下测 backlog、
  前台 p99 和孤儿收敛；如需确定完成时限再讨论 D5。Files: 相关 cleanup 测试、负载工具。

### P14 — Catalog 与凭据

- [ ] **P14.1 审查 root/context/初始化回读**：记录正常请求内重复 GET 和初始化写，复用已验证
  immutable 事实，保留撤销和 catalog reset 的准入边界。凭据持久化不是 reader session。Files: Iceberg `catalog/{repository,storage}.rs`、Access `credentials.rs`、`credentials/ensure.rs`。

## 其他性能任务

- [ ] **F1 请求级成本计数**：记录 GET/bounded GET/scan/CAS/PUT/delete、重复 key、冲突与
  unknown、真实 routed RPC、串行阶段和复制字节；用测试 wrapper/已有 metrics，不加生产
  每 RPC 日志或 profiling 锁。Files: Dataset/S3/Iceberg 测试 store、现有指标、`tools/sdk/`。
- [ ] **F2 复核 legacy/多源读取的重复准入**：现代 continuation 的 selection→HTTP/native
  delivery 已共用请求局部的 Snapshot/schema/membership，Split 同 Snapshot 也复用准入。
  剩余复核 legacy `DatasetReadService`、多源/祖先元数据，独立入口和不同 Snapshot 仍各自准入；
  不跨请求缓存删除许可。Files: Dataset `transport.rs`、`inspection/`、`split/`。
- [ ] **F3 避免无状态读取重复规划**：大 shuffle group + 小 batch 会反复重建同一 group；
  selection 窗口还重复加载 schema/membership。比较有界客户端 group 缓存、请求内规划复用
  与 token 大小；不恢复持久化 session。Files: Dataset `read_query/page.rs`、`inspection/evaluate.rs`、SDK iterable。
- [ ] **F4 替换 legacy scan 全量规划**：`scan_surface` 每页重建所有窗口，造成 CPU/内存放大；
  追踪调用者后迁移到有界 inspection 或增量 planner。Files: Dataset `transport.rs`、`planner/`、legacy API 和 scan 测试。
- [ ] **F6 剩余 inspection 并发与 routed batch**：delivery 的 membership/field/locator 已有界
  并发；matrix/selection/composition source 仍有串行步骤，bounded point read 在 chunk store
  内还转成单 key scan。比较有字节界限的 routed batch，保留 materialize 前的聚合预算、
  共同截止时间、结果顺序和 missing/tombstone；不可把并发当作 RPC 数下降。
  Files: Dataset `inspection/`、`chunk_store/metadata.rs`、相关 RPC 支持。
- [ ] **F7 比较预读与 create-CAS 总成本**：新 immutable 页的 GET+CAS 可能比直接 CAS 更贵，
  replay 则可能相反；比较新写/重放比例，复用本请求已知值，不能无证明改普通 PUT。Files: Dataset Composition/Split receipts、Iceberg `operation/payload.rs`、journal validation。
- [ ] **F8 减少 payload 复制**：审查 HTTP 收整 part、`BytesMut` 拼接、inline copy、兼容
  reader 之外的 S3 coalescing；UploadedChunkReader 已改为最终 Vec 一次拼接。先查活跃调用，再移交 owner 或流式
  发送。Files: Dataset payload/upload、Access HTTP batch、S3/native 接收路径。验证部分帧、非整齐 size、held buffer 和实际复制字节。
- [ ] **F9 分解剩余摘要/编码/分配 CPU**：单 part complete/native field 已复用同范围的已验证
  MD5，multipart 仍校验各 part 和整体，不能拼合摘要。继续测大 owner/part-index
  编码、JSON/base64、header clone 和 allocations。Files: Dataset upload、native `dataset/read/field.rs`、inspection response。
- [ ] **F10 混合大小准入与调度**：测 count/bytes 限制、慢消费者占用、单线程多个 in-flight、
  有界 prefetch、batch 等待、1 ms arena 重试和 10 ms cancellation 轮询。区分 Access、CQ/WR、
  replication/disk 瓶颈，保留鉴权/校验，禁止仅增大容量刷 TPS。Files: native runtime/arena、Dataset cancellation、现有 workload/transport 指标。

## 执行顺序与验收 checkbox

- 第一批：F1 建成本基线，P1 去掉 GET 写入；R4 验证孤儿发现/回收，R1–R3 确定各写入场景。
- 第二批：按已确认 D1/D2 实施 P2/P3、P4/P5/P7，按 R1–R3 处理 P8.2/P9.2/P10；
  只有确有中间状态必要性的例外等待单独 review。涉及孤儿的删除先通过 R4。
- 第三批：在保留下来的真实路径上做 P4.2/P5.2/P8.1/P9.1、P11、F2/F7 等 RPC 优化（F5 已完成）；
  路径若随状态删除则移除对应优化任务，避免先优化即将删除的 journal。
  F3/F4/F6/F8/F9/F10、P12/P13 按测量瓶颈排序；P6/P14 可独立审查。
- 每个可独立验证的改动完成后运行对应检查，不等所有任务完成才验证。

- [ ] **V1 单元/组件与成本回归**：正常请求、1/多次真实 CAS 冲突、缺失/终态、丢回复、重启、
  独立对象并发与同 key 竞争；新增无过程记录的孤儿回收、part 整体重传、
  无条件覆盖与 expected-base 重试测试。断言总 GET+写，不仅是零写。Files: Dataset `tests/*cost*`、`read_query*`、upload/Composition/Split/inspection；S3/Iceberg 对应 repository 测试。
- [ ] **V2 HTTP/native/SDK 集成**：各受影响 route 的鉴权、错误、协议/SDK 一致性；保留删除准入、
  迟到写者、非空 namespace、共享 owner、held buffer、cancel/drop 和损坏内容测试。Files: Access/native tests、`sdk/tests/`、native Python consumer。
- [ ] **V3 工程门禁与文档**：每批执行适用 fmt/clippy/测试，全部经 `pixi run`；同步 owning
  design/spec/OpenAPI/SDK。只修改文档时检查 diff、引用和任务状态，不为此跑整套服务测试。
- [ ] **V4 同源真实负载验收**：重新构建当前源码的镜像，运行现有 HTTP/native Rust/native Python
  混合 size、restart 和本机 HCA 对比；固定核心、并发、请求完成口径、数据集和完整性校验。
  记录 p50/p99、TPS、CPU/RSS、RPC、复制/摘要、清理积压，不缩小负载掩盖失败。
  Files: `tools/sdk/dataset_workload.py`、相关 examples、`e2e-test/`、本机 HCA runner。
  物理多主机验证继续留在独立计划，不并回此处。

## 成本口径与集中代码索引

- `G` 是一个逻辑 point GET，`B` 是有界读，`S` 是 scan，`C` 是条件 mutation，`W` 是无条件 mutation。
  `C(None)` 是 create-if-absent，也消耗 RPC。上述数字不含嵌套路由/复制/WAL/物理 IO，
  单函数静态数不能当端到端 RTT 或 TPS 结果；批量 RPC 也未必减少后端 IO。
- CAS 必要性按用例证明：发布 head 的 expected base、名称唯一、part/finalize/abort、
  共享引用 vs 回收、迟到写者隔离。明确失败冲突可复用 observation；unknown 按该操作语义处理，不默认追加查询。
- Dataset 路径前缀：`lib/crowdb-access-dataset/src/`；重点 `read_query/`、`upload/`、
  `composition/`、`split/`、`inspection/`、`authority/`、`transport.rs`。
- S3 路径前缀：`lib/crowdb-access-s3/src/`；重点 `bucket.rs`、`publication.rs`、`metadata/`。
- Iceberg 路径前缀：`lib/crowdb-access-iceberg/src/`；重点 `table/`、`metadata_projection/`、
  `operation/`、`namespace/`、`commit/`、`catalog/`、`file/`、`gc/`。
- 入口/客户端：`app/crowdb-access-server/src/`、`lib/crowdb-sdk-native/src/`、
  `lib/crowdb-sdk-native-python/`、`sdk/`。检查现有调用者后再删除代码，不把 legacy 算作活跃路径。
- 先前检查以上 Access production trees 未发现直接 Mutex/RwLock；不代表依赖、FFI、内核无锁。
  下层改造应由 profiling 证据触发。共享 `crowdb-access-multipart` 主要做计算/验证，不发 storage RPC。

## 已完成与验证记录

- **2026-10-24，V2.3**：当前 Snapshot 源码镜像
  `crowdb-node:access-cleanup-snapshot-head-20261024` 的 Dataset 全部五项容器用例通过，
  包含 JSON 发布/持久化重启、鉴权、Split 历史、真实 Rust/installed Python SDK 数据与
  直接 head API。首轮四项通过，Split fixture 仍要求完整但未发布 Snapshot 返回 404；
  按已确认的合同改为 summary 200、latest 仍 absent，重启后完整内容相同，再发布。
  第二轮五项全通过，artifact 为
  `.crowdb-runtime/artifacts/container-e2e/run-542c939cb0b24685b432f4c1e5412dc6`；
  失败首轮 `run-248a6a5222644cca983da2ca5bf7f5de` 保留，未掩盖失败。
  image/client revision `ed9fa01a54b92447749f7a77f9c2a2b42a58b7ce`、
  source tree `b5571cd807daed143af91ff0a724e9c60efb59a2`，server_sources_match=true；
  OCI digest `sha256:1f632c08a0cb09a9dcfb0c17fb1d50ead4672b85cb857d3cb07b4a8320979e30`，
  Docker identity `sha256:50fbad326aa9764cf0a3b8f0b717cc2780532c99e65f586a8e6909a196c45fa2`。
  仅客户端断言调整，服务端源码保持同源；e2e-client all-target clippy、fmt、diff 通过。
  此为本机 TCP 功能/重启验证，未测 TPS/HCA 或完整孤儿 GC，V4 和 R4 仍未完成。

- **2026-10-24，R2.2d**：新增 `PUT .../datasets/{dataset}/head`、
  `snapshot-head-update` 能力、Rust/Python `update_snapshot_head`；只接收完整 Snapshot ID
  和必填 nullable expected head，body 上限 4096 字节。不读取草稿、使用 attempt 或写请求
  账本；不支持能力时 SDK 拒绝写入，不降级。409 提供 typed observed head，Python 同步和
  异步均保留 Snapshot identity，重试由调用者明确决定。GET head 删除重复读取，由 3G
  降至 2G；PUT 正常 3G + 1C，相同当前结果/过期条件 3G + 0C。真实 listener 覆盖显式
  复用观察值、删除资源、缺失、鉴权、错误条件和超限 body，非法输入零存储调用。
  示例在开始计算前固定 head 条件，新 Dataset 直接使用已知 absent；不会计算后刷新
  base 强行覆盖并发结果。13 项 native Snapshot/legacy/cost 测试、6 项 listener/consumer
  测试、独立 SDK 32 项测试和 installed wheel 10 项 Python 测试通过；真实 Rust 和 Python
  Split consumer 均已改为直接 Snapshot 发布，包含同步/异步冲突。最终 sdist/wheel 位于
  `.crowdb-runtime/artifacts/sdk-snapshot-head-20261024`。三 crate all-target/all-feature
  clippy、SDK all-target clippy、两个 workspace fmt、SDK boundary、diff 检查通过，均经
  pixi。OpenAPI 解析及 338 个本地引用校验通过。新源码容器复验仍由 V2/V4 跟踪。

- **2026-10-24，R2.2c/R1.1**：Snapshot 新发布只写 version 4 fenced head，删除 CA/CR/SC
  写入、head 成功补写和新 Composition Committing/Published 转换。当前同目标确认效果，
  后续目标遮住的旧条件返回 observed latest/stable/generation，真实 CAS 失败直接解码返回值，
  unknown 不确认 GET。原生 `SnapshotHeadUpdate` 不含 attempt ID；compatibility response
  仅回显调用者输入。正常/真实冲突各 3G + 1C，当前目标重试/过期条件各 3G + 0C；
  四次真实冲突后客户端直接复用返回代际，共五次请求 15G + 5C，没有单独 head 查询。
  Composition adapter 只多一次草稿 GET（4G + 1C），Prepared 状态保持不变。
  cleanup 在最终 owner 就绪后可释放 staging owner，Prepared 计算 scratch/edit payload
  也可回收，最终数据及初始 Split 仍保留。旧 Committing 只读取既有结果后复位；旧回执
  和 version 3 attempt head 只读，不补写。旧 CB v1 Published 通过有界身份/digest/初始
  Split 校验保持可读，新暂存绑定用 version 3，不能旁路最终根。旧无 CB 资源仅保留已有
  bounded legacy ancestry anchor；stable-only 旧根也覆盖，现代 head 不做该查询/写入。
  同时删除 JSON 发布的重复预检、整次二次 prepare、完成回查和响应 operation/Snapshot
  回读；获取 owner 前先保留候选可发现性。单 inline 请求真实 listener 冷发布从 28G + 25
  mutations 降为 12G + 15；同内容重放从 24G 降为 11G、零写。40 个 sample 实测 12G +
  132 mutations，重放仍 11G、零写，immutable 行只准备一遍；去掉一份 Manifest Vec 复制。
  上述均为逻辑元数据调用，未等同于 routed 网络 RPC 或 TPS/CPU 实测。
  Dataset 全目标与追加 cost/legacy/ownership 回归通过；HTTP restart/冲突观察值重试、
  Rust/installed Python SDK 与 payload consumer、11 项 native 读取测试通过。三 crate
  all-target/all-feature clippy、独立 SDK 与 e2e-client clippy、两个 workspace fmt、diff
  检查通过；OpenAPI 解析及 333 个本地引用验证通过。均经 pixi。
  R2.2d 跟进直接 REST/SDK head API；P2/P3/R4 继续删除计算过程与补全孤儿 GC，V2/V4
  仍需新源码镜像复验，不能把此批算作全部清理完成。

- **2026-10-24，R2.1/R2.1b**：新 Snapshot 最后将已有 CB 条件替换为 version 2 完整根，
  Manifest/index、初始 Split 与必要 upload owners 就绪后，才能按 ID 读取/引用、列入
  inventory/history、retain 或设 stable。版本 1 准备绑定和 Published 标志本身不够；旧成功
  记录仍只读兼容。放弃/替换草稿保留完整结果及 owner；未完成清理先竞争同一 CB 封口，
  迟到完成失败。清理 CAS 观察到完成者后直接保留，无补 GET；完成回复未知直接传播，
  不确认查询。子索引先写，完成前再检查 parent；未终态退休的子索引保守保护 parent。
  完整 ID summary 实测三次 bounded GET（删除标记、CB、summary）、零 mutation，
  不查 head/SC/CA/CR/LC。六项新测试覆盖完成丢回复、双方竞争、父删除两个顺序、
  跨实例 continuation、retention 与删除。上传测试覆盖 owner 未就绪时 ID/原始发布均拒绝，
  放弃后仅释放 staging owner；初始 Split 的完整成员可保留并清理 scratch。
  JSON 与旧 bincode Manifest 入口新增统一只读准入，未完成 404、完成未发布 200、删除 409。
  legacy scan/空结果仍独立准入，移除与 Snapshot 读取重复的外层删除检查；单窗口仍一次
  删除标记读取。修复 prepare→重启→publish 对完整结果误早退，以及当前旧 head 补 Manifest
  后漏安装最终根的问题，均有回归。
  Dataset 全目标与受影响追加回归、Access HTTP/安装 SDK、11 项 native 读取测试通过；
  三 crate all-target/all-feature clippy、fmt、diff 检查通过，均经 pixi。
  Snapshot CA/CR/SC/helper 尚未删除，R2.2 继续处理；未引用结果期限/独立孤儿 GC 仍属 R4。
  同源码容器最终复验由 V2/V4 跟踪，此批不声称 TPS 或 GC 完成。

- **2026-10-24，F1.1**：同一 SDK client/clone/control wrapper 共享固定有界 atomic bitset，
  复用已成功发现的路由能力。连续五次 head 读取实测由十次 HTTP 请求降为六次；Split
  冲突显式重试由 capabilities GET → PUT → capabilities GET → PUT 降为 GET → PUT → PUT，
  使用冲突返回的 observed head，不补 head GET。每个业务请求仍独立取凭据并由服务端鉴权、
  检查删除与 expected base；未新增锁、持久化、TTL 准入缓存或依赖。
  显式 capabilities() 总是重新发现并替换提示；不缓存缺失能力；未知传输/解码结果不当成
  撤销，已解码的不支持版本清空提示。冷启动并发允许有界重复发现，无 single-flight 状态；
  取消不安装提示，服务能力变化由真实版本化路由拒绝，不能降级写入。
  独立 SDK workspace 的 29 项测试、all-target clippy、fmt、边界检查通过；sdist 在仓库外
  独立测试并构建 wheel，安装后九项 Python 单元测试及 Rust/Python 两项真实 HTTP Split
  consumer 通过。wheel 位于 `.crowdb-runtime/artifacts/sdk-capabilities-20261024`。
  SDK 变更不改变服务端镜像；此处是实际请求数与功能证据，没有声称 TPS 提升比例。

- **V2.2（2026-10-24）**：源码 `5f18bd58`、source tree
  `1f22dcff0524d743893bc993824350827dc6049f` 稳定构建的新镜像
  `crowdb-node:access-cleanup-split-head-20261024`，OCI digest
  `sha256:cd582599b79107dcd45ce814f642f0c949dfd30e68f431e0ffb23e91eb29724f`，
  Docker identity `sha256:cee85d588e1c317e61d0bd462bc74b5cef65b058fe8e80dd7666c2d949b80c27`。
  全部五个 Dataset 容器用例通过：发布/读取/重启、Split 完整候选和无新 receipt、鉴权、
  实际 payload 重启、独立 Rust 与新安装 Python wheel 的直接更新/冲突观察值。
  客户端同 revision；完整日志/身份/用时位于
  `.crowdb-runtime/artifacts/container-e2e/run-a8f9817b8c8b40958ff08325a52265d5/`，
  status=pass，client build 33.35 s、SDK build 21.92 s、场景 54.58 s。
  build/import/验收均经 `pixi run`；首次 build 的 Git 快照写被沙箱只读 `.git` 拒绝，
  同命令提升权限后完成且保留 source guard；镜像只导入独立本地 tag。
  此项是同主机 TCP 正确性/重启验收，不替代吞吐、HCA、独立 GC 发现或 Snapshot 主线清理。

- **R2.2b（2026-10-24）**：新增 `PUT snapshots/{snapshot}/split-head` 与
  `split-head-update` 能力，Rust/Python SDK 提供 `update_split_head`。仅携带完整 revision
  和两个必须显式提供的 nullable expected base，不携带 attempt ID，不查 draft，不保存过程。
  Native 共用 `SplitHeadUpdate`；旧 attempt 入口仅适配响应。条件失败携带 boxed head
  observation，早期 base 不符和真实 CAS loser 都不回读 head；观测值可能随后过期，客户端
  审阅后显式组成下一次请求，SDK 不自动 rebase。旧服务无能力时不回退到 draft/v1。
  Rust `SplitHead::from_conflict`、Python `ConflictError.observed_head` 完整保留观察值，
  旧错误无该字段仍可解码。新增两个 HTTP 用例实际删除 draft 身份后发布，断言首次
  5G+1C、重复确认 4G+0C、冲突 4G+0C、基于返回值再提交 4G+1C；鉴权/缺省字段失败
  不接触 storage，缺失最终资源 1G+0C。读计数包含 bounded GET，不能只看旧 reads 字段。
  Dataset 全 target、十一项 HTTP/Rust SDK、独立 SDK workspace、四 crate all-feature
  clippy 和独立 SDK workspace clippy、fmt/diff/boundary 均通过。sdist 独立测试并打包的新
  Python wheel 安装到 `.crowdb-runtime/artifacts/sdk-split-head-20261024/consumer/`，
  显式运行安装后 Python HTTP consumer 验证更新与观察值通过。SDK 抓包同时发现每调用
  都多 GET capabilities，已先登记 F1.1，未把当前两次 HTTP 往返称作单次。容器验证由 V2.2
  继续，Snapshot 完整准入与 P2/P3/P4/R4 主线仍未完成。

- **R2.2a（2026-10-24）**：新增直接接受完整 revision/显式 expected revision+generation
  的 `publish_split_revision`，旧 draft 发布仅作适配；新发布只 CAS currentSplit，不再写
  CA/CR/SRC、Committing/Published，也不帮助补写前一发布。相同当前结果返回当前事实，
  H0→H1→H2 后重试 H0→H1 返回条件冲突；未知 mutation 原样返回，不确认 GET。
  legacy attempt/receipt/head 解码与旧 Committing 恢复仅用于旧输入，新操作不创建它们。
  直接首次发布为 5G+1C（含空 head 时 initial-Split 查询），已有 head 时 4G+1C；确认相同
  结果或拒绝旧 base 为 4G+0C。实际 key 差集只新增 Split head，CAS definite loser 的返回
  值直接解码，unknown 没有额外读。兼容 REST 当前仍多两次 draft GET，由 R2.2b 消除。
  回收 fence 同时修正为复用 CAS observation；有/无 initial/current revision 均连续四次
  冲突后只读 head 一次，缺省 initial binding 在本请求只查一次，不做跨请求缓存。
  新增五个成本/故障用例及现有 publication/reclaim 合同回归；Dataset 全部 target 通过，
  新增 H1 丢回复再被 H2 覆盖、缺省 head 竞争的聚焦用例随后通过。七个 HTTP/Rust SDK
  用例及显式 `--ignored` 安装后 Python consumer 通过；Python 使用 V4.2 同一 SDK 源码的
  已安装 wheel，当前服务为新代码。四个受影响 crate（含 e2e-test）all-target/all-feature
  clippy、两 workspace fmt/diff 通过，均经 `pixi run`。容器 fixture 已增加未发布 ID 读取与
  无新 Split receipt 断言，尚未运行新镜像；新真实验收继续由 V2/V4 跟踪。

- **R2.1a（2026-10-24）**：完整 Split 的 ID 读取、history、冻结继承和 continuation
  直接使用最终 `SplitRevisionRecord`；移除读取 SRC/currentSplit 的门槛和闲置 initial-Split
  proof helper，Snapshot 准入与成员页校验保持。无新持久化记录、锁或后台 reader 状态。
  scratch cleanup 与最终 root 在同一 key 上 create-if-absent：已有完整 root 或冲突返回完整
  root 就保留成员/summary，仅清理 evaluation intent；清理先封口时迟到 prepare 失败。
  CAS 返回值直接使用，不再为确认结果读第二次 root；storage unknown 原样返回。
  新增四例覆盖 CAS 输家、最终 root 丢回复、清理 CAS 竞争、迟到准备、未发布结果继承及
  owner 删除；增加 head CAS 丢回复后的直接读取。无发布 helper 的 ID 读取实际为 3 个 bounded
  GET（Snapshot 删除准入、Snapshot 现有成功记录、Split root）+0 mutation；root 只取一次。
  Dataset `cargo test --all-targets`、七个 HTTP/独立 Rust SDK 用例、三个相关 crate 的
  all-target/all-feature clippy、SDK example clippy、两 workspace fmt、SDK boundary 与
  diff check 通过，全部经 `pixi run`。HTTP 首次因沙箱禁止本地 bind 失败，提升权限后相同
  用例全部通过；继承旧测试仍要求未发布结果不可用，已按用户选定合同更新并保留缺失名称
  与删除拒绝断言。文档同步 control design/spec/OpenAPI/SDK。Snapshot 完整准入、
  发布账本删除、独立结果期限 GC 尚未完成；此批未构建新镜像或声称吞吐提升。

- **V4.2（2026-10-24）**：稳定源码树重新构建成功；本地镜像
  `crowdb-node:access-cleanup-acd25325`，revision `d82721f1`，source tree
  `7e19feabf2c73f469fa93156b6239cb79b040244`，OCI digest
  `sha256:7ddb31ac6ebd3c1cab0fb0c5b118ab0209d0b4c1139bd3cca32fb4572ddfce55`，
  Docker import identity `sha256:21d9ec146e42f4c3c36453232d7c0566766372ebe283e55984b10a3641e39ba0`。
  全部五个 Dataset 容器用例通过，记录在 `container-e2e/run-e2c9c096a5b34e7e9d1ea0aefe5d0757`；
  同 V4.1 参数的完整 TCP workload、native Python 安装/parity/held buffer/cancel 与存储重启
  校验均通过，18 档试验 errors=0，记录在 `dataset-workload/e538038bee3f4153b78d01bca7829aac`。
  两个路径均相对于 `.crowdb-runtime/artifacts/`。并发 1 的中位 MiB/s 为 HTTP 169.52 /
  Rust 185.83 / Python 189.66，并发 3 为 258.62 / 254.11 / 264.70；最大 client RSS
  分别约 19/32/56 MiB。相对 V4.1，HTTP 接近，Rust 低约 7%–11%，Python 并发 1 低约 9%、
  并发 3 接近；这次短负载没有证明吞吐提高。4 KiB 并发 1 中位 p50 为 578/712/829 µs，
  每档每尺寸仅 16 个延迟样本，不作尾延迟生产结论。当前 workload 单 sample-field，不能
  证明宽投影并发收益，也没有逐请求 RPC、复制计数或 HCA 对比，V4/F1/F6 继续跟踪。
  首次构建因中途修改非忽略文档/测试触发 source guard 失败，重跑保留 guard；成功记录不
  覆盖失败记录。构建/import/容器/workload 命令全部通过 `pixi run`，镜像仅导入本地。

- **F8.3（2026-10-24）**：`ChunkPayloadStorage::read` 在读前按逻辑长度限制最大 4 MiB，
  单块完整结果确认 EOF 后直接移交 Bytes owner；多块只按最终长度分配一次，复制当前片段后
  立即释放其读 credit 再 pull，避免单 slot 死等。没有新增 metadata/RPC/持久化，仍检查
  空/短/超长片段和迟到 EOF 错误，后续 whole-part MD5 校验保留。三项 collector 回归用
  实际 allocation pointer 验证单块未复制、owner 随返回 Bytes 存活；多块模拟单 credit，
  覆盖非整齐长度、4 MiB、失败后释放。非法“完整对象后额外数据”的故障流单独允许越界输出，
  不套用合法对象的 credit 假设；collector 仍明确拒绝，未降低该错误断言。
  upload read/stream/完整性共十三例、HTTP batch 与独立 SDK payload 两例、native
  digest/payload/read 八例通过；Dataset all-target/all-feature clippy、fmt/diff 通过。
  尚无本项的同源性能测量，F8 的 HTTP 收整 body 和其他产品复制继续审查。

- **P6.1（2026-10-24）**：核实新前台上传不调用全局 credits；旧 `initialize/reserve` 创建
  接口限于 test-util 并命名 `*_for_tests`，生产仅保留历史资源的 load/settle/release。
  不删除未结算的旧 pending reserve：该记录可能是尚未建立 MultipartSession 的唯一恢复入口；
  旧 terminal release 仍需防止计数重复返还。它们不再由新上传创建，后续迁移继续由 P6.2 审查。
  旧 `part` 前后各 load（root+session）总计 5G，仍服务 legacy tree completion/publication；
  新 admitted streamed part 为 1G+1C。正常 begin 新增断言 1G+1C/无 credit，两条 streamed
  part 为 2G+2C，key 集合没有 policy/OperationPayload。六项旧 admission、三项 record、
  八项 repository、六项 recovery/budget 测试通过；prod-only clippy 与 Iceberg/E2E
  all-target/all-feature clippy、fmt、diff check 通过。没有引入替代配额或放宽业务资源边界。

- **F6.1（2026-10-24）**：delivery 按最多 16 个 sample-field cell 分窗，同层 field/祖先和
  value 分阶段并发，membership 独立分窗；相同祖先只准入一次。每条 bounded read 发起前
  预留 response cap，在途最多 16 条/2 MiB，已完成但等前序的响应仍占预留；释放未用额度后
  才缩小最后一条读取的 cap，防止临时预留导致错误 Limit。全请求共用字节预算和绝对期限，
  返回保持 ID/字段绑定；零长度 inline、Chunk 长度/摘要、墓碑及继承原有校验均保留。
  没有增加 GET/写、跨请求缓存、session 或锁；32 sample/三字段/两代 fixture 仍为
  32 membership + 160 field + 32 value GET，祖先 summary 各一次，并发峰值不超过 16。
  新增五例验证乱序完成、预算前置、接近预算的合法前缀、缺失成员、错误摘要/Chunk 长度、
  storage failure、取消/超时释放和 HTTP 所需 Send future。Dataset `--all-targets`、HTTP
  batch/read-query/contract 七例、native digest/payload/read 八例通过；三个 crate 的
  all-target/all-feature clippy、fmt、diff check 通过。新增测试先修正了父 Snapshot fixture，
  集成编译发现跨 await 借用 iterator 的 Send 约束，已用有界具体窗口修复并加编译期断言。
  本项未取得新镜像吞吐结果，V4.1 数字是本项之前的基线。

- **V4.1（2026-10-24）**：`tools/sdk/dataset_workload.py` 完整模式退出 0，三条路径同一
  Snapshot，size 为 0/128/513/4096/65535/262144/1048576/4194321 字节，每路径并发 1/3、
  每档三次 128 请求，另有 warmup；全部校验且 errors=0，native Python parity/held lease/
  cancellation 和存储重启后实际 payload 校验通过。client CPU 2、server CPU 4–15、metadata
  每节点 2 GiB；镜像身份同 V2.1，客户端源码 `3992a23c`。完整 trial、安装 wheel、cgroup、
  日志和 manifest 位于 `.crowdb-runtime/artifacts/dataset-workload/36ba3a8f4e84427db86f7761e4ddd67d/`。
  每档三次中位吞吐（MiB/s），并发 1：HTTP 166.59 / native Rust 208.09 / native Python 208.83；
  并发 3：260.96 / 274.56 / 265.55。client CPU 每完成请求约 2.19–2.52 ms，最大 RSS 分别
  17/31/56 MiB；Python `bytes()` 复制包含在读耗时/CPU，runtime compatibility copy 为 0。
  4 KiB 中位 p50（并发 1）为 586/790/727 µs；大消息占用和并发使小消息尾延迟明显增长，
  继续按 F10 分析。各 size 每次仅 16 个延迟样本，p99 不作稳定生产结论。该运行是同主机
  TCP 短负载，未得到历史版本对照、逐请求真实 RPC/复制数或 HCA 数据，V4/F1 仍待完成。

- **V2.1（2026-10-24）**：迁移旧 RetryLedger/property journal fixture，不恢复生产请求账本。
  property 在实际 authority CAS 丢回复后检查资源已落地且 key 集合未增加，重启后重试返回
  当前 missing 状态；手工构造 legacy inline/paged response 验证存储兼容。范围通知保留精确
  revision/byte range，重复接受不声称物理容量释放，dedicated chunk 仍拒绝错误范围调用。
  新源码镜像 `crowdb-node:access-cleanup-5195dfff`，OCI digest
  `sha256:7bffacfc3824f55e06d6b109070207965af5ad2c4bee0de25594a9f7971c271b`，source tree
  `95d97a857d52552128d439baf08f00e0b1988120`；Docker import identity
  `sha256:31f9acf7749663e987d0de8ee143d87f1af8d727aaed35725525f8582b3e509f`。
  catalog 重启 `run-67ab0f42fd984da290d8d222fea37b38`、ChunkDB lifecycle/range 通知
  `run-bd3471d493434467b60c6679e60f51f1`、全部五个 Dataset HTTP/Rust/安装后 Python SDK 用例
  `run-0731e2669f5042db81212c94a59e6ab2`、namespace CRUD/restart
  `run-97eb01b45c2c4ebe91af5f73e66eef84` 全部通过。记录位于
  `.crowdb-runtime/artifacts/container-e2e/`。E2E check/clippy、fmt/diff 通过。
  首轮 catalog 失败是 Python fixture 仍要求已删除的 idempotency 广告，已改为检查不广告
  和重复删除的当前资源效果。首轮 Dataset 发现 Python iterable 把合法 worker 空窗口交给
  应用；已修复本地推进，保留非空和零长度字段的显式 ack，补空终页/无进展/隐藏 payload
  回归，重新打包安装 wheel 后整套通过。两个失败 run 均保留，未增 timeout 或降低校验。

- **P14.3（2026-10-24）**：Group CAS 的首次比较、foreign slot 后重比较、失败 slot
  结算后重比较及 owner value 比较，均直接移交同一 engine lookup 的 key/revision/value。
  可选 `FBKvCompareObservation` 贯穿 typed operations、RPC 和 client error；旧 peer 与
  非比较拒绝为 None，revision=0 的显式 observation 才表示观察到缺失，空值可保留非零
  revision。owner/record 条件带实际失败 key。成功不加 GET/写/状态；错误附加数据仅在
  比较失败时分配，避免放大每个成功 proposal 的 enum/response。凭据 definite loser 直接
  校验 observation，不再确认 GET；busy/unknown/旧 peer 仍按已有合同读最终资源。
  真实 RPC 测试覆盖连续四次冲突无 GET、缺失/空值/删除重建、owner/record 两种失败；
  synthetic wire 覆盖旧 peer、非法值/代际、busy/unknown 和缺字段。三副本测试覆盖实际
  prior accepted foreign value，异步 apply fence、内部 Group-0、凭据六例均通过。
  Protocol wrapper 十三例及五个受影响 crate 的 all-target/all-feature clippy 通过。
  E2E 编译另发现此前清理留下的旧 fixture API，列 V2.1 继续处理；未声称容器验收已通过。

- **F4.1 / F9.2（2026-10-24）**：legacy scan 只移交当前窗口的 ID，public batch adapter
  移交 ID allocation；ReadPlan limits/shuffle 一次选择累计预算，shuffle 每次执行只计算一次
  plan identity，删除逐 sample 的选中集合反查和逐 ID 的 plan 序列化/哈希。保持既有 MD5
  顺序、二进制 ID、各入口独立的预算口径和 group/offset/end cursor 合同。legacy 全量
  Manifest/选择集规划仍未消除，由 F4 跟踪；未增加跨请求缓存或持久化。
  `legacy_planning_test` 新增两例独立参考排序、边界预算和实际 service 分页验证；
  Dataset `--all-targets`、Access `dataset_contract_test`/`dataset_read_query_contract_test`
  共六个 HTTP 用例通过；Dataset all-target/all-feature clippy、workspace fmt、diff check 通过。
  本批为 CPU/复制路径改进，没有减少正常请求 RPC 或测得端到端 TPS 提升的声明。

- **2026-10-24，P13.4**：Snapshot 回收 head fence 与固定 Snapshot 退休都只初读一次，
  CAS conflict 直接接 observed，不再循环 GET。四次实际 head nonce 竞争及一次竞争退休
  中禁止读取对应 key，仍成功；合计省去五次逻辑 GET。fence 不改变 latest/stable，固定
  Snapshot 不换目标；head 的未收尾 commit evidence 先保留，竞争后的新 nonce 仍有该证明。
  missing 与 unknown 不额外查询或谎称成功；未知结果可由下一次独立请求恢复。
  新增 observed 两项、candidate publication 六项、reclaim race 一项、deletion grace 三项、
  branch reclaim 三项及 Dataset all-target/all-feature clippy、fmt/diff 通过。

- **2026-10-24，P13.1**：删除旧整 upload expiry 入口；新的 `expire_step` 每次发现至多
  20 key/1 MiB，跳过 upload 子树；每个清理窗口只扫描实际存在的至多 32 part/1 MiB，
  不逐个 GET 最多 10,000 个声明位置，通知预算至多 64 个范围。发现与清理分别限两秒；
  timeout/unknown 保留尚未接受的精确范围。通知被接受后删除 Aborted upload 的 part 资源，
  该 upload 不能再接收 part/owner；没有增加 phase/progress/queue key。队列最多 32 项，
  满时仅丢弃已服务的本地 hint，始终给新发现留位；已删除 part 不会因 hint 丢失而反复扫描，
  永久 deferred 的旧任务不会挡住后来的小任务。扫描 hint 丢失从资源事实恢复。
  Snapshot due page 也有两秒总预算，超时行保留标记并让后续行推进。服务按独立到期时间
  调度 Snapshot 15 秒、upload backlog 50 ms；失败且仍有工作时 500 ms，空闲发现 15 秒。
  10,000 声明/33 实际 part、64 个 deferred 后新增小任务、队列容量、32-range part、超时、
  cursor loss、claimed/损坏行和 Snapshot slow IO 共 8 项通过；Dataset `--all-targets`、
  HTTP lifecycle observation、两 crate all-target/all-feature clippy、fmt/diff 通过。
  这是预算和故障验证；未测真实 foreground p99/全量 backlog，P13.3 与 V4 仍未完成。

- **2026-10-24，P14.2**：删除 PreparedUser、prepare_user、准备记录格式/验证和相应状态错误；
  ensure-user 的稳定公开 key 由 user 域分离摘要确定，secret/nonce 仍随机。在原有 credential
  scope 一次 create-CAS 保存正式加密资源，没有新索引或协调 key，成功直接返回已知 token。
  初次正常路径由静态 2 scan + 1 prepare GET + 2 CAS 变为 1 scan + 1 CAS；保留一次 legacy
  inventory 查找。Group-0 conflict/busy/unknown 只直接读唯一目标，校验 user/AEAD/启用状态，
  absent 仍报原错误，不能假定成功；P14.3 追踪底层 observation。显式 issue-user 可签发
  多 token，ensure-user 没有对该独立管理操作提供跨 key 原子唯一性，既有重复仍拒绝。
  旧正式凭据保留，旧准备记录不再参与认证或恢复；实际旧 hash key 的损坏准备记录不阻塞
  创建，删除正式资源后不会复活旧 secret，disabled 也不覆盖。token 解密结果移交 Vec，
  不额外 clone secret。真实 Group-0 16 并发调用选出同一 token、restart、legacy duplicate、
  4097-row 不完整 inventory、disabled 共 6 项及 secret 5 项、两 crate all-target/all-feature
  clippy、fmt/diff 通过。没有 routed 性能数字或 TPS 提升声明。

- **2026-10-24，P14.1a**：Catalog management install/audit 复用 ledger locator 的 absent/value，
  authority publication 与退休 CAS 复用 observed，删除重复 GET。initialize 正常路径实测
  19G + 7C（原 22G + 7C），重复已完成请求 2G + 0C（原 3G + 0C）；只是逻辑 store 成本，
  不是 routed TPS。没有改变 root/context、维护 grace 或旧 management 恢复/审计合同。
  management 12、admission 3、catalog contract 6 项、Iceberg all-target/all-feature clippy、
  fmt/diff 通过；包含并发初始化 100 轮、slot collision overflow 与各写入丢回复恢复。
  management journal 和凭据准备状态的必要性仍待各自审查，未据此宣称清理完成。

- **2026-10-24，R3.1 / P9.2（rename 部分）**：新 rename 只在参与父 authority 中保存一个
  `NamespaceRename`：原精确 mapping、目标 namespace/name、head generation/fence 和本次
  reservation ID。父目录按稳定 ID 占用，不等待第二个；冲突直接使用 observed，四次真实
  冲突中禁止额外 GET 的回归通过。名字 reservation 后一次原 head CAS 切换，发布目标名，
  条件删除原 mapping，清父占位。同父目录为 6 次写入，跨目录 8 次（含 conditional delete）；
  成功后 key 总数不变，只有一个新目标名 key，没有新 journal/input/output/response key。
  未切换 head 的恢复先 CAS 推进 fence，再撤占位；已切换则补目标名。普通读不登记。
  同请求重试使用新的 reservation ID，阻止旧取消请求迟到清理新 reservation 的 ABA。
  namespace drop 遇占位先恢复再返回 Busy；下一次独立探测保持严格非空失败。反向跨目录
  并发通过 barrier 同时占首父目录，不形成等待环。每个成功 mutation 的丢回复/进程重建、
  head CAS 前后暂停、并发 commit/drop、重建源名及旧 journal 恢复均通过。
  rename 不保留历史响应：源已移动可返回 404，重建源与已占目标冲突；进行中的旧 CAS
  失败不隐式换 base，新独立请求可选择当前资源。drop/create 的旧 driver 仍在 P9.2。
  Iceberg `--all-targets`、HTTP lifecycle/namespace 9 项、两 crate all-target/all-feature clippy、
  workspace fmt 和 diff 检查通过。没有 TPS 提升实测；ChunkDB 物理回收仍独立。

- **2026-10-24，F9.1**：单 part 完成仍读取并验证实际存储内容，但其长度/MD5 与整个 upload
  相同时不再重复哈希同一范围。VerifiedUploadPart 仅在成功 EOF 后给出长度＋MD5 证据；
  native 单 part 字段消费该证据并核对字段期望值，不再次计算整体 MD5。两条路径的逻辑
  摘要输入由 2L 变为 L，IO/元数据 RPC 未减少；multipart 与 inline 继续原有整体摘要。
  失败/部分读取没有证据；同长度替换内容且伪造 part MD5 仍不能冒充原字段的整体证明，
  native EOF 报 sticky integrity error，已交付 owner 保持有效。没有放宽完整性合同或新增状态。
  upload stream 2/upload 7、native digest 1/payload 3/read 4 项及两 crate
  all-target/all-feature clippy、fmt、diff 检查通过。尚无本项的同源 CPU/TPS 实测。

- **2026-10-24，F2.2 / F8.2**：增加 `next_with_locators`，HTTP/native continuation 在同一
  调用内复用 Snapshot 准入、schema 和已扫描成员。私有准备结果绑定 authority/identity/
  Snapshot，不向调用者暴露可复用 admission；普通 next/显式 batch 仍独立检查。
  81 个候选、shuffle 取 7 个 sample：两次独立操作实测 27 bounded GET + 2 schema scan，
  组合操作为 17 bounded GET + 1 schema scan，member scan 都是五次，零 mutation。
  HTTP 无 Split/有 Split 每 next 均仅一次删除准入；旧 continuation 在后续请求仍被删除拒绝。
  Split 初始 revision 复用已完成的 Snapshot 可见性证明，没有降低 revision 绑定检查。
  HTTP 不再把 ID encode→decode，locator→body 使用移交 Vec，删除两次 inline payload 复制。
  Dataset `--all-targets`、HTTP batch/read-query 四项、native payload/read 七项和三 crate
  all-target/all-feature clippy、fmt、diff 检查通过。保留短尾 part、digest、held buffer、
  cancel/drop、空 Split、二进制 ID、未知字段/成员和跨实例重放回归。没有新增持久化/缓存。

- **2026-10-24，P12.1a / F8.1**：新建索引同时推进最多 16 条 immutable row CAS、
  队列最多 2 MiB 编码 key/value，另有一条正在编码的有界 row；没有额外拼接或复制批次。
  所有 row 成功后才写 inventory/child/ready。延迟 store 证明存在并发、条数与字节界限
  都生效；12 个 sample 的 37 条 row mutation 保持不变，不能据此宣称 RPC 数减少。
  中间行失败保持索引不可见，重建 authority 后可验证并重做 immutable rows。
  UploadedChunkReader 直接填最终预分配 Vec，删除 BytesMut→Vec 的整段复制和扩容。
  新增 inspection write 两项及 Dataset `--all-targets` 全部通过；包含既有跨 part range、
  digest、cancel、Composition/Split 及不可见性回归。Dataset all-target/all-feature clippy、
  workspace fmt 与 diff 检查通过。grouped RPC 比较及其他活跃 payload 路径继续在待办中。

- **2026-10-24，P8.2（P8.1 随路径删除）**：namespace/table 写入口不再 begin/finish
  通用 HTTP RetryLedger，删除 admission 重试、New/Resume/Replay 状态、response 编码/装载
  driver 及对应指标。表生命周期从真实业务操作读取恢复所需版本，不依赖外层 Resume 提示；
  changed request 冲突映射保留。写者鉴权、context、direct commit expected base 均保留。
  旧 Retry/RetryResult 格式仍由 codec 和 GC 识别；没有新写入口，不批量删除未过期记录。
  namespace create 的缺失父目录/无业务记录错误在父目录建好后可用原 key 成功，
  malformed 输入可修正重试；已有业务操作仍验证其身份，不靠 HTTP 缓存遮住新状态。
  相同 hash slot 的两个 UUID 请求不创建 retry binding/overflow，仍独立成功。
  namespace HTTP 4、table write 8、lifecycle 5、route 2 项通过；组件 GC 21、namespace
  update 11/create 4/drop 6、request identity 1 项通过。两 crate all-target/all-feature clippy、
  fmt 与 diff 检查通过。内部 journal 尚未删除，不能把本项当作 R2/R3 已完成。


- **2026-10-24，P9.3，R1 的 namespace 属性场景**：属性请求直接修改现有 authority，
  删除新请求的 NamespaceOperation、输入/响应 payload、pending marker 和 HTTP RetryLedger。
  请求不再携带 operation/principal 作为存储层属性参数；入口仍逐次鉴权。CAS 用于保留未提及
  的属性及删除/名称代际；连续四次真实冲突直用 observed authority，零额外 GET。
  单层 namespace 实测正常 5G + 1C（含三次 context 检查），无变化重试 5G + 0C；
  元数据 key 总数不增加。unknown 无确认 GET，客户端重试得到当次 removed/missing，
  可重写同属性，并不保证重放原响应。旧 property journal 仍可由恢复器结清，正常请求不创建。
  配置移除全局 PT24H 响应重放广告，避免把部分旧路由的能力宣称为所有 mutation 的合同。
  namespace update 11、recovery 4、HTTP write 4、route 2 项通过；create/drop/admission/model
  回归通过。两 crate all-target/all-feature clippy、workspace fmt 与 diff 检查通过。
  HTTP 服务测试已在允许本地监听的环境验证；初次沙箱 bind 拒绝不属于代码失败。


- **2026-10-24，P5.2 与 P5.1 装载部分**：S3 MPU 的内部 exchange 保留并校验 typed
  observation；freeze、abort、settle、发布完成标记和 object predecessor 冲突不再 GET
  同一 key。绑定 upload 后直读权威记录，校验 bucket/object/upload，阶段间不重读列表索引；
  reserve 成功复用本次 generation 与 pointer，后续帮助恢复仍加载其证据。
  routed actor 实测新 part `2G + 4C`、相同内容重试 `2G + 0C`、单 part freeze `3G + 1C`、
  publish `4G + 2C`；不含外层 bucket/auth/payload IO。unknown 保留现有核对语义。
  固定次数会把帮助其他 part 的有效进展算作失败，改用五秒工作期限，不放大重试次数。
  MPU repository 20 项含同 part 十个覆盖者、不同 part 十个写者、两类 object 冲突、
  终态 observation、丢回复与 exact generation；S3 `--all-targets` 及 native MPU 五项通过。
  all-target/all-feature clippy、fmt 和 diff 检查通过；没有新增过程记录、锁或 wire 字段。

- **2026-10-24，P4.2**：part 索引、abort、cleanup 完成标记、part 清理均直接沿 CAS 的
  observed value 重试；初始 part 枚举的结果也直接交给清理，不重复 GET。
  索引连续四次冲突保留其他四个 part；abort 观察到新 owner 立即停止；cleanup 在通知前后
  的连续冲突不追加 GET。create 成功 0G + 1C，冲突复用 descriptor；单页 complete
  正常/重复均 2G，复用本次已校验 parts，不回读 descriptor/index。unknown 直接传播，
  客户端可重新请求；不把 timeout 当作 definite conflict。没有改变上传生命周期或删除 intent。
  新增 `upload_mutation_cost_test` 六项均通过；Dataset `--all-targets`、all-target/all-feature
  clippy、workspace fmt 和 diff 检查通过，均经 pixi。P4.1/P4.3 与 R4 继续跟踪状态删除和争用。

- **2026-10-24，R4.4**：按用户明确的职责边界，DeleteChunkRange 成功即返回 Accepted，
  不等待 `reclaimed`；已接受的 dedicated 删除也不再检查 strips/cleanup intents 是否排空。
  Dataset/Iceberg 同步使用 Accepted，Iceberg reclaimed_bytes 注明为逻辑通知完成量，
  不代表磁盘可复用容量。无新增持久化、轮询或确认 RPC；不支持/未准入仍 Deferred，
  错误仍传播。物理回收实验另存 stash，未提交；R4.5 保留前置 QueryChunk 的简化任务。
  pixi 下 `reclamation_test` 7、`split_reclaim_test` 2、`gc_worker_test` 21、
  `iceberg_gc_budget_test` 2 项通过；四 crate all-target/all-feature clippy 与 fmt 通过。

- **2026-10-24，R4.3**：生产 DeleteChunkRange 支持 expected layout revision、封口用户共享
  chunk 的精确区间；在相同 KV group 以 chunk KV revision 条件批量更新
  `/chunk-ranges/<id>` 和原样 chunk。资源包含 writer/封口身份、布局代际、有界合并区间；
  不改变 QueryChunk 内容，不新增锁或上传 journal。重复退休零写，新范围遇 stale revision
  返回冲突，unknown 不确认 GET。该批曾让客户端等待 `reclaimed`；用户随后明确此边界过重，
  R4.4 改为成功通知即结束。当前服务仍拒绝无 qualification revision 的旧请求。
  `range_retirement_test` 7 项（真实 KV 与生产 RPC）、`reclamation_test` 6 项、
  `chunkdb_wrappers_test` 10 项通过；覆盖精确相邻/重叠、live gap、重启重放、并发、stale、
  物理身份复用、4096 上限/合并、Active/system/非法范围和未释放不误报完成。
  均经 pixi，真实服务测试先 clean-env；四 crate all-target/all-feature clippy 和 fmt 通过。
  尚未完成混存物理释放、无过程状态孤儿扫描和提交期限隔离；R4/R95/R92 未完成。

- **2026-10-24，F3.1**：同一 next 多窗口使用请求局部的 selection 准备结果，绑定
  authority/Dataset/Snapshot/规则，复用准入、summary、schema 和 explicit-ID 校验。
  81 个候选、五个窗口、两个 explicit ID：独立逐页调用实测 25 bounded GET + 10 scan，
  请求内复用实测 5 bounded GET + 6 scan；零 mutation。新请求重新准入，不共享缓存或 session。
  小 batch 跨请求重建 group 仍未处理。每窗口预算、group 两秒/5000 页/8 MiB ID 和取消保留。
  `read_query_planning_test`、`read_query_cost_test`、`read_query_edges_test`、`split_read_query_test`、
  `inspection_pages_test`、`selection_v2_test` 共 16 项通过；HTTP read-query 与 native read
  共 7 项通过。三 crate all-target/all-feature clippy、workspace fmt 通过，均经 pixi。

- **2026-10-24，P11.1**：bucket 首次 PutIfAbsent 后，tombstone 冲突直接沿 observed CAS；
  active 直接返回，missing 才重新 PutIfAbsent。名称唯一、删除重建新代际和八次上限保留；
  unknown 直接报错，无额外确认 GET。连续四次 tombstone 冲突总计五次 mutation，零 GET。
  `bucket_cas_cost_test`、`bucket_namespace_test`、`publication_store_test` 共 12 项通过。
- **2026-10-24，F2.1**：移除 bind、next、preview、非空 HTTP/native inspection batch 外层的
  重复删除准入 GET；空/legacy batch 仍独立准入。bind 和 preview 各一次删除标记读取，
  96 个并发 next 共 96 次；无 Split 的 HTTP next 加交付由四次降到两次，零 mutation。
  selection→delivery 及大 group 多页复用尚未完成，仍由 F2/F3 跟踪。
  Dataset `read_query_cost_test`、`read_query_edges_test`、`inspection_read_test`、`read_query_test`、
  `split_read_query_test` 共 10 项；Access read-query/preview/batch cost 与 native read/payload
  共 13 项通过，包含删除后拒绝 continuation 和跨实例重放。
- **2026-10-24，P10.3**：旧 Snapshot 无 LC proof 时共用有界只读 ancestry 判断，不再读时补写。
  现存 LC 继续验证；目标 Snapshot 身份/Published 状态必须正确，新候选不得走旧格式旁路。
  inspection 共用调用者的总字节/时间/取消预算；已有 head 直接传入，不重复 GET。
  `legacy_read_cost_test`、`legacy_publication_test`、`inspection_legacy_test`、
  `candidate_publication_test`、`inspection_read_test` 共 17 项通过；覆盖重复/重建 authority
  读取零写、Prepared/Aborted/身份损坏/循环拒绝、预算/取消和新候选不可见。
- 上述测试均经 `pixi run cargo test`，HTTP/native 测试先执行 `pixi run clean-env`。
  Dataset、S3、Access Server、native SDK 的 all-target/all-feature clippy、workspace fmt
  通过；没有新增锁、unsafe、过程记录或 wire 字段。成本是逻辑调用数，不是 TPS 实测。

- **2026-10-24，P1.1**：删除 TableLoader 持久化投影 GET、miss 填充和验证回执代码。
  冷读、重复读、32 个并发读取及 conditional GET 均无投影读取和 CAS；refs/all、ETag、
  parser limits、损坏拒绝和 namespace/head 变化检查保留。历史投影不参与表读取，未批量删除。
  file-level 通用 projection 仍有独立调用者，不在这批删除。
- **2026-10-24，F5**：增加请求内 `UploadReadDescriptor`，私有字段绑定原 location/storage；
  HTTP batch、native、preview、兼容 range reader 均复用一次只读描述信息。
  P 个 part 的上传元数据 GET 从 native `1 + 2P`、HTTP batch `3P + 1` 降为 `1 + P`；
  这是逻辑元数据调用数，不含 Snapshot/locator 准入、路由复制和物理 payload IO。
  不持久化、不登记 reader、不跨请求缓存准入；继续验证 part index/length/digest，
  已回收 part 报错，已交付 buffer 保持有效。HTTP 增加 locator 与描述信息的 length/md5 一致性检查。
- 本批功能回归共 **59 个测试通过**：Iceberg `table_projection_test`、`metadata_projection_test`、
  `table_load_test`（21）；Access `iceberg_table_http_test`、`iceberg_table_acceptance_test`（14）；
  Dataset `upload_read_cost_test`、`upload_stream_test`、`upload_test`（12）；
  Access `dataset_batch_cost_test`、`dataset_preview_contract_test`、`dataset_read_query_contract_test`（5）；
  native `dataset_payload_test`、`dataset_read_test`（7）。均通过 `pixi run cargo test`；
  HTTP/native 集成先执行 `pixi run clean-env`，完成短尾 part、跨 part range、校验损坏、
  回收、取消、held buffer 和零写/RPC 次数验证。
- 本批门禁：四个受影响 crate 的 `pixi run cargo clippy --all-targets --all-features -- -D warnings`、
  workspace fmt 和 `git diff --check`。无新锁、无 unsafe 例外、无 HTTP wire 协议变化。
  测试辅助代码的依赖/格式 lint 已修正；HTTP 测试释放客户端后再关闭 listener。
- **R4 未完成**：已定位物理孤儿发现缺口，并记录用户确认的未发布 part 有效期。
  已补精确范围退休，尚未完成物理释放/独立孤儿扫描，也尚未删除上传/Composition/Split
  的过程状态；不能将读取优化或退休记录算作无过程状态 GC 已完成。
  尚无当前源码的 HCA/TPS 对比，不声称吞吐提升比例。


- `489ae507`：移除 Dataset 持久化读取 session、远端 ack/resume/close、TTL/配置和 legacy
  cursor storage；HTTP 使用 `reads/bind`、`reads/next`，原 v1 直接 `reads` 保留。
  native/Python 同步迁移，读取进度归客户端；旧 session key 不再读写，尚未物理批量清除。
- 已通过 Dataset 全目标及最终 query 专项、Access Server 全目标、native SDK 全目标、独立
  HTTP SDK 测试、安装后 Python wheel 的真实 HTTP Split/read 测试、相关 all-feature clippy、
  SDK clippy、两个 workspace fmt，以及 OpenAPI 解析/本地引用检查。
- 零写回归覆盖 bind、96 个并发 fetch、跨实例重放和 legacy scan；另有 Snapshot/Split 固定、
  worker 覆盖、删除/取消和 token 大小验证。每帧 session GET 已随旧实现删除。
- 曾出现链接磁盘不足和 `reads` 路由冲突：前者仅清理可重建 incremental 缓存，后者改为
  独立 `reads/bind`；修复后完整 Access suite 通过。这不是未处理的测试阻塞。
- 以上是功能与局部成本证据。全量混合 size/restart、本机 HCA 性能验收仍未完成；
  `878ee901` 的旧 OCI 镜像不能用于声称当前改动已验收，R220 尚未完成。
