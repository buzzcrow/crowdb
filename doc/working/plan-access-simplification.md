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

## 已确定的约束

- Dataset 读取无服务端 session：无持久化 reader、选中 ID 页、进度、续租、ack/close。
  continuation 固定 Dataset、Snapshot、Split revision、投影、分区和 shuffle；
  客户端处理成功后保存 next，重用原 token 可重放，不依赖固定 Access 实例。
- Snapshot 删除：持久化标记，拒绝新请求，默认等待五分钟后回收。
  重试/重启不重置期限；已准入流可继续，过久的流允许失败，不能截断后报成功。
  Access Server 不登记读者、不互相确认删除、不异步写释放记录。
- **非空 Iceberg namespace 删除必须失败**，已由用户决定，不再提出放宽选项。
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

**继续核查（2026-10-24）**：生产 `chunkdb_rpc_service/mutations.rs::handle_delete_range`
明确返回 `Unimplemented`，没有更新 used-range/bitmap；`reclaim_location` 将其当作 Deferred。
因此测试替身返回 Reclaimed 不能证明实际范围已释放。底层依赖是
[R95](../backlog/R95-chunkdb-chunk-range-delete.md)（范围资格与孤儿发现）及
[R92](../backlog/R92-chunkdb-in-chunk-gc.md)（混存 strip 的物理回收，当前仅占位设计）。
不能用“把 RPC 改为成功”或把共享 chunk 整块删除替代实现；R95 旧文档的 reader/session
约束需按本计划已确认的删除 grace、part 期限重新整理。R168 的逐对象 cleanup record
也不是本计划自动批准的新状态。涉及新持久化资源字段先列出用途、写入频率和替代方案。

**需要 review 的物理资源事实（尚未实施）**：R95 若按范围逐步回收，需要在 chunk/strip
元数据中保留已经判定死亡的精确区间及布局代际。它表示“哪些物理字节已经不可再引用”，
不是 upload 的步骤、进度或 owner session；由后台 GC 按 chunk 合并写，前台上传不追加
cleanup/journal RPC。现有 `usage_bitmap` 描述的粒度不能直接用于任意字节范围，不能把
一个 frame 的删除向邻居取整。部分死亡 strip 暂时只记录死亡事实，释放整块分配或搬迁
存活部分后才能报告物理 reclaimed；R92 负责后者及逻辑地址到新物理布局的映射。

失败流程：GC 先完成发布隔离与活引用检查 → 以 chunk revision 条件更新死亡范围/布局 →
物理释放。进程在二者间退出时，从资源事实重做释放；丢回复时重复操作不得伤及新代际。
如果不保留死亡范围，每次必须重新从全部最终引用计算；可以先只释放完全无引用的整 chunk，
但长期混存 chunk 的孤儿空间不会收敛，不能据此宣称 R4/R92 完成。这个容量与实现复杂度
取舍需要用户确认后再扩充底层持久化结构；独立的 RPC/CAS 清理不受此决策影响。

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
- [ ] **P4.2 修复 CAS 冲突重读**：`index_part`、`abort/cleanup/cleanup_part` 直接使用 observation；
  合并不同 part 的索引更新时保留双方结果。claim/release 已正确使用 observation，不重复改写。Files: `upload/{index,ownership,retirement}.rs`。
- [ ] **P4.3 缩小 owner/part 争用范围**：测 128-part 索引页、增长的 retired owners 的争用和编码；
  比较更小记录，证明 claim-vs-reclaim 和迟到 claim 不破坏共享引用。Files: `upload/{index,ownership,retirement,reclaim}.rs`、相关并发/丢回复测试。

### P5 — S3 multipart（客户端控制 part；协议资源与过程状态分开审查）

- [ ] **P5.1 精简 MPU 持久化与重复装载**：先说明 UploadId/ListParts/Complete 所需资源事实，
  删除无用过程阶段；复核旧实现新 part `7G + 4C` 路径，把已知 session/generation/
  pointer 传给 settle；验证 identity index 是否只是列表元数据，不能未经证明删除权威检查。Files: S3 `metadata/multipart_repository{.rs,/parts.rs}`。
- [ ] **P5.2 保留 typed CAS 结果**：exchange 返回 applied/observed，消除 complete/abort/settle
  对同 key 的 conflict 后 GET；同时覆盖同 part 替换、不同 part 并发、complete/abort 和不确定写入。Files: S3 `metadata/multipart_repository/{completion,publication,terminal}.rs`。

### P6 — Iceberg 旧 multipart / 全局 credits

- [ ] **P6.1 追踪活跃与恢复入口**：新 part 前台已是准入后 `1G + 1C`，旧 `part` helper 可有
  `5G`；分别计数。列出旧记录兼容/结算责任，不能把“无前台调用”当成“可以删”。Files: Iceberg `file/multipart{,_credits,_recovery,_repository}.rs` 及子模块、Access file 路由。
- [ ] **P6.2 删除确证无用的协调**：没有调用/恢复义务的代码可删；有旧记录则给出迁移/结算方案。
  只有仍有全局硬配额需求才触发 D4。Files: `file/multipart_admission.rs`、`file/multipart_credits/settle.rs` 及恢复测试。

### P7 — Iceberg 物理写入（执行 D2；必要安全例外先 review）

- [ ] **P7.1 区分安全事实与续传检查点**：逐一标明 ownership/generation、write intent、偏移、
  completed file、adoption receipt 防止的失败；可读 bytes 不等于已确认提交。Files: Iceberg `file/{writer,write_intent,injection}.rs` 和 writer 子模块。
- [ ] **P7.2 精简可重做阶段**：按 D2 删除未完成偏移/步骤恢复，复用请求已知 descriptor；
  保留复用存储拒绝能力；若认为须保留 intent/checkpoint，先给出具体 R4 反例和替代方案。用丢失写回复、旧实例迟到、重启/回收注入验证。Files: 同 P7.1 及 file recovery 测试。

### P8 — Iceberg RetryLedger（执行 D3；按 route 判定必要性）

- [ ] **P8.1 清掉确定冲突后的查询**：begin 使用返回 binding，仅真实碰撞才查 overflow；finish
  使用已返回 result，不重新 GET。Files: Iceberg `operation/{retry,ledger,result}.rs`。
- [ ] **P8.2 按实际用途删除重试账本**：按 route 对照业务 journal 与 HTTP binding/input/result，判定实际重试效果；
  普通覆盖删除 ledger/body/状态，条件或多资源操作按 R2/R3 处理；不默认保留“最小回执”。迁移测试和 SDK。Files: `operation/{retry,result,payload}.rs`、Access `iceberg/{table_write,namespace_write}`。

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

- [ ] **P12.1 降低索引写入粒度**：当前 F 个 schema field、N 个 sample、E 个非空 sample-field
  产生 `F + N + 2E` 行写和 3 个 summary/index 写，不含 Manifest/publication。
  比较批量与有界并发，保留顺序、预算和未完整索引不可见性。Files: Dataset `inspection/{index,preparation}.rs`。
- [ ] **P12.2 精简索引准备/回收检查点**：保留服务必需的不可变索引，按 D1 删除辅助恢复进度，失败重做；
  未发布索引/候选纳入 R4 孤儿清理。显式 legacy preparation 仍是 mutation，读 miss 不自动持久化构建。Files: `inspection/{legacy,retirement,reclaim}.rs` 及索引恢复测试。

### P13 — GC / expiry / cleanup

- [ ] **P13.1 拆开有界发现与回收**：复核 15 秒/20 key 调度，避免一个最多 10,000-part 的
  upload 串行清理阻塞 snapshot sweep；分时间/字节/IO 预算公平推进。Files: Access Dataset `start_cleanup`、Dataset `upload/{expiry,ownership,reclaim}.rs`。
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
- [ ] **F2 合并一次请求的删除准入**：`DatasetReads::next`、HTTP/native batch 和 inspection
  仍可重复查询删除/提交状态。建立 snapshot-bound 请求内结果，独立入口仍需准入；不跨请求
  缓存删除许可。Files: Dataset `read_query/`、`inspection/`、Access `dataset/api/batch.rs`、native `dataset/read.rs`。
- [ ] **F3 避免无状态读取重复规划**：大 shuffle group + 小 batch 会反复重建同一 group；
  selection 窗口还重复加载 schema/membership。比较有界客户端 group 缓存、请求内规划复用
  与 token 大小；不恢复持久化 session。Files: Dataset `read_query/page.rs`、`inspection/evaluate.rs`、SDK iterable。
- [ ] **F4 替换 legacy scan 全量规划**：`scan_surface` 每页重建所有窗口，造成 CPU/内存放大；
  追踪调用者后迁移到有界 inspection 或增量 planner。Files: Dataset `transport.rs`、`planner/`、legacy API 和 scan 测试。
- [ ] **F6 批量或并发 locator 读取**：membership + 每 sample/field/ancestor 元数据 + value
  读取目前串行；bounded point read 在 chunk store 内还转成单 key scan。保留 materialize 前
  字节/期限限制、结果顺序和 missing/tombstone。Files: Dataset `inspection/`、`chunk_store/metadata.rs`、相关 RPC 支持。
- [ ] **F7 比较预读与 create-CAS 总成本**：新 immutable 页的 GET+CAS 可能比直接 CAS 更贵，
  replay 则可能相反；比较新写/重放比例，复用本请求已知值，不能无证明改普通 PUT。Files: Dataset Composition/Split receipts、Iceberg `operation/payload.rs`、journal validation。
- [ ] **F8 减少 payload 复制**：审查 HTTP 收整 part、`BytesMut` 拼接、inline copy、兼容
  UploadedChunkReader 的拼接后 `to_vec` 和 S3 coalescing；先查活跃调用，再移交 owner 或流式
  发送。Files: Dataset payload/upload、Access HTTP batch、S3/native 接收路径。验证部分帧、非整齐 size、held buffer 和实际复制字节。
- [ ] **F9 分解摘要/编码/分配 CPU**：complete 重读 part 并做 part/whole MD5，native part 与
  field 还各有摘要；MD5 不能直接拼合，只有同范围可信校验证据才可复用。测大 owner/part-index
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
  尚未新增范围 GC 或删除上传/Composition/Split 的过程状态，不能将本批读取优化算作该清理完成。
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
