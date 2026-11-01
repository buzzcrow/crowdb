<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Physical Node Test Plan

这是可复用的物理测试用例目录。按“大类 → 用例 → 场景”组织，不记录本轮
完成状态。每轮选择的场景、实际命令、源码/镜像身份和结果保存在独立 run
目录；历史 PASS 不能直接归给新镜像。

上游：[部署与恢复](../design/deploy/design-crowdb-deploy.md)、
[OCI 构建与运行](../../container/oci-image/README.md)、
[容器验收](../../e2e-test/README.md)、
[KV 恢复](../design/kv/design-crowdb-kv-slot.md)。

## 1. 执行约定

### 环境与范围

- 默认两个物理故障域：A `192.168.31.129`，B `192.168.31.184`。
  每轮重新核对主机身份、接口和地址；IP 不是资源归属证明。
- A1/A2/A3、B1/B2 共五个固定 voter，分别使用独立持久卷和发现身份。
  A 的端口 offset 为 0/1/2，B 为 0/1；按当前启动接口核对实际接受值。
- 每个容器固定 host network、1 CPU、2048 MiB、512 PIDs；每个服务一个
  Tokio worker。记录实际 cgroup 限制、存储容量、预留量和偏差。
- 仅通过停止本轮指定主机上的容器模拟主机故障。不改变共享网络、路由、
  防火墙、内核或 Docker daemon。三节点变体必须另立 profile，不能代替五节点。
- 使用现有 SSH key 和非交互 BatchMode。凭据通过私有输入提供，不把密码、
  私钥、token、登录身份或带认证参数的命令写入提交的文档/报告。
- 所有构建、测试和可执行程序，包括远端命令，都通过 `pixi run`。

### 用例范围与结果

- P00–P19 保留为父用例编号；`.1`、`.2` 等是可分别核验的子场景。
  默认主流程覆盖全部父用例及其普通场景。明确标为“扩展”的场景仅在所选
  profile 中要求执行；未选择不表示它已经通过。
- 启动前冻结用例范围、并发、资源、数据规模、超时和验收条件。新增回归单独
  记录；不能在失败后删去场景、放宽断言或改 profile 来取得 all-pass。
- 每个子场景记录前置条件、注入对象/时刻、实际动作、断言、耗时、结果、证据
  路径和输入身份。结果使用 `PASS / FAIL / NOT RUN`，未触发的注入计覆盖缺口。
- 按依赖顺序运行，首错停止依赖它的后续工作。保留原失败和已通过结果；修复后
  只重跑输入或行为受影响的场景，再继续未完成项。不要盲目重放部分执行的阶段。
- mutation 在调用前落盘 operation/key/ID 和预期内容，成功后落盘 acknowledgment。
  超时或断连记为 `OutcomeUnknown`，只读查证；测试控制器不得重放不确定写入。
- 不增加 timeout，不削弱校验，不添加调用方重试。P14 仅观察生产 client 原有
  明确安全重试策略，不为测试改变它。
- 构建先于消费者；同一输出目录不并发构建。源码冻结后构建 OCI，保留 source
  tree、commit/dirty diff、客户端摘要和镜像摘要。源码变更后重新判断受影响范围。
- CI 与物理结果分别记录。用户要求不检测 CI 时，不查询、不推断新 run，也不把
  等待 CI 作为继续物理测试的前置；本地受影响门禁仍须完成。

### 所有恢复用例共同遵守的断言

- 故障前保存容器 ID、卷名/CreatedAt、发现 UUID、node ID、bootstrap binding、
  35 个服务的 container/PID/start-time 身份、catalog 和副本状态。
- 连续观察覆盖“注入 → 恢复 → 完整数据审计”。完整采样间隔不超过五秒；
  单次缺少 `/proc` 的故障目标仍记录为 absent，不能丢掉整次兄弟服务样本。
- 只允许该场景指定的进程变化。故障恢复期间，仍然存活的 monitor 应持续提供
  新鲜的真实观察；故障目标离线可以不可观察，不能把旧 Ready 当作当前证据。
  观察新鲜度和服务健康是两个断言，Unavailable 不能靠刷新旧健康时间戳伪装。
- 恢复门禁必须取得所有节点当前、健康、晚于本容器启动且接收时不超过三秒的
  monitor 观察。此时固定全部 35 个进程身份，直到完整审计结束；不能再次设置
  基线来容忍第二次重启。
- 分别测量 catalog 接管 180 秒、服务恢复 180 秒、Groups 0/1/3 每组固定
  checkpoint catch-up 180 秒。每个门禁有自己的起点和耗时，不用整个阶段耗时
  代替。P10 的 60 秒、P13 的 30 秒及生产 client 的更短限制另行保留。
- 恢复后核对 P03 基线、P04/P05 数据、P07 已确认存活/删除记录和所有历史
  witness，并验证本场景的新写入。经全部五个恢复后的入口读取，完整校验内容。
- EC 的实际 fragment 分布决定 payload 可用性；KV quorum 不能替代它。临时
  超出丢失预算时的可用性结论，与恢复后的完整历史数据正确性分别报告。

## 2. 大类 A：主机、镜像与集群形成

### P00 环境、认证与镜像

- **P00.1 主机与资源**：验证两台真实主机、SSH key、时钟、接口、跨主机
  listener 可达性、Docker/cgroup、空闲磁盘和内存。清点已有资源的归属，记录
  五节点限制与存储预算；准备不足即失败，不静默缩小数据集。
- **P00.2 同一镜像**：两台主机导入同一已校验的镜像传输文件。校验传输摘要、
  manifest 引用的 config 内容哈希、平台、源码/runtime labels 和有序 rootfs
  layers。Docker 可能分别显示 manifest ID 和 config ID，不能仅比较 ID 字符串。
- **P00.3 拒绝错误输入**：离线回归覆盖损坏传输文件、源码/runtime label 不匹配、
  layer 顺序不一致和架构不匹配；失败时不能开始升级节点。
- **证据**：脱敏主机清单、镜像/构建 receipts、两台主机实际 inspect、来源映射。

### P01 五节点发现、初始化与身份

- **P01.1 正常形成**：通过正常生产 policy、发现、key admission 和存储准备形成
  五个 voter。验证五个不同 discovery UUID/node ID、实际 accepted initial_voters、
  固定成员和真实 listener；使用当前支持的五 voter 路径，不杜撰 CLI 参数。
- **P01.2 地址与端口**：逐节点核对 offset 只应用一次、同主机无冲突、跨主机
  不使用对方不可达的 loopback，以及两个 rack/host 的归属。
- **P01.3 保留身份**：已有节点重启只恢复原 binding、卷和身份，不重新 bootstrap。
  过期绑定、外来卷和相同名称下被替换的容器必须拒绝采用。
- **证据**：accepted policy、成员/监听地址、卷映射、bootstrap 和发现身份。

### P02 全集群 readiness 与持续观察

- **P02.1 基线**：五个 Web 入口均 Ready，每节点七个服务、合计 35 个真实存活
  进程；同时验证健康、依赖和 DiskDB 当前 ownership lease 的持续续约。
- **P02.2 新鲜度**：记录接收时刻，拒绝早于容器启动或年龄超过三秒的 monitor
  快照。区分进程存活、listener 存活和数据服务 Ready。
- **P02.3 authority 退化**：Group0 切换/不可用或一个远端观察变慢时，本地 monitor
  仍能报告当前进程和真实健康。服务意图读取不能拖住本地健康发布或诊断控制。
  不允许旧 healthy 值只更新时间；不因 readiness 暂退就重启仍存活的服务。
- **P02.4 观察器负例**：回归旧快照、错误容器/PID/start-time、OOM、兄弟进程
  丢失、超过五秒完整采样间隔，以及恢复后第二次身份变化；均必须失败。
- **证据**：完整原始样本、各门禁 receipt、恢复身份 fence 和观察器结果。

## 3. 大类 B：原生数据、协议数据与规模

### P03 五入口基线数据和分配

- **P03.1 KV**：验证 Groups 0/1/3 的五副本映射和当前 leader/lease。执行单次
  single/batch 写入，从五入口读回完整 key/value；记录 acknowledgment。
- **P03.2 ChunkDB**：生产 native client 分配并查询同一个 Chunk ID，核对位置、
  大小和身份，不以一个新 ID 的成功查询替代原分配结果。
- **P03.3 S3**：经 `crowdb-tpc-loader` 生成独立 S3 SF1 数据，并保存每个对象的
  URI、长度和摘要；经五入口完整读回八张表对应文件。另保留至少 96 KiB 的
  run-unique witness，核对完整字节和 hash。
- **P03.4 Iceberg**：独立生成八表 SF1，核对 namespace/table UUID、snapshot、
  metadata location、manifest 文件集合、文件长度/摘要、所有行解码和独立扫描。
  schema、属性提交和数据文件分别验证；只读 footer 或十行 sample 不能代替全读。
- **P03.5 Dataset**：生成含 inline fields、multipart payload 和不可变 sample
  snapshot 的数据。逐份报告、逐字段、经五入口完整读取；核对实际报告数。
- **P03.6 协议隔离**：原生压测 key 必须位于实际安全范围，避开 S3/Iceberg/
  Dataset 的保留元数据前缀；给协议 key 添加测试后缀仍可能破坏协议扫描。
- **证据**：数据/ack ledger、loader 来源和命令、全部生成报告、五入口校验结果。

### P04 512 条短复现与五 owner 覆盖

- **P04.1 旧 key 形态**：独立运行历史 512 条复现，记录其真实 owner 覆盖；
  不把它自动计为五 owner 场景。
- **P04.2 正式形态**：完整读取 catalog，在五个 Serving owner 的真实范围选择
  run-unique 安全 key。32 批 × 16 条 × 64 KiB，共 512 条/32 MiB；保留每批
  默认五秒 deadline。统一首字节分布不能代替路由 owner 证明。
- **P04.3 完整性**：逐批记录确认与未知结果，完整回读；保存 split/cutover、
  各阶段延迟和错误。未知写入只读查证，不重放。

### P05 更大工作集与持久存储边界

- **P05.1 规模**：P04 通过后，新 namespace 至少 4096 条确定性 64 KiB 记录，
  共 256 MiB，覆盖五个实际 Serving owner。预算 mirrors、metadata、WAL、
  reclamation 和 artifacts，测试 media 保持至少 30% 空闲。
- **P05.2 extent 边界**：根据实际 block 大小构造跨文件尾部的持久数据，冷打开后
  触发异步读取跨一个及多个 extent。核对完整内容和一次 completion；记录实际
  offset/length/边界，不能只因工作集大就声称覆盖该路径。
- **P05.3 错误传播**：组件回归覆盖缺失后续 extent、尾部截断和范围溢出，保留
  真实 I/O 错误，拒绝短读成功、重复 completion 或自动重试掩盖错误。
- **证据**：owner/key/字节数、全量摘要、实际 extent 路径和组件/物理覆盖分别列明。

## 4. 大类 C：30 分钟混合负载与最终一致性

### P06 持续混合负载

- **P06.1 参数**：开始前声明并发、比例、pacing、key/version 规则和有界数据量。
  负载覆盖两个主机和五个 owner，复用/回收数据时仍持续产生成功操作。
- **P06.2 原生与协议并发**：混合 native 插入、覆盖、删除、读回；同时执行 S3
  读写删、Iceberg commit/load/cleanup 和 Dataset publication/readback。
- **P06.3 有效时长**：至少 30 连续分钟成功混合工作重叠，不含构建、部署、空等。
  固定 seed 后等待不算负载。修复或控制器重启打断负载后重新完成连续 30 分钟。

### P07 确认语义和数据审计

- **P07.1 ledger**：mutation 前后保存 operation、key/version、预期长度/hash、
  ack 状态和时间；同 key 覆盖串行化或有明确版本合同，避免 verifier 自身竞态。
- **P07.2 在线抽查与收尾全查**：连续读取已确认数据，结束时核对所有 live keys、
  覆盖后的值、完整协议 payload、witness，以及所有已确认删除的内容仍然不存在。
- **P07.3 未知结果**：超时/断连保存 OutcomeUnknown；恢复后存在时验证预期内容，
  不能直接断言不存在，也不能为消除未知而重放 mutation。
- 正常负载中的请求失败、内容不符、OOM 或未计划进程替换均使该负载阶段失败。

### P08 性能与资源观测

- **P08.1 完整采样**：至少每五秒记录吞吐、成功/失败/未知数、p50/p95/p99/max、
  CPU、RSS/cgroup memory、空闲空间、进程身份、catalog 和 reclamation。
- **P08.2 分段统计**：分别列 build、启动、seed、持续负载、故障和审计耗时；
  保留成功操作/字节、有效重叠时长和不同阶段延迟，不混算或仅报告容器存活。
- **P08.3 主机异常**：保存 diskstats、I/O/memory pressure、cgroup throttling/
  OOM 和有权限读取的内核错误。低 CPU/充足内存不能排除 fsync 或设备 I/O 停顿。

## 5. 大类 D：进程、容器与主机故障

### P09 原卷冷启动与镜像升级

- **P09.1 冷重建**：停止并重建全部五个容器，沿用原卷、policy、UUID/node ID
  和 bootstrap。验证 35 服务、所有固定 checkpoint 和五入口完整历史数据。
- **P09.2 同卷升级**：生产修复后验证新镜像内容身份，再替换容器；所有卷的名称/
  CreatedAt 和数据身份不变。镜像传输 tar 是分发文件，不是数据卷备份。
- **P09.3 操作竞态**：重建前再次核对旧容器 ID、stopped 状态/时间、挂载和归属；
  若准备期间曾被其他操作启动/替换，则停止，不盲目重复 remove/create。

### P10 单个无响应 ChunkKV

- **P10.1 注入**：仅向一个已核对 container/PID/start-time 的 ChunkKV 发 SIGSTOP。
  不暂停整容器或误伤同名服务。
- **P10.2 恢复**：60 秒内旧进程退出并被新 PID 替换，其余 34 个服务身份不变。
  验证旧数据和本场景新确认写入；恢复后全量审计期间不允许再次静默替换。
- **P10.3 失败收尾**：保留错误后解除仍存活旧进程的 STOP，防止遗留注入污染后续。

### P11 跨主机 KV 生命周期

- **P11.1 Follower**：选择与当前 leader 不同物理主机的 follower，经它的 Web
  stop 本地 KV；核对 completed Stop receipt 和实际退出。远端 authority 仍可
  管理该节点，单次 restart 得到新 PID，其余 34 服务不变，完整历史审计通过。
- **P11.2 Leader**：重新识别当前 leader，执行 Stop；即使它已退出，原 completed
  receipt 仍可读取。等待存活节点的新 authority，再单次重启原节点并完整审计。
- **P11.3 切换窗口**：全过程检查幸存节点的本地 monitor 新鲜度与真实进程身份。
  不能只检查最终 Ready，从而漏掉服务意图读取阻塞、旧健康快照或兄弟服务重启。
- **P11.4 退出证据**：出现 native abort、自 join、WAL drain 失败或其他异常退出，
  先保存对应 PID/容器代际的应用、RPC、tree、stderr 和 crash/core，再诊断。

### P12 B 主机两个节点同时离线

- **P12.1 真正接管**：故障前证明 B1/B2 都是当前 Serving owner，再停止它们。
  A 的三 voter 在 180 秒内接管 catalog；执行单次 KV 写/读，记录真实新 ack。
- **P12.2 停机期间新 payload**：B 保持离线，经 A 新写 S3 witness 和 Iceberg
  property commit；新 payload 的实际布局须能由 A 独立读取，三个 A 入口全读。
- **P12.3 EC 边界**：记录旧对象每主机 fragment 数和丢失预算。例如 EC 8+4 在
  B 上有六片时，不能要求 B 离线期间该对象仍可重建，也不能宣称已有 rack
  保护。此时的新写可用性结论与旧 EC 暂不可用分别记录，不混为一个成功。
- **P12.4 恢复与持久化**：恢复 B，再分别用原卷重建 B1/B2，验证原身份、五副本
  固定 checkpoint 和五入口全部历史数据，包括停机时不可用的旧 EC 对象。
- **扩展**：若要验证旧 EC 数据也能抗整机故障，单独选择满足丢片预算的真实布局，
  并在停机期间全读旧数据；不能由三 voter 存活推导这个保证。

### P13 A 主机三个节点同时离线

- **P13.1 失去 quorum**：停止 A1/A2/A3，B 在 30 秒内报告当前不可用；新 mutation
  不得确认成功。记录失败与未知结果，不把 connection loss 等同于没有写入。
- **P13.2 租约安全退出**：因原租约到期而退出的幸存 ChunkKV 必须有同一进程的
  lease 失效、主动停止、RPC worker join 和退出证据。native abort 或未知原因
  的替换不能归类为正常 lease fencing。
- **P13.3 恢复与协议读取**：恢复 A，验证全部服务和固定五副本 checkpoint，
  再核对所有原生/协议历史。每个 Iceberg reader 独立保存 endpoint、PID、
  table/object/stage、时间、stdout 和退出码，再聚合结果。
- **P13.4 错误归属**：区分真实 curl/GetObject 首错、其他 reader 的中止和后续未
  执行项。S3 原文件通过不能替代 Iceberg 全读；不能按并发输出顺序猜测入口。

## 6. 大类 E：分配、积压恢复与空间正确性

### P14 分配级显式安全重试

- **P14.1 有界故障**：保留所有 KV 进程和 Group0 leader/term/lease，在 Group1
  注入 leadership step-down，三秒内至少一次被当前 leader 接受。
- **P14.2 实际触发**：单次启动生产分配 client，必须观察到 allocation-level
  明确 Unavailable safe retry。仅最终成功、transport timeout 或未触发故障均
  不能代替该断言。
- **P14.3 原策略完成**：释放注入，用同一 ID 在原五次重试/十秒限制内完成
  allocate/query。保存真实 retry/ack 证据；instrumentation 不改变分支或策略。

### P15 超过 1024 slots 的积压与全冷恢复

- **P15.1 实际 gap**：停止一个真实 KV follower，经存活副本确认至少 1100 次
  单写；记录 offline checkpoint 上界、写前/写后目标，证明实际差值大于 1024。
- **P15.2 catch-up 与冷启动**：恢复 follower，验证 FetchGap 有界并发和真实进度；
  然后全停五节点并沿原卷冷启动。故障前固定 Groups 0/1/3 的 ack target，
  每组五副本健康引擎都必须达到目标，再核对全部数据。不能随新写入移动目标。
- **扩展 P15.3**：分别制造 reclaimed WAL、empty sealed WAL、high-promise/
  old-chosen 等物理边界，记录触发证据。未实际制造就保留覆盖缺口；组件回归
  和普通大 gap 的成功不等于这些物理边界已经通过。

### P16 最终数据与 DiskDB 占用

- **P16.1 最终门禁**：catalog 全部 Serving，五入口 Ready，35 服务健康；下一样本
  与最终审计结束时仍是相同进程身份。
- **P16.2 新旧数据**：新 KV batch、新 ChunkDB allocation/query 与所有历史数据
  分别校验，继续区分 acknowledgment、已确认删除和 OutcomeUnknown。
- **P16.3 全量占用**：只读扫描所有本轮 DiskDB occupancy，覆盖完整 disk/zone/
  continuation，无截断；核对 live mirror segments 不跨 chunk 重叠，并保存
  实际 live、allocated、free 和 reserve 数。

## 7. 大类 F：首错诊断、修复和清理

### P17 失败证据与分类

- **P17.1 首错**：打印失败用例、操作和完整错误，调试时启用
  `CROWDB_E2E_PRINT_SERVICE_ERRORS=1`。保留原始客户端输出，不仅保存包装层 panic。
- **P17.2 同时间线**：保存 inspect/exit/OOM、当前 PID/start-time、健康时间戳、
  KV role/term/lease/checkpoint、catalog/transfer、DiskIO 和磁盘资源。
- **P17.3 native 与主机**：进程异常先分析 crash/core；I/O 停顿补 diskstats/
  pressure 和可读的阻塞线程栈。观察事实、推断和未证实原因分开写；后续 PASS
  不会反向证明旧无栈 abort 或旧无入口标签超时的原因。
- **P17.4 完整日志**：按实际 PID、容器启动时间和日志类型选择 application、RPC、
  metrics、tree 文件。历史 stderr 不能算作新进程错误，也不能因日志非空就丢弃
  新错误；保存前后证据或能绑定本次执行的增量。

### P18 修复和续跑

- **P18.1 最小复现**：找第一次偏离，区分代码、时序/注入、观察器和环境原因；
  对代码缺陷补能够在旧代码失败的 focused regression，再运行受影响组和门禁。
- **P18.2 实际输入**：生产输入变化后重新构建、校验、分发镜像及客户端；先修复
  会使后续慢/挂/失败的依赖，不让队列继续反复撞同一问题。
- **P18.3 续跑账本**：每次 attempt 使用独立身份和不可覆盖 receipt，保留原失败、
  已确认写入和未知结果。观察器失败不能靠重设基线或移动 stop marker 自动消除；
  必须记录修复/环境核对和新审计范围，再继续未完成项。
- **P18.4 覆盖边界**：组件、容器、物理和 soak 分开记；新镜像的局部 PASS 不
  继承旧镜像的全部覆盖。负载被修复打断须新跑 30 分钟；后续恢复修复是否影响
  旧 soak 输入须明确说明，不为了制造“同镜像 PASS”重复不受影响的全部测试。

### P19 停机留证与精确清理

- **P19.1 双主机预检**：先固定两台主机全部待清理对象；核对 run label、名称、
  container ID、原卷名称/CreatedAt、mount、最后 stopped 状态/时间。任一项改变
  即停止，不能只因名称相同继续删除。
- **P19.2 停机留证**：停止本轮节点后保存完整服务日志、crash、inspect 和数据
  ledger；两台主机全部留证成功后才开始第一次删除。
- **P19.3 数据卷备份策略**：默认不新增整卷归档，依照用户本轮保留/清理要求执行；
  已存在的私有备份继续保留。只有用户明确要求时才做额外卷备份并验证完整性。
  服务日志留证、镜像传输文件和数据卷备份是不同对象，不互相替代。
- **P19.4 精确删除**：只删除已获授权且预检成功的归属对象，数量与清单完全
  相符；不使用 daemon-wide prune，不清共享 runtime 或其他任务资源。
- **P19.5 清理后验证**：两主机本轮目标资源均不存在、所属监听端口关闭、SSH key
  仍可用；保留 run artifacts、原始错误和约定保留文件。不得在报告中隐去之前
  发生的越界删除或无法恢复的数据丢失。

## 8. 每轮交付与验收结论

统一目录：`.crowdb-runtime/artifacts/physical-nodes/<run-id>/`。保存 harness 的
实际源码与命令；当前没有一个仓库 Pixi task 实现本目录的全部流程，不能依赖
先前临时文件或把方案描述成已有的一键任务。

- `run.json`：所选父/子场景及扩展 profile、两主机、节点/端口/资源映射、归属
  清单、源码/镜像/客户端身份、dirty diff、harness 来源和构建/传输 receipts。
- `results.json`：各场景 PASS/FAIL/NOT RUN、独立门禁起点/限制/耗时、实际注入
  是否触发、未执行原因、所属 attempt 和证据路径。旧结果不可覆盖。
- `data-ledger.jsonl`、`metrics.jsonl`、`checkpoints.json`：数据及确认语义、分段
  统计、固定目标和实际恢复进度；大文件分段保存。
- `loader/` 与 `audits/`：各协议源版本、生成/注入/验证命令、完整报告和每个入口
  的独立结果。未实现路径标 NOT RUN，不以其他协议或组件结果替代。
- `observations/`、`first-failures/`：注入至完整审计结束的连续观察、身份 fence、
  首错、完整日志、crash 和明确仍未证实的原因。
- `cleanup.json` 与 `summary.md`：精确清理结果、实际源码代际覆盖、未完成/未触发
  场景、数据保留/丢失事实和下次起点。明确区分“结束运行”与“所选必需场景全过”。

最终结论只覆盖本轮明确选择并实际通过的场景。人工共同 review 与测试执行分别
记录；测试 PASS 不自动完成共同 review。

## 9. 实现和回归入口

- 启动/镜像：`container/oci-image/{run-node,runtime,artifact,transfer}.py`、
  `container/single-node-container/entrypoint.sh` 和
  `container/crowdb-monitor/src/node/startup/policy.rs`。
- 形成/观察：`lib/crowdb-protocol/src/mgmt/node/`、`app/crowdb-web/src/node/`、
  `lib/crowdb-console-shared/src/ops/cluster/readiness/` 和
  `container/crowdb-monitor/src/node/runtime/`。
- 数据/恢复：`lib/crowdb-{kv,kv-client,chunk-kv-client,chunkdb-client}/`、
  `app/crowdb-chunkdb/src/topology/`、`lib/crowdb-chunk-stream/src/production_chunk/`
  和 `lib/crowdb-tree/src/backend/local/`。
- 组件回归：KV backlog/FetchGap durability、ChunkStream finalized-empty、Monitor
  discovery/health/service receipts、ChunkDB allocation retry/topology availability、
  BlockAsyncPageStore 跨 extent 读取。按实际改动选择受影响组，通过 `pixi run` 执行。
- 容器/协议回归：`e2e-test/tests/deployment/`、`e2e-test/tests/console/`、
  `e2e-test/tests/access_iceberg/` 和对应 S3/native SDK 入口；它们与真实跨主机
  故障、连续 30 分钟负载分别计覆盖。
