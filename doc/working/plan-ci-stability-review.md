<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CI Stability Review Plan

Upstream: [first failed run](https://github.com/buzzcrow/crowdb/actions/runs/41003611925),
[second failed run](https://github.com/buzzcrow/crowdb/actions/runs/41128190276),
[newer failed run](https://github.com/buzzcrow/crowdb/actions/runs/41159636766),
[container acceptance](../../e2e-test/README.md), and
[CI workflow](../../.github/workflows/ci.yml).

Goal: review all 16 failed jobs individually, verify their fixes and regression
coverage, and identify the remaining work needed to improve CI stability.

用户要求保留这份 TODO，待本轮测试、提交、推送和新 CI 完成后逐项共同 review。
测试通过只更新验证证据，不自动勾选 review。全部 review 完成后，归档结论，
将仍需实现的工作保留到对应任务，再删除这份临时清单。

2026-11-01 用户调整顺序：收尾当前 RPC 生命周期修复后，优先完成 CI 稳定性
改动与验证，然后 commit/push 并检查新 CI；之后恢复 P13 和其余物理测试。
提交时如实保留未完成的物理验证，不将其提前勾选。

2026-11-01 用户恢复执行。此前暂停于 GC 派发前；恢复后加强版 GC 三例、
辅助服务/Journal 三轮组合、Go SDK 三轮和 Dataset 分阶段验证均已通过。
本地补充验证控制器正常退出；首次推送后的三个新失败已修复并完成本地复验，
修复已推送为 `70ae86b1`，完整 CI 为 26/27 PASS；新发现的 terminal writer
恢复故障已定位为 split 物化与父分区租约转移的冲突，修复和本地复验已完成，
已推送为 `5af2737f`，run `41304660599` attempt 1 已结束，25/27 PASS。
terminal writer recovery 已远端通过；auxiliary 与六节点部署修复已推送为
`18015caf`，run `41311084753` attempt 1 全部 27 个 job 通过。
后续只读复核发现早期 placement settle 不能排除自动 split，正在补强
Access Restart 前的实际依赖就绪观察；该测试修正仍需独立发布和精确 SHA CI。
物理容器在恢复前已停止，原因未知；七个容器、镜像和卷身份均保持不变。

## 当前执行队列

- [x] **完成 CI 稳定性修复**：保留 auxiliary lifecycle 原有 combined 三秒
  predicate，将 `/api/servers` 的采集移出 HTTP 读路径；authority 与各节点
  独立采样，新鲜度从采样开始计，绑定/端点/operation 与 mutation fence
  阻止旧 PID 复活。保留 Group 0 不可用时 authenticated monitor 的实际
  PID 观察，以及真实 Access readiness。补慢 peer、过期/错身份、乱序、
  失败恢复及 sampler 退出的确定性回归。同时修复 browser batch 未派发
  peer 的错误归因，明确未执行，不能计为 PASS。
  文件：`app/crowdb-web/src/services/observation/`、`services/remote.rs`、
  monitor 私有 process reply、`e2e-test/browser_batch.py`、`fixture.py`
  及对应测试。
- [x] **恢复后完成最终本地验证**：受影响组件、fmt/clippy/C++ gate、共享 clients
  与新 OCI 镜像、原失败精确场景、396 个 canonical case 及并发隔离；
  每次首错停止后续派发，保留已通过项与完整错误。
  当前 core 1165、access/monitor 2039、console 426、完整 fixture 301 项通过；
  UI、C++/FFI、storage、fmt、full clippy 与 CI task mapping 通过。
  新代码的 helper 拆分及 Rust 1.75 兼容修正后，受影响 focused 组再次通过；
  没有增加 lint allow、延长 timeout 或改变 UI/schema。新镜像已构建并核对
  production source；原 auxiliary 三秒断言通过，加强后的真实 PID/namespace、
  周期前后 operation、注册唯一性和三节点 sibling 身份断言也通过，
  Playwright 从原断言基线 9.7 秒到加强版 12.4 秒。native-operations 全组通过。
  新镜像全量已收尾：396 个 canonical case 的最新最终结果全部 PASS，
  必需的四例并发隔离通过，七个保留物理容器及卷身份不变。加强版 CLI
  五例随后全部通过，用时 102.757 秒。恢复后正常刷新 clients：GC 三例
  通过；浏览器组合曾在第二轮发现 Access 依赖恢复边界缺失，修正后三个
  全新组合轮次全部通过，约 45–46 秒。Go 三轮普通和注入后 cleanup 均通过。
  Dataset browse client 278.32 秒，其中 seed 2.276 秒、默认 balance
  260.002 秒、512 条完整回读 0.273 秒；12 partitions、三个 owners。
  最终 fmt/full clippy、受影响 C++ format/tree-lint、TypeScript 和 scope
  guard 通过。新增断言均有独立实际运行证据，不能与旧 bundle 的全量混算。
- [x] **提交并推送修复**：CROWDB `task-fix-build` 一个一致提交；
  记录精确 SHA、run/attempt 和全部展开 job，修复真实失败后推进。
  已推送 `76fc6508189dd31018e953674a8f1c693011498d`，新 run `41295426521`
  attempt 1 已结束：27 项中 24 成功、3 失败。CI merge checkout 与 head
  的完整 tree 一致。三个新失败的本地修复与受影响组复验均已通过，已推送
  `70ae86b1c62e095be2754ed178890736e49be5a4`；新 run `41299662435`
  attempt 1 已结束，26/27 PASS，只有 console-api 的 terminal writer recovery
  失败；上一轮三个修复场景已远端通过。全部日志和校验过的失败 artifact
  保留于 `ci/published-70ae86b1/`。此前完整日志与三个 artifact 保留在
  `ci/published-76fc6508/`，不并入原 16 项人工 review。
  最新修复已推送为 `1814405e`；本地新镜像 19-case 操作组、UI 369 和
  fmt/clippy/TypeScript 全部通过，提交钩子通过。
- **CI 检测已按用户要求停止**：不再查询 `1814405e` 的 run `41320020131`，
  不推断最新结果，保留已收集的证据；不作为物理工作的前置。
- [~] **恢复物理验证**：用户取消新增卷归档。B 重启后已连通并核对主机身份，
  七个原始停止容器及卷身份均通过检查。镜像传输及 SHA-256 校验通过；
  A 使用 manifest ID、B 使用 config ID，已通过内容哈希及完整层列表证明一致。
  五个当前节点已用原卷升级，完整历史审计通过，包括五端点 S3/Iceberg SF1、
  187 份 Dataset 报告及全部见证/删除记录。625 次观察通过，35 个身份不变。
  新 P09 在 A2 Group 0 复现真实的跨块异步短读，已停止五个节点保留原卷，
  P09–P19 尚未完成。最小回归重现完全相同的 958464/1212416 字节错误；
  跨块读取修复、19 项 focused 检查及完整 C++/FFI、core、storage、fmt、
  tree-lint、clippy 门禁均通过，已推送 `c063a169`；新镜像同卷升级通过。
  新启动全部 35 服务在 37.439 秒恢复，原生历史与五端点 S3 SF1 通过。
  Iceberg 校验时 A 主机 NVMe 约十秒 I/O 停顿，随后 monitor 过期、Group0
  超时和五个 ChunkKV 租约退出；OOM 为零，设备/文件系统根因尚未证实。
  单对象原限制全读/校验通过；新的完整只读历史审计随后通过，1230.088 秒，
  597 次观察、最大间隔 3.843 秒、35 身份不变；187 份 Dataset 报告及全部
  五端点协议/见证/删除记录通过。原失败不改写；c063 的新 P09 完整通过（1164.469 秒），P10 也通过
  （1023.819 秒）。P11 follower 完整审计通过；leader 恢复门禁通过但 B2
  monitor 观察过期 3519 ms，后续 P12–P19 未派发。
  根因是本地健康发布前同步等待 Group0 desired-service scan。旧代码的慢
  authority 回归失败；改为一个有界后台只读任务并保留操作前权威 fence 后，
  focused 八项通过；完整 fmt/clippy、Access/Monitor 2041、Console 426 及正常
  task 附属检查通过。未改动的 C++/core/storage 门禁保留 c063 输入证明。
  发布、新镜像和受影响物理复验待完成；五节点已停止、原七卷保留，失败和
  修复证据分别留存。可复用物理方案已按测试用例重组。
  未查询新 CI。
  这不是新增 CI job 失败，不并入原 16 项共同 review。
  A 已完成的三个备份和全部原始失败证据继续保留，不再新增备份。

## Run 41295426521：首次发布后的三个新失败

- **Chunk allocation 丢回复注入**：旧代理丢弃首段 TCP 数据，其中可能只有
  `0xfffe` capability handshake，allocation 尚未执行，busy bytes 仅为 1 MiB。
  复用共享 frame-aware 代理，要求恰好转发一次握手、丢弃一次 application
  response，保留 Unreachable、精确 2 MiB 和不重放 allocation 的断言。
  原精确/默认 scope/串行基线均通过；该 scope 只有一个 case，不能据此声称
  复现了多 case 竞争。修复精确场景通过；完整失败 job 的 47 个剩余场景
  本地通过，保留三个未受影响的远端 PASS。
- **Shell lazy inventory**：原 trace 在 N-3 选中后、DOM 断言返回前发生正常
  topology refresh；四次 inventory 均只请求 N-3，均返回 200。现按 topology
  观察周期精确断言 `[3]`，保留初始零发现、全部 DOM 断言和三秒期限。
  原精确基线 4.2 秒，修复后 3.9 秒；原 shell/capacity 并发组合分别
  4.8/10.8 秒通过。生产 UI 不变。
- **CLI final readiness**：自动 split 发布 generation 2 后，旧 grant 短暂
  StaleCatalog，最终即时 check 返回 degraded；真实进程仍存活。最终 check
  现在仅使用启动时原 180 秒绝对期限的剩余量，调用一次官方 `--wait`，保留
  Ready、cluster identity 和整个等待前后的 lifecycle equality。过期不派发、
  失败不重试、错 cluster 拒绝均有回归；不新增 mutation replay 或等待预算。
  原精确和五例并发基线通过；修复后六例通过，包含被独立 fixture 首错中断的
  terminal writer recovery。该 peer 不是另一个已证实的产品失败。本地修复轮
  没有复现原 StaleCatalog 窗口，不能以快速 PASS 声称复现了该时序。
- **门禁与保留**：fixture 304 tests、fmt、full clippy、TypeScript、diff check
  全部通过。生产 source tree 仍为 `376fdc26a0e96d7810313ea670bf043c4f0dfcae`；
  正常 clients builder 已刷新对应输入。七个物理容器及卷身份未变；尚未进行
  物理故障、升级或退休。完整执行证据见 `local-repair-closure.json`。

## Run 41299662435：split 物化与租约恢复

- 完整 CI 为 26/27 PASS；console-api 的 92 例中 91 例通过，只有
  `terminal_chunk_writer_recovery` 在原 180 秒恢复期限内未恢复。
  故障解除后的首次持续分歧是 Group 0 monitor 的 exact writer pair 错误，
  80 次错误阻止后续 serving grant 更新，最终两个 ChunkKV 范围 LeaseExpired。
- 已提交 split 的父分区因租约失效转移后，其 marker 正常更新为 transfer ID；
  子分区独立可恢复时，旧物化逻辑仍要求两个 split marker。确定性回归在
  旧代码重现同一错误，也发现仅计数会接受复用 marker 的错误身份。
- 修复从持久化 split proof 核对每个剩余 marker 的完整 assignment；只有
  CatalogCommitted 才允许另一分区已被后继转移取代，并保持其新状态不变。
  ChildPrepared 仍需两个精确 successor，旧 epoch、错误 artifact 和复用
  marker 均拒绝。原超时、错误处理、故障时长及写入次数保持不变。
- 精确容器基线通过，但不据此声称复现远端时序。旧代码确定性回归两例失败，
  修复后的完整 KV Server 86 tests 通过，包含四个新回归。全新生产镜像、
  ChunkKV Server 112 tests（另有一个 opt-in RDMA ignore）、fmt 和 full clippy
  均通过。新镜像精确 terminal 151.1 秒通过，恢复 check 21.2 秒；默认并发
  CLI 九例、ChunkKV 两例、balance 一例和 console-api 剩余 83 例全部通过，
  共 95 个不同 canonical case。七个保留物理容器及卷身份不变。
  证据：`ci/split-materialization-fixed/`；已推送 `5af2737f`，其精确 SHA
  CI run `41304660599` attempt 1 已结束；原 terminal recovery 已通过。
  此前 396-case PASS 属于旧生产镜像，不能将两轮输入混为最终镜像全量验证。

## Run 41304660599：依赖等待与生命周期前置条件

- 全部 27 个 job 日志和两个失败 artifact 已保留并校验；25 PASS / 2 FAIL。
  `console-native-operations` 为 18 PASS / 1 FAIL，`console-native-data` 为
  13 PASS / 1 FAIL。六节点 trace 为 27.4 MB，超过原 16 MiB 导出上限，
  远端未保留；该部分只能依据完整失败日志与本地重现诊断。
- Auxiliary 外部 Stop、OS 进程退出和页面 Failed 均通过，随后 Restart
  在 ChunkKV 初始放置的 TransferFencing 窗口返回 409 Waiting。原精确
  场景 12.4 秒 PASS；原并发组合 12.1 秒复现；串行对照 13.7 秒 PASS。
  首次修复沿用 placement settle，后续复核发现其 transfer observation
  不覆盖同一或下一 tick 的自动 split。现改为每次 Access Restart 前直接
  观察同节点 ChunkKV /ready 与 instance identity，保留三秒观察期限，
  不调整生产 balance 策略、response/PID/OS/sibling/readiness 断言。
- 六节点 Access Waiting 缺少分类，本地另复现带换行的 ChunkKV Waiting
  不匹配原正则。monitor 已恢复实际进程，计划仍为 failed。新增两例确定性
  回归先红；修正分类后又捕获同 waiting 状态保留旧 detail，导致已登记启动
  误用十秒缺盘退避。修复按已有 CLU-03/SVC-03 契约观察目标 monitor，
  回归要求最终 deployed，且 deploy 一次、restart 零次。
- 原有序并发组在更早的 KV lifecycle 失败；精确串行同样失败。停止 Group 0
  leader 后过早提交 restart，权威校验三秒到期。增加同 cluster/operation
  的权威恢复观察，使用已有选举预算，再提交一次 mutation，保留响应预算。
- 新镜像的 48 个独立 case 全部通过：native operations 19、browser 27、
  六节点和固定端口/失败重试两例。先通过三个精确场景，再在原有序生命周期
  组复验。六节点两次 Playwright 为 51.2/49.2 秒；固定端口为 5.4 秒。
  数据组虽配置 concurrency 2，资源预算实际使两例顺序执行，不能声称并发。
- UI 366、plan-hook 28（含两例新回归）、fixture 304、完整 TypeScript、
  fmt/full clippy 均通过。七个物理容器及卷身份未变。证据保存在
  `ci/lifecycle-dependencies-fixed/`；已推送 `18015caf`，对应 run
  `41311084753` attempt 1 全部 27 个 job 通过，完整日志保留。
  原 16 项人工 review 保持未勾选。

## Run 41311084753 后：Access Restart 的实际依赖边界

- 27/27 PASS，上一轮 auxiliary 和六节点部署失败组均通过；CI merge
  checkout 与 HEAD 完整 tree 相同。源码一致不表示本地与 CI 二进制相同。
- 复核 balance monitor 发现 transfer observation 可报告 within tolerance，
  同一 tick 仍可规划 split。早期 settle 通过不证明后续 Restart 依赖就绪。
  新辅助函数在两处 Access Restart 前 GET 本机 ChunkKV /ready，核对
  instance_id 与真实 ready，再保留原单次 mutation 和响应断言。
- 首次新单例因使用不存在的 url 字段报 Invalid URL；已改为 API 实际
  mgmt_url，保留失败证据。修正后精确单例 35.6 秒通过，原并发组合中的
  auxiliary Playwright 14.5 秒通过。完整操作组为 17 PASS / 1 FAIL / 1 未执行。
  生产镜像输入未变，继续使用 lifecycle-fixed 镜像并核对 source projection。
- 新失败为 S3 multipart 删除后旧行超过原三秒期限。完整 trace 显示
  DELETE 204 用时 1.279 秒，后续 list 200/KeyCount 0 用时 2.036 秒；
  页面成功删除后仍等待 list 才清除缓存行。精确单例和并发组合对照通过，
  串行对照再次复现同一断言。建议收到成功后立即移除精确 Bucket/key 行，
  再执行原刷新；失败/未知结果保留行，原三秒 E2E 断言不变。
  用户已同意该行为，先更新了 S3-04 和 ASCII 布局。可控延迟列表回归在
  旧 UI 复现原行未移除，单行生产修正后三项通过，覆盖精确 key、其他行、
  刷新失败以及删除拒绝/未知结果。初始测试 provider/字段标签错误单独保留，
  不作为产品 red 证据。新镜像已通过精确 multipart（Playwright 4.6 秒）、
  原 auxiliary 并发组合（auxiliary 12.7 秒）和全部 19-case 操作组
  （417.230 秒；multipart 再次 4.1 秒通过）。UI 369、TypeScript、
  fmt 和 full clippy 通过；新测试不支持的 exact 选项所致类型错误与
  修正后的门禁分别保留。新的提交、推送和 27-job CI 尚待完成。
  证据：`ci/access-readiness-fixed/s3-delete/`、`ci/s3-ui-fixed/`。
- 物理 observer 的离线回归补充了恢复后固定 35 个进程身份的 fence，
  数据审计期间不再沿用故障目标的宽松重启许可。两项 fence 回归及七项
  closure 接受性检查通过；未派发物理启动、故障、归档或退休。

## 通过场景的稳定性改善

用户要求同时处理 PASS 但不稳定的场景。本节记录额外改善与待验证边界，
不增加历史失败 job 数，也不代替后面的 16 项人工 review。

- **GC 与真实 SDK 的重叠窗口**：前一镜像 matrix 虽通过，实际删除增长与
  前台工作同时活跃的七个窗口合计仅 1.763 秒；冷启动 SDK 发生在已删除
  一个文件后。现先在可执行 purge marker 发布前启动四个真实 SDK worker，
  等 namespace 请求成功并 parked，再执行原三次重启，直到实际删除边界
  才放行请求与小写入。warmup、seed、run 共用原 720/3300 秒绝对期限。
  保留九文件、三次重启、64 次小写入、六类各至少 48 次成功 SDK 操作和
  成功请求完成时刻落在真实删除增长区间的断言。独占进程组正常清理失败
  必须使 case 失败；取消与 Drop 保留首错及完整 stdout/stderr。
  六项纯协议回归、full clippy 和独立只读复核通过；三个受影响真实 GC
  case 在新 bundle 均已通过，队列用时 480.220 秒。convergence/matrix
  分别有 240/81 个 SDK 完成时刻落在 5/7 个真实删除区间，均保留 64 次
  写入；matrix 三次重启先于放行。证据：`resume-gc-proof.json`。
- **Journal 刷新响应归属**：原 pathname-only waiter 可被选择 partition
  自动产生的 GET，或同路径的旧 cursor/树页请求满足。现先完成选择时的
  自动观察，再关联本次 Refresh 的具体 Request 和完整资源/cursor，
  等响应 body 完成并核对 Journal record。全部新增等待共用原三秒预算，
  owner-loss、restart、stale-cursor 断言保留。四项事件回归和完整 E2E
  TypeScript 检查通过；最终与 auxiliary 配对三个全新轮次均通过。
- **辅助服务故障的依赖恢复边界**：恢复后的浏览器组合第二轮在 ChunkKV
  已返回新 PID、Access 尚未恢复时进入下一个周期，触发原即时 `up` 断言。
  GET inventory 为 0.3–12 毫秒，Access PID 未变化。精确单例和原组合串行
  基线通过；原并发组合的只读采样确认依赖故障期间真实 readiness 503，
  后续恢复 200。现把同节点 Access 恢复加入已有三秒 replacement predicate，
  保留即时检查、真实 PID/namespace/operation、兄弟进程和注册唯一性。
  没有额外等待窗口、sleep、超时放宽或 mutation 重试。随后三个全新配对
  轮次均通过；首错和诊断保留在 `ACCESS-DEPENDENCY-CLOSURE.md` 及
  `first-resumed-browser-failure/`。这不是生产 false-down 根因修复的声明。
- **CLI 生命周期快照**：本轮全量首错为
  `plain_docker_target_attachment` 的 `KeyError: 'pid'`；cluster Ready
  不承诺独立 `/api/servers` cache 瞬时填齐可选 PID。现核对 owned
  容器/镜像/binding，读取 21 个服务的 monitor PID/generation/operation，
  并在对应容器 `/proc` 拒绝已经退出的旧正 PID；显式要求 Web 加六类服务。
  同时保留 18 个唯一注册、所有端点字段与已有 cached PID/namespace
  一致性检查。四项回归通过；最终加强版五例已在新 bundle 全部实际执行
  并通过，用时 102.757 秒。没有对可选 PID 加等待或调用方重试。
- **Dataset 准备阶段余量**：前一镜像五个相关 client 为 280.68–288.44 秒，
  其中 browser 仅 4.72–11.20 秒，主要耗时为正常 production balance
  收敛，外层 client 为 360 秒。不能声称并行 seed 能解决约 270 秒的主耗时。
  保留 512 条记录、至少 12 partitions、三个 owners、默认 policy 和原期限，
  新增 seed/balance/逐条验证的独立阶段时间；本轮及新 SHA CI 继续核对余量。
  当前新镜像的全量验证中，browse 为 281.284 秒，import 为 321.901 秒，
  recovery 为 331.468 秒；后两者离 360 秒外层期限仅余 38.099/28.532 秒。
  两者到 272 秒仍只有九个 partitions，302 秒才观察到十二个；尚无证据
  表明条件已满足后存在可去掉的空等。30 秒为测试日志节流，生产 planner
  实际为两秒 heartbeat，不能把打印间隔当成决策周期。只读审计及下次复现
  所需的有界 planner/transition/freshness 证据见
  `ci/stability-final/DATASET-HEADROOM-AUDIT.md`。
  本轮新增计时的 browse 已通过，client 278.32 秒，默认 balance 占
  260.002 秒；该项尚未证明有行为级性能改善，不能仅因一次通过关闭风险。
- **有针对性的重复验证**：新的辅助服务/Journal 组合与 Go SDK cleanup
  各三个全新 fixture 轮次均通过；此前首错停止记录完整保留。
  Go 原始 cleanup 原因仍未知；重复通过只提供当前覆盖，不能声称已找到
  历史根因或证明长期失败率。执行记录保留在 `ci/stability-final/`。

## Review 规则与证据

- 两次原始 run 分别有 27 个 job，失败数为 8 和 5；没有 cancelled job。
- 用户追加的最新 run `41159636766` 同样有 27 个展开后的 job，24 成功、
  3 失败、无 cancelled；attempt 为 1，head 为
  `8885d2adc351d9c99f0219e18ecf0c4782bc3079`，分支为 `task-fix-build`。
  原始 13 项保留，新一轮逐项追加为 14–16；相同 job 名不代表同一个首错。
- 原始测试版本分别为 `39440d023d7fb162dd61c377e8eb17eefa8c4c9a` 和
  `25059cf7d9a5f08d61479f4749659aa6f3fe119f`。
- 已获取前两轮全部 13 个失败 job 的完整日志、7 份原始诊断 artifact，并逐项映射
  32 条容器失败记录及 2 个非容器失败 case。
- 第三轮另有 3 份完整失败 job 日志和 3 份诊断 artifact；下载档案的 SHA256
  均与 GitHub metadata digest 相符。累计 16 个失败 job、35 条容器失败记录，
  case 名称有跨 run 重复，不能说成 35 个不同 case。
- 区分首个真实错误、同一故障的后续症状、同批启动失败导致的未执行项、
  故意注入的失败和包装层 panic。不能把它们都算作独立根因。
- 保持原有超时、断言和调用方重试策略；修复真实前置条件、竞态和资源隔离问题。
- 单次 PASS 是覆盖证据，不是长期失败率结论。发布后的精确 SHA、所有子 job、
  attempt 和结果也必须记录。
- 已有组件与前一镜像的完整 396-case 账本均通过。上一版物理 P12 修复镜像
  全量 396 个 canonical case 及并发隔离均通过；共 400 个完整 fixture，0 失败。
- 本地证据保存在
  `.crowdb-runtime/artifacts/physical-nodes/physical-20261031T194149Z/ci/prior-ci-audit/`：
  `REPORT.md`、`job-matrix.json`、`case-coverage.json`、完整 job 日志及原始 artifact。
  第三轮另见 `run-41159636766/REPORT.md`、`job-matrix.json`、
  `artifact-sha256.json` 及 `current-exact-case-validation.json`。
  该版记录位于同一 run 的 `ci/repair-target-fixed/`；当前新验证队列位于
  `ci/stability-final/`，不能将旧镜像的 PASS 直接归给新增 RPC/观察路径修复。
- 上一版 P12 修复镜像输入 tree 为 `76f794e03757249f37efa96cd0bfcad064be4388`；
  32 条历史容器失败记录所对应的 case 已全部在此镜像实际执行并通过，
  精确映射见 `new-image-case-progress.json`。全量结束后的 scope guard 确认
  原有 7 个停止容器及卷身份完全不变。物理恢复仍须完成；用户随后取消
  继续检测 CI，不再要求新提交的远端 CI 作为物理验收前置。

## Run 41003611925：8 个失败 job

- [ ] **01 / AccessTests**：review accepted policy 的测试前置条件。
  [原始 job](https://github.com/buzzcrow/crowdb/actions/runs/41003611925/job/123893403740)。
  - 首错：`retained_quorum_startup_does_not_block_discovery_or_confuse_liveness_with_health`
    在 retained quorum 启动时返回 `accepted node policy is absent`。
  - 根因已确认：fixture 未先接受启动所要求的 node policy。
  - 当前处理：启动 trust/profile 前先 `accept_node_policy`，明确 3 voters。
    文件：`container/crowdb-monitor/tests/kv_resume_test.rs`。
  - 验证：后一次原始 run 的该 job 成功；本地 access 完整组及 exact case 通过。
  - Review：policy 接受顺序是否符合真实启动流程；是否仍分别验证 discovery、
    liveness 与 health；新 SHA 的 AccessTests 结果待补。

- [ ] **02 / ContainerDeployment (deployment-reference)**：review diskless
  policy 对 Group 0 的要求。
  [原始 job](https://github.com/buzzcrow/crowdb/actions/runs/41003611925/job/123899615882)。
  - 首错：`direct_docker_three_nodes` 初始化 HTTP 500，明确报告
    `accepted node policy has no enabled Paxos KV listeners`。
  - 根因已确认：diskless/manual fixture 把初始化 Group 0 所需的 Paxos KV 也禁用了。
  - 当前处理：共享 setup choices 保留已接受的 KV listener 10000/10100。
    文件：`e2e-test/profiles/setup_choices.py`、`e2e-test/console_native_policy.py`。
  - 验证：前一镜像 exact case 与 fresh reference 重跑通过；P12 修复镜像
    Docker 三节点完整通过，约 215 秒。
  - Review：diskless 与“无 Group 0”是否正确区分；单/三节点、保留卷、显式非 voter
    恢复及 Compose 覆盖是否完整；清理必须只作用于自己记录的资源。

- [ ] **03 / S3E2E**：review Go SDK 注入后的 cleanup 证据缺口。
  [原始 job](https://github.com/buzzcrow/crowdb/actions/runs/41003611925/job/123899615935)。
  - 首错：`s3.sdk::go` 普通流程先通过；`after-mpu` 故意失败本应返回 42，
    后续 `cleanup FAILED` 把退出码改为 1。
  - **旧根因未确定**：日志没有具体操作/API code，原始 artifact 也无法区分
    ListMultipartUploads、AbortMultipartUpload、ListObjectsV2、DeleteBucket 等失败点。
    不能声称 cleanup 算法已被修复。
  - 当前处理：记录脱敏的 Smithy operation/API error code；保留原 cleanup 流程与期限。
    文件：`e2e-test/clients/s3-go/main.go`、`multipart.go`、`e2e-test/clients/s3-sdk.sh`。
  - 验证：第二次原始 run、前一镜像完整验收及P12 修复镜像 exact case 均通过；
    本轮日志明确确认注入后的 bucket/MPU cleanup 完成。
  - Review：确认此项保留为历史原因未知；核对新 SHA 的 Go 场景。
    若复现，用独立 artifact root 跑 `s3.sdk::go`，保留增强错误、Access 请求日志及
    bucket/MPU 只读状态后再修复；不增加重试、不放宽清理成功条件。

- [ ] **04 / ContainerConsole (console-native-data)**：review 9 个数据/拓扑场景。
  [原始 job](https://github.com/buzzcrow/crowdb/actions/runs/41003611925/job/123899615992)。
  - 首错：Dataset browse 初始化的 disabled-KV policy 错误；其余表现为
    No route、SSH 断开和 socket 错误。
  - 根因已确认：9 个实际失败 fixture 的节点原始日志均有 policy 错误和 exit 1，
    发生在各自网络症状之前，不能归因为跨 job 的共享 Docker 清理。
  - 当前处理：复用保留 Group 0 listener 的 diskless policy。
    文件：`e2e-test/profiles/setup_choices.py`、`e2e-test/console_native_policy.py`。
  - 验证：前一镜像 9 个 exact case 全通过；第二次原始 run 的该 job 成功。
    P12 修复镜像 9 个 exact case 也已全部通过。
  - Review：逐一核对 Dataset browse/Split/capabilities、六节点往返、三种多 rack/
    子集拓扑、mixed units 和 maintenance/status badges；启动失败与实际 UI 断言分开。
    同时检查预算边界：`e2e-test/scenarios.py` 中 Dataset browser client 总期限为
    360 秒，而 `e2e-test/tests/console/native_balance.rs` 的
    `wait_balanced` 内部上限为 600 秒；本轮 browse/import 的
    总 client 时间约 287 秒。明确各阶段应达到的准备条件和计时，不直接延长期限。
  - 对应 case（均以 `console::` 开头）：`browser_dataset::browse`、
    `browser_dataset::split_revisions`、`browser_dataset::capabilities`、
    `browser_data::six_node_round_trips`、`browser_topology::multi_rack_leaders`、
    `browser_topology::independent_subsets`、`browser_topology::simple_and_complex`、
    `native_geometry::mixed_units_reject_consumers_without_starting_services`、
    `native_browser::disk_maintenance_and_status_badges`。

- [ ] **05 / ContainerConsole (console-browser)**：review 两个 Group 0 bootstrap 场景。
  [原始 job](https://github.com/buzzcrow/crowdb/actions/runs/41003611925/job/123899616023)。
  - 首错：replica navigation 和 logical create 的 public bootstrap 返回 No route。
  - 根因已确认：两个 fixture 的三个节点均先因 disabled-KV policy exit 1，
    inspect 中 IP 已被清空。
  - 当前处理：共享 accepted diskless policy 保留 Paxos KV。
    文件：`e2e-test/profiles/setup_choices.py`、`e2e-test/console_native_policy.py`。
  - 验证：两个 exact case 的前一镜像完整验收通过；第二次原始 run 该 job 成功。
    P12 修复镜像两个场景均已实际执行并通过。
  - Review：不把已退出进程的连接错误当成独立网络缺陷，核对 bootstrap 后的实际断言。

- [ ] **06 / ContainerConsole (console-inspection)**：review inspection 启动与 UI 边界。
  [原始 job](https://github.com/buzzcrow/crowdb/actions/runs/41003611925/job/123899616075)。
  - 首错：`page_layout_and_iceberg_inspection` 在 bootstrap 阶段 No route。
  - 根因已确认：三个节点原始日志有 disabled-KV policy 错误、exit 1、OOM false 和空 IP。
    失败发生在 inspection 断言之前。
  - 当前处理：统一 accepted policy；文件同上。
  - 验证：前一镜像 exact case 通过；第二次原始 run 该 job 成功。
    P12 修复镜像进入全部 13 个 Playwright 断言并通过，总 client 约 326 秒。
  - Review：核对布局和 Iceberg inspection 的实际断言，区分 fixture 启动错误。

- [ ] **07 / ContainerConsole (console-transition)**：review journal transition 的真实前置条件。
  [原始 job](https://github.com/buzzcrow/crowdb/actions/runs/41003611925/job/123899616076)。
  - 首错：`production_split_and_transfer_journals` bootstrap 超时。
  - 根因已确认：node0 先因 disabled-KV policy exit 1，随后 bootstrap 等待超时；
    不能把这次失败说成 journal 状态转换断言失败。
  - 当前处理：统一 accepted policy；文件同上。
  - 验证：前一镜像 exact case 通过；第二次原始 run 该 job 成功。
    P12 修复镜像实际执行 split/transfer journal 断言并通过。
  - Review：检查 split/transfer journal 的状态转换，保留原有等待预算。

- [ ] **08 / ContainerConsole (console-native-operations)**：review 真实启动失败与同批级联。
  [原始 job](https://github.com/buzzcrow/crowdb/actions/runs/41003611925/job/123899616153)。
  - 首错：`diskdb_and_kv_independence` 初始化中的 SSH authentication failed；
    该 job 共打印 11 个失败 case。
  - 根因已确认：10 个实际失败 fixture 均有本地 disabled-KV policy exit 1。
    `binding_recommendations` 已准备但未派发浏览器：后一个 fixture 启动失败，
    `ExitStack` 将同一异常传给已进入的 peer，后者按自己的 case 名打印失败。
  - 当前处理：修复实际 policy 前置条件；没有跨 fixture 凭据或 SSH target 共享的证据。
    后续新增 `CaseNotExecuted`，同批未派发 peer 保留完整清理，并在 fixture、
    report 和 measurement 单独记为 `not_executed`，不能计作 PASS 或独立 case
    失败。真正的启动错误和 peer 自身的诊断/清理错误分别保留，batch 仍失败。
    正常 client 后的清理失败打印自己的 case；已有首错时，后续清理错误单独
    打印且保留原异常优先，不增加重复的 `FAILED CASE` 记录。
    文件：`e2e-test/browser_batch.py`、`e2e-test/fixture.py`、
    `e2e-test/fixture_result.py`、`e2e-test/report.py`、
    `tools/test-metrics/measure.py` 及对应 Python 测试，以及共享 policy。
  - 验证：前一镜像 11 个 exact case 均通过；partial-startup 回归确认不派发 clients
    且清理已启动 peer。P12 修复镜像 11 个场景也全部通过。新增归因回归在旧实现
    确定性失败，修复后完整 fixture suite 290 tests 通过；没有操作真实容器。
  - Review：逐项核对这 11 个场景及其实际派发状态，并核对新的未执行统计、
    startup/client/cleanup 各自归属及 fail-fast 清理；不将它扩大解释为凭据共享 bug。
  - 对应 case（均以 `console::` 开头）：`browser_lifecycle::diskdb_and_kv_independence`、
    `browser_capacity::binding_recommendations`（原 run 未执行）、
    `browser_capacity::mounted_crud_and_owner_binding`、`native_browser::auxiliary_process_menus`、
    `native_browser::interrupted_plan_reconciliation`、
    `native_browser::storage_windows_and_access_recovery`、
    `native_browser::large_journal_records_and_restart`、
    `native_load::production_load_keeps_heartbeats_and_durable_bytes_across_owner_restart`、
    `native_browser::chunk_ownership_scope`、
    `native_browser::multipart_upload_and_physical_navigation`、
    `native_binding::prebound_groups_acquire_live_owners_and_require_explicit_binding_repair`。

## Run 41128190276：5 个失败 job

- [ ] **09 / UnitTests**：review leader role 与可提交状态的区别。
  [原始 job](https://github.com/buzzcrow/crowdb/actions/runs/41128190276/job/124312015280)。
  - 首错：`g3_leader_change::leader_change_simulation` 的第二笔单次写入未通过新 leader 提交。
  - 已支持的原因：旧等待只看 leader role；focused regression 证明 role 可能先于
    当前 proposing term 和有效 quorum lease。旧日志没有保留该次 RPC reply code，
    不应拿后续故意注入的 WAL 错误替代首错。
  - 当前处理：等待既有 proposing-term/quorum-lease readiness，并保护健康 leader
    在当前 quorum lease 内不投票离任。
    文件：`lib/crowdb-kv/tests/group_test/g3_leader_change_test.rs`、
    `lib/crowdb-kv/src/cluster/local_replica_election.rs`、
    `lib/crowdb-kv/tests/election_test/lease_test.rs`、
    `lib/crowdb-kv/tests/election_test/vote_test.rs`。
  - 验证：exact case、role-before-lease 和 quorum-lease vote 回归均通过；
    本地 core 1165 tests / 91 binaries 通过。
  - Review：准备条件不能替代提交断言；检查选举/恢复交互、无新增热路径锁，
    并核对新 SHA 的完整 UnitTests。

- [ ] **10 / ContainerDeployment (deployment-services)**：review KV-only readiness 与实际端口。
  [原始 job](https://github.com/buzzcrow/crowdb/actions/runs/41128190276/job/124321492635)。
  - 首错：`monitor_creates_real_kv_groups_then_validates_after_restart` 与
    `selected_kv_services_recover_acknowledged_data` 一直不能 Ready。
  - 已确认问题：diskless KV-only policy 被要求提供普通 storage metadata；member
    收集还假定 Web 使用 9090，而未使用已注册端口。
  - 当前处理：按 `requires_default_preparation` 收集确实需要的依赖，使用 `node.web_port`。
    文件：`lib/crowdb-console-shared/src/ops/cluster/readiness/collect.rs`、
    `lib/crowdb-console-shared/src/ops/cluster/readiness/collect/members.rs`、
    `lib/crowdb-console-shared/tests/cluster_readiness_test.rs`、
    `lib/crowdb-console-shared/tests/cluster_storage_observation_test.rs`。
  - 验证：前一镜像 deployment-services 完整组、两个 exact case，以及
    KV-only offset-port/no-storage-preparation 回归通过。
  - Review：检查依赖来源符合 accepted policy，普通 storage readiness 仍严格验证；
    P12 修复镜像两个 exact case 已通过；新 SHA 完整 job 仍须核对。

- [ ] **11 / ContainerConsole (console-api)**：review listener 来源和 terminal fault 的作用域。
  [原始 job](https://github.com/buzzcrow/crowdb/actions/runs/41128190276/job/124321492786)。
  - 首错：plain Docker attachment 向旧 S3 端口 9091 请求；terminal writer
    检查旧 health 端口 9094，连接失败。
  - 当前处理：plain Docker public URI 改为此 fixture 使用的 S3/Iceberg/Dataset
    端口 16000/17000/18000；terminal health 从 accepted listener policy 读取。
    terminal 用例在 owned network namespace 注入 DiskIO outage。之后本地真实复现的
    ChunkKV 同步 native I/O 阻塞 executor/lease 问题另由 worker offload 修复。
    文件：`e2e-test/console_cli_fixture.py`、`e2e-test/console_cli_recovery.py`、
    `e2e-test/console_cli_target.py`、`e2e-test/console_cli_io_fault.py`、
    `lib/crowdb-chunk-kv/src/partition/native.rs`、
    `app/crowdb-chunk-kv-server/src/storage.rs` 及相关回归。
  - 验证：accepted-listener 回归、console-api 完整组和两个 exact case 通过；
    P12 修复镜像这两个 case 已完整通过。
  - Review：区分旧端口错误与后来复现的 liveness bug；故障确实作用于 I/O，
    原 lease/terminal replay/Stop fencing 断言和资源限制保留。

- [ ] **12 / IcebergE2E (iceberg-gc-cleanup)**：review 前台负载与实际删除窗口的重叠。
  [原始 job](https://github.com/buzzcrow/crowdb/actions/runs/41128190276/job/124321492803)。
  - 首错：`restarted_cleaners_finish_large_and_sparse_backlogs_beside_an_invalid_catalog`
    未观察到成功 SDK request 的完成时刻落在观测到的 active-deletion 区间。
  - 根因证据：GC 在约 117 秒完成并执行了 3 次 restart；有限、同步步调的
    48 cycles 没有保证请求与删除窗口相交，负载还先于 cleanup 结束。
    这不证明 GC 自身未完成。
  - 当前处理：压力持续至 GC 和小写入均结束，保留每类最少 48 次操作，
    错开 4 个 workers，继续要求真实成功请求重叠；FileReader leaf cache 的
    性能修复单独记录。
    文件：`e2e-test/tests/access_iceberg/gc/sdk_pressure.py`、
    `e2e-test/tests/access_iceberg/gc/convergence/pressure.rs`、
    `e2e-test/tests/access_iceberg/gc/notifications/matrix.rs`、
    `e2e-test/tests/access_iceberg/gc/notifications/matrix/records.rs`、
    `lib/crowdb-access-iceberg/src/file/reader.rs`。
  - 验证：focused 重跑、前一镜像完整验收通过；新镜像 exact matrix 约 123 秒通过，
    inline GC 前台并发及完整退休宽限期也通过。
  - Review：确认 overlap 是从实际事件/单次请求时间计算，原断言未放宽；
    不从该场景推断单独的生产 latency SLO。

- [ ] **13 / ContainerConsole (console-native-operations)**：review journal UI 的 coherent observation。
  [原始 job](https://github.com/buzzcrow/crowdb/actions/runs/41128190276/job/124321492894)。
  - 首错：`journal_identity_and_owner_recovery` 导航后，current-owner 条件
    在原 3000 ms 窗口内未选出 coherent journal observation。
  - 当前处理：Rust fixture 先创建 owned witness、等待已有分区的 ownership 稳定；
    浏览器验证 witness 并保留同一 identity/lifecycle 断言和 step timing。
    之后本地 native executor/lease 复现由 offload 修复。
    文件：`e2e-test/tests/console/native_browser.rs`、
    `e2e-test/tests/console/native_balance.rs`、
    `e2e-test/clients/console-ui/e2e/flows/55-chunk-kv-catalog.spec.ts`、
    `lib/crowdb-chunk-kv/tests/native_runtime_test.rs`。
  - 验证：focused native-journal 约 28 秒通过，前一镜像 exact case 通过；
    P12 修复镜像对应场景约 16 秒通过，原 3 秒 UI 断言不变。
  - Review：检查 fixture 的真实前置条件、UI 的 current-owner/epoch/cursor 一致性，
    保留 3 秒断言以及 owner-loss/restart/stale-cursor 场景。

## Run 41159636766：3 个失败 job

这轮来自更新的远端 head；三个失败都不能套用同名旧 job 的首错。完整日志、
浏览器 trace、API 时间线和当前 exact-case 映射保存在证据目录的
`run-41159636766/` 下。

- [ ] **14 / ContainerConsole (console-native-operations)**：review 目标进程的
  生命周期与聚合观察延迟。
  [原始 job](https://github.com/buzzcrow/crowdb/actions/runs/41159636766/job/124418859705)。
  - 唯一真实失败：`console::native_browser::auxiliary_process_menus`。
    DiskIO Start 响应成功，但 `diskio-1 new process` 的 3000 ms predicate 超时。
  - 已确认的首个分歧：Start 响应已有新 PID `1389`；该次 predicate 内的
    `GET /api/servers` 约 3012 ms 才返回，晚于 predicate 的失败时刻。
    返回内容也已包含目标 PID 和 health up，不能解释为 DiskIO 没有启动。
    同响应中另一节点的整组 PID 缺失，支持等待无关节点 SSH 的解释；
    但原记录没有各内部阶段的慢 span，不能确定 intent refresh、remote process、
    health probe 各耗时多少，**该节点观察为什么不可用仍未确定**。
  - 当前修复：authority、每个节点的实际进程和每个服务的 endpoint health
    独立采样发布；GET 读取带身份/新鲜度 fence 的 remote facts，不等待 KV、
    SSH 或 endpoint 探测。仍读取本地 binding 以立即拒绝旧身份。
    三秒从采样开始计，读取不刷新 age；mutation 期间及失败返回后的迟到结果
    也必须失效。monitor 私有 Processes 将 live PID 与实际执行 operation/
    bootstrap 绑定，保留 Group 0 故障时的 actual PID 观察。
    新校验同时发现 KV 自动恢复后旧 receipt PID 未更新的边界，补只更新
    已验证当前执行回执的恢复路径，不再次 restart，不接纳 foreign bootstrap。
    不将上述新发现说成原 CI 的已证实现场根因。
    文件：`e2e-test/clients/console-ui/e2e/flows/11-cluster-server-lifecycle.spec.ts`、
    `e2e-test/clients/console-ui/e2e/fixtures/nativeProcess.ts`，以及
    `app/crowdb-web/src/services/observation.rs`、
    `app/crowdb-web/src/services/remote.rs`、`services/observation/`、
    `container/crowdb-monitor/src/node/runtime/services/` 及其 process observation。
  - 验证：前一镜像 exact case 实际通过，Playwright 单 test 约 13.3 秒。
    新生产观察路径的九个 focused 回归和两个真实 monitor 进程回归通过：慢 peer/同节点慢 sibling、
    concurrent read、expiry/identity、same-op late sample、unknown mutation、
    Group 0 故障、pending/exhaustion 和删除不复活；完整组件与 lint 已通过，
    新镜像和 E2E 门禁仍待完成。原 combined 三秒 predicate 保留，不通过移动 deadline 修复。
  - Review：分别计时 Start response、目标 typed process、聚合 inventory 和 DOM。
    通过 owned monitor PID 与同 namespace `/proc` 核对 typed target，继续保留
    inventory 的唯一 identity、18 条记录、DOM 和健康检查及 combined 三秒。
    完成原 test 的当前精确计时后，再补 Start body identity/data-preserved 与
    siblings PID/generation/operation 快照；不能把一次 PASS 说成长期稳定率。

- [ ] **15 / ContainerDeployment (deployment-services)**：review retained KV
  restart 后读取 readiness 的顺序。
  [原始 job](https://github.com/buzzcrow/crowdb/actions/runs/41159636766/job/124418859709)。
  - 唯一真实失败：
    `deployment::kv_bootstrap::monitor_creates_real_kv_groups_then_validates_after_restart`。
    verify 的首个 `/stores/0/groups/1/ready` 请求返回 503，原因是
    `no leader elected`；预期为 200。不是上一轮 KV-only storage dependency 错误。
  - 已确认问题：verify 在重启后直接断言已有 Group 1 ready，没有先等待选举完成。
  - 当前处理：`cdf023d6` 让 write/verify 两个阶段都先调用现有
    `wait_for_witness_group`，再执行单次 200 断言和 acknowledged-data 验证；
    保留原有四秒准备期限，不重试写入。
    文件：`e2e-test/tests/deployment/kv_bootstrap.rs`。
  - 验证：P12 修复镜像 exact case 通过；新 SHA 的完整 deployment-services 待核对。
  - Review：ready 必须包含已选 leader 和可提交条件，不能只看进程存活；核对
    重启后的 retained group/data 验证确实执行，且 readiness 期限没有延长。

- [ ] **16 / ContainerDeployment (deployment-reference)**：review 自动加入与
  显式 admit 的排序。
  [原始 job](https://github.com/buzzcrow/crowdb/actions/runs/41159636766/job/124418859712)。
  - 唯一真实失败：`deployment::reference::direct_docker_three_nodes`。
    Docker 三节点主体验证已经 PASS，后续手工 admit 一个 nonvoter 得到 409，
    原始响应说明 `service operation already running`。
  - 已确认问题：手工 admit 与已有 service operation 冲突；不是第一轮的
    disabled-KV policy 启动错误。旧 fixture 未先停 AutoJoin，源码与针对性修复
    支持 AutoJoin/manual admission 竞争的解释；原 artifact 没有记录持有 operation
    的 actor/id，确切竞争者身份仍属有依据的推断。
  - 当前处理：`cdf023d6` 先停 AutoJoin 并等待 disabled 状态，再执行一次显式 admit。
    文件：`e2e-test/oci_reference_recovery.py`。
  - 验证：P12 修复镜像 exact case 完整通过；原先的主体验证、nonvoter 行为及
    scoped cleanup 都保留。
  - Review：确认自动操作已结束后再手工操作；保留单次 admit 及 nonvoter 断言，
    不用重试 409 掩盖竞争；核对新 SHA 的完整 deployment-reference。

## 最后逐项 review 的记录

- [ ] **补齐最终验收证据**：整理 396-case 历史账本、后续修复的受影响
  组件/E2E 与物理恢复，保留各自源码/镜像身份和覆盖边界，不将早期 PASS
  归给当前镜像。远端仅保留已经取得的结果；用户已取消继续检测 CI，
  不再要求本次新 SHA 的 27 个远端 job。
- [ ] **逐项共同 review 01–16**：每项明确“原因确认程度、修复是否充分、
  回归是否能捕获原问题、还需做什么”。保留 Go 旧原因未知的事实，
  核对 browser batch 新增的未执行/级联错误归因及独立统计。
- [ ] **归档结论与后续工作**：将确定需要实现的稳定性工作拆成具体任务；
  不以反复重跑得到一次绿色、延长时间或删除断言代替修复。
- [ ] **Review 物理 P12 的 EC 可用性边界**：此项不是新增失败 CI job。
  新镜像的 catalog 接管在 40.184 秒通过；新 S3 witness 两份镜像均在 A，
  B 停止时三个 A 入口都完整读回。旧 baseline witness 已转为 EC 8+4，
  B 上有 6 个分片，超出 4 个分片的恢复预算；旧 harness 在 fault witness
  验证中额外遍历所有历史 witness，因旧对象暂时不可读而失败。
  只读复现已区分新/旧对象，B 返回后两者五入口均通过，未重放 mutation。
  Review 既有 P12 合同的两阶段验证：停机时严格验证本次新写入；恢复后
  保留所有历史数据的完整审计。若需要旧 EC 数据也抗整机故障，须另行确定
  拓扑、放置和可用性合同；KV quorum 不能替代该承诺。
- [ ] **复核 P12 幸存者的 native 崩溃修复及物理验证**：此项也不是新增失败 CI job。
  完整区间核对发现 A3 ChunkKV 从 PID 451/generation 2 变为
  PID 66135/generation 3，其余 20 个 A 服务身份稳定。此前 stderr 为空，
  新增 `std::system_error: Resource deadlock avoided`；monitor 记录了停止与重启，
  尚无原进程 core。已通过真实 Rust FFI 最后 owner eviction 复现同样异常，
  gdb 证实 `RpcServer` 析构在 response completion 内触发 `Worker::join` 错误 35。
  共享路由 owner 已改为外部生命周期线程释放，覆盖 response/close/timeout、
  递归退休、阻塞析构的 backlog 和进程退出顺序；修复后的完整 FFI、storage、
  fmt/clippy 已通过。旧物理进程的精确栈仍未知，不能声称已直接取得它的栈。
  Review 另发现 C ABI send/chain/slab 在同步 completion 释放 wrapper 后仍会
  读它；四个确定性原生用例在修复前失败，保活 native client/connection/transport
  至返回后均通过。完整 C++、owned tree transport、storage、fmt/tree-lint 已通过。
  生命周期修复的本地验证与发布已经完成；CI 按用户要求停止检测。
  当前 c063 新镜像的 P12/P13 物理复验尚待完成，P14 门禁继续保留。
  文件：`lib/crowdb-tree/ffi/src/chunk/transport.rs`、
  `lib/crowdb-rpc/ffi/src/owned_route.rs`、
  `lib/crowdb-rpc/src/client/client.cpp`、
  `lib/crowdb-rpc/src/transport/socket_transport.cpp`。
  不以跳过 join、捕获后忽略异常、扩大健康预算或容忍静默重启代替生命周期修复。
- [ ] **定位 P13 恢复后的 Iceberg 完整读取超时**：此项不是新增失败 CI job。
  五个入口的 S3 SF1 原始文件均完整读回；并发 Iceberg 审计先后打印
  `NETWORK_CONNECTION during GetObject`、curl 28、三秒低速超时。
  三个 verifier 真实失败，另两个在保留首错后收到定向 SIGTERM；后两项计为
  中止，Dataset/history 尚未执行，不能把包装层的 exit -15 当成最初根因。
  并发 stdout 没有入口标签，三次错误不能仅按打印顺序归属到 A1。
  不能因原始 S3 校验通过就忽略 Iceberg scan 的失败，也不延长客户端期限掩盖原因。
  保留本次并发、镜像、服务日志和原始错误，先确认进程身份与恢复状态，
  再复现首错并检查其与 native 崩溃的关系。
  B1/B2 此次 ChunkKV 重启已有 lease 到期、主动退出、正常 join 的日志闭环，
  stderr 为空；不是已证实的同一 C++ 异常。下次每个入口单独保存 stdout、
  endpoint、起止时间与退出码，再聚合状态，避免并发错误归属缺失。
  c063 启动审计另捕获 A 主机约十秒 NVMe I/O 停顿，随后 Group0 超时、
  五个 ChunkKV 租约退出和五个具名 Iceberg checksum reader 的 curl28；
  无 OOM，设备/文件系统原因未证实，不能据此倒推旧 P13 的根因。
  单对象原限制验证及新的完整五端点只读历史审计随后通过；新 P09 冷启动
  及完整历史审计也已通过，后续仍须完成 P12/P13 的独立故障验证。

## 文件与验证入口

- 文件按上面 01–16 各项归属列出；跨项共同检查 policy、fixture 生命周期与
  `e2e-test/oci_shortcuts.py` 的资源归属清理，不将独立问题混为一个根因。
- 单元/组件：KV election/readiness、console cluster readiness、accepted-listener、
  partial browser startup、OCI shortcut cleanup、native worker 和 FileReader 回归。
- 集成：完整 core/access/console/storage 组，以及精确的原始非容器失败 case。
- E2E：396 个 canonical scope/case、并发隔离、原失败 case 的实际执行、Go
  故意注入后的 cleanup、GC 真实 overlap，以及新镜像上的物理恢复。
- 远端：保留已记录的精确 SHA、attempt 和展开 job。用户已要求停止检测 CI，
  不查询或推断当前提交的新 run，也不将它作为后续物理验收条件。
