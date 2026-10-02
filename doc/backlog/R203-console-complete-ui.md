<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R203: console — 完整 Web UI 与 Container 操作界面

#### Problem

- 当前 Web 的顶层入口是 Cluster、KV、Capacity。Cluster 展示物理布局；
  KV 提供逻辑资源管理与键值操作；Capacity 提供 DiskDB 容量操作。
- Capacity 内的 Chunk 子页仍复用拓扑画布，没有完整的 Chunk 列表、Strip
  组成和实际落盘位置展示。Iceberg、S3 没有对应的顶层操作页。
- Container 当前进入独立的 `ManagedPreview`，无法使用完整 Console。
  Container 应具有相同的数据操作界面，但物理拓扑与进程部署由部署配置、
  crowdb-monitor 管理，用户不能从 Web 改写这些信息。
- 独立 Web 的首次引导不能要求先存在 Group 0。用户应能从空 UI 添加
  Rack、Node、部署 Server，然后在 KV 中初始化 Group 0。此前删除本地
  临时配置及恢复入口的改动打断了此流程。当前分支的修复尚未完成全面验证，
  本需求不能把它当作已验收的持久化能力。
- 具体场景：定位一个 Iceberg/S3 请求使用的存储资源；检查一个 Chunk 的
  Mirror/EC Strip 落在哪个 Node、DiskGroup、Disk；重启 Web 后继续管理
  原集群；在 Container 中执行数据 CRUD 而不改变物理部署。
- 根设计：[Console architecture](../design/console/design-crowdb-console.md)、
  [Console UI](../design/console/design-crowdb-console-ui.md)。本需求修订独立
  Web 的引导边界，保留 Container 中 Group 0 为已初始化配置权威的原则。

#### Solution

##### 1. 共用框架与信息架构

- 五个顶层 tab：`Cluster | KV | Capacity | Iceberg | S3`。
  用户可见名称为 Iceberg；仓库现有 `iceberge` 文档路径不在本需求中改名。
- 共用 Header、左侧资源树/筛选、中间业务面板、右侧 Inspector。
  Inspector 提供 Details 与 Activity，可折叠。左右栏可调整宽度。
- 同域选择同步左树、中心内容和 Inspector。切换域清除不适用的选中实体。
  跨域跳转携带目标身份；加载后展开目标，缺失目标给明确提示。
- 查询/写入表单参考现有 KV 面板：作用域选择、操作栏、查询结果、选中项
  编辑区。每个域显示自己的操作语义，不能把所有资源变成通用 JSON 编辑。
- 每个 tab 有独立 scope：Cluster 为物理实体，KV 为 Store/Group，Capacity
  为硬件容量或 Chunk prefix/ID，Iceberg 为 Catalog/Namespace/Table，S3 为
  授权作用域/Bucket/prefix。scope 在对应面板明确可见；刷新、过滤、写操作
  只作用于该 scope。切域可保留筛选，不能沿用其他域的写入目标。
- Demo 操作分域提供，标注实际写入目标；KV 示例键、S3 示例对象及 Iceberg
  示例表使用可识别的 demo 名称，确认清理的精确范围。禁止往 Group 0 系统
  配置键写 demo。Chunk/Capacity 只展示真实数据，不注入假布局；没有数据时
  引导到对应部署或数据写入流程。Demo 使用正常 API 与权限，不能绕过协议。
- 查询列表使用服务端筛选与有界分页；默认每页 100 项，Load more 追加。
  没有精确总数时只显示已加载数量及是否还有下一页，不扫描全域计算总数。
- 修改操作等待后端结果再刷新对应资源，不用客户端缓存伪造成功。
  Activity 记录当前会话的操作、目标、时间、结果和关联请求信息；它不承诺
  作为持久化审计日志。不同域不会共用一个笼统的“Backend unreachable”。

```text
+----------------------------------------------------------------------------+
| CrowDB Console   deployment/status   Cluster KV Capacity Iceberg S3 Refresh |
+-------------------+------------------------------------+-------------------+
| Scope / filter    | Domain toolbar                     | Details | Activity|
|                   +------------------------------------+-------------------+
| Resource tree     |                                    | Selected identity |
| or prefix groups  | Topology / CRUD / capacity / strips | Fields / status   |
|                   |                                    | Cross-domain links|
+-------------------+------------------------------------+-------------------+
```

##### 2. 启动、配置权威与部署能力

- 独立 Web：无参数启动到固定 `default` 工作目录；首次为空，不自动创建
  Rack、Node、Server 或 Group 0，也不自动部署示例集群。
- Group 0 创建前，UI 的配置修改原子落盘到工作目录的临时配置文件；本地
  启动信息与数据目录稳定。重启读取同一目录，恢复已配置的 Server。
  仍存活的受管理进程应被重新识别，不能重复启动或仅凭旧 PID 控制进程。
- KV Init 在已部署、可达的 KV 节点上创建 Group 0，确认将硬件、逻辑配置
  写入 Group 0。保留可恢复的初始化意图，成功确认前不能删掉唯一引导信息。
- 初始化后，Group 0 成为集群配置权威。本地只承担连接提示、私密凭据引用、
  进程启动与恢复输入；本地缓存不能覆盖 Group 0，也不能在其不可达时变成
  可写的备用拓扑。重启先恢复 Server/Group 0，再读取其已确认信息。
- Header 显示 `Empty / Configuring / Initializing / Ready / Degraded /
  Unavailable` 中对应状态。Web 可达但尚无 Group 0 不等于后端故障。
  局部服务失败只影响相关功能；已初始化的 Group 0 不可达时显示配置不可用，
  禁用依赖它的修改，保留诊断和受管理的恢复操作。
- Container：相同五个 tab、相同资源展示。Rack、Node、Server 部署/删除、
  进程启动/停止/重启、DiskGroup/Disk 添加/删除/移动/状态修改不可操作。
  不允许手动 Init/Reset 已由 Container 管理的集群。
- Container 可以在认证角色允许时进行 Store/Group/Replica 管理、用户 KV、
  Iceberg、S3 数据操作，以及部署 profile 明确支持的存储运行时维护。
  单节点 profile 的复制/EC 限制仍由服务端校验，不能通过 UI 绕过。
- 权限按能力提供：拓扑修改、进程管理、逻辑管理、数据读/写、运行时维护。
  页面整体 `readonly` 仍禁用所有写操作，但不能用它代替 Container 的
  分域权限。隐藏/禁用按钮只是展示；后端也必须拒绝越权请求。

```text
first standalone start
         |
         v
default directory -> empty UI -> Rack/Node -> deploy Servers
         ^                                      |
         |                                      v
         +---------- durable temporary config <-+
                                                |
                                             KV Init
                                                v
                         sealed intent -> create Group 0 -> publish config
                                                |
                                                v
restart -> local launch hints -> restore Servers/Group 0 -> confirmed config
                                                |
                          Group 0 unavailable --+--> diagnostics, no fallback writes
```

##### 3. Cluster：物理资源与 Server 生命周期

- 左侧：Datacenter → Rack → Node → Server。Server 按类型展示 KV、DiskDB、
  DiskIO、ChunkDB、Chunk-KV、Access Server；仅展示实际支持/已部署的类型。
  分配给 DiskDB 的 DiskGroup/Disk 可以在该 Server 下显示，未分配资源在
  Capacity 管理。Server 名称带类型、实例身份与状态，避免混淆。
- 中间：物理层级布局。一个 Node 卡片内列出所属 Server 和状态；Rack 包含
  Node。图是导航和部署状态投影，不表示 KV 副本关系或 Chunk Strip 关系。
- 操作：Add Rack、Add Node；Node 的 Deploy Server 按类型选择并校验启动
  参数；Server 的 Restart/Stop/Delete；实体 Details 和所属资源跳转。
  删除须明确影响范围；Container 隐藏物理修改操作并显示托管说明。
- Group 0 初始化属于 KV；Cluster 可以提示下一步并跳转 KV Init，不重复
  实现初始化业务。尚未提供完整部署能力的 Server 显示能力说明，不伪造入口。

```text
+-------------------+---------------------------------------+----------------+
| CLUSTER           | Rack A                                | Node 1         |
| DC                | +--------------+  +--------------+    | host / rack    |
|  Rack A           | | Node 1       |  | Node 2       |    | service status |
|   Node 1          | | KV       Up  |  | KV       Up  |    | deployment     |
|    KV             | | DiskDB   Up  |  | DiskDB   Up  |    | inputs / links |
|    DiskDB         | | ChunkDB  Up  |  | DiskIO   Up  |    |                |
|    ChunkDB        | +--------------+  +--------------+    | Activity       |
|   Node 2          | [Add Rack] [Add Node] [Deploy Server] |                |
+-------------------+---------------------------------------+----------------+
```

##### 4. KV：初始化、逻辑资源与键值 CRUD

- 左侧改为逻辑树：Store → Group → Replica。物理资源管理留在 Cluster。
  Replica 标注 Node、健康和 Leader；点击可跳转 Cluster 的所属 Server。
- 未初始化时中间显示 Init 引导：选择已部署且可达的 KV 节点，展示将创建
  Store 0/Group 0 及所选成员。无可用节点时引导到 Cluster Deploy。
- 初始化后：管理 Store、Group、Replica；选中 Store 可扫描所有 Group，
  选中 Group 进入其 KV 操作面板，选中 Replica 查看详情并保留所属 Group
  作用域。Replica 管理调用现有成员变更流程，不能编辑裸配置替代它。
- 中間为 Prefix/Key 筛选、Scan/Get、结果列表、选中键的 UTF-8/Hex
  查看、Put/Delete、Load more。Put 编辑的是值；Key 改名必须明确为新建
  和删除两个操作。扫描游标按 Group 独立保存，全 Store 结果标注 Group。
- 系统 Store 0/Group 0 明确标为 System，普通用户 KV CRUD 不可改写系统
  配置键。系统资源变更走对应管理操作；避免在通用 KV 面板破坏集群权威。

```text
+-------------------+---------------------------------------+----------------+
| KV                | Store 7 / Group 2                     | Group 2        |
| Store 0 [System]  | Prefix [        ] [Scan] [Get]        | leader / state |
| Store 7           | Key       Value preview      Revision | replicas       |
|  Group 1          | key-a     ...                123      | Node links     |
|  Group 2          | key-b     ...                124      |                |
|   Replica 1 N1    | [Load more]                           | Activity       |
|   Replica 2 N2    | Key [key-a]  Value [UTF-8 / Hex]       |                |
| [Add Store/Group] | [Put] [Delete]                        |                |
+-------------------+---------------------------------------+----------------+
```

##### 5. Capacity：DiskDB 容量与 ChunkDB 检查

- 保留二级 `Capacity | Chunk`。Capacity 的左树是物理容量作用域；Chunk 的
  左树是 Chunk 类型/prefix 分类，两者的筛选和选中实体分别管理。
- Capacity 保留 Cluster/Rack/Node/DiskGroup/Disk 逐级容量、DiskDB 实例
  状态、Zone 网格和 Bitmap；Scan/Recalc/Compact/Rebuild 显示准确作用域。
  DiskGroup/Disk 管理仍在此域。Container 可查看全部层级；硬件修改禁止，
  运行时维护依赖独立能力。单个实例不可达显示部分结果及缺失来源。
- Chunk 子页默认是只读诊断，不提供裸 Chunk/Strip 删除或布局手工改写。
  生命周期仍由所属业务和既有服务流程控制，避免绕过引用与回收规则。
- 左侧分类使用 Chunk ID 的真实类型字节以及可输入的十六进制 prefix。
  类型名/编码来自协议定义，不复制旧文档中的枚举数字。不认识的类型显示
  原始值；ID 类型与记录不符显示异常。任意 prefix 筛选与类型分类可组合。
- 分类是列表筛选，不是 ChunkDB hash range、KV Group 或存储所有权。
  后端跨相关所有者执行有界查询，处理分页、路由变化与部分不可达；不能在
  浏览器抓取整个 Chunk 集合再筛选。直接输入完整 ID 可定位单个 Chunk。
- 中间上部：Chunk ID、类型、生命周期状态、版本/代次（若服务暴露）、
  逻辑容量、已写入范围/使用情况（若服务可确认）、Strip 数量与查询时间。
  已分配容量、已写字节、物理占用分开显示，缺失量不能推导为 0。
- 中间下部：按逻辑顺序展示 Strip，使用 Strip 的稳定 sequence 身份；
  删除/替换后不能按数组下标重新编号。Mirror 标注实际 copy 数，EC 标注
  实际 k+m 和编码状态。同一 Chunk 可以包含不同布局，不假设全 Chunk
  使用同一种 Mirror/EC 参数。
- 点击 Strip：显示其覆盖的逻辑 offset/range、布局、写入/编码/健康状态、
  Segment/fragment 明细。每个 fragment 显示 Mirror copy 或 EC data/parity
  角色、Rack → Node → DiskGroup → Disk、物理 offset/length，以及协议
  提供的分配单位信息。拓扑解析失败保留 Disk ID 并标注 Unknown。
- 片段位置图采用有限可视窗口和按需详情；大量 Strip/fragment 不一次画成
  全量关系网。布局记录与物理位置必须标注同一查询代次或分别标注观测时间；
  正在转换/迁移时不能把两个版本拼成不存在的 Strip。
- fragment 可跳转 Capacity 的具体 Disk；存在可验证 Zone 映射时才定位
  Zone/Bitmap。Node/Server 跳转 Cluster。反向容量详情可链接到相关 Chunk
  查询，但服务未提供反查能力时不虚构“此 Disk 上全部 Chunk”。

```text
+-------------------+---------------------------------------+----------------+
| CHUNK             | [Capacity] [Chunk]                    | Strip seq 8    |
| Type / ID prefix  | Prefix [0a..] [Query] [ID lookup]     | logical range  |
| All               | Chunk ID  Type   State   Capacity     | layout/state   |
| WAL               | 0a...     ...    Sealed  ...          | physical spans |
| Tree / Index      +---------------------------------------+----------------+
| S3 / Iceberg      | Selected chunk: ID / state / totals   | Placement links|
| Other (raw type)  | seq 7  Mirror x2  [copy 0] [copy 1]   |                |
|                   | seq 8  EC 4+2     [D0][D1][D2][D3]    | Activity       |
|                   |                   [P0][P1]           |                |
+-------------------+---------------------------------------+----------------+

Chunk logical ranges -> stable Strip sequence -> actual fragment locations

Strip seq 7: Mirror x2
  copy 0 -> Rack A / Node 1 / DG 101 / Disk a -> offset, length
  copy 1 -> Rack B / Node 2 / DG 201 / Disk b -> offset, length

Strip seq 8: EC 4+2 (example only; profile determines allowed placement)
  D0 -> N1 / DG101 / Disk a       P0 -> N5 / DG501 / Disk e
  D1 -> N2 / DG201 / Disk b       P1 -> N6 / DG601 / Disk f
  D2 -> N3 / DG301 / Disk c
  D3 -> N4 / DG401 / Disk d
```

##### 6. Iceberg：Catalog、Namespace、Table 操作

- 左侧：当前可访问 Catalog → Namespace → Table。Header/操作栏显示当前
  Catalog 与权限；只有一个 Catalog 时直接进入，不伪造多 Catalog 能力。
- Namespace 支持 list/load/create/update properties/drop。Table 支持
  list/load/create/rename/drop，以及服务广告支持的结构化 metadata commit。
  请求沿用原生 Iceberg REST Catalog 语义，显示校验失败、冲突和未知结果。
- 选中 Table 中间分为 `Overview | Schema | Snapshots | Files`：Overview
  展示 UUID、location、format version、当前 snapshot；Schema 展示字段 ID、
  类型、required、partition/sort spec；Snapshots 展示父关系、时间和摘要；
  Files 展示该 Table 的受支持元数据引用与文件信息、授权下载。
- Files 必须有已支持的表关联/manifest 读取来源。不能把任意原生 FileIO
  全域 prefix listing 当作已实现能力；没有来源时显示明确的能力缺口。
- Create Table 用 Schema 和属性表单；变更 Schema/properties 使用结构化
  操作与明确前置版本，发生并发提交冲突后保留输入并要求重新确认。
  Drop 明确区分取消目录注册和服务支持的物理清理语义，不默认为立即释放
  全部底层空间。不能通过编辑 JSON 文件绕过 metadata commit。
- Table 内逐行查询、Insert/Update/Delete 不由 REST Catalog 自动提供。
  其 UI 设计与执行途径属于下面的 Open Questions，未决前不发布假的行 CRUD。
  已知 Chunk 引用可跳转 Chunk 详情，没有引用则不猜测对象到 Chunk 的映射。

```text
+-------------------+---------------------------------------+----------------+
| ICEBERG           | Catalog / Namespace / Table           | Table details  |
| Catalog           | [Create] [Rename] [Properties] [Drop] | UUID / head    |
|  analytics        +---------------------------------------+----------------+
|   events          | Overview | Schema | Snapshots | Files | Format / state |
|   users           | field ID / name / type / required     | Catalog links  |
|  staging          | snapshot ID / parent / time / summary | Chunk links    |
| [Add Namespace]   | selected item details / commit form   | Activity       |
+-------------------+---------------------------------------+----------------+
```

##### 7. S3：Bucket 与 Object CRUD

- 左侧：当前授权 S3 作用域 → Bucket → prefix。prefix 是 key 的虚拟分组，
  不是可独立删除的目录；键中的大小写、重复斜杠等按协议保留。
- 中间：Bucket selector、prefix/key 查询、对象分页列表、选中对象预览和
  操作区。列表标注 Key、size、ETag、last modified（服务提供时）。
- Bucket 支持 create/list/head/delete；非空 Bucket 的删除错误原样展示，
  不隐式递归删除。Object 支持 upload/replace、head/get/download、delete。
  Update 是替换对象内容，不能把 ETag 当作内容或可编辑属性。
- 小文本对象可有界预览 UTF-8/Hex 和编辑；超出预览上限或二进制文件使用
  文件上传/下载。大对象保持流式与取消语义，不在 Web 或浏览器内整体缓冲。
  Multipart 展示上传进度/状态；取消后报告是否仍有待处理上传，遵循既有
  abort/recovery 语义。失败/结果未知不会被表示为已保存。
- 首版不依赖 CopyObject、UploadPartCopy、批量 DeleteObjects、版本历史
  或 IAM 管理。只有相应服务能力落地后才增加入口。
- Object Inspector 显示可得的原生引用；通过授权管理查询才能跳转 Chunk，
  不把 S3 key prefix 与 Chunk ID prefix 混为一种分类。

```text
+-------------------+---------------------------------------+----------------+
| S3                | Bucket [datasets] Prefix [raw/]       | Object details |
| authorized scope  | [List] [Upload] [Create Bucket]       | full key       |
|  datasets         | Key       Size     ETag / modified    | size / ETag    |
|   raw/            | raw/a     ...      ...                | content type   |
|   output/         | raw/b     ...      ...                | Chunk links    |
|  logs             | [Load more]                           | Activity       |
|                   | Preview [UTF-8 / Hex]                 |                |
|                   | [Download] [Replace] [Delete Object]  |                |
+-------------------+---------------------------------------+----------------+
```

##### 8. 服务边界、凭据与能力缺口

- 浏览器统一通过 crowdb-web 的 API 前缀访问；KV、ChunkDB、DiskDB 查询由
  `crowdb-console-shared` 与各自 typed client 复用；Iceberg/S3 通过 Access
  Server 的真实协议操作，不直接修改其底层 KV/Chunk 元数据。
- Access endpoint 来自确认的服务发现或受校验的部署输入。Iceberg Catalog
  与 S3 Bucket 的作用域分别管理。切换 endpoint/作用域取消旧请求并清理
  不再授权的数据，不把 A 集群响应插入 B 集群页面。
- UI 登录/连接状态显示有效能力。S3 签名凭据和 Iceberg 的 read/write/
  management 角色按现有协议分离；management token 不自动拥有 writer
  权限。服务端持有的密钥不回传浏览器或写入 Group 0；客户端凭据输入与
  存储政策须遵守所选部署的认证契约。
- 后端提供可用能力和限制，前端不能只凭 domain 或 deployment 字符串猜测。
  `readonly`、Container 硬件只读、当前认证角色、协议/profile 的限制合取。
- 新增 UI 所需的 Chunk list/detail、placement lookup、Access proxy 和
  capability API 都属于本需求交付范围。现有接口不足时补 typed adapter
  或有界查询接口；不保留只能展示假数据的“已完成”面板。
- 本需求不改变 ChunkDB 的分区模型、Mirror/EC 算法、原生数据协议、GC
  或权限角色含义。既有能力缺口必须在 UI 明确表示。

Work items:

1. 在 `app/crowdb-web/src/main.rs`、`state.rs`、`standalone/` 和
   `mgmt/cluster_init.rs` 补全独立启动、临时配置、初始化与恢复契约；
   `config/web.rs` 与 Container managed router 保持显式部署输入边界。
2. 在 `ui/src/App.tsx`、`contexts/`、`shell/`、`views/` 实现五域框架、
   能力控制、逻辑 KV 左树、空状态和跨域选择；减少根组件的领域业务堆积。
3. 在 `lib/crowdb-console-shared/src/ops/` 与 Web 领域路由接通实际支持的
   Server 生命周期；共用启动/恢复输入与结果，不为 Container 另建业务实现。
4. 在 ChunkDB client、Web 新 Chunk 领域路由、`ui/src/views/ChunkView.tsx`
   和领域组件补齐 Chunk 查询、真实 prefix 分组、Strip 和 placement 展示。
5. 在 Web Access 领域适配层、`ui/src/api.ts`、新 Iceberg/S3 views 与领域
   组件实现所列协议操作、流式传输、凭据/作用域和能力错误处理。
6. 让 `ManagedPreview` 的模式分支进入共用 UI，复用 Group 0/monitor 投影，
   后端与前端共同落实 Container 的权限；更新 Container 的 Web 验收。
7. 维护领域类型、单元/集成/真实后端浏览器用例及永久 Console/UI 设计。
   实施顺序、具体文件拆分和接口签名留给 working plan。

#### Dependencies

- 入依赖：现有 Console、KV CRUD/成员管理、DiskDB 容量 API、ChunkDB
  ListChunks 与 chunk/strip records、Group 0 服务发现、原生 Iceberg REST
  Catalog 与 FileIO、基本 S3/multipart，以及 Container profile/monitor。
- 命名产物：[ChunkDB model](../design/chunkdb/design-crowdb-chunkdb.md)、
  [Iceberg contract](../design/access-server/iceberge/design-crowdb-iceberg.md)、
  [S3 contract](../design/access-server/s3/design-crowdb-access-s3.md)、
  `container/single-node-container/profile.toml`、`ui/e2e/`。
- R96 原为 ChunkDB Console/CLI 占位需求。本需求拥有完整 Web 界面，包括
  Chunk 子页；R96 保留 CLI 范围及与本需求复用的操作能力，避免重复实现。
- R202 的 ChunkDB partition 设计不阻塞只读浏览；当前路由查询必须服从
  已落地的 ownership，后续适配新分区接口，不能先假定一种未来分区布局。
- R193 不阻塞显示已有 Mirror/EC；UI 显示当前 profile 的真实限制，不开放
  尚未落地的 failure-budget 配置。
- R194 未完成前不承诺 Iceberg 全域 FileIO prefix listing；Files 页使用
  已支持的 Table 关联引用，缺少可分页来源时交付明确的能力状态。
- R198/R199 未完成不阻塞基本 S3 CRUD，不显示 copy/批量删除入口。
  R168/R169/R147 的物理回收尚未完成时，删除成功只表示协议承诺的逻辑
  删除，不能声称空间已回收。R189 的引擎集成未完成不能当作行 CRUD 基础。
- 出依赖：本需求形成共用 UI/capability/领域 API 契约，供后续访问协议功能
  和物理诊断扩展使用；不要求这些后续需求先落地。

#### Acceptance

- **A1 / 启动权威**：空 default 目录且无 Group 0 → 无配置启动 Web →
  显示 Empty、可 Add Rack/Node，没有自动部署，也不提示后端不可达。E2E test
- **A2 / 初始化前持久化**：添加 Rack、Node、部署 KV Server，未 Init →
  终止 Web 后重新启动 → 配置与端口/数据目录保留，活进程重新识别，停止的
  auto-start Server 恢复，不重复部署。Integration test
- **A3 / 原子配置**：已有可读配置，注入写失败/中断 → 写入并重启 →
  只能读取完整旧版或完整新版，错误被报告，损坏文件不被空配置覆盖。Integration test
- **A4 / 初始化可恢复**：可用 KV 节点，初始化中断于创建或发布阶段 →
  重启/重试 → 意图保留，成员身份不改变，全部配置确认写入 Group 0 后才
  宣告 Ready。Integration test
- **A5 / Group 0 权威恢复**：初始化并写入用户 KV，篡改本地拓扑缓存 →
  停止 Web/Server 再启动 → Server 与 Group 0 恢复，显示 Group 0 配置，
  原 KV 数据可读；不可达时不启用本地缓存写入。Integration test
- **A6 / 三栏与跨域选择**：多种实体已存在 → 树/图选择、切换域、跨域跳转 →
  Inspector 身份正确，目标展开，缺失目标明确提示，没有旧域实体残留。E2E test
- **A7 / Cluster 责任**：多 Node、多类型 Server → 选择并部署实际支持的
  Server、查看布局、Stop/Restart/Delete → 类型/状态/影响范围准确，KV Init
  跳转而非重复实现；不支持类型无可执行假入口。E2E test
- **A8 / KV 逻辑导航**：多 Store/Group/Replica，包含 Leader → 选择 Group
  和 Replica → 左侧为逻辑树，Group 显示 CRUD，Replica 显示所属 Group，
  可定位 Cluster Server；成员操作经过真实协议。E2E test
- **A9 / KV CRUD 与游标**：不同 Group 中有超过一页的相同 prefix 键与
  二进制值 → Scan/Load more/Get/Put/Delete → 游标按 Group 隔离、结果注明
  Group，UTF-8/Hex 正确，写后刷新；系统配置键不接受普通 KV 修改。E2E test
- **A10 / 局部服务失败**：一个 DiskDB 或 Access endpoint 不可达 → 查看各域 →
  失败范围和部分结果明确，其他域仍可用，空域与失败域不混淆。E2E test
- **A11 / Capacity 能力**：有 Disk/Zone/Bitmap → 逐级选择并执行获授权维护 →
  图与请求作用域一致；缺失实例标记 Partial；维护限制由后端执行。E2E test
- **A12 / Chunk prefix 与分页**：多 owner 中有多种类型、未知类型和大量 Chunk →
  类型筛选、任意合法 hex prefix、ID lookup、Load more → 返回有界真实记录，
  无 owner 遗漏伪装成完整结果，未知/不匹配类型标识明确，不展示虚构总数。Integration test
- **A13 / Strip 正确性**：一个 Chunk 含 Mirror/EC 与不连续 seq → 选择 Chunk/
  Strip → 按真实 logical range/稳定 seq 展示实际 copy/k+m 和编码状态，
  不混淆逻辑容量、已写量与物理占用。E2E test
- **A14 / Placement 一致性**：fragment 位于多个 Disk，期间发生布局版本变化，
  一个 Disk 拓扑缺失 → 刷新并跨域跳转 → 不拼接不同布局，缺失位置标记 Unknown，
  有据可查才定位 Zone，无反查接口不虚构反向结果。Integration test
- **A15 / 有界渲染**：Chunk 含大量 Strip/fragment → 滚动并选择详情 →
  仅渲染可视窗口和按需详情，选中 seq 稳定，不加载全量关系图。E2E test
- **A16 / Iceberg CRUD**：有有效 writer 和支持的格式 profile → 创建/改属性/
  删除 Namespace，创建/加载/改 Schema/rename/drop Table → 使用原生语义，
  field ID/head/snapshot 准确，冲突保留输入，删除清理语义明确。Integration test
- **A17 / Iceberg 能力边界**：Reader、manager、writer 及缺少 file-list 来源
  的 Table → 操作或查看 Files → 角色不互相继承，未支持 listing/行 CRUD 不
  伪装成功，metadata 更新不能走裸 JSON/file 覆盖。E2E test
- **A18 / S3 CRUD**：授权 scope 中有空/非空 Bucket、文本和二进制对象 →
  create/list/head/upload/get/replace/delete → 真实协议结果；非空 Bucket
  不隐式清空，prefix 不被当目录，特殊 key 原样保留。Integration test
- **A19 / 流式传输**：大对象与 multipart，上传中取消/断线 → 操作并恢复 →
  内存有界，已确认字节/状态准确，未完成和未知结果不表示为成功。Integration test
- **A20 / 凭据与作用域**：两个不同 endpoint/scope 和不同权限 → 查询未完成时
  切换 → 旧响应不进入新 scope，密钥不出现在响应/Group 0/活动日志中，
  所有越权请求后端拒绝。Integration test
- **A21 / Container 共用 UI**：启动单节点 Container → 遍历五个域、执行获授权
  用户数据 CRUD，直接请求物理修改/部署/Init/Reset → 共用 UI 可用，全部
  硬件与进程管理写入拒绝，逻辑/数据能力按角色/profile 生效。E2E test
- **A22 / 会话与写入反馈**：有失败、冲突、结果未知和成功操作 → 查看 Activity
  并刷新页面 → 每次结果准确，没有缓存伪造成功；会话记录不被宣称持久审计。E2E test
- **A23 / 独立 scope 与 demo**：各域分别选定作用域 → 切换域、刷新、执行
  demo 并清理 → 写入目标始终可见且准确，只清理对应示例资源，不修改系统
  元数据，不使用假 Chunk/Capacity 数据替代真实结果。E2E test

#### Delivery status — 2026-10-03

- 首版五域 UI、持久化启动恢复、真实 Chunk/Strip、Iceberg metadata CRUD、
  S3 CRUD/multipart 和 Container 共用 UI 已实现。
- 验证与剩余验收边界见 [implementation plan](../working/plan-console-complete-ui.md)。
  Docker 镜像验证受本机镜像源代理拒绝连接阻断；完整服务链已在隔离目录中验证。
- 本需求暂不关闭：保留下面需要用户决定的产品问题，以及 plan 中明确列出的
  故障注入、多 owner 和传输边界验收工作。

#### Open Questions

- **Iceberg 表内行操作**：首版只做 Namespace/Table metadata CRUD，还是同时
  支持样本行读取与写入？推荐先完成 metadata CRUD；行查询需要选定真实
  engine/client 执行途径，Insert/Update/Delete 还涉及文件生成、delete 语义
  和原子 commit。不得把此决策隐藏在 Catalog API 的实现里。
- **Iceberg/S3 到 Chunk 的业务引用**：首版只展示已经可查询的引用，还是新增
  授权的对象到 Chunk 诊断查询？前者范围小但部分对象无法跳转；后者需要
  明确业务身份、鉴权与分页，不能暴露全域内部引用。Chunk 独立浏览不受阻。

Implementation verification commands (record results when implemented; this
documentation change does not run the implementation suites):

```sh
pixi run test-console-server
pixi run test-console-shared
pixi run test-console-ui
pixi run test-chunkdb-client
pixi run test-chunkdb
pixi run test-access-server
pixi run test-access-iceberg
pixi run test-access-s3
pixi run test-single-node-container
pixi run ts-lint
pixi run rs-fmt-check
pixi run rs-lint
```
