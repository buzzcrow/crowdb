<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R235: console — Cluster Hardware Workspace and OCI Disk Lifecycle

**Problem**:

- Cluster currently handles nodes and services while Capacity separately handles
  DiskGroups, disks, zones, and allocation inspection. Operators must leave node
  setup to prepare storage and return to inspect dependent service readiness.
- The single-container profile creates four 16 GiB sparse files. Monitor restart
  checks prevent truncation and recreation of completed missing files, but this
  does not define cluster admission for supplied files and raw devices, candidate
  inventory, deliberate removal, or the complete UI interaction.
- Docker anonymous-volume defaults and global unused-volume cleanup do not give
  operators a reliable node data lifecycle across container replacement.
- Node admission and initial voting-member selection are currently manual.
  Add an enabled-by-default Auto join policy and one shared Store-0/Group-0
  initialization lifecycle: three initial voters in standard mode, one in the
  explicit single-node mode, without automatic later voting expansion.
- Root designs: [Console](../design/console/design-crowdb-console.md),
  [Console UI](../design/console/design-crowdb-console-ui.md),
  [OCI deployment](../design/deploy/design-crowdb-oci-image.md),
  [DiskIO](../design/diskio/design-crowdb-diskio.md), and
  [DiskDB](../design/diskdb/design-crowdb-diskdb.md).

**Solution**:

1. **One runtime-independent storage contract.** Update `container/oci-image`,
   `container/single-node-container`, and `container/crowdb-monitor` to accept
   multiple file or block-device inputs, including mixed inputs, beneath
   `/opt/crowdb/data/disks/`. Mount the persistent node root at
   `/opt/crowdb/data`; map external files individually and raw devices as device
   nodes at distinct child paths. A device node is not a filesystem mount and
   must not be formatted with a host filesystem by CROWDB.
   - Docker and containerd/nerdctl use the same image and application contract.
     Deployment helpers translate host sources to runtime mounts/device access;
     CROWDB services do not require a runtime socket or privileged container.
   - Require an explicit persistent bind directory or named volume for managed
     cluster startup. Do not depend on automatic anonymous-volume creation.
     Preserve data when stopping, replacing, or removing a container. Cleanup
     never prunes unrelated or retained volumes; deleting node data is explicit.
   - Deployment validates host mount availability, UID/GID access, and device
     permission enforcement before launch. Raw-device support initially targets
     verified Linux runtime configurations; unsupported rootless access fails
     preflight without an implicit privileged fallback.
   - The external mount/device declaration is retained outside the container so
     replacement can reproduce it. A matching target path alone is not proof of
     a matching medium. Reject unexpectedly absent external mappings, including
     a regular placeholder file where a mapped device/file was expected.
   - First release applies new external mappings by controlled container
     replacement, retaining node identity. Creating a managed file needs no
     mount change. Live device hotplug is outside this requirement.
   - OCI specifies mounts and device access, not CROWDB admission policy:
     [runtime mounts](https://github.com/opencontainers/runtime-spec/blob/main/config.md),
     [Linux devices](https://github.com/opencontainers/runtime-spec/blob/main/config-linux.md),
     [nerdctl options](https://github.com/containerd/nerdctl/blob/main/docs/command-reference.md).

2. **Discover candidates and retain authoritative identity.** Add node-local
   inventory to `container/crowdb-monitor`, expose it through
   `lib/crowdb-console-shared` and `app/crowdb-web`, and reconcile admitted disks
   with Group-0 hardware records and `app/crowdb-diskio`.
   - Scan direct children only; reject symlinks, directories, sockets and path
     escapes. An arbitrary file is not automatically a blank disk. Externally
     supplied files must be explicit inputs with valid size/geometry and must
     pass identity/use checks. Check duplicate underlying resources, not only
     duplicate paths. Never infer emptiness from absent recognized signatures.
   - Keep backend (`file` or `block-device`) separate from existing media type.
     Preserve existing protocol meanings; do not reinterpret persisted values.
     New backend/media fields require explicit schema/version handling.
   - Report path, backend, media classification (including unknown), capacity,
     durable disk identity when present, admission state, eligibility reason,
     backing filesystem/device identity, and observation time. DiskIO remains
     authoritative for whether a registered disk is actually accessible.
   - Group 0 is the authority for disk ID, placement, backend/path and geometry.
     No new on-medium identity header or disk-format migration is required.
     Before Group 0 is quorum-ready, discovery and file preparation are only
     provisional inventory: no effective disk admission or allocatable capacity.
     After readiness, register through Group 0 and verify DiskIO availability.
   - Validate expected runtime mappings, type, capacity and available backing
     identity against Group-0 records. Without an on-medium signature, do not
     promise detection of every same-size replacement or ownership discovery from
     arbitrary bytes. Preserve existing formats and use supported recovery paths.
   - Block duplicate resources, in-use devices and recognized foreign signatures.
     Absent signatures do not prove emptiness. Supplied storage requires explicit
     provisioning authorization; unknown contents are never silently imported.
     Cross-node moves, destructive wipe/import and format migration are excluded.
   - Multiple files/partitions on one medium do not establish independent failure
     domains. Preserve node-level protection; report shared backing storage and
     treat unknown physical independence conservatively.

3. **Simple defaults and one node-admission submission.** Extend the existing
   node admission/deployment flow in `lib/crowdb-console-shared` and
   `app/crowdb-web` rather than add a second node wizard.
   - The manual dialog remains available; item 8 adds automatic node admission
     through the same durable validation and execution path. Auto join is a node
     policy, not automatic assignment of disks to the first DiskGroup.
   - The Add Node dialog has Node, Services, and Storage sections in one flow.
     Storage lists discovered inputs and defaults eligible new disks to selected.
     One final submission admits the node and persists the storage/service plan.
     The summary explicitly identifies each medium to be initialized; raw-device
     initialization needs an acknowledgement in this same confirmation, not an
     additional wizard. Unknown-content media is not eligible for this action.
   - A fresh managed cluster node with no supplied disks proposes one 16 GiB
     managed file, preallocated by default, with editable capacity. No default
     file is added when explicit inputs exist, even if all inputs are invalid.
     The operator may deselect storage and join a diskless node.
   - Keep the explicit `single` development profile's existing four 16 GiB sparse
     disks and automatic admission. Its UI identifies sparse allocation and its
     lack of production protection; do not modify an existing profile on restart.
   - File creation supports explicit sparse allocation as an advanced choice.
     Preallocation failure never silently falls back to sparse. Validate geometry,
     allocation bounds, and host space before creation. Default file geometry uses
     the current supported zone/allocation constraints, not user-invented offsets.
   - New disks default to one node-local DiskGroup for each configured media
     class (unknown is its own class). Backend alone does not split a group.
     Reuse a compatible group; expose an optional group selector. Existing disks
     retain their group. Authoritative ownership and metadata binding remain
     separate and must satisfy existing provisioning prerequisites.
   - Persist node identity and intent once. Pending Group-0 or service prerequisites
     produce visible waiting states, not duplicate nodes or fake Online disks.
     Show per-disk results; partial success preserves successful resources.
   - Restart recovers admitted disks without confirmation. Missing disks remain
     admitted and unhealthy, never replaced by fallback files. Newly mapped disks
     after node admission appear as candidates and support bulk Join. Creating a
     file through the UI includes admission in the same accepted operation.

4. **UI triggers initialization; monitor executes it.** Reuse the existing
   node/monitor bootstrap lifecycle, rather than introduce a generic task engine.
   - UI or automatic startup submits the intent; monitor durably accepts it before
     returning operation identity/status. Closing the UI, navigation, disconnect
     or disabling Auto join does not cancel accepted initialization.
   - Monitor executes independently and exposes phase/member/error observations.
     Existing manifest and bootstrap acceptance govern retries/restarts. Same-intent
     triggers reconcile; conflicting intents are rejected. No new leader lease,
     automatic initiator takeover or distributed scheduler is required.
   - An unreachable/failed initiator shows its condition and may need explicit
     retry. Storage CRUD reports committed/failed/unknown and reconciles Group 0
     before retry; partial failure never truncates an unrelated existing file.
   - Disk removal is an administrative Offline-then-Remove flow. Commit Offline
     through Group 0; apply existing status exclusion so the disk cannot receive
     new allocations/data operations. Remove is unavailable for an Online disk.
   - Before membership removal, confirm owner/DiskIO have applied exclusion and
     outstanding I/O is quiescent. If not observable, show the unmet condition;
     heartbeat loss alone is not proof. Do not require evacuation, zero usage or
     completed automatic recovery. Occupied disks can be removed explicitly.
   - Removal retains disk identity, original placement/path/geometry, allocation
     metadata and bytes needed to interpret surviving references. Persist a removed
     marker in Group 0 and exclude membership without freeing referenced blocks,
     recycling IDs or rewriting chunk layouts as if migration had occurred.
   - The removed resource moves to Candidates with reason Removed. Discovery,
     restart and Auto join do not readmit it automatically. Missing registered
     disks stay in their original group. Errors from unavailable referenced data
     remain visible; R235 does not promise restored protection or recovery.
   - Explicit Rejoin of the original resource at its recorded placement restores
     Offline membership with the same ID and retained allocation state. Only
     validated existing recovery/open checks permit Online; invalid retained state
     stays blocked. Never treat a removed occupied disk as a blank new disk.
     A merely Offline registered disk can return Online after the same checks.
   - Remove confirmation states that no automatic migration/recovery occurs,
     data may become unavailable, and the medium is retained. Do not label this
     Safe removal. No physical Delete File action in this iteration: removal is
     not proof of an unreferenced file. Host destruction is outside the UI flow.
     Node/DiskGroup cascade deletion must not bypass these disk preconditions.

5. **Merge Capacity into Cluster navigation.** Implement in
   `app/crowdb-web/ui` with matching `app/crowdb-web` routes.
   - Remove the top-level Capacity tab. Existing Capacity deep links resolve to
     the corresponding Cluster scope and selected disk/zone; KV, Chunk, Chunk-KV
     and Access links use the same canonical selection.
   - Left tree: Datacenter / Rack / Node, then Services and Storage. Storage
     contains DiskGroups/disks and a Candidates entry with count. Stop at Disk;
     Zones and allocation blocks belong in the center workspace.
   - Left navigation defaults to 240 px (resizable 200-360 px), right properties
     to 320 px (280-480 px). Center fills remaining space with a 480 px usable
     minimum. Below 1100 px workspace width, properties becomes a dismissible
     drawer; navigation can collapse. Reuse existing theme and component tokens.
   - The center is the selected scope's work area; the right panel is the selected
     object's properties and operation details. Clicking a row/card selects it
     for the right panel without replacing the center. Clicking its name or Open
     enters its scope and synchronizes the tree and breadcrumb.
   - Back/breadcrumb restores scoped filters, pagination and scroll. Keep bounded
     navigation state. Selection uses durable resource IDs, never row indexes.
   - Cluster/Rack/Node offer Topology and Capacity views with shared selection.
     DiskGroup and Disk use their dedicated workspace without redundant toggles.
     Node/Storage and Candidates tree entries open the Node storage section and
     candidate filter respectively. Services opens the Node service section.

6. **Define the center panel and dialogs.** Preserve existing service lifecycle
   controls and disk diagnostics while consolidating the hardware workflow.
   - Cluster/Rack Topology: grouped node cards showing health, service readiness,
     observed used/total capacity, candidate count and alerts. Do not draw every
     disk/zone in the global topology. Add Node is scoped to the selected Rack.
   - Node Topology: node header/breadcrumb and node menu; service status cards and
     deployment progress first; Storage section second, with Create File Disk
     and Refresh Discovery. Show admitted disks grouped by DiskGroup, followed
     by Candidates. Disk rows show name, backend/media, capacity, used percentage,
     health/operation state and overflow actions. No mandatory dragging.
   - Capacity view: at Cluster/Rack scope show sortable, filterable node rows;
     at Node scope show disk rows grouped by DiskGroup. Include used, free, total,
     observation freshness and exceptions. Row actions and selection match the
     topology workspace; do not create a separate mutation implementation.
   - DiskGroup: summary, owner and metadata-binding status, then disk rows and
     Join Disks action. Physical location never implies DiskDB ownership.
   - Disk: summary and operation banner, then 32-zone pages. Selecting a Zone
     keeps its disk and list visible and opens the bitmap below; right properties
     show the selected zone/block's exact offsets and allocation units. Preserve
     bounded bitmap rendering and existing scoped maintenance operations.
   - Right panel: identity, physical location, container path, backend/media,
     ownership/binding, geometry, observation time, eligibility/error explanation,
     and current task details. Avoid duplicating primary center action buttons.
   - Create File Disk dialog: generated name/path, capacity, group, advanced
     sparse choice, host-space observation and creation/admission summary. The
     pending row appears immediately after durable acceptance; failure remains
     visible with Retry, and cannot be presented as usable capacity.
   - Join Candidates dialog: multiselect eligible rows, group assignment, exact
     initialization/recovery summary, one submit and per-row outcomes. Disabled
     rows show why. Removed candidates require explicit selection.
   - Offline dialog identifies the disk and explains stopping its participation.
     Remove is enabled only for authoritative Offline with applied exclusion;
     its confirmation explains unavailable data, deferred recovery and retained
     bytes. Keep the row registered until confirmed removal, then show Removed
     in Candidates. Candidate actions are Join for new resources and Rejoin for
     retained removed resources. No physical Delete File or evacuation progress.
   - Before Group 0 is ready, center Storage shows provisional inventory with a
     Waiting for Group 0 banner; effective disk mutations are disabled. Center
     header holds breadcrumb, scope actions and view toggle. Initial pending-node
     list shows UUID order and selected initiator. Offline and Missing rows stay
     visible in the registered section. Right properties explain observed versus
     authoritative state and errors without duplicating center action controls.
   - Define loading, empty, candidates-only, stale, unavailable, missing-disk,
     failed, Offline and unknown-outcome states separately. Preserve last-known rows after refresh
     errors. Keyboard selection, focus restoration and labeled controls must
     provide the same actions as pointer interaction; status is not color-only.

7. **Bound observations and expose honest capacity.** Reuse shared disk inventory
   and service projections, with server-side operations in `app/crowdb-web` and
   existing DiskDB usage reporting.
   - Fetch only opened inventory branches; use at most four workers with duplicate
     request coalescing. Do not fetch runtime usage, scan all disks or load bitmaps
     merely because Cluster opens. Root/Rack Capacity shows cached summaries
     with time until explicit Refresh requests aggregate runtime observations;
     opened DiskGroup/Disk scopes request their corresponding runtime data.
   - Distinguish logical capacity/allocated/free, file physical allocation, and
     backing filesystem available space. Deduplicate filesystem observations and
     never sum its shared available space once per file. File creation preflight
     cannot guarantee future space against other host writers; ENOSPC must remain
     a visible I/O error with no false write acknowledgement. Maintain a
     configurable host-space reserve; default 1 GiB for new file creation.
   - Unknown backing topology/usage remains unknown, not zero/free. Preallocated
     and sparse files are labeled. File virtual capacity is not reserved hardware
     capacity. Full filesystem recovery requires refreshed observation and actual
     successful I/O, not an automatic reset of disk identity.
   - Discovery/status reads are non-destructive. Mutation results return durable
     operation identity and per-disk status; candidate admission eligibility is
     rechecked server-side. Browser mode controls are not authorization.
   - Manual OCI cluster mode permits the authorized hardware operations above.
     Explicit readonly configuration and the automatic single preview retain
     their restrictions. Replace the blanket Docker physical-mutation ban with
     mode/capability checks in both UI and API. Document these intentional changes
     in the Console UI and OCI/container guides during implementation.

8. **Auto join nodes with mode-selected initial membership.** Extend deployment
   admission/bootstrap in `lib/crowdb-console-shared`, the monitor's node runtime,
   and the Cluster UI. Reuse `bootstrap_intent` and existing system-bootstrap
   acceptance in the KV server; do not create a parallel bootstrap protocol.
   - Cluster header exposes `Auto join`, checked by default for a new manual-mode
     deployment workspace. Persist the setting and show its target cluster or
     initial bootstrap draft. Standard mode defaults to three initial voters.
     Explicit `CROWDB_STARTUP_MODE=single` selects the special single-node mode:
     one local eligible node auto-starts initialization of Store 0 and Group 0,
     independently of the standard Auto join toggle. It does not collect remote
     candidates for initial membership.
   - Both modes use one initialization state machine: determine required initial
     members, persist the immutable intent, prepare and accept that same manifest
     on every selected participant, initialize Store 0 / Group 0, wait for quorum,
     and publish initial metadata. Mode selects initial voter count and admission
     policy, not a separate storage/bootstrap implementation. Persist the mode
     and selected membership with the accepted intent; a single standard-mode
     node never implicitly enables single mode.
   - Restart resumes that mode/intent and existing Store-0/Group-0 state. A
     conflicting startup mode is rejected without rewriting membership. Existing
     accepted single-profile data must have a validated compatibility path into
     this shared lifecycle without reinitialization. Later conversion or voting
     changes use explicit supported reconfiguration, not changing an environment
     variable. Existing profile storage/read-only choices remain independent of
     the shared bootstrap state machine.
   - Here the first group means the initial Group-0 voting membership, not a
     DiskGroup or three independent Paxos groups. Auto join defaults to the first
     configured target in this workspace and persists that target identity; it
     never means the first unauthenticated LAN advertisement. If multiple foreign
     clusters are visible without a configured target, show Choose target and
     wait for selection rather than arbitrarily joining or merging a cluster.
   - Eligible compatible unbound nodes with authenticated management access and
     accepted admission policy automatically enter the target's pending node list.
     Preserve rack assignment where configured; otherwise use the target's default
     rack. Pending nodes before Group 0 exists are visibly uncommitted, not yet
     authoritative cluster members. Missing credentials/trust show Needs access;
     Auto join does not bypass the existing handshake or acquire credentials.
   - In standard mode, fewer than three compatible authenticated eligible nodes
     show Waiting (N/3). Within the configured target, order eligible unbound
     reachable nodes by UUID. The smallest UUID node initiates using the first
     three in that order; other nodes observe preparation rather than deliberately
     submit their own bootstrap. Freeze exactly three identities in the manifest.
   - This is discovery-based deterministic selection, not a new distributed
     election or global uniqueness guarantee. Divergent discovery can still race;
     use existing durable acceptance/conflict handling. All selected participants
     accept the same manifest before replicas become election-eligible. Expose
     conflicts/partial acceptance for existing cleanup/retry, without automatic
     repair, takeover, leases or replacement bootstrap after timeout.
   - Before acceptance eligibility can be recomputed; after acceptance retain the
     same operation and members. Four or more candidates still select three voters;
     extras wait for quorum-backed readiness/publication before nonvoting admission.
   - Monitor owns accepted initialization independently of UI. Repeated triggers
     and restarts reuse existing bootstrap records; unreachable initiators require
     visible recovery/retry, not a new background scheduler. Disjoint accepted
     groups remain separate clusters under the existing conflict model.
   - Once established, Group 0 stays at its mode-selected configured voters
     (three for standard, one for single). Subsequent
     automatic node admission changes neither voting membership nor quorum.
     A voter going offline does not free an automatic membership slot. Quorum
     loss waits for recovery, never creates another Group 0. Existing clusters
     with manually configured membership are preserved; Auto join does not resize
     them to three. Explicit existing reconfiguration remains an operator action.
   - Turning Auto join off stops scheduling new automatic admissions/bootstrap;
     pending unaccepted nodes stay candidates. Already accepted work reconciles
     to completion or a visible blocked state; the switch is not cancellation,
     disk removal or voter eviction. Turning it on reevaluates eligible candidates
     without duplicating bindings or operations.
   - Center Cluster panel shows explicit mode, target, policy, pending node list,
     waiting count N/required, Preparing, Initializing Store 0 / Group 0,
     Publishing, Ready (actual voters), Needs access, or
     Conflict with per-node progress. Right details expose accepted operation,
     selected voters and conflict reason. After readiness, new-node progress says
     Joining cluster (nonvoter), not Expanding Group 0.
   - Automatic node admission resumes the same selected/default service and safe
     disk plan as manual admission. Disk discovery remains non-destructive:
     previously authorized initialization can proceed, but a required raw-device
     acknowledgement remains Needs storage confirmation. Group-0 preparation
     does not wait for data-disk readiness; dependent storage services do.
   - This intentionally replaces the deployment guide's rule that every node
     admission and initial voter selection must be manually submitted. Explicit
     target/trust, foreign-cluster isolation and bootstrap-conflict safeguards
     remain. Update that guide during implementation together with the UI rules.

**Implementation decisions for later review**:

- The user reiterated on 2026-10-15 that implementation should continue without
  waiting for conflict confirmation. Prefer an option supported by existing
  authority and ownership rules; record any deferred conflicting scope here.
- Online completion records successful retained-medium validation and the Group-0
  Up transition. It does not promise that every DiskIO watcher has already opened
  its descriptor. Display current service health separately; acceptance observes
  the actual descriptor before testing its exclusive claim and retained data.
  Do not reinterpret an absent descriptor as successful I/O or add blanket read retries.
- The registered Node row uses observed durable provisioning intent for media
  class. If that bounded history has not been observed, show Unknown rather than
  infer HDD from legacy geometry defaults; Properties can fetch its exact receipt.
- In managed standard-node mode, legacy disk-create endpoints remain available
  for metadata-only entries with no device path. Physical paths require the
  owner-verified Create File Disk or Join Candidates flow. Retain existing
  standalone/legacy deployment contracts until their explicit migration.
- Standard-node menus and disk detail route lifecycle operations to Node storage.
  Retain diagnostic maintenance controls, but replace legacy direct Up/Down/Delete
  controls. Group creation menus use the same verified file provisioning dialog.
  Empty-group deletion remains separate and cannot cascade through physical disks.
- Short-lived SSH control requests use a current-thread runtime, independent of
  long-running service worker settings. They must not create one host-sized
  thread pool per status observation. Preserve the Node PID/resource budget;
  do not raise it to mask control-query thread pressure.
- Explicit single mode retains its fixed four-file development profile; supplied
  external media require standard/manual mode. Reject that combination before
  launching. The launcher does not write an empty external-mapping receipt into
  a pristine single root, because it would race bootstrap's clean-root check.
  Do not relax that check or delete an existing unknown root entry automatically.
- A pristine single-mode data root is initialized by its own bootstrap. External
  supplied-media mappings are a standard-node flow; do not precreate arbitrary
  entries in an uninitialized single root to emulate standard admission.
- The user authorized choosing a well-supported option or skipping a disputed
  part without waiting for confirmation. Record unresolved consequences here;
  skipped acceptance criteria remain incomplete. Retain this requirement and
  its decisions after implementation until the user has reviewed them; do not
  delete this review record as routine requirement cleanup.
- Allocate numeric DiskGroup IDs from Group 0 and retain a global placement
  reservation. Existing allocation/service interfaces address groups by numeric
  ID, so independent per-node counters would create ambiguous ownership.
- Accepted node setup uses the target monitor's durable intent and existing
  service desired-state reconciliation. A later explicit Stop/Restart/Delete
  takes precedence over setup retries; the browser only submits and observes.
- Raw-device physical capacity and usable allocation geometry are distinct.
  Preserve any unusable tail; do not resize a supplied medium to match zones.
- Default storage reuses a compatible group with an existing ordinary metadata
  binding. Otherwise choose the lowest existing ordinary store/group and retain
  that choice before publication. Wait when no such group exists; do not create
  consensus groups or change an operator's existing unbound placement.
- Explicit cluster cleanup retires accepted setup and drains its current pass
  before deleting system metadata. If a pass is still active, report pending
  cleanup and retain state for the existing explicit cleanup retry.
- SSH/storage preflight only reads authenticated identity and inventory. It does
  not install peer keys or grant new access that could trigger another Console's
  Auto join. The browser keeps an initial credential only in memory until final
  submission/cancel; no password enters persistent preflight or plan records.
  A pending local review is excluded from that Console's automatic admission.
- OCI node admission owns the KV bootstrap listeners (10000/10100). Setup cannot
  silently accept other KV port overrides. Optional service choices retain
  existing deployments; unselecting KV is not a request to stop the admitted KV.

- Managed-service startup has a durable five-attempt budget for consecutive failed startup/readiness attempts per service
  operation. Successful readiness clears that failure budget. Monitor restart does not reset it.
  Exhaustion is a visible Failed setup step with the process/readiness error;
  a later explicit service command owns a new operation and can recover after
  the operator fixes prerequisites. Replaying node setup alone cannot erase the
  service failure budget or override a later operator command.

- Cluster cleanup sends the already confirmed bootstrap command to at most four
  retained nodes concurrently and saves each result as it arrives. Unreachable
  nodes remain pending; their timeout does not delay dispatch to all other nodes.
  Completion still requires every retained member, and retry keeps the same
  identity and explicit deletion confirmation.

- Access deployment on multiple nodes shares one Group-0-reserved pair of
  Console catalog Initialize/Activate operation IDs. Concurrent nodes replay
  those same commands. Existing local pending identities may seed the reservation;
  conflicting local journals remain visible and are never rewritten or adopted
  by guessing. Existing ready operator catalogs retain identity and policy.

- Concurrent Access bootstrap reuses one user-scoped encrypted credential
  preparation in Group 0 before publishing its access-key record. Response loss
  and another node's retry reuse the same candidate; publication is conditional
  and cannot replace an existing credential. Existing duplicate/disabled users
  remain visible failures for later operator review, without guessed deletion or
  credential rotation. Secret generation and encryption keep their existing format.
  Credential enumeration is bounded at 4096 records; a truncated/timed-out result
  fails explicitly rather than authorizing new issuance or a partial cache.

- New external-device admission requires launcher-verified physical resource
  lineage, including a loop device's backing-file identity. Unknown stacked or
  ambiguous aliases remain ineligible; an initialization checkbox cannot supply
  missing identity evidence. Exact opened device/inode identity and canonical
  physical resource identity serve different checks. This provisioning work is
  still pending and is not counted as accepted raw-device support yet.
- Verified resources use a sticky Group-0 reservation keyed by physical host and
  canonical resource identity, shared across Rack mutation domains and backend
  types. Interrupted admission and Remove retain that reservation; replay cannot
  change its disk placement, operation, opened identity or physical capacity.
  Unknown legacy raw lineage, unknown legacy host identity, or physical membership
  without a backing receipt requires explicit migration. Do not guess that an
  apparently different path is safe. Legacy version-1 bootstrap remains a trusted
  compatibility interface; new verified provisioning must use version 2.
- Initial external raw support accepts whole physical devices and whole-file
  loop devices. Reject partitions, devices containing partitions, stacked devices,
  unknown virtual-device lineage, and loop offset/size slices. This deliberately
  favors verifiable non-overlap over broader device coverage; additional device
  kinds need a canonical lineage contract before admission is enabled.
- Supplied-medium provisioning reads every usable byte before publishing Init,
  retaining its exact descriptor (and exclusive raw-device claim) through that
  publication. DiskIO then checks the usable bytes through its own descriptor
  before Online. Unusable physical tails remain untouched. Interrupted partial
  scans restart at byte zero; a failed blank check never wipes or publishes
  membership and retains its original reservation for an explicit retry.
  Already-Up replay validates the retained receipt without treating user data as
  a new blank medium. Missing live DiskIO ownership is visible waiting; malformed
  ownership, changed identity and nonzero usable bytes are failures.
- Node setup seals each selected medium's canonical observation, disk operation
  and media-class placement before dispatch. Different classes cannot claim the
  same initially empty group. A failed disk preserves successful siblings;
  explicit retry consumes one retry authorization for that original operation.
  Earlier accepted supplied plans without sealed lineage require explicit review
  or migration rather than silently taking a new observation as the old medium.
- Raw-medium submission must echo the canonical resource observed during review;
  changed loop backing is a refresh/review conflict. Legacy file selections may
  omit that field only when their exact opened file identity supplies the same
  canonical identity. Newly prepared managed files publish version-2 receipts and
  participate in global reservations. Existing version-1 file receipts replay
  unchanged; this is not an automatic metadata or disk-format migration.

- Cluster/Rack defaults to node cards, with an explicit Diagram switch retaining
  existing topology operations. Capacity at those scopes is a sortable/filterable
  node table. Clicking a card/row inspects properties; its name opens that scope.
  Add Node reuses authenticated candidate admission and preselects the selected
  Rack. Existing accepted/preflight Rack choices take precedence over that default.
- Root/Rack observations do not fan out to runtime disk queries. The overview may
  reuse the most recent scoped usage observation, but a node total is known only
  when that observation covers every registered disk in that node. Other values
  remain Unknown; no accumulated partial reports are presented as one complete
  snapshot. An explicit Capacity Refresh may collect a runtime snapshot for that
  selected scope; ordinary overview polling does not repeat that fan-out. Existing
  scanner actions remain explicit operator commands. Candidate/setup summaries are bounded to 64 observed nodes, explicitly
  last observed, and do not cause background node-by-node polling. Current service
  health remains separate from a completed setup receipt.

- Manual Join on an existing node chooses an existing DiskGroup explicitly per
  medium. It does not allocate/reassign groups on HTTP retry. Initial node setup
  retains its separate sealed automatic per-class placement. Join accepts at most
  16 selected media, submits at most four requests concurrently, and requires
  general blank-media consent plus a separate acknowledgement for each raw device.
  Closing the dialog stops unsent requests; each already accepted disk continues
  independently on its monitor. Successful siblings remain accepted when another
  submission fails. A lost reply reuses the same operation and exact inputs.
- Candidate operation history is paged (at most 16 records and 56 KiB per private
  reply). The Node workspace keeps at most 64 active/terminal observations and
  identifies incomplete candidate coverage. Reopening reads the durable journals;
  missing and unreadable journals are distinct outcomes. Physical resource/host,
  physical capacity, usable geometry and unused tail are separate properties.
- Concurrent first disks of different media classes targeting the same empty
  group may race. Validate compatibility before reserving the physical resource
  and again under the existing publication fence. The loser remains a visible
  failure with its original intent; do not silently relocate it or add a new lock.
  Choosing another placement after such an accepted conflict requires later
  explicit operator review. No automatic recovery or resource-release policy is
  introduced here.

- Compatible-group validation must observe backing receipts and membership at
  one stable Rack revision. Concurrent publication cannot reinterpret a new
  Unknown-class disk as legacy HDD. A typed local DiskNotFound during initial
  handoff is waiting only while Group 0 retains that exact Init intent; ordinary
  reads retain their nonretryable missing-disk behavior. Real I/O errors are not
  reclassified as readiness. External files with fully allocated observed bytes
  are not described as proven managed preallocation.

**Dependencies**:

- Reuse current durable Group-0 bootstrap exclusion, immutable intent, monitor
  acceptance, node admission, hardware records and DiskIO status propagation.
- R80 rebalance and R83/R84 recovery are not prerequisites. This requirement does
  not promise evacuation, restored redundancy or automatic recovery. Preserve
  metadata for later recovery work; no silent reference or data destruction.
- No new disk format, distributed scheduler, automatic initiator failover,
  physical file deletion or device hotplug. R102 binding/physical moves excluded.
- R234 is reserved on another branch; do not assume its behavior. Reconcile
  overlaps when available. Implementation updates the permanent UI/deployment
  docs, retaining existing conflict protections and explicit mode capabilities.

**Acceptance**:

- A01: Given the same image on Docker/nerdctl with files and disposable loop-backed
  devices -> start/register/write/read/recreate -> preserve bytes and Group-0
  identity; missing mounts/permissions fail without substitutes. Invariant:
  persistent runtime-independent storage. E2E test.
- A02: Given new default, supplied-input, diskless and single profiles -> start ->
  produce the specified file plan; invalid inputs do not trigger fallback and
  failed preallocation never becomes sparse. Invariant: deterministic defaults.
  Integration test.
- A03: Given no ready Group 0 -> discover/prepare files and request admission ->
  show provisional inventory and reject effective mutation; establish Group 0 ->
  register accepted plans and validate DiskIO. Invariant: Group-0 authority.
  E2E test.
- A04: Given duplicate resources, invalid paths/types/sizes or recognized in-use/
  foreign media -> join -> reject without changing bytes; existing supported disks
  recover without a new header. Invariant: authoritative identity and format
  preservation. Integration test.
- A05: Given a missing registered disk, new external disk and removed disk ->
  restart/refresh -> respectively show Missing, new candidate and Removed candidate;
  create no substitutes. Create File Disk after Group-0 readiness -> create and
  register. Invariant: discovery is not replacement. E2E test.
- A06: Given three or more eligible nodes with consistent discovery -> enable
  Auto join -> smallest UUID initiates with first three ordered UUIDs; with fewer
  than three wait. Invariant: deterministic bounded bootstrap. Integration test.
- A07: Given divergent views, competing manifests and partial acceptance -> retry/
  restart -> retain accepted intent and enforce existing conflict protection;
  never bypass all-member acceptance or silently take over. Invariant: automatic
  selection preserves bootstrap safety. Integration test.
- A08: Given explicit single mode -> auto start with one local node -> shared
  Store-0/Group-0 initialization with one voter; one standard node still waits.
  Restart/contradict mode -> resume accepted state or reject, without recreating
  data. Invariant: explicit mode chooses initial count. E2E test.
- A09: Given ready Group 0 -> admit additional nodes or lose a voter -> do not
  automatically expand/replace voters. Disable Auto join -> stop new triggers,
  retain accepted work. Missing authentication or ambiguous target waits visibly.
  Invariant: admission is scoped and does not reconfigure quorum. Integration test.
- A10: Given accepted init -> close browser/disconnect/toggle Auto join -> monitor
  continues; restart/retrigger -> reconcile original intent. Invariant: UI triggers
  but does not execute initialization. E2E test.
- A11: Given Online disk -> Remove -> reject; Offline -> apply exclusion/quiesce
  I/O, then Remove -> exclude membership and retain bytes, identity and allocation
  metadata, including occupied disks. Invariant: removal is not evacuation or
  destruction. Integration test.
- A12: Given unreachable owner, delayed status or unknown result -> Offline/Remove
  -> show unmet condition/reconcile Group 0 before success; parent cascade cannot
  bypass checks. Invariant: authoritative exclusion precedes removal.
  Integration test.
- A13: Given Removed candidate -> explicitly Rejoin -> restore original ID and
  allocations Offline, validate before Online; incompatible state stays blocked.
  Access data with a missing disk -> truthful errors without claiming recovery;
  UI offers no physical Delete File. Invariant: removal preserves recovery inputs.
  E2E test.
- A14: Given all scopes and old Capacity links -> navigate/select/toggle/return
  at wide and narrow widths -> correct center/properties/drawer, canonical selection,
  retained context and no separate Capacity tab. Invariant: unified workspace.
  E2E test.
- A15: Given waiting init, candidates, Offline/Missing disks and failures -> use
  dialogs with keyboard/pointer -> correct disabled actions, confirmations and
  authoritative row transitions. Invariant: UI reports real state. E2E test.
- A16: Given large unopened inventory and more than 32 zones -> open Cluster,
  expand selected branches and inspect a zone -> bounded/coalesced reads, paging,
  inline bitmap and preserved rows after refresh failure. Invariant: scoped reads.
  E2E test.
- A17: Given shared file backing, unknown usage and ENOSPC -> inspect/create/write
  -> deduplicate space, distinguish allocation modes and propagate failures without
  false durability. Invariant: honest capacity. Integration test.
- A18: Given manual writable/readonly/single-preview modes -> invoke UI/API actions
  -> enforce capabilities. Stop/replace/clean containers -> preserve retained and
  unrelated data. Invariant: authorization and owned-resource cleanup.
  Integration test.

Verification commands (implementation gates; not required for this prose edit):

- `pixi run cargo test -p crowdb-monitor`
- `pixi run test-console`
- `pixi run test-console-ui`
- `pixi run test-container-e2e`
- `pixi run test-storage`
- `pixi run test-cpp`
- `pixi run test-e2e-console`
- `pixi run test-e2e-diskio`
- `pixi run test-e2e-diskdb`
- `pixi run test-oci-image`
- `pixi run test-single-node-container`
- `pixi run rs-fmt-check`
- `pixi run rs-lint`
- `pixi run tree-fmt`
- `pixi run tree-lint`
