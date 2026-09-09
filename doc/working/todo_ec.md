<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# EC Implementation Follow-ups

This file records issues discovered while implementing R113, R136, and R137.
Resolved items are removed; any remaining item includes its observed impact,
current safe behavior, and the work needed to close it.

## Active

- **Consumed-reservation recycling fence:** current DiskIO requests carry only
  physical addressing, not `allocation_ts`. Until end-to-end generation fencing
  exists, lease expiry may reclaim only never-consumed reservations. A consumed
  reservation must be confirmed or remain persistently tracked; it must never
  be recycled merely due to elapsed time.
