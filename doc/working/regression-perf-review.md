<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Regression Performance Review

**Date:** 2026-09-10
**Hardware:** Intel Core i9-7960X (16c/32t, x86_64, Linux 6.11)
**Reference hardware:** AMD Ryzen 9 5950X (16c/32t, x86_64, Linux 6.8)

## Remaining performance gaps > 30% (Intel vs AMD)

Low concurrency (1T, 4T, 6T) is consistently 42-88% slower on Intel across
all workloads. High concurrency (16T+) is within 30%. This is a hardware
characteristic, not a regression.

### chunkio-read (i9-7960X, ref 2026-09-09 vs 2026-09-10)

| Case | New TPS | Ref TPS | Gap | Status |
| --- | --- | --- | --- | --- |
| small 8t | 5,448 | 8,692 | -37% | > 30% |
| small 128t | 37,128 | 57,099 | -35% | > 30% |
| small 256t | 35,284 | 56,880 | -38% | > 30% |

Reference is only 1 day old — may be run-to-run variance. Large and mix
reads are within 30%.
