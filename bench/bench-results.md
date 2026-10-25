- date: 2026-10-25T06:34Z
- os/arch: Darwin arm64
- cpu: Apple M1 Pro, 8 logical cores
- rustc: rustc 1.97.1 (8bab26f4f 2026-07-14)
- commit: f60d024

# tailsample benchmark

## Core sampler throughput (single thread, simulated clock)

Workload: 100000 generated traces at 2000 traces/s of simulated time; reference policies (`examples/sampler.toml`).

| run | spans | wall time | spans/s | ns/span |
|---:|---:|---:|---:|---:|
| 1 | 1231218 | 0.757 s | 1627434 | 614 |
| 2 | 1234549 | 0.710 s | 1737729 | 575 |
| 3 | 1229315 | 0.691 s | 1777825 | 562 |

Median: **1737729 spans/s**.

## Memory bound under sustained overload

Workload: 60000 traces streamed at 4000 traces/s (~48k spans/s), 5 s decision wait,
so an unbounded buffer would need ~240k spans in memory. `peak heap` is the counting
allocator's high-water mark during the run; it includes the load generator's own
in-flight spans, which the 64 KiB control row measures. `sampler heap` = peak heap minus
that control, i.e. memory attributable to the buffer, decision cache and bookkeeping.

| max_bytes | eviction | peak est. buffer | peak heap | sampler heap | sampler heap / limit | decided early | evicted | rejected spans | error traces kept |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 KiB (control) | DecideEarly | 0.1 MiB | 18.2 MiB | 0.0 MiB | 0.00 | 59992 | 0 | 0 | 485/1091 (44.5%) |
| 8 MiB | DecideEarly | 8.0 MiB | 27.9 MiB | 9.6 MiB | 1.20 | 58983 | 0 | 0 | 1080/1091 (99.0%) |
| 32 MiB | DecideEarly | 32.0 MiB | 57.0 MiB | 38.8 MiB | 1.21 | 55980 | 0 | 0 | 1083/1091 (99.3%) |
| 128 MiB | DecideEarly | 128.0 MiB | 170.8 MiB | 152.5 MiB | 1.19 | 43882 | 0 | 0 | 1091/1091 (100.0%) |
| 32 MiB | DropOldest | 32.0 MiB | 56.9 MiB | 38.7 MiB | 1.21 | 0 | 55980 | 0 | 73/1091 (6.7%) |
| 32 MiB | RejectNew | 32.0 MiB | 55.6 MiB | 37.4 MiB | 1.17 | 0 | 0 | 586468 | 259/1091 (23.7%) |
| 1024 MiB | DecideEarly | 156.4 MiB | 205.2 MiB | 187.0 MiB | 0.18 | 0 | 0 | 0 | 1091/1091 (100.0%) |

## End-to-end OTLP/gRPC over loopback

Clients replay pre-generated requests (<=512 spans each) as fast as possible, rewriting
trace ids on every pass so all traffic is new traces; a refused request is counted and
retried after 5 ms. Server: 4 tokio worker threads, reference policies, 1 s decision wait.

| scenario | clients | duration | accepted spans/s | refused requests | exported spans/s | peak heap (clients + server) |
|---|---:|---:|---:|---:|---:|---:|
| null exporter | 8 | 10 s | 322000 | 497 | 3184 | 325.3 MiB |
| exporter capped at ~50k spans/s (always sample) | 8 | 10 s | 41858 | 6897 | 33233 | 137.6 MiB |

