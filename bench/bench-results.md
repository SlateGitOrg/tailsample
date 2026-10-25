- date: 2026-10-25T06:52Z
- os/arch: Darwin arm64
- cpu: Apple M1 Pro, 8 logical cores
- rustc: rustc 1.97.1 (8bab26f4f 2026-07-14)
- commit: 1ec75ca

# tailsample benchmark

## Core sampler throughput (single thread, simulated clock)

Workload: 100000 generated traces at 2000 traces/s of simulated time; reference policies (`examples/sampler.toml`).

| run | spans | wall time | spans/s | ns/span |
|---:|---:|---:|---:|---:|
| 1 | 1231218 | 0.965 s | 1276112 | 784 |
| 2 | 1234549 | 0.712 s | 1734381 | 577 |
| 3 | 1229315 | 0.707 s | 1739610 | 575 |

Median: **1734381 spans/s**.

## Memory bound under sustained overload

Workload: 60000 traces streamed at 4000 traces/s (~48k spans/s), 5 s decision wait,
so an unbounded buffer would need ~240k spans in memory. `peak heap` is the counting
allocator's high-water mark during the run; it includes the load generator's own
in-flight spans, which the 64 KiB control row measures. `sampler heap` = peak heap minus
that control, i.e. memory attributable to the buffer, decision cache and bookkeeping.

| max_bytes | eviction | peak est. buffer | peak heap | sampler heap | sampler heap / limit | decided early | evicted | rejected spans | error traces kept |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 KiB (control) | DecideEarly | 0.1 MiB | 18.2 MiB | 0.0 MiB | 0.00 | 59995 | 0 | 0 | 477/1091 (43.7%) |
| 8 MiB | DecideEarly | 8.0 MiB | 27.8 MiB | 9.6 MiB | 1.20 | 59002 | 0 | 0 | 1080/1091 (99.0%) |
| 32 MiB | DecideEarly | 32.0 MiB | 56.9 MiB | 38.6 MiB | 1.21 | 56015 | 0 | 0 | 1083/1091 (99.3%) |
| 128 MiB | DecideEarly | 128.0 MiB | 170.2 MiB | 152.0 MiB | 1.19 | 43978 | 0 | 0 | 1091/1091 (100.0%) |
| 32 MiB | DropOldest | 32.0 MiB | 56.8 MiB | 38.5 MiB | 1.20 | 0 | 56015 | 0 | 73/1091 (6.7%) |
| 32 MiB | RejectNew | 32.0 MiB | 55.5 MiB | 37.2 MiB | 1.16 | 0 | 0 | 587128 | 259/1091 (23.7%) |
| 1024 MiB | DecideEarly | 157.1 MiB | 205.3 MiB | 187.1 MiB | 0.18 | 0 | 0 | 0 | 1091/1091 (100.0%) |

## Memory bound with one span per request

Workload: 20000 requests of one span each, every request carrying its own 4 KiB
resource attribute, 10 min decision wait (only the memory limit removes traces).
Requests are built one at a time, so `sampler heap` is the counting allocator's
high-water mark above the live heap at the start of the row; it includes the decision
cache, which grows by one entry per decided or evicted trace. `buffer heap` is the live
heap the buffer frees when flushed at the end of the row (buffer full).

| max_bytes | eviction | peak est. buffer | of which shared context | buffered spans | buffer heap | buffer heap / limit | sampler heap (incl. decision cache) | cached decisions |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 1 MiB | DecideEarly | 1.00 MiB | 0.93 MiB | 215 | 1.17 MiB | 1.17 | 2.53 MiB | 19785 |
| 8 MiB | DecideEarly | 8.00 MiB | 7.45 MiB | 1724 | 9.38 MiB | 1.17 | 11.25 MiB | 18276 |
| 8 MiB | DropOldest | 8.00 MiB | 7.45 MiB | 1724 | 9.38 MiB | 1.17 | 11.25 MiB | 18276 |
| 8 MiB | RejectNew | 8.00 MiB | 7.45 MiB | 1724 | 9.31 MiB | 1.16 | 9.70 MiB | 0 |

## End-to-end OTLP/gRPC over loopback

Clients replay pre-generated requests (<=512 spans each) as fast as possible, rewriting
trace ids on every pass so all traffic is new traces; a refused request is counted and
retried after 5 ms. Server: 4 tokio worker threads, reference policies, 1 s decision wait.

| scenario | clients | duration | accepted spans/s | refused requests | exported spans/s | peak heap (clients + server) |
|---|---:|---:|---:|---:|---:|---:|
| null exporter | 8 | 10 s | 319557 | 549 | 2920 | 313.6 MiB |
| exporter capped at ~50k spans/s (always sample) | 8 | 10 s | 38552 | 6992 | 32030 | 105.4 MiB |

