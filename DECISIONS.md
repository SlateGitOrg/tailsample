# Decisions

Rulings made while building tailsample, in the format
`Ruling: <what> — <why> — <cost if wrong>`.

## Protocol and wire format

- Ruling: use the official `opentelemetry-proto` crate (pinned `=0.33.1`, features `gen-tonic`, `trace`, `with-serde`) for all OTLP types instead of hand-written prost structs — its generated code is what the OpenTelemetry Rust SDK uses, so the wire format and the OTLP/JSON mapping (hex ids, string-encoded 64-bit ints) come for free — it pulls `opentelemetry_sdk` in as a compile-time dependency and upgrades need a coordinated tonic/prost bump.
- Ruling: OTLP/HTTP accepts `application/x-protobuf` and `application/json`, `identity` or `gzip` content encoding, and answers in the encoding of the request — this is the subset the OTLP spec requires of servers — clients that send `br`/`zstd` get 415.
- Ruling: every transient refusal maps to gRPC `UNAVAILABLE` (with a `retry-after` metadata hint) — the OTLP spec makes `UNAVAILABLE` unconditionally retryable, whereas `RESOURCE_EXHAUSTED` is only retryable with `RetryInfo` — a client that treats `UNAVAILABLE` as "server down" may log noisier errors than necessary.
- Ruling: OTLP/HTTP refusals are 429 (ingest queue full) and 503 (buffer admission closed / shutting down), both with `Retry-After` — both are in the spec's retryable set and the split tells operators which limit was hit — none expected.
- Ruling: the HTTP exporter is plaintext `http://` only and there is no TLS or auth on receivers — TLS termination is normally done by a sidecar or load balancer and adding rustls would double the dependency tree — cannot be deployed across an untrusted network as-is (listed in Limitations).

## Core design

- Ruling: the sampler core (`Sampler`) is a synchronous state machine on a logical millisecond clock, owned by a single tokio task; receivers talk to it through a bounded channel — deterministic tests and simulations, no locks on the hot path — the core is single-threaded, capping one process at the core throughput measured in `bench/bench-results.md`; scaling out needs trace-id-aware load balancing across instances.
- Ruling: the decision wait starts at a trace's first span arrival, like the OpenTelemetry Collector's `tail_sampling` processor — simple, needs no root-span detection, and makes deadlines monotone — traces longer than the wait are decided incomplete (quantified in eval section 4).
- Ruling: one FIFO (`VecDeque<(deadline, trace_id)>`) serves both expiry and eviction — with a constant wait, first-arrival order equals deadline order, so both are O(1) — a per-trace or adaptive wait would need a heap or timer wheel.
- Ruling: memory is accounted per span by walking owned allocations (`estimate_span_bytes`), and container slack is not attributed — cheap, monotone, and close to reality — the benchmark measures real heap at about 1.2x the estimate, so operators must size `max_bytes` with headroom (documented in README).
- Ruling (fix round 1): shared resource/scope context is charged to the buffer once per distinct `Arc` while any buffered span references it (scope refcounted per span, resource per charged scope, keyed by `Arc` address), not ignored and not charged per span or per trace — ignoring it let one-span-per-request traffic with a 4 KiB resource hold ~12x `max_bytes`; charging per span or per trace would overcount batched traffic, where hundreds of spans and traces share one resource — two hash-map operations per stored span and a context walk on first charge (core throughput unchanged within noise: 1.73M vs 1.74M spans/s median); identical resources arriving in separate requests are still stored and charged once per request (no content interning), so per-span-processor clients get a smaller effective buffer rather than a smaller footprint.
- Ruling: three explicit eviction policies (`decide_early` default, `drop_oldest`, `reject_new`) — decide-early keeps error traces under memory pressure (97.5% vs 3% for drop-oldest in eval section 3) while the other two serve "never decide on partial data" and "push back instead of losing data" operators — `decide_early` degrades latency policies most (traces are decided before slow spans arrive).
- Ruling: traces dropped by `drop_oldest` are recorded as "not sampled" in the decision cache — otherwise their stragglers would re-create half traces — a trace that would have been sampled later is lost for good.
- Ruling: a late span of a sampled trace is exported on its own immediately rather than held for re-batching — keeps the cache path O(1) and memory-free — downstream sees the trace's spans in two exports (normal for OTLP).
- Ruling: the decision cache is two FIFO-bounded sets (sampled / not sampled) instead of an LRU — O(1), no per-hit reordering, and the two capacities reflect the different cost of forgetting each outcome — a long-lived trace with periodic late spans can be forgotten while still active.

## Policies

- Ruling: top-level policies are evaluated in order and stop at the first that samples (OTel's collector evaluates all of them) — stateful budgets of later policies are only consumed by traces nobody earlier wanted, which makes budgets predictable — configs ported from the collector may attribute decisions differently.
- Ruling: composite budgets are per fixed one-second window on the logical clock; a sub-policy that wants a trace but lacks budget passes it to the next sub-policy; unallocated percentage is split equally among sub-policies without an explicit share — matches the collector's composite semantics and keeps the hard cap testable — fixed windows allow up to 2x the budget across a window boundary within any sliding second (the tests assert per fixed window).
- Ruling: the top-level `rate_limiting` policy is a token bucket over spans (burst defaults to one second of rate) — smoother than fixed windows and different from composite on purpose — a trace with more spans than the burst can never be sampled by it.
- Ruling: `probabilistic` hashes `seed ‖ trace_id` with FNV-1a 64 and compares against a threshold — deterministic across instances sharing a seed — it is not the W3C/OTEP-235 consistent-probability (`th` in tracestate) scheme, so it does not compose with upstream head sampling.
- Ruling: `composite` may only appear at top level and `and` may appear at top level or directly inside a composite (max nesting depth 2) — covers "budgeted 5% of non-health-check traffic" without recursive configs — deeper boolean logic is not expressible.

## Backpressure and bounds

- Ruling: the ingest queue is bounded both by message count and by span permits (`backpressure.max_queued_spans`), and a shared semaphore caps in-flight request decoding across both receivers (`receiver.max_concurrent_requests`) — a message-count bound alone let 8 benchmark clients queue >300 MiB — permits are capped per request, so one oversized request can briefly exceed the span bound.
- Ruling: the sampler awaits export-queue capacity (span permits) instead of dropping sampled data — slow downstreams become client-visible pushback (429/`UNAVAILABLE`) and clients retry — a hung exporter stalls ingestion for up to `max_attempts x timeout_ms` per batch before the batch is dropped.
- Ruling: `block` backpressure mode waits at most `block_timeout_ms` for queue space before refusing — gives short bursts a chance without unbounded request latency — clients with short deadlines may time out before the refusal arrives.

## Tooling and evaluation

- Ruling: configuration is TOML (`serde_yaml` is deprecated upstream) — collector users expect YAML — config files are not copy-paste compatible with the collector.
- Ruling: the load generator uses its own SplitMix64 PRNG and a hand-written e-commerce topology — bit-identical workloads across dependency upgrades, exact ground truth — the topology is synthetic; real traffic shapes (fan-out, payload sizes) differ.
- Ruling: evaluation (`examples/eval.rs`) uses seeds 5001+ that no test uses, and the reference config was written before the eval ran — avoids tuning on the reported data — still synthetic data from one generator.
- Ruling: coverage is measured with `cargo llvm-cov` over the library (inline unit-test modules are part of the measured files) and excludes `src/main.rs`, `src/bin/` and `examples/`, which are exercised by `make demo`/`make bench` — CLI glue is not unit-testable without a process harness — the 80% gate does not cover argument parsing.
- Ruling: no Docker-based `make integration` against a real OpenTelemetry Collector — the image is large on a disk-constrained build machine and the official proto crate already fixes the wire format — interop with real collectors/SDKs is untested (listed in Limitations).
