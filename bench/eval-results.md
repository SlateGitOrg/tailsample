- date: 2026-10-25T06:52Z
- os/arch: Darwin arm64
- cpu: Apple M1 Pro, 8 logical cores
- rustc: rustc 1.97.1 (8bab26f4f 2026-07-14)
- commit: 1ec75ca

# tailsample policy evaluation

## 1. Reference policy at nominal load

30,000 traces at 500 traces/s (~6k spans/s), 2% injected errors, 2% injected latency spikes,
1% of traces with a span delivered 6-9 s late; `examples/sampler.toml` (5 s decision wait).

| seed | error traces kept | complete | slow kept | other kept | span reduction | peak error spans/s (budget 1800) |
|---:|---:|---:|---:|---:|---:|---:|
| 5001 | 540/540 | 100.0% | 100.0% | 4.6% | 91.4% | 259 |
| 5002 | 598/598 | 100.0% | 100.0% | 4.6% | 91.2% | 249 |
| 5003 | 600/600 | 100.0% | 100.0% | 4.7% | 90.9% | 259 |

## 2. Error storm: budgets are caps

Same policy, 20,000 traces at 500 traces/s, injected error rate varied. Error budget is
60% of 3000 = 1800 spans/s.

| error rate | error traces | kept | peak error spans/s | mean error spans/s exported |
|---:|---:|---:|---:|---:|
| 5% | 911 | 100.0% | 419 | 250 |
| 20% | 3750 | 100.0% | 1626 | 1036 |
| 40% | 7251 | 77.4% | 1800 | 1555 |
| 80% | 14663 | 41.8% | 1800 | 1667 |

## 3. Eviction policy under memory pressure

12,000 traces at 1,000 traces/s with `max_bytes` = 4 MiB (about a third of a second of
traffic against a 5 s decision wait).

| eviction | error traces kept | slow kept | traces decided early | traces dropped undecided | spans refused | peak est. bytes |
|---|---:|---:|---:|---:|---:|---:|
| DecideEarly | 97.5% | 19.5% | 11503 | 0 | 0 | 4.0 MiB |
| DropOldest | 3.0% | 9.1% | 0 | 11503 | 0 | 4.0 MiB |
| RejectNew | 19.8% | 8.2% | 0 | 0 | 129151 | 4.0 MiB |

## 4. Decision wait versus completeness

Reference policy, 20,000 traces at 500 traces/s. Latency spikes add 1.2-3 s; late spans
arrive 6-9 s after their trace ends.

| decision_wait_ms | slow traces kept | kept traces complete | spans after decision: forwarded / dropped | peak buffered traces |
|---:|---:|---:|---:|---:|
| 1000 | 17.4% | 100.0% | 162 / 2079 | 511 |
| 2000 | 67.0% | 100.0% | 181 / 979 | 1014 |
| 3500 | 100.0% | 100.0% | 29 / 150 | 1764 |
| 5000 | 100.0% | 100.0% | 29 / 150 | 2515 |
| 10000 | 100.0% | 100.0% | 1 / 0 | 5011 |
