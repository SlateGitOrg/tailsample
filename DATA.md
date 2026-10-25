# Data

tailsample uses no external datasets.

| Data | Location | Source | License |
|---|---|---|---|
| Synthetic traces | generated at runtime by `src/loadgen/` | written for this project (deterministic SplitMix64 PRNG over the topology in `src/loadgen/topology.rs`) | MIT (this repository) |
| OTLP/JSON fixture: one two-span error trace | `fixtures/checkout-error.json` | hand-written for this project following the OTLP/JSON encoding rules | MIT (this repository) |

Everything the tests, demo, evaluation and benchmark use is synthetic. Service names, versions and
host names in generated traces are invented. No real telemetry was used to tune anything.
