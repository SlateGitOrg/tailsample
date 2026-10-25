//! Property tests: buffer limits and span conservation hold for arbitrary
//! interleavings of ingest and tick under every eviction policy.

use std::collections::HashSet;
use std::sync::Arc;

use opentelemetry_proto::tonic::trace::v1::{Span, Status};
use proptest::prelude::*;

use tailsample::cache::DecisionCache;
use tailsample::model::{ScopeContext, SpanRecord};
use tailsample::sampler::{ReleaseKind, Released, Sampler};
use tailsample::{Config, TraceId};

#[derive(Debug, Clone)]
enum Op {
    /// (trace id 1..=24, error, payload bytes)
    Ingest(Vec<(u8, bool, u16)>),
    /// advance the clock and tick
    Tick(u16),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => prop::collection::vec((1u8..=24, any::<bool>(), 0u16..600), 1..12).prop_map(Op::Ingest),
        1 => (0u16..400).prop_map(Op::Tick),
    ]
}

fn rec(trace: u8, seq: u64, error: bool, payload: u16) -> SpanRecord {
    let id = TraceId([trace; 16]);
    SpanRecord::new(
        id,
        Span {
            trace_id: id.0.to_vec(),
            span_id: seq.to_be_bytes().to_vec(),
            name: "p".repeat(payload as usize),
            start_time_unix_nano: 1,
            end_time_unix_nano: 2 + u64::from(payload) * 1_000_000,
            status: Some(Status {
                code: if error { 2 } else { 0 },
                message: String::new(),
            }),
            ..Default::default()
        },
        Arc::new(ScopeContext::default()),
    )
}

fn config(eviction: &str, max_traces: usize, max_bytes: usize, per_trace: usize) -> Config {
    Config::from_toml_str(&format!(
        r#"
        [buffer]
        decision_wait_ms = 300
        max_traces = {max_traces}
        max_bytes = {max_bytes}
        max_spans_per_trace = {per_trace}
        eviction = "{eviction}"
        admission_resume_ratio = 0.8
        [decision_cache]
        sampled_capacity = 8
        not_sampled_capacity = 8
        [[policies]]
        name = "errors"
        type = "status_code"
        codes = ["ERROR"]
        [[policies]]
        name = "budget"
        type = "composite"
        max_total_spans_per_second = 40
        sub_policies = [{{ name = "slow", type = "latency", threshold_ms = 300 }}, {{ name = "some", type = "probabilistic", percentage = 30.0 }}]
        "#
    ))
    .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn limits_and_conservation_hold(
        ops in prop::collection::vec(op(), 1..60),
        eviction in prop::sample::select(vec!["decide_early", "drop_oldest", "reject_new"]),
        max_traces in 1usize..10,
        max_bytes in 600usize..6_000,
        per_trace in 1usize..6,
    ) {
        let cfg = config(eviction, max_traces, max_bytes, per_trace);
        let mut s = Sampler::new(&cfg).unwrap();
        let mut now = 0u64;
        let mut seq = 0u64;
        let mut out: Vec<Released> = Vec::new();
        let mut released_spans = 0u64;
        for op in ops {
            match op {
                Op::Ingest(spans) => {
                    let recs = spans.into_iter().map(|(t, e, p)| { seq += 1; rec(t, seq, e, p) }).collect();
                    s.ingest(recs, now, &mut out);
                }
                Op::Tick(dt) => {
                    now += u64::from(dt);
                    s.tick(now, &mut out);
                }
            }
            let g = s.gauges();
            prop_assert!(g.traces <= max_traces, "traces {} > {}", g.traces, max_traces);
            prop_assert!(g.bytes <= max_bytes, "bytes {} > {}", g.bytes, max_bytes);
            prop_assert_eq!(s.stats().spans_received, s.accounted_spans());
            if eviction != "reject_new" {
                prop_assert!(g.admission_open);
                prop_assert_eq!(s.stats().spans_rejected_full, 0);
            }
            if eviction != "decide_early" {
                prop_assert_eq!(s.stats().traces_decided_early, 0);
            }
            for r in out.drain(..) {
                prop_assert!(!r.spans.is_empty());
                prop_assert!(r.spans.iter().all(|x| x.trace_id == r.trace_id));
                prop_assert!(r.spans.len() <= per_trace || r.kind == ReleaseKind::Late);
                released_spans += r.spans.len() as u64;
            }
        }
        s.flush(now, &mut out);
        released_spans += out.iter().map(|r| r.spans.len() as u64).sum::<u64>();
        let st = s.stats();
        prop_assert_eq!(released_spans, st.spans_sampled + st.spans_late_sampled);
        prop_assert_eq!(s.buffered_spans(), 0);
        prop_assert_eq!(st.spans_received, s.accounted_spans());
        prop_assert!(s.admission_open());
    }

    #[test]
    fn every_buffered_error_trace_is_sampled_without_pressure(
        traces in prop::collection::vec((1u8..=50, any::<bool>()), 1..80),
    ) {
        // Large limits: no eviction, so the error policy sees every trace.
        let cfg = config("decide_early", 1_000, 10_000_000, 100);
        let mut s = Sampler::new(&cfg).unwrap();
        let mut out = Vec::new();
        let mut errored = HashSet::new();
        for (i, (t, e)) in traces.iter().enumerate() {
            if *e { errored.insert(*t); }
            s.ingest(vec![rec(*t, i as u64, *e, 0)], 0, &mut out);
        }
        s.tick(1_000, &mut out);
        let sampled: HashSet<u8> = out.iter().map(|r| r.trace_id.0[0]).collect();
        for t in errored {
            prop_assert!(sampled.contains(&t), "error trace {} not sampled", t);
        }
    }

    #[test]
    fn decision_cache_never_exceeds_capacity(
        ids in prop::collection::vec((any::<u64>(), any::<bool>()), 0..300),
        cap_s in 0usize..20,
        cap_n in 0usize..20,
    ) {
        let mut c = DecisionCache::new(cap_s, cap_n);
        for (id, sampled) in &ids {
            let d = if *sampled { tailsample::cache::Decision::Sampled } else { tailsample::cache::Decision::NotSampled };
            c.record(TraceId::from_u128(u128::from(*id) + 1), d);
            let (a, b) = c.sizes();
            prop_assert!(a <= cap_s && b <= cap_n);
        }
        // The most recent decision of each kind is remembered when capacity allows.
        if let Some((id, sampled)) = ids.last() {
            let cap = if *sampled { cap_s } else { cap_n };
            if cap > 0 {
                prop_assert!(c.get(&TraceId::from_u128(u128::from(*id) + 1)).is_some());
            }
        }
    }
}
