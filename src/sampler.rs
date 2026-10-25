//! The deterministic sampling core.
//!
//! [`Sampler`] is a synchronous state machine driven by a logical clock in
//! milliseconds. The async runtime feeds it spans and periodic ticks; tests
//! and benchmarks drive it directly with simulated time. It never blocks and
//! never allocates beyond the configured limits (plus the decision cache).

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::buffer::{Append, BufferLimits, TraceBuffer, TraceEntry};
use crate::cache::{Decision, DecisionCache};
use crate::config::{Config, ConfigError, EvictionPolicy};
use crate::ids::TraceId;
use crate::model::SpanRecord;
use crate::policy::{PolicyEngine, TraceView};

/// Why spans are being released downstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseKind {
    /// Trace decided after its full decision wait.
    Decided,
    /// Trace decided before its decision wait because the buffer was full.
    DecidedEarly,
    /// Late span of a trace already sampled.
    Late,
    /// Decided during shutdown flush.
    Flushed,
}

/// Spans of one trace selected for export.
#[derive(Debug, Clone)]
pub struct Released {
    /// Trace id.
    pub trace_id: TraceId,
    /// Spans to export.
    pub spans: Vec<SpanRecord>,
    /// Label of the policy that kept the trace (`"late"` for late spans).
    pub policy: Arc<str>,
    /// Why released.
    pub kind: ReleaseKind,
}

/// Cumulative counters. Every received span ends up in exactly one bucket:
///
/// `spans_received = spans_invalid + spans_rejected_full + spans_rejected_oversize
///   + spans_dropped_trace_limit + spans_late_sampled + spans_late_dropped
///   + spans_sampled + spans_not_sampled + spans_evicted + buffered spans`
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SamplerStats {
    /// Spans handed to the sampler (including invalid ones).
    pub spans_received: u64,
    /// Spans with an invalid trace id.
    pub spans_invalid: u64,
    /// Spans refused by `reject_new` because the buffer was full.
    pub spans_rejected_full: u64,
    /// Spans larger than the entire buffer.
    pub spans_rejected_oversize: u64,
    /// Spans dropped because their trace hit `max_spans_per_trace`.
    pub spans_dropped_trace_limit: u64,
    /// Late spans forwarded because their trace was sampled.
    pub spans_late_sampled: u64,
    /// Late spans dropped because their trace was not sampled.
    pub spans_late_dropped: u64,
    /// Spans of sampled traces released at decision time.
    pub spans_sampled: u64,
    /// Spans of traces the policies did not sample.
    pub spans_not_sampled: u64,
    /// Spans dropped by `drop_oldest` eviction.
    pub spans_evicted: u64,
    /// Traces created in the buffer.
    pub traces_received: u64,
    /// Traces sampled.
    pub traces_sampled: u64,
    /// Traces not sampled.
    pub traces_not_sampled: u64,
    /// Traces decided before their decision wait (`decide_early`).
    pub traces_decided_early: u64,
    /// Traces dropped undecided (`drop_oldest`).
    pub traces_evicted: u64,
    /// Times admission was closed (`reject_new`).
    pub admission_closures: u64,
    /// Sampled traces per policy label.
    pub sampled_by_policy: BTreeMap<String, u64>,
}

/// Point-in-time gauges.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct SamplerGauges {
    /// Buffered traces.
    pub traces: usize,
    /// Buffered spans.
    pub spans: usize,
    /// Estimated buffered bytes.
    pub bytes: usize,
    /// Decision cache entries (sampled, not sampled).
    pub cache_sizes: (usize, usize),
    /// Whether new traces are admitted.
    pub admission_open: bool,
}

/// The sampling core.
#[derive(Debug)]
pub struct Sampler {
    buffer: TraceBuffer,
    cache: DecisionCache,
    policies: PolicyEngine,
    eviction: EvictionPolicy,
    resume_ratio: f64,
    admission_open: bool,
    stats: SamplerStats,
}

impl Sampler {
    /// Build from validated configuration.
    pub fn new(cfg: &Config) -> Result<Self, ConfigError> {
        let b = &cfg.buffer;
        Ok(Sampler {
            buffer: TraceBuffer::new(BufferLimits {
                max_traces: b.max_traces,
                max_bytes: b.max_bytes,
                max_spans_per_trace: b.max_spans_per_trace,
                decision_wait_ms: b.decision_wait_ms,
            }),
            cache: DecisionCache::new(
                cfg.decision_cache.sampled_capacity,
                cfg.decision_cache.not_sampled_capacity,
            ),
            policies: PolicyEngine::from_config(&cfg.effective_policies())?,
            eviction: b.eviction,
            resume_ratio: b.admission_resume_ratio,
            admission_open: true,
            stats: SamplerStats::default(),
        })
    }

    /// Cumulative counters.
    pub fn stats(&self) -> &SamplerStats {
        &self.stats
    }

    /// Current gauges.
    pub fn gauges(&self) -> SamplerGauges {
        SamplerGauges {
            traces: self.buffer.len(),
            spans: self.buffer.span_count(),
            bytes: self.buffer.bytes(),
            cache_sizes: self.cache.sizes(),
            admission_open: self.admission_open,
        }
    }

    /// Whether receivers should accept new data. Only ever false with the
    /// `reject_new` eviction policy.
    pub fn admission_open(&self) -> bool {
        self.admission_open
    }

    /// Next time (ms) at which [`tick`](Self::tick) has work to do.
    pub fn next_deadline(&self) -> Option<u64> {
        self.buffer.next_deadline()
    }

    /// Count spans that were rejected before reaching the sampler (invalid
    /// trace ids found while flattening a request).
    pub fn record_invalid(&mut self, n: u64) {
        self.stats.spans_received += n;
        self.stats.spans_invalid += n;
    }

    /// Ingest spans at logical time `now_ms`; sampled output is appended to
    /// `out` (late spans of sampled traces and early decisions).
    pub fn ingest(&mut self, records: Vec<SpanRecord>, now_ms: u64, out: &mut Vec<Released>) {
        for rec in records {
            self.stats.spans_received += 1;
            self.ingest_one(rec, now_ms, out);
        }
        self.update_admission();
    }

    fn ingest_one(&mut self, rec: SpanRecord, now_ms: u64, out: &mut Vec<Released>) {
        let limits = self.buffer.limits();
        if rec.cost > limits.max_bytes {
            self.stats.spans_rejected_oversize += 1;
            return;
        }
        let id = rec.trace_id;
        if self.buffer.contains(&id) {
            if self
                .buffer
                .get(&id)
                .is_some_and(|e| e.spans.len() >= limits.max_spans_per_trace)
            {
                self.buffer.append(rec, now_ms); // records truncation stat
                self.stats.spans_dropped_trace_limit += 1;
                return;
            }
            if !self.make_room(rec.cost, false, now_ms, out) {
                self.stats.spans_rejected_full += 1;
                return;
            }
            if self.buffer.contains(&id) {
                self.store(rec, now_ms);
                return;
            }
            // The trace itself was evicted while making room: fall through
            // to the late-span path so the span follows its trace's fate.
        }
        match self.cache.get(&id) {
            Some(Decision::Sampled) => {
                self.stats.spans_late_sampled += 1;
                out.push(Released {
                    trace_id: id,
                    spans: vec![rec],
                    policy: Arc::from("late"),
                    kind: ReleaseKind::Late,
                });
            }
            Some(Decision::NotSampled) => self.stats.spans_late_dropped += 1,
            None => {
                if !self.make_room(rec.cost, true, now_ms, out) {
                    self.stats.spans_rejected_full += 1;
                    return;
                }
                self.stats.traces_received += 1;
                self.store(rec, now_ms);
            }
        }
    }

    fn store(&mut self, rec: SpanRecord, now_ms: u64) {
        match self.buffer.append(rec, now_ms) {
            Append::Stored => {}
            Append::TraceFull => self.stats.spans_dropped_trace_limit += 1,
        }
    }

    /// Ensure `cost` bytes (and a new trace slot if `new_trace`) fit,
    /// evicting according to policy. Returns false when the span must be
    /// rejected instead.
    fn make_room(
        &mut self,
        cost: usize,
        new_trace: bool,
        now_ms: u64,
        out: &mut Vec<Released>,
    ) -> bool {
        while !self.buffer.fits(cost, new_trace) {
            if self.eviction == EvictionPolicy::RejectNew {
                if self.admission_open {
                    self.admission_open = false;
                    self.stats.admission_closures += 1;
                }
                return false;
            }
            let Some((id, entry)) = self.buffer.pop_oldest() else {
                return false;
            };
            match self.eviction {
                EvictionPolicy::DecideEarly => {
                    self.stats.traces_decided_early += 1;
                    self.decide(id, entry, now_ms, ReleaseKind::DecidedEarly, out);
                }
                _ => {
                    self.stats.traces_evicted += 1;
                    self.stats.spans_evicted += entry.spans.len() as u64;
                    self.cache.record(id, Decision::NotSampled);
                }
            }
        }
        true
    }

    fn decide(
        &mut self,
        id: TraceId,
        entry: TraceEntry,
        now_ms: u64,
        kind: ReleaseKind,
        out: &mut Vec<Released>,
    ) {
        let view = TraceView {
            id,
            spans: &entry.spans,
            stats: &entry.stats,
        };
        let n = entry.spans.len() as u64;
        match self.policies.evaluate(&view, now_ms) {
            Some(policy) => {
                self.cache.record(id, Decision::Sampled);
                self.stats.traces_sampled += 1;
                self.stats.spans_sampled += n;
                *self
                    .stats
                    .sampled_by_policy
                    .entry(policy.to_string())
                    .or_default() += 1;
                out.push(Released {
                    trace_id: id,
                    spans: entry.spans,
                    policy,
                    kind,
                });
            }
            None => {
                self.cache.record(id, Decision::NotSampled);
                self.stats.traces_not_sampled += 1;
                self.stats.spans_not_sampled += n;
            }
        }
    }

    /// Decide every trace whose decision wait has elapsed at `now_ms`.
    pub fn tick(&mut self, now_ms: u64, out: &mut Vec<Released>) {
        while let Some((id, entry)) = self.buffer.pop_expired(now_ms) {
            self.decide(id, entry, now_ms, ReleaseKind::Decided, out);
        }
        self.update_admission();
    }

    /// Decide every buffered trace now (graceful shutdown).
    pub fn flush(&mut self, now_ms: u64, out: &mut Vec<Released>) {
        while let Some((id, entry)) = self.buffer.pop_oldest() {
            self.decide(id, entry, now_ms, ReleaseKind::Flushed, out);
        }
        self.update_admission();
    }

    fn update_admission(&mut self) {
        if !self.admission_open && self.buffer.utilisation() < self.resume_ratio {
            self.admission_open = true;
        }
    }

    /// Buffered span count (for conservation checks).
    pub fn buffered_spans(&self) -> usize {
        self.buffer.span_count()
    }

    /// Sum of all terminal span buckets plus buffered spans; equals
    /// `stats().spans_received` at all times.
    pub fn accounted_spans(&self) -> u64 {
        let s = &self.stats;
        s.spans_invalid
            + s.spans_rejected_full
            + s.spans_rejected_oversize
            + s.spans_dropped_trace_limit
            + s.spans_late_sampled
            + s.spans_late_dropped
            + s.spans_sampled
            + s.spans_not_sampled
            + s.spans_evicted
            + self.buffer.span_count() as u64
    }

    /// Decision cache evictions so far.
    pub fn cache_evictions(&self) -> u64 {
        self.cache.evictions()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ScopeContext, SpanRecord};
    use opentelemetry_proto::tonic::trace::v1::{Span, Status};

    fn rec(trace: u128, span: u64, error: bool, attr_bytes: usize) -> SpanRecord {
        let id = TraceId::from_u128(trace);
        let s = Span {
            trace_id: id.0.to_vec(),
            span_id: span.to_be_bytes().to_vec(),
            start_time_unix_nano: 1,
            end_time_unix_nano: 2,
            name: "x".repeat(attr_bytes),
            status: error.then(|| Status {
                code: 2,
                message: String::new(),
            }),
            ..Default::default()
        };
        SpanRecord::new(id, s, Arc::new(ScopeContext::default()))
    }

    fn sampler(extra: &str) -> Sampler {
        let cfg = Config::from_toml_str(&format!(
            r#"
            [buffer]
            decision_wait_ms = 1000
            max_traces = 3
            max_spans_per_trace = 4
            {extra}

            [decision_cache]
            sampled_capacity = 100
            not_sampled_capacity = 100

            [[policies]]
            name = "errors"
            type = "status_code"
            codes = ["ERROR"]
            "#
        ))
        .unwrap();
        Sampler::new(&cfg).unwrap()
    }

    fn conserve(s: &Sampler) {
        assert_eq!(s.stats().spans_received, s.accounted_spans());
    }

    #[test]
    fn decides_after_wait_and_handles_late_spans() {
        let mut s = sampler("");
        let mut out = Vec::new();
        s.ingest(
            vec![rec(1, 1, false, 0), rec(1, 2, true, 0), rec(2, 1, false, 0)],
            0,
            &mut out,
        );
        assert!(out.is_empty());
        s.tick(999, &mut out);
        assert!(out.is_empty());
        s.tick(1_000, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].trace_id, TraceId::from_u128(1));
        assert_eq!(out[0].spans.len(), 2);
        assert_eq!(&*out[0].policy, "errors");
        assert_eq!(out[0].kind, ReleaseKind::Decided);
        out.clear();
        // late spans follow the cached decision
        s.ingest(
            vec![rec(1, 3, false, 0), rec(2, 2, false, 0)],
            1_500,
            &mut out,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].kind, ReleaseKind::Late);
        assert_eq!(s.stats().spans_late_sampled, 1);
        assert_eq!(s.stats().spans_late_dropped, 1);
        assert_eq!(s.stats().sampled_by_policy.get("errors"), Some(&1));
        conserve(&s);
    }

    #[test]
    fn decide_early_eviction_keeps_errors() {
        let mut s = sampler("");
        let mut out = Vec::new();
        s.ingest(
            vec![rec(1, 1, true, 0), rec(2, 1, false, 0), rec(3, 1, false, 0)],
            0,
            &mut out,
        );
        assert!(out.is_empty());
        s.ingest(vec![rec(4, 1, false, 0)], 10, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].kind, ReleaseKind::DecidedEarly);
        assert_eq!(s.stats().traces_decided_early, 1);
        assert_eq!(s.gauges().traces, 3);
        conserve(&s);
    }

    #[test]
    fn drop_oldest_eviction_drops_and_caches() {
        let mut s = sampler("eviction = \"drop_oldest\"");
        let mut out = Vec::new();
        s.ingest(
            vec![rec(1, 1, true, 0), rec(2, 1, false, 0), rec(3, 1, false, 0)],
            0,
            &mut out,
        );
        s.ingest(vec![rec(4, 1, false, 0), rec(1, 2, true, 0)], 10, &mut out);
        assert!(out.is_empty());
        assert_eq!(s.stats().traces_evicted, 1);
        // trace 1 was evicted, then its late span was dropped via the cache.
        assert_eq!(s.stats().spans_late_dropped, 1);
        assert_eq!(s.stats().spans_evicted, 1);
        conserve(&s);
    }

    #[test]
    fn reject_new_closes_and_reopens_admission() {
        let mut s = sampler("eviction = \"reject_new\"\nadmission_resume_ratio = 0.5");
        let mut out = Vec::new();
        s.ingest(
            vec![
                rec(1, 1, false, 0),
                rec(2, 1, false, 0),
                rec(3, 1, false, 0),
                rec(4, 1, false, 0),
            ],
            0,
            &mut out,
        );
        assert_eq!(s.stats().spans_rejected_full, 1);
        assert!(!s.admission_open());
        // existing traces still accept spans while there is byte room
        s.ingest(vec![rec(1, 2, false, 0)], 1, &mut out);
        assert_eq!(s.stats().spans_rejected_full, 1);
        s.tick(1_000, &mut out);
        assert!(s.admission_open());
        assert_eq!(s.stats().admission_closures, 1);
        conserve(&s);
    }

    #[test]
    fn per_trace_limit_and_oversize() {
        let cfg = Config::from_toml_str(
            "[buffer]\nmax_bytes = 2000\nmax_spans_per_trace = 2\ndecision_wait_ms = 10",
        )
        .unwrap();
        let mut s = Sampler::new(&cfg).unwrap();
        let mut out = Vec::new();
        s.ingest(
            vec![
                rec(1, 1, false, 0),
                rec(1, 2, false, 0),
                rec(1, 3, false, 0),
                rec(2, 1, false, 5_000),
            ],
            0,
            &mut out,
        );
        assert_eq!(s.stats().spans_dropped_trace_limit, 1);
        assert_eq!(s.stats().spans_rejected_oversize, 1);
        s.record_invalid(2);
        assert_eq!(s.stats().spans_invalid, 2);
        s.flush(5, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].kind, ReleaseKind::Flushed);
        assert_eq!(s.buffered_spans(), 0);
        assert!(s.next_deadline().is_none());
        conserve(&s);
    }

    #[test]
    fn byte_pressure_on_existing_trace_can_evict_itself() {
        let one = rec(1, 1, false, 0).cost;
        let cfg = Config::from_toml_str(&format!(
            "[buffer]\nmax_bytes = {}\nmax_traces = 10\ndecision_wait_ms = 10\n[[policies]]\nname=\"a\"\ntype=\"always_sample\"",
            one * 2 + one / 2
        ))
        .unwrap();
        let mut s = Sampler::new(&cfg).unwrap();
        let mut out = Vec::new();
        s.ingest(vec![rec(1, 1, false, 0), rec(1, 2, false, 0)], 0, &mut out);
        // third span of the only trace: the trace is decided early (sampled),
        // and the new span follows as a late sampled span.
        s.ingest(vec![rec(1, 3, false, 0)], 1, &mut out);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].kind, ReleaseKind::DecidedEarly);
        assert_eq!(out[1].kind, ReleaseKind::Late);
        assert_eq!(s.cache_evictions(), 0);
        conserve(&s);
    }
}
