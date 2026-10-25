//! Bounded in-memory trace buffer.
//!
//! Traces are kept in a hash map keyed by trace id. A FIFO of
//! `(deadline, trace_id)` pairs orders them by first arrival; because every
//! trace gets the same decision wait, deadlines are non-decreasing along the
//! FIFO, so both "oldest trace" (for eviction) and "next expired trace" (for
//! the decision tick) are the FIFO front: O(1) amortised per operation.
//!
//! The buffer only enforces structure and accounting. Deciding *what* to do
//! when a limit is hit (decide early, drop, reject) is the sampler's job.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};

use crate::ids::TraceId;
use crate::model::SpanRecord;

/// Aggregates maintained incrementally as spans arrive, so policies do not
/// need to rescan the span list for common questions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceStats {
    /// Earliest span start (unix nanos).
    pub min_start_ns: u64,
    /// Latest span end (unix nanos).
    pub max_end_ns: u64,
    /// Spans with status `ERROR`.
    pub error_spans: usize,
    /// Spans dropped because the trace hit `max_spans_per_trace`.
    pub truncated_spans: u64,
}

impl Default for TraceStats {
    fn default() -> Self {
        TraceStats {
            min_start_ns: u64::MAX,
            max_end_ns: 0,
            error_spans: 0,
            truncated_spans: 0,
        }
    }
}

impl TraceStats {
    /// Fold one span into the aggregates.
    pub fn observe(&mut self, rec: &SpanRecord) {
        self.min_start_ns = self.min_start_ns.min(rec.span.start_time_unix_nano);
        self.max_end_ns = self.max_end_ns.max(rec.span.end_time_unix_nano);
        if rec.span.status.as_ref().is_some_and(|s| s.code == 2) {
            self.error_spans += 1;
        }
    }

    /// Trace duration in milliseconds (0 when no timing information).
    pub fn duration_ms(&self) -> u64 {
        if self.min_start_ns == u64::MAX || self.max_end_ns < self.min_start_ns {
            return 0;
        }
        (self.max_end_ns - self.min_start_ns) / 1_000_000
    }
}

/// A buffered trace awaiting its decision.
#[derive(Debug)]
pub struct TraceEntry {
    /// Buffered spans in arrival order.
    pub spans: Vec<SpanRecord>,
    /// Logical time (ms) the first span arrived.
    pub first_seen_ms: u64,
    /// Sum of span cost estimates.
    pub bytes: usize,
    /// Incremental aggregates.
    pub stats: TraceStats,
}

/// Hard limits enforced by the sampler around this buffer.
#[derive(Debug, Clone, Copy)]
pub struct BufferLimits {
    /// Maximum number of traces.
    pub max_traces: usize,
    /// Maximum estimated bytes.
    pub max_bytes: usize,
    /// Maximum spans per trace.
    pub max_spans_per_trace: usize,
    /// Decision wait in ms.
    pub decision_wait_ms: u64,
}

/// Outcome of [`TraceBuffer::append`].
#[derive(Debug, PartialEq, Eq)]
pub enum Append {
    /// Span stored.
    Stored,
    /// Trace already holds `max_spans_per_trace` spans; span discarded.
    TraceFull,
}

/// The buffer itself.
#[derive(Debug)]
pub struct TraceBuffer {
    limits: BufferLimits,
    traces: HashMap<TraceId, TraceEntry>,
    order: VecDeque<(u64, TraceId)>,
    bytes: usize,
    spans: usize,
}

impl TraceBuffer {
    /// Empty buffer with the given limits.
    pub fn new(limits: BufferLimits) -> Self {
        TraceBuffer {
            limits,
            traces: HashMap::new(),
            order: VecDeque::new(),
            bytes: 0,
            spans: 0,
        }
    }

    /// Configured limits.
    pub fn limits(&self) -> BufferLimits {
        self.limits
    }

    /// Number of buffered traces.
    pub fn len(&self) -> usize {
        self.traces.len()
    }

    /// True when no traces are buffered.
    pub fn is_empty(&self) -> bool {
        self.traces.is_empty()
    }

    /// Estimated bytes of buffered spans.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Number of buffered spans.
    pub fn span_count(&self) -> usize {
        self.spans
    }

    /// Whether a trace is buffered.
    pub fn contains(&self, id: &TraceId) -> bool {
        self.traces.contains_key(id)
    }

    /// Read access to a buffered trace.
    pub fn get(&self, id: &TraceId) -> Option<&TraceEntry> {
        self.traces.get(id)
    }

    /// Would `cost` more bytes (and possibly one more trace) fit?
    pub fn fits(&self, cost: usize, new_trace: bool) -> bool {
        let traces_ok = !new_trace || self.traces.len() < self.limits.max_traces;
        traces_ok && self.bytes + cost <= self.limits.max_bytes
    }

    /// Fraction of the tighter of the two limits currently in use.
    pub fn utilisation(&self) -> f64 {
        let t = self.traces.len() as f64 / self.limits.max_traces as f64;
        let b = self.bytes as f64 / self.limits.max_bytes as f64;
        t.max(b)
    }

    /// Store a span. The caller must have checked [`fits`](Self::fits); this
    /// method never evicts. Creates the trace entry if needed.
    pub fn append(&mut self, rec: SpanRecord, now_ms: u64) -> Append {
        let id = rec.trace_id;
        let entry = match self.traces.entry(id) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => {
                self.order
                    .push_back((now_ms + self.limits.decision_wait_ms, id));
                v.insert(TraceEntry {
                    spans: Vec::new(),
                    first_seen_ms: now_ms,
                    bytes: 0,
                    stats: TraceStats::default(),
                })
            }
        };
        if entry.spans.len() >= self.limits.max_spans_per_trace {
            entry.stats.truncated_spans += 1;
            return Append::TraceFull;
        }
        entry.stats.observe(&rec);
        entry.bytes += rec.cost;
        self.bytes += rec.cost;
        self.spans += 1;
        entry.spans.push(rec);
        Append::Stored
    }

    /// Remove and return the oldest trace.
    pub fn pop_oldest(&mut self) -> Option<(TraceId, TraceEntry)> {
        while let Some((_, id)) = self.order.pop_front() {
            if let Some(entry) = self.remove_entry(&id) {
                return Some((id, entry));
            }
        }
        None
    }

    /// Remove and return the oldest trace if its decision wait has elapsed.
    pub fn pop_expired(&mut self, now_ms: u64) -> Option<(TraceId, TraceEntry)> {
        match self.order.front() {
            Some(&(deadline, _)) if deadline <= now_ms => self.pop_oldest(),
            _ => None,
        }
    }

    /// Deadline of the oldest trace, if any.
    pub fn next_deadline(&self) -> Option<u64> {
        self.order.front().map(|(d, _)| *d)
    }

    fn remove_entry(&mut self, id: &TraceId) -> Option<TraceEntry> {
        let entry = self.traces.remove(id)?;
        self.bytes -= entry.bytes;
        self.spans -= entry.spans.len();
        Some(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ScopeContext, SpanRecord};
    use opentelemetry_proto::tonic::trace::v1::{Span, Status};
    use std::sync::Arc;

    fn limits() -> BufferLimits {
        BufferLimits {
            max_traces: 3,
            max_bytes: 100_000,
            max_spans_per_trace: 2,
            decision_wait_ms: 1_000,
        }
    }

    fn rec(trace: u8, start: u64, end: u64, error: bool) -> SpanRecord {
        let span = Span {
            trace_id: vec![trace; 16],
            span_id: vec![1; 8],
            start_time_unix_nano: start,
            end_time_unix_nano: end,
            status: error.then(|| Status {
                code: 2,
                message: String::new(),
            }),
            ..Default::default()
        };
        SpanRecord::new(
            TraceId([trace; 16]),
            span,
            Arc::new(ScopeContext::default()),
        )
    }

    #[test]
    fn append_tracks_stats_and_accounting() {
        let mut b = TraceBuffer::new(limits());
        assert!(b.is_empty());
        assert_eq!(
            b.append(rec(1, 5_000_000, 9_000_000, false), 0),
            Append::Stored
        );
        assert_eq!(
            b.append(rec(1, 1_000_000, 4_000_000, true), 10),
            Append::Stored
        );
        assert_eq!(b.append(rec(1, 0, 1, false), 10), Append::TraceFull);
        let e = b.get(&TraceId([1; 16])).unwrap();
        assert_eq!(e.stats.duration_ms(), 8);
        assert_eq!(e.stats.error_spans, 1);
        assert_eq!(e.stats.truncated_spans, 1);
        assert_eq!(e.first_seen_ms, 0);
        assert_eq!(b.span_count(), 2);
        assert_eq!(b.bytes(), e.bytes);
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn expiry_follows_first_arrival_order() {
        let mut b = TraceBuffer::new(limits());
        b.append(rec(1, 0, 0, false), 0);
        b.append(rec(2, 0, 0, false), 500);
        b.append(rec(1, 0, 0, false), 900);
        assert_eq!(b.next_deadline(), Some(1_000));
        assert!(b.pop_expired(999).is_none());
        let (id, e) = b.pop_expired(1_000).unwrap();
        assert_eq!(id, TraceId([1; 16]));
        assert_eq!(e.spans.len(), 2);
        assert!(b.pop_expired(1_499).is_none());
        assert!(b.pop_expired(1_500).is_some());
        assert!(b.is_empty());
        assert_eq!(b.bytes(), 0);
        assert_eq!(b.span_count(), 0);
        assert!(b.pop_oldest().is_none());
    }

    #[test]
    fn fits_respects_both_limits() {
        let mut b = TraceBuffer::new(BufferLimits {
            max_traces: 2,
            max_bytes: 10_000,
            ..limits()
        });
        let r = rec(1, 0, 0, false);
        let cost = r.cost;
        b.append(r, 0);
        b.append(rec(2, 0, 0, false), 0);
        assert!(!b.fits(1, true));
        assert!(b.fits(1, false));
        assert!(!b.fits(10_000 - 2 * cost + 1, false));
        assert!(b.utilisation() >= 1.0);
    }

    #[test]
    fn stats_without_timing() {
        let s = TraceStats::default();
        assert_eq!(s.duration_ms(), 0);
    }
}
