//! Stateless policies.

use std::sync::Arc;

use super::{Policy, TraceView};
use crate::config::StatusCodeName;

/// Samples everything.
#[derive(Debug)]
pub struct AlwaysSample {
    label: Arc<str>,
}

impl AlwaysSample {
    /// New policy.
    pub fn new(label: Arc<str>) -> Self {
        AlwaysSample { label }
    }
}

impl Policy for AlwaysSample {
    fn name(&self) -> &str {
        &self.label
    }
    fn evaluate(&mut self, _trace: &TraceView<'_>, _now_ms: u64) -> Option<Arc<str>> {
        Some(Arc::clone(&self.label))
    }
}

/// Samples traces whose end-to-end duration falls in `[lower, upper]`.
#[derive(Debug)]
pub struct LatencyPolicy {
    label: Arc<str>,
    lower_ms: u64,
    upper_ms: Option<u64>,
}

impl LatencyPolicy {
    /// New policy.
    pub fn new(label: Arc<str>, lower_ms: u64, upper_ms: Option<u64>) -> Self {
        LatencyPolicy {
            label,
            lower_ms,
            upper_ms,
        }
    }
}

impl Policy for LatencyPolicy {
    fn name(&self) -> &str {
        &self.label
    }
    fn evaluate(&mut self, trace: &TraceView<'_>, _now_ms: u64) -> Option<Arc<str>> {
        let d = trace.stats.duration_ms();
        let ok = d >= self.lower_ms && self.upper_ms.is_none_or(|u| d <= u);
        ok.then(|| Arc::clone(&self.label))
    }
}

/// Samples traces containing a span with one of the configured status codes.
#[derive(Debug)]
pub struct StatusCodePolicy {
    label: Arc<str>,
    codes: Vec<i32>,
}

impl StatusCodePolicy {
    /// New policy.
    pub fn new(label: Arc<str>, codes: &[StatusCodeName]) -> Self {
        StatusCodePolicy {
            label,
            codes: codes.iter().map(|c| c.as_i32()).collect(),
        }
    }
}

impl Policy for StatusCodePolicy {
    fn name(&self) -> &str {
        &self.label
    }
    fn evaluate(&mut self, trace: &TraceView<'_>, _now_ms: u64) -> Option<Arc<str>> {
        // Fast path: errors are pre-counted by the buffer.
        if self.codes == [2] {
            return (trace.stats.error_spans > 0).then(|| Arc::clone(&self.label));
        }
        let hit = trace.spans.iter().any(|r| {
            let code = r.span.status.as_ref().map_or(0, |s| s.code);
            self.codes.contains(&code)
        });
        hit.then(|| Arc::clone(&self.label))
    }
}

/// Samples traces whose span count is within bounds.
#[derive(Debug)]
pub struct SpanCountPolicy {
    label: Arc<str>,
    min: usize,
    max: Option<usize>,
}

impl SpanCountPolicy {
    /// New policy.
    pub fn new(label: Arc<str>, min: usize, max: Option<usize>) -> Self {
        SpanCountPolicy { label, min, max }
    }
}

impl Policy for SpanCountPolicy {
    fn name(&self) -> &str {
        &self.label
    }
    fn evaluate(&mut self, trace: &TraceView<'_>, _now_ms: u64) -> Option<Arc<str>> {
        let n = trace.spans.len();
        let ok = n >= self.min && self.max.is_none_or(|m| n <= m);
        ok.then(|| Arc::clone(&self.label))
    }
}

/// Deterministic trace-id-ratio sampling: FNV-1a(seed ‖ trace id) compared
/// against `percentage` of the 64-bit hash space. Every instance configured
/// with the same seed makes the same choice for the same trace.
#[derive(Debug)]
pub struct ProbabilisticPolicy {
    label: Arc<str>,
    threshold: u64,
    always: bool,
    seed: u32,
}

impl ProbabilisticPolicy {
    /// New policy; `percentage` in `[0, 100]`.
    pub fn new(label: Arc<str>, percentage: f64, seed: u32) -> Self {
        let frac = (percentage / 100.0).clamp(0.0, 1.0);
        ProbabilisticPolicy {
            label,
            threshold: (frac * u64::MAX as f64) as u64,
            always: frac >= 1.0,
            seed,
        }
    }
}

/// FNV-1a 64-bit over the seed and trace id bytes.
pub fn trace_hash(seed: u32, id: &[u8; 16]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    seed.to_le_bytes()
        .iter()
        .chain(id.iter())
        .fold(OFFSET, |h, b| (h ^ u64::from(*b)).wrapping_mul(PRIME))
}

impl Policy for ProbabilisticPolicy {
    fn name(&self) -> &str {
        &self.label
    }
    fn evaluate(&mut self, trace: &TraceView<'_>, _now_ms: u64) -> Option<Arc<str>> {
        let hit = self.always || trace_hash(self.seed, &trace.id.0) < self.threshold;
        hit.then(|| Arc::clone(&self.label))
    }
}

#[cfg(test)]
mod tests {
    use super::super::testutil::trace;
    use super::*;

    fn l(s: &str) -> Arc<str> {
        Arc::from(s)
    }

    #[test]
    fn latency_bounds() {
        let mut p = LatencyPolicy::new(l("lat"), 100, Some(200));
        assert_eq!(p.name(), "lat");
        assert!(p
            .evaluate(&trace(1, "s", &[(150, 0, vec![])]).view(), 0)
            .is_some());
        assert!(p
            .evaluate(&trace(1, "s", &[(99, 0, vec![])]).view(), 0)
            .is_none());
        assert!(p
            .evaluate(&trace(1, "s", &[(201, 0, vec![])]).view(), 0)
            .is_none());
        let mut open = LatencyPolicy::new(l("lat"), 100, None);
        assert!(open
            .evaluate(&trace(1, "s", &[(10_000, 0, vec![])]).view(), 0)
            .is_some());
    }

    #[test]
    fn status_codes() {
        let mut err = StatusCodePolicy::new(l("e"), &[StatusCodeName::Error]);
        assert_eq!(err.name(), "e");
        assert!(err
            .evaluate(&trace(1, "s", &[(1, 0, vec![]), (1, 2, vec![])]).view(), 0)
            .is_some());
        assert!(err
            .evaluate(&trace(1, "s", &[(1, 1, vec![])]).view(), 0)
            .is_none());
        let mut ok = StatusCodePolicy::new(l("o"), &[StatusCodeName::Ok, StatusCodeName::Unset]);
        assert!(ok
            .evaluate(&trace(1, "s", &[(1, 1, vec![])]).view(), 0)
            .is_some());
        assert!(ok
            .evaluate(&trace(1, "s", &[(1, 2, vec![])]).view(), 0)
            .is_none());
    }

    #[test]
    fn span_count_bounds() {
        let mut p = SpanCountPolicy::new(l("c"), 2, Some(3));
        assert_eq!(p.name(), "c");
        let one = trace(1, "s", &[(1, 0, vec![])]);
        let two = trace(1, "s", &[(1, 0, vec![]), (1, 0, vec![])]);
        let four = trace(1, "s", &vec![(1, 0, vec![]); 4]);
        assert!(p.evaluate(&one.view(), 0).is_none());
        assert!(p.evaluate(&two.view(), 0).is_some());
        assert!(p.evaluate(&four.view(), 0).is_none());
    }

    #[test]
    fn probabilistic_rate_is_close_and_deterministic() {
        let mut p = ProbabilisticPolicy::new(l("p"), 25.0, 7);
        assert_eq!(p.name(), "p");
        let hits = (1..=20_000u128)
            .filter(|i| {
                let t = trace(
                    i.wrapping_mul(0x9e37_79b9_7f4a_7c15),
                    "s",
                    &[(1, 0, vec![])],
                );
                p.evaluate(&t.view(), 0).is_some()
            })
            .count();
        assert!((4_500..=5_500).contains(&hits), "hits = {hits}");
        let t = trace(42, "s", &[(1, 0, vec![])]);
        let a = p.evaluate(&t.view(), 0).is_some();
        let b = ProbabilisticPolicy::new(l("q"), 25.0, 7)
            .evaluate(&t.view(), 0)
            .is_some();
        assert_eq!(a, b);
        let mut all = ProbabilisticPolicy::new(l("all"), 100.0, 0);
        let mut none = ProbabilisticPolicy::new(l("none"), 0.0, 0);
        assert!(all.evaluate(&t.view(), 0).is_some());
        assert!(none.evaluate(&t.view(), 0).is_none());
    }

    #[test]
    fn always() {
        let mut p = AlwaysSample::new(l("a"));
        assert_eq!(p.name(), "a");
        assert!(p.evaluate(&trace(1, "s", &[]).view(), 0).is_some());
    }
}
