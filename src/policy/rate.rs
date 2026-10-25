//! Token-bucket rate limiting on spans.

use std::sync::Arc;

use super::{Policy, TraceView};

/// Samples a trace when the bucket holds at least as many tokens as the
/// trace has spans, then removes that many tokens. The bucket refills at
/// `spans_per_second` up to `burst` tokens. A trace larger than `burst`
/// can never be sampled by this policy.
#[derive(Debug)]
pub struct RateLimitingPolicy {
    label: Arc<str>,
    rate_per_ms: f64,
    burst: f64,
    tokens: f64,
    last_ms: Option<u64>,
}

impl RateLimitingPolicy {
    /// New policy; the bucket starts full.
    pub fn new(label: Arc<str>, spans_per_second: u64, burst: u64) -> Self {
        RateLimitingPolicy {
            label,
            rate_per_ms: spans_per_second as f64 / 1_000.0,
            burst: burst as f64,
            tokens: burst as f64,
            last_ms: None,
        }
    }

    fn refill(&mut self, now_ms: u64) {
        if let Some(last) = self.last_ms {
            let elapsed = now_ms.saturating_sub(last) as f64;
            self.tokens = (self.tokens + elapsed * self.rate_per_ms).min(self.burst);
        }
        self.last_ms = Some(self.last_ms.map_or(now_ms, |l| l.max(now_ms)));
    }
}

impl Policy for RateLimitingPolicy {
    fn name(&self) -> &str {
        &self.label
    }

    fn evaluate(&mut self, trace: &TraceView<'_>, now_ms: u64) -> Option<Arc<str>> {
        self.refill(now_ms);
        let need = trace.spans.len() as f64;
        if self.tokens >= need {
            self.tokens -= need;
            Some(Arc::clone(&self.label))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::testutil::trace;
    use super::*;

    #[test]
    fn enforces_rate_with_refill() {
        let mut p = RateLimitingPolicy::new(Arc::from("rl"), 10, 10);
        assert_eq!(p.name(), "rl");
        let t = trace(1, "s", &vec![(1, 0, vec![]); 4]);
        assert!(p.evaluate(&t.view(), 0).is_some());
        assert!(p.evaluate(&t.view(), 0).is_some());
        assert!(p.evaluate(&t.view(), 0).is_none()); // 2 tokens left
        assert!(p.evaluate(&t.view(), 199).is_none()); // ~3.99 tokens
        assert!(p.evaluate(&t.view(), 200).is_some()); // 4 tokens
                                                       // A clock going backwards never refills.
        assert!(p.evaluate(&t.view(), 100).is_none());
        // Refill is capped at burst.
        assert!(p.evaluate(&t.view(), 100_000).is_some());
        assert!(p.evaluate(&t.view(), 100_000).is_some());
        assert!(p.evaluate(&t.view(), 100_000).is_none());
    }

    #[test]
    fn oversized_trace_never_sampled() {
        let mut p = RateLimitingPolicy::new(Arc::from("rl"), 2, 2);
        let t = trace(1, "s", &vec![(1, 0, vec![]); 3]);
        assert!(p.evaluate(&t.view(), 0).is_none());
        assert!(p.evaluate(&t.view(), 1_000_000).is_none());
    }
}
