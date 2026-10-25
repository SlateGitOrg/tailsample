//! Composite (budgeted) and AND policies.

use std::sync::Arc;

use super::{Policy, TraceView};
use crate::config::RateAllocation;

/// Compute per-sub-policy span budgets (spans per second) from explicit
/// allocations; sub-policies without an allocation share the remainder
/// equally. Budgets are floored, so their sum never exceeds `total`.
pub fn allocate(total: u64, names: &[&str], allocations: &[RateAllocation]) -> Vec<u64> {
    let explicit: Vec<Option<f64>> = names
        .iter()
        .map(|n| {
            allocations
                .iter()
                .find(|a| a.policy == *n)
                .map(|a| a.percent)
        })
        .collect();
    let used: f64 = explicit.iter().flatten().sum();
    let implicit = explicit.iter().filter(|e| e.is_none()).count();
    let share = if implicit > 0 {
        ((100.0 - used).max(0.0)) / implicit as f64
    } else {
        0.0
    };
    explicit
        .iter()
        .map(|e| ((total as f64) * e.unwrap_or(share) / 100.0).floor() as u64)
        .collect()
}

/// Evaluates sub-policies in order. A sub-policy that wants a trace gets it
/// only if the trace's spans fit in that sub-policy's remaining budget for
/// the current one-second window; otherwise the next sub-policy is tried.
/// Budgets reset at every window boundary (`now_ms / 1000`).
#[derive(Debug)]
pub struct CompositePolicy {
    label: Arc<str>,
    subs: Vec<Box<dyn Policy>>,
    budgets: Vec<u64>,
    used: Vec<u64>,
    window: Option<u64>,
}

impl CompositePolicy {
    /// New composite. `budgets[i]` is the per-second span budget of `subs[i]`.
    pub fn new(label: Arc<str>, subs: Vec<Box<dyn Policy>>, budgets: Vec<u64>) -> Self {
        let used = vec![0; subs.len()];
        CompositePolicy {
            label,
            subs,
            budgets,
            used,
            window: None,
        }
    }

    /// Per-sub-policy budgets (spans per second).
    pub fn budgets(&self) -> &[u64] {
        &self.budgets
    }
}

impl Policy for CompositePolicy {
    fn name(&self) -> &str {
        &self.label
    }

    fn evaluate(&mut self, trace: &TraceView<'_>, now_ms: u64) -> Option<Arc<str>> {
        let window = now_ms / 1_000;
        if self.window != Some(window) {
            self.window = Some(window);
            self.used.iter_mut().for_each(|u| *u = 0);
        }
        let n = trace.spans.len() as u64;
        for i in 0..self.subs.len() {
            if self.used[i] + n > self.budgets[i] {
                continue;
            }
            if let Some(label) = self.subs[i].evaluate(trace, now_ms) {
                self.used[i] += n;
                return Some(label);
            }
        }
        None
    }
}

/// Samples only when every sub-policy samples; the label is the AND policy's.
#[derive(Debug)]
pub struct AndPolicy {
    label: Arc<str>,
    subs: Vec<Box<dyn Policy>>,
}

impl AndPolicy {
    /// New AND policy.
    pub fn new(label: Arc<str>, subs: Vec<Box<dyn Policy>>) -> Self {
        AndPolicy { label, subs }
    }
}

impl Policy for AndPolicy {
    fn name(&self) -> &str {
        &self.label
    }

    fn evaluate(&mut self, trace: &TraceView<'_>, now_ms: u64) -> Option<Arc<str>> {
        let all = self
            .subs
            .iter_mut()
            .all(|p| p.evaluate(trace, now_ms).is_some());
        all.then(|| Arc::clone(&self.label))
    }
}

#[cfg(test)]
mod tests {
    use super::super::testutil::trace;
    use super::super::{AlwaysSample, LatencyPolicy, StatusCodePolicy};
    use super::*;
    use crate::config::StatusCodeName;

    fn alloc(p: &str, pct: f64) -> RateAllocation {
        RateAllocation {
            policy: p.into(),
            percent: pct,
        }
    }

    #[test]
    fn allocation_splits_remainder() {
        assert_eq!(
            allocate(1000, &["a", "b", "c"], &[alloc("a", 50.0)]),
            vec![500, 250, 250]
        );
        assert_eq!(allocate(1000, &["a", "b"], &[]), vec![500, 500]);
        assert_eq!(allocate(10, &["a"], &[alloc("a", 33.0)]), vec![3]);
        assert_eq!(
            allocate(100, &["a", "b"], &[alloc("a", 100.0)]),
            vec![100, 0]
        );
    }

    fn comp() -> CompositePolicy {
        let subs: Vec<Box<dyn Policy>> = vec![
            Box::new(StatusCodePolicy::new(
                Arc::from("c/err"),
                &[StatusCodeName::Error],
            )),
            Box::new(AlwaysSample::new(Arc::from("c/rest"))),
        ];
        CompositePolicy::new(Arc::from("c"), subs, vec![6, 4])
    }

    #[test]
    fn composite_respects_budgets_per_window() {
        let mut c = comp();
        assert_eq!(c.name(), "c");
        assert_eq!(c.budgets(), &[6, 4]);
        let err = trace(1, "s", &vec![(1, 2, vec![]); 3]);
        let ok = trace(2, "s", &vec![(1, 0, vec![]); 2]);
        assert_eq!(c.evaluate(&err.view(), 0).as_deref(), Some("c/err"));
        assert_eq!(c.evaluate(&err.view(), 10).as_deref(), Some("c/err"));
        // err budget exhausted (6/6): error trace falls through to "rest".
        assert_eq!(c.evaluate(&err.view(), 20).as_deref(), Some("c/rest"));
        assert_eq!(c.evaluate(&ok.view(), 30), None); // rest: 3 + 2 > 4
                                                      // New window resets budgets.
        assert_eq!(c.evaluate(&ok.view(), 1_000).as_deref(), Some("c/rest"));
        assert_eq!(c.evaluate(&err.view(), 1_001).as_deref(), Some("c/err"));
    }

    #[test]
    fn and_requires_all() {
        let subs: Vec<Box<dyn Policy>> = vec![
            Box::new(StatusCodePolicy::new(
                Arc::from("a/err"),
                &[StatusCodeName::Error],
            )),
            Box::new(LatencyPolicy::new(Arc::from("a/slow"), 100, None)),
        ];
        let mut a = AndPolicy::new(Arc::from("a"), subs);
        assert_eq!(a.name(), "a");
        assert!(a
            .evaluate(&trace(1, "s", &[(200, 2, vec![])]).view(), 0)
            .is_some());
        assert!(a
            .evaluate(&trace(1, "s", &[(20, 2, vec![])]).view(), 0)
            .is_none());
        assert!(a
            .evaluate(&trace(1, "s", &[(200, 0, vec![])]).view(), 0)
            .is_none());
    }
}
