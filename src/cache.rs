//! Decision cache: remembers recent decisions so spans arriving after their
//! trace was decided ("late spans") follow the same decision.
//!
//! Two independent FIFO-bounded sets are used, one per outcome, because the
//! cost of forgetting differs: forgetting a "sampled" decision splits a kept
//! trace, forgetting "not sampled" merely re-buffers a few stragglers.

use std::collections::{HashSet, VecDeque};

use crate::ids::TraceId;

/// A remembered decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Trace was exported.
    Sampled,
    /// Trace was dropped (by policy or by eviction).
    NotSampled,
}

#[derive(Debug)]
struct BoundedSet {
    cap: usize,
    set: HashSet<TraceId>,
    order: VecDeque<TraceId>,
}

impl BoundedSet {
    fn new(cap: usize) -> Self {
        BoundedSet {
            cap,
            set: HashSet::new(),
            order: VecDeque::new(),
        }
    }

    /// Insert; returns true when an older id was evicted to make room.
    fn insert(&mut self, id: TraceId) -> bool {
        if !self.set.insert(id) {
            return false;
        }
        self.order.push_back(id);
        if self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            }
            return true;
        }
        false
    }

    fn contains(&self, id: &TraceId) -> bool {
        self.set.contains(id)
    }
}

/// Bounded decision cache. Memory is `O(sampled_capacity + not_sampled_capacity)`.
#[derive(Debug)]
pub struct DecisionCache {
    sampled: BoundedSet,
    not_sampled: BoundedSet,
    evictions: u64,
}

impl DecisionCache {
    /// Create a cache with the given capacities (0 disables that half).
    pub fn new(sampled_capacity: usize, not_sampled_capacity: usize) -> Self {
        DecisionCache {
            sampled: BoundedSet::new(sampled_capacity),
            not_sampled: BoundedSet::new(not_sampled_capacity),
            evictions: 0,
        }
    }

    /// Record a decision.
    pub fn record(&mut self, id: TraceId, decision: Decision) {
        let evicted = match decision {
            Decision::Sampled => self.sampled.insert(id),
            Decision::NotSampled => self.not_sampled.insert(id),
        };
        if evicted {
            self.evictions += 1;
        }
    }

    /// Look up a decision. "Sampled" wins if a trace id is in both halves
    /// (it was re-buffered and later kept).
    pub fn get(&self, id: &TraceId) -> Option<Decision> {
        if self.sampled.contains(id) {
            Some(Decision::Sampled)
        } else if self.not_sampled.contains(id) {
            Some(Decision::NotSampled)
        } else {
            None
        }
    }

    /// Entries currently held (sampled, not sampled).
    pub fn sizes(&self) -> (usize, usize) {
        (self.sampled.set.len(), self.not_sampled.set.len())
    }

    /// Total entries evicted due to capacity.
    pub fn evictions(&self) -> u64 {
        self.evictions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u128) -> TraceId {
        TraceId::from_u128(n)
    }

    #[test]
    fn remembers_and_forgets_fifo() {
        let mut c = DecisionCache::new(2, 1);
        c.record(id(1), Decision::Sampled);
        c.record(id(2), Decision::Sampled);
        c.record(id(2), Decision::Sampled);
        assert_eq!(c.sizes(), (2, 0));
        c.record(id(3), Decision::Sampled);
        assert_eq!(c.get(&id(1)), None);
        assert_eq!(c.get(&id(3)), Some(Decision::Sampled));
        c.record(id(4), Decision::NotSampled);
        c.record(id(5), Decision::NotSampled);
        assert_eq!(c.get(&id(4)), None);
        assert_eq!(c.get(&id(5)), Some(Decision::NotSampled));
        assert_eq!(c.evictions(), 2);
    }

    #[test]
    fn sampled_wins_over_not_sampled() {
        let mut c = DecisionCache::new(4, 4);
        c.record(id(9), Decision::NotSampled);
        c.record(id(9), Decision::Sampled);
        assert_eq!(c.get(&id(9)), Some(Decision::Sampled));
    }

    #[test]
    fn zero_capacity_disables() {
        let mut c = DecisionCache::new(0, 0);
        c.record(id(1), Decision::Sampled);
        assert_eq!(c.get(&id(1)), None);
        assert_eq!(c.sizes(), (0, 0));
    }
}
