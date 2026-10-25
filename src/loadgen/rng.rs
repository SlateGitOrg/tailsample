//! Small deterministic PRNG (SplitMix64). Used instead of an external crate
//! so generated workloads stay bit-identical across dependency upgrades.

/// SplitMix64 generator.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    /// Seeded generator.
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }

    /// Next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, 1)`.
    pub fn f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// True with probability `p`.
    pub fn chance(&mut self, p: f64) -> bool {
        self.f64() < p
    }

    /// Uniform integer in `[lo, hi]`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            return lo;
        }
        lo + self.next_u64() % (hi - lo + 1)
    }

    /// Log-normal sample with the given median and shape `sigma`.
    pub fn lognormal(&mut self, median: f64, sigma: f64) -> f64 {
        // Box-Muller transform for a standard normal.
        let u1 = self.f64().max(f64::MIN_POSITIVE);
        let u2 = self.f64();
        let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
        median * (sigma * z).exp()
    }

    /// Pick an index according to `weights`.
    pub fn weighted(&mut self, weights: &[f64]) -> usize {
        let total: f64 = weights.iter().sum();
        let mut x = self.f64() * total;
        for (i, w) in weights.iter().enumerate() {
            if x < *w {
                return i;
            }
            x -= w;
        }
        weights.len().saturating_sub(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_and_in_range() {
        let mut a = Rng::new(1);
        let mut b = Rng::new(1);
        assert_eq!(a.next_u64(), b.next_u64());
        for _ in 0..1_000 {
            let f = a.f64();
            assert!((0.0..1.0).contains(&f));
            let r = a.range(3, 5);
            assert!((3..=5).contains(&r));
        }
        assert_eq!(a.range(4, 4), 4);
        assert_eq!(a.range(9, 2), 9);
    }

    #[test]
    fn lognormal_median_is_close() {
        let mut r = Rng::new(7);
        let mut v: Vec<f64> = (0..10_001).map(|_| r.lognormal(10.0, 0.5)).collect();
        v.sort_by(|a, b| a.total_cmp(b));
        let median = v[5_000];
        assert!((9.0..11.0).contains(&median), "{median}");
    }

    #[test]
    fn weighted_choice_distribution() {
        let mut r = Rng::new(3);
        let mut counts = [0usize; 3];
        for _ in 0..10_000 {
            counts[r.weighted(&[0.7, 0.2, 0.1])] += 1;
        }
        assert!((6_500..7_500).contains(&counts[0]), "{counts:?}");
        assert!((500..1_500).contains(&counts[2]), "{counts:?}");
        assert_eq!(r.weighted(&[]), 0);
    }
}
