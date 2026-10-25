//! Offline simulation: drive the deterministic [`Sampler`] with a generated
//! workload on a logical clock and score the result against ground truth.
//!
//! This is what the budget tests, `tailsample simulate` and the core
//! benchmark use; no sockets or wall-clock time are involved, so results are
//! exactly reproducible for a given configuration and seed.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use crate::config::{Config, ConfigError};
use crate::ids::TraceId;
use crate::loadgen::{GenConfig, Schedule};
use crate::model::flatten;
use crate::sampler::{ReleaseKind, Released, Sampler, SamplerStats};

/// Simulation parameters.
#[derive(Debug, Clone)]
pub struct SimOptions {
    /// Traces to generate.
    pub traces: u64,
    /// Arrival rate, traces per second of simulated time.
    pub rate: f64,
    /// Workload generator settings.
    pub gen: GenConfig,
    /// Clock step in ms (delivery and tick granularity).
    pub step_ms: u64,
}

impl Default for SimOptions {
    fn default() -> Self {
        SimOptions {
            traces: 20_000,
            rate: 500.0,
            gen: GenConfig::default(),
            step_ms: 50,
        }
    }
}

/// Outcome for one class of traces.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClassReport {
    /// Traces generated.
    pub generated: u64,
    /// Traces with at least one exported span.
    pub kept: u64,
    /// Kept traces whose every span was exported.
    pub complete: u64,
    /// Spans generated.
    pub spans_generated: u64,
    /// Spans exported.
    pub spans_kept: u64,
}

impl ClassReport {
    /// Record one trace: spans generated and spans that reached the output.
    pub fn record(&mut self, generated_spans: usize, kept_spans: usize) {
        self.generated += 1;
        self.spans_generated += generated_spans as u64;
        self.spans_kept += kept_spans as u64;
        if kept_spans > 0 {
            self.kept += 1;
            if kept_spans == generated_spans {
                self.complete += 1;
            }
        }
    }

    /// Kept fraction in percent.
    pub fn kept_pct(&self) -> f64 {
        if self.generated == 0 {
            return 0.0;
        }
        100.0 * self.kept as f64 / self.generated as f64
    }
}

/// Simulation result.
#[derive(Debug, Clone, Default)]
pub struct SimReport {
    /// Traces with an injected error.
    pub errors: ClassReport,
    /// Traces with an injected latency spike and no error.
    pub slow: ClassReport,
    /// Everything else.
    pub normal: ClassReport,
    /// Final sampler counters.
    pub stats: SamplerStats,
    /// Highest estimated buffer bytes observed after any step.
    pub peak_buffer_bytes: usize,
    /// Highest buffered trace count observed after any step.
    pub peak_buffer_traces: usize,
    /// Highest number of spans released by a policy label within one
    /// one-second window (decision-time releases only).
    pub max_window_spans: BTreeMap<String, u64>,
    /// Simulated duration in ms.
    pub sim_ms: u64,
    /// Late spans generated.
    pub late_generated: u64,
    /// Decision-cache evictions.
    pub cache_evictions: u64,
}

impl SimReport {
    /// All classes combined.
    pub fn total(&self) -> ClassReport {
        let mut t = ClassReport::default();
        for c in [&self.errors, &self.slow, &self.normal] {
            t.generated += c.generated;
            t.kept += c.kept;
            t.complete += c.complete;
            t.spans_generated += c.spans_generated;
            t.spans_kept += c.spans_kept;
        }
        t
    }
}

/// Run a simulation.
pub fn simulate(cfg: &Config, opts: &SimOptions) -> Result<SimReport, ConfigError> {
    let mut sampler = Sampler::new(cfg)?;
    let mut schedule = Schedule::new(opts.gen.clone(), opts.traces, opts.rate, true);
    let mut exported: HashMap<TraceId, usize> = HashMap::new();
    let mut window_spans: HashMap<(String, u64), u64> = HashMap::new();
    let mut report = SimReport::default();
    let mut out: Vec<Released> = Vec::new();
    let step = opts.step_ms.max(1);
    let mut now = 0u64;

    let mut absorb = |out: &mut Vec<Released>, now: u64, report: &mut SimReport| {
        for r in out.drain(..) {
            *exported.entry(r.trace_id).or_default() += r.spans.len();
            if r.kind != ReleaseKind::Late {
                let key = (r.policy.to_string(), now / 1_000);
                let v = window_spans.entry(key).or_default();
                *v += r.spans.len() as u64;
                let m = report
                    .max_window_spans
                    .entry(r.policy.to_string())
                    .or_default();
                *m = (*m).max(*v);
            }
        }
    };

    while !schedule.done() {
        for req in schedule.due(now) {
            let flat = flatten(req);
            sampler.record_invalid(flat.invalid);
            sampler.ingest(flat.records, now, &mut out);
        }
        sampler.tick(now, &mut out);
        absorb(&mut out, now, &mut report);
        let g = sampler.gauges();
        report.peak_buffer_bytes = report.peak_buffer_bytes.max(g.bytes);
        report.peak_buffer_traces = report.peak_buffer_traces.max(g.traces);
        now += step;
    }
    // Let remaining traces reach their deadline instead of flushing early.
    while let Some(deadline) = sampler.next_deadline() {
        now = now.max(deadline);
        sampler.tick(now, &mut out);
        absorb(&mut out, now, &mut report);
    }
    report.sim_ms = now;
    report.stats = sampler.stats().clone();
    report.cache_evictions = sampler.cache_evictions();

    if let Some(truth) = schedule.truth() {
        for (id, t) in truth {
            let kept = exported.get(id).copied().unwrap_or(0);
            report.late_generated += u64::from(t.has_late_span);
            let class = if t.error {
                &mut report.errors
            } else if t.slow {
                &mut report.slow
            } else {
                &mut report.normal
            };
            class.record(t.spans, kept);
        }
    }
    Ok(report)
}

impl fmt::Display for SimReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{:<8} {:>9} {:>9} {:>8} {:>9} {:>11} {:>11}",
            "class", "traces", "kept", "kept%", "complete", "spans_in", "spans_out"
        )?;
        let total = self.total();
        for (name, c) in [
            ("error", &self.errors),
            ("slow", &self.slow),
            ("normal", &self.normal),
            ("total", &total),
        ] {
            writeln!(
                f,
                "{:<8} {:>9} {:>9} {:>7.2}% {:>9} {:>11} {:>11}",
                name,
                c.generated,
                c.kept,
                c.kept_pct(),
                c.complete,
                c.spans_generated,
                c.spans_kept
            )?;
        }
        writeln!(f)?;
        writeln!(
            f,
            "sampled traces by policy (max spans released in any 1s window):"
        )?;
        for (k, v) in &self.stats.sampled_by_policy {
            let w = self.max_window_spans.get(k).copied().unwrap_or(0);
            writeln!(f, "  {k:<28} {v:>8} traces   max {w:>6} spans/s")?;
        }
        let s = &self.stats;
        writeln!(f)?;
        writeln!(
            f,
            "buffer peak: {} traces, {:.1} MiB estimated | decided early: {} | evicted: {} | rejected spans: {}",
            self.peak_buffer_traces,
            self.peak_buffer_bytes as f64 / (1024.0 * 1024.0),
            s.traces_decided_early,
            s.traces_evicted,
            s.spans_rejected_full
        )?;
        writeln!(
            f,
            "spans arriving after their trace's decision: {} forwarded, {} dropped ({} injected late spans) | cache evictions: {}",
            s.spans_late_sampled, s.spans_late_dropped, self.late_generated, self.cache_evictions
        )?;
        write!(
            f,
            "simulated {:.1}s, span reduction {:.1}%",
            self.sim_ms as f64 / 1_000.0,
            100.0 - 100.0 * total.spans_kept as f64 / total.spans_generated.max(1) as f64
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn always_sample_keeps_everything_complete() {
        let cfg = Config::from_toml_str("[buffer]\ndecision_wait_ms = 1000").unwrap();
        let r = simulate(
            &cfg,
            &SimOptions {
                traces: 500,
                rate: 200.0,
                ..SimOptions::default()
            },
        )
        .unwrap();
        let t = r.total();
        assert_eq!(t.generated, 500);
        assert_eq!(t.kept, 500);
        assert_eq!(
            t.complete, 500,
            "late spans must be forwarded via the cache"
        );
        assert_eq!(
            r.stats.spans_received,
            r.stats.spans_sampled + r.stats.spans_late_sampled
        );
        let text = r.to_string();
        assert!(text.contains("total"));
        assert!(text.contains("always"));
        assert_eq!(ClassReport::default().kept_pct(), 0.0);
    }
}
