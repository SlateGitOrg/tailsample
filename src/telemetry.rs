//! Self-telemetry: lock-free counters for the async shell plus a snapshot
//! of the sampler's counters, rendered in Prometheus text format.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::sampler::{SamplerGauges, SamplerStats};

/// A monotonically increasing counter.
#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    /// Add `n`.
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }
    /// Add one.
    pub fn inc(&self) {
        self.add(1);
    }
    /// Current value.
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Snapshot of the sampling core, refreshed by the runtime.
#[derive(Debug, Clone, Default)]
pub struct CoreSnapshot {
    /// Cumulative counters.
    pub stats: SamplerStats,
    /// Gauges.
    pub gauges: SamplerGauges,
    /// Decision-cache evictions.
    pub cache_evictions: u64,
}

/// All process telemetry.
#[derive(Debug, Default)]
pub struct Telemetry {
    /// Export requests received over gRPC.
    pub grpc_requests: Counter,
    /// Export requests received over HTTP.
    pub http_requests: Counter,
    /// Requests refused because the ingest queue was full.
    pub rejected_queue_full: Counter,
    /// Requests refused because buffer admission was closed.
    pub rejected_admission: Counter,
    /// Requests refused during shutdown.
    pub rejected_shutdown: Counter,
    /// Requests that failed to decode.
    pub rejected_malformed: Counter,
    /// Spans accepted into the ingest queue.
    pub spans_enqueued: Counter,
    /// Export requests sent successfully.
    pub export_requests_ok: Counter,
    /// Spans exported successfully.
    pub export_spans_ok: Counter,
    /// Export attempts that failed (each retry counts).
    pub export_attempt_failures: Counter,
    /// Spans dropped after exhausting retries or on a permanent error.
    pub export_spans_dropped: Counter,
    /// Maximum ingest queue depth observed.
    pub ingest_queue_high_water: AtomicU64,
    core: Mutex<CoreSnapshot>,
}

impl Telemetry {
    /// Replace the core snapshot.
    pub fn publish(&self, snap: CoreSnapshot) {
        if let Ok(mut g) = self.core.lock() {
            *g = snap;
        }
    }

    /// Latest core snapshot.
    pub fn core(&self) -> CoreSnapshot {
        self.core.lock().map(|g| g.clone()).unwrap_or_default()
    }

    /// Track the ingest queue depth high-water mark.
    pub fn observe_queue_depth(&self, depth: u64) {
        self.ingest_queue_high_water
            .fetch_max(depth, Ordering::Relaxed);
    }

    /// Prometheus text exposition (version 0.0.4).
    pub fn render_prometheus(&self) -> String {
        let mut out = String::new();
        let mut counter = |name: &str, help: &str, v: u64| {
            let _ = writeln!(
                out,
                "# HELP {name} {help}\n# TYPE {name} counter\n{name} {v}"
            );
        };
        counter(
            "tailsample_receiver_requests_grpc_total",
            "OTLP/gRPC export requests.",
            self.grpc_requests.get(),
        );
        counter(
            "tailsample_receiver_requests_http_total",
            "OTLP/HTTP export requests.",
            self.http_requests.get(),
        );
        counter(
            "tailsample_receiver_rejected_queue_full_total",
            "Requests refused: ingest queue full.",
            self.rejected_queue_full.get(),
        );
        counter(
            "tailsample_receiver_rejected_admission_total",
            "Requests refused: buffer admission closed.",
            self.rejected_admission.get(),
        );
        counter(
            "tailsample_receiver_rejected_shutdown_total",
            "Requests refused during shutdown.",
            self.rejected_shutdown.get(),
        );
        counter(
            "tailsample_receiver_rejected_malformed_total",
            "Requests that failed to decode.",
            self.rejected_malformed.get(),
        );
        counter(
            "tailsample_receiver_spans_enqueued_total",
            "Spans accepted into the ingest queue.",
            self.spans_enqueued.get(),
        );
        counter(
            "tailsample_exporter_requests_ok_total",
            "Successful export requests.",
            self.export_requests_ok.get(),
        );
        counter(
            "tailsample_exporter_spans_ok_total",
            "Spans exported.",
            self.export_spans_ok.get(),
        );
        counter(
            "tailsample_exporter_attempt_failures_total",
            "Failed export attempts.",
            self.export_attempt_failures.get(),
        );
        counter(
            "tailsample_exporter_spans_dropped_total",
            "Spans dropped by the exporter.",
            self.export_spans_dropped.get(),
        );

        let core = self.core();
        let s = &core.stats;
        let spans = [
            ("received", s.spans_received),
            ("invalid", s.spans_invalid),
            ("rejected_full", s.spans_rejected_full),
            ("rejected_oversize", s.spans_rejected_oversize),
            ("dropped_trace_limit", s.spans_dropped_trace_limit),
            ("late_sampled", s.spans_late_sampled),
            ("late_dropped", s.spans_late_dropped),
            ("sampled", s.spans_sampled),
            ("not_sampled", s.spans_not_sampled),
            ("evicted", s.spans_evicted),
        ];
        let _ = writeln!(out, "# HELP tailsample_spans_total Spans by outcome.\n# TYPE tailsample_spans_total counter");
        for (k, v) in spans {
            let _ = writeln!(out, "tailsample_spans_total{{outcome=\"{k}\"}} {v}");
        }
        let traces = [
            ("received", s.traces_received),
            ("sampled", s.traces_sampled),
            ("not_sampled", s.traces_not_sampled),
            ("decided_early", s.traces_decided_early),
            ("evicted", s.traces_evicted),
        ];
        let _ = writeln!(out, "# HELP tailsample_traces_total Traces by outcome.\n# TYPE tailsample_traces_total counter");
        for (k, v) in traces {
            let _ = writeln!(out, "tailsample_traces_total{{outcome=\"{k}\"}} {v}");
        }
        let _ = writeln!(out, "# HELP tailsample_sampled_traces_by_policy_total Sampled traces per policy.\n# TYPE tailsample_sampled_traces_by_policy_total counter");
        for (k, v) in &s.sampled_by_policy {
            let _ = writeln!(
                out,
                "tailsample_sampled_traces_by_policy_total{{policy=\"{}\"}} {v}",
                k.replace('\\', "\\\\").replace('"', "\\\"")
            );
        }
        let g = &core.gauges;
        let mut gauge = |name: &str, help: &str, v: f64| {
            let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {v}");
        };
        gauge(
            "tailsample_buffer_traces",
            "Buffered traces.",
            g.traces as f64,
        );
        gauge("tailsample_buffer_spans", "Buffered spans.", g.spans as f64);
        gauge(
            "tailsample_buffer_bytes",
            "Estimated buffered bytes (spans plus shared resource/scope context).",
            g.bytes as f64,
        );
        gauge(
            "tailsample_buffer_context_bytes",
            "Part of tailsample_buffer_bytes charged for distinct resource/scope contexts.",
            g.context_bytes as f64,
        );
        gauge(
            "tailsample_decision_cache_sampled",
            "Cached sampled decisions.",
            g.cache_sizes.0 as f64,
        );
        gauge(
            "tailsample_decision_cache_not_sampled",
            "Cached not-sampled decisions.",
            g.cache_sizes.1 as f64,
        );
        gauge(
            "tailsample_admission_open",
            "1 when new traces are admitted.",
            if g.admission_open { 1.0 } else { 0.0 },
        );
        gauge(
            "tailsample_ingest_queue_high_water",
            "Maximum ingest queue depth observed.",
            self.ingest_queue_high_water.load(Ordering::Relaxed) as f64,
        );
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_counters_and_snapshot() {
        let t = Telemetry::default();
        t.grpc_requests.inc();
        t.export_spans_ok.add(5);
        t.observe_queue_depth(3);
        t.observe_queue_depth(1);
        let mut snap = CoreSnapshot::default();
        snap.stats.spans_received = 9;
        snap.stats
            .sampled_by_policy
            .insert("budget/\"err\"".into(), 2);
        snap.gauges.admission_open = true;
        snap.gauges.context_bytes = 640;
        t.publish(snap);
        let text = t.render_prometheus();
        assert!(text.contains("tailsample_receiver_requests_grpc_total 1"));
        assert!(text.contains("tailsample_exporter_spans_ok_total 5"));
        assert!(text.contains("tailsample_spans_total{outcome=\"received\"} 9"));
        assert!(text.contains("policy=\"budget/\\\"err\\\"\"} 2"));
        assert!(text.contains("tailsample_admission_open 1"));
        assert!(text.contains("tailsample_buffer_context_bytes 640"));
        assert!(text.contains("tailsample_ingest_queue_high_water 3"));
        assert_eq!(t.core().stats.spans_received, 9);
    }
}
