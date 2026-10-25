//! Synthetic trace generator with injected slow and error traces.

use std::sync::Arc;

use opentelemetry_proto::tonic::common::v1::KeyValue;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::Event;
use opentelemetry_proto::tonic::trace::v1::{Span, Status};

use super::rng::Rng;
use super::topology::{Kind, Op, Protocol, ENTRY_POINTS, OPS, SERVICES};
use crate::ids::TraceId;
use crate::model::{kv_int, kv_str};

/// Generator parameters.
#[derive(Debug, Clone)]
pub struct GenConfig {
    /// PRNG seed.
    pub seed: u64,
    /// Probability a trace gets an injected failure.
    pub error_rate: f64,
    /// Probability a trace gets an injected latency spike.
    pub slow_rate: f64,
    /// Extra latency added by a spike, inclusive range in ms.
    pub slow_extra_ms: (u64, u64),
    /// Probability that one span of a trace is exported late.
    pub late_rate: f64,
    /// Delay of a late span after the trace's last regular span, in ms.
    pub late_delay_ms: (u64, u64),
    /// Delay between a span ending and its export arriving, in ms.
    pub export_delay_ms: (u64, u64),
    /// Wall-clock origin (unix nanos) for span timestamps.
    pub epoch_unix_nanos: u64,
}

impl Default for GenConfig {
    fn default() -> Self {
        GenConfig {
            seed: 42,
            error_rate: 0.02,
            slow_rate: 0.02,
            slow_extra_ms: (1_200, 3_000),
            late_rate: 0.01,
            late_delay_ms: (6_000, 9_000),
            export_delay_ms: (0, 200),
            epoch_unix_nanos: 1_760_000_000_000_000_000,
        }
    }
}

/// Ground truth recorded for a generated trace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Truth {
    /// At least one span has status ERROR.
    pub error: bool,
    /// A latency spike was injected.
    pub slow: bool,
    /// Number of spans.
    pub spans: usize,
    /// Root span duration in ms.
    pub duration_ms: u64,
    /// One span is delivered late.
    pub has_late_span: bool,
    /// Root is the health-check endpoint.
    pub health_check: bool,
}

/// A generated span plus delivery metadata.
#[derive(Debug, Clone)]
pub struct GenSpan {
    /// Index into [`SERVICES`].
    pub service: usize,
    /// The span.
    pub span: Span,
    /// Arrival time at the sampler, in ms on the schedule clock.
    pub arrival_ms: u64,
    /// Whether this is the trace's late span.
    pub late: bool,
}

/// A generated trace.
#[derive(Debug, Clone)]
pub struct GenTrace {
    /// Trace id.
    pub id: TraceId,
    /// Spans with arrival times.
    pub spans: Vec<GenSpan>,
    /// Ground truth.
    pub truth: Truth,
}

struct Node {
    service: usize,
    parent: Option<usize>,
    children: Vec<usize>,
    self_ms: f64,
    start_ms: f64,
    end_ms: f64,
    error: bool,
    error_origin: bool,
    name: String,
    kind: Kind,
    protocol: Protocol,
}

/// Deterministic trace generator.
#[derive(Debug, Clone)]
pub struct Generator {
    cfg: GenConfig,
    rng: Rng,
    resources: Vec<Arc<Resource>>,
    next_span_id: u64,
}

impl Generator {
    /// New generator.
    pub fn new(cfg: GenConfig) -> Self {
        let resources = SERVICES
            .iter()
            .enumerate()
            .map(|(i, (name, version))| {
                Arc::new(Resource {
                    attributes: vec![
                        kv_str("service.name", *name),
                        kv_str("service.version", *version),
                        kv_str("deployment.environment.name", "loadgen"),
                        kv_str("host.name", format!("{name}-{}", 7 + i)),
                    ],
                    ..Default::default()
                })
            })
            .collect();
        let rng = Rng::new(cfg.seed);
        Generator {
            cfg,
            rng,
            resources,
            next_span_id: 1,
        }
    }

    /// Shared resource of a service.
    pub fn resource(&self, service: usize) -> Arc<Resource> {
        Arc::clone(&self.resources[service])
    }

    /// Generate one trace whose root starts at `start_ms` on the schedule clock.
    pub fn trace(&mut self, start_ms: u64) -> GenTrace {
        let id = loop {
            let hi = u128::from(self.rng.next_u64()) << 64;
            if let Some(id) = TraceId::from_slice(&(hi | u128::from(self.rng.next_u64())).to_be_bytes()) {
                break id;
            }
        };
        let weights: Vec<f64> = ENTRY_POINTS.iter().map(|e| e.1).collect();
        let root_op = ENTRY_POINTS[self.rng.weighted(&weights)].0;
        let mut nodes = Vec::new();
        self.build(root_op, None, &mut nodes);

        let health_check = root_op == 15;
        let error = !health_check && self.rng.chance(self.cfg.error_rate) && nodes.len() > 1;
        if error {
            let victim = self.rng.range(1, nodes.len() as u64 - 1) as usize;
            nodes[victim].error_origin = true;
            let mut cur = Some(victim);
            while let Some(i) = cur {
                nodes[i].error = true;
                cur = nodes[i].parent;
            }
        }
        let slow = !health_check && self.rng.chance(self.cfg.slow_rate);
        if slow {
            let victim = self.rng.range(0, nodes.len() as u64 - 1) as usize;
            let (lo, hi) = self.cfg.slow_extra_ms;
            nodes[victim].self_ms += self.rng.range(lo, hi) as f64;
        }
        layout(&mut nodes, 0, 0.0);

        let has_late_span = nodes.len() > 1 && self.rng.chance(self.cfg.late_rate);
        let late_idx = if has_late_span {
            Some(self.rng.range(1, nodes.len() as u64 - 1) as usize)
        } else {
            None
        };
        let root_end = nodes[0].end_ms;
        let truth = Truth {
            error,
            slow,
            spans: nodes.len(),
            duration_ms: (nodes[0].end_ms - nodes[0].start_ms) as u64,
            has_late_span,
            health_check,
        };
        let span_ids: Vec<u64> = nodes
            .iter()
            .map(|_| {
                let v = self.next_span_id;
                self.next_span_id += 1;
                v
            })
            .collect();
        let mut spans = Vec::with_capacity(nodes.len());
        for (i, n) in nodes.iter().enumerate() {
            let (lo, hi) = self.cfg.export_delay_ms;
            let mut arrival = start_ms + n.end_ms as u64 + self.rng.range(lo, hi);
            let late = late_idx == Some(i);
            if late {
                let (lo, hi) = self.cfg.late_delay_ms;
                arrival = start_ms + root_end as u64 + self.rng.range(lo, hi);
            }
            let span = self.to_span(id, n, span_ids[i], n.parent.map(|p| span_ids[p]), start_ms, i == 0);
            spans.push(GenSpan {
                service: n.service,
                span,
                arrival_ms: arrival,
                late,
            });
        }
        GenTrace { id, spans, truth }
    }

    fn build(&mut self, op_idx: usize, parent: Option<usize>, nodes: &mut Vec<Node>) {
        let op: &Op = &OPS[op_idx];
        // Cross-service RPCs get a client span in the caller.
        let parent = match parent {
            Some(p) if op.kind == Kind::Server && nodes[p].service != op.service => {
                let idx = nodes.len();
                nodes.push(Node {
                    service: nodes[p].service,
                    parent: Some(p),
                    children: Vec::new(),
                    self_ms: self.rng.lognormal(0.3, 0.4),
                    start_ms: 0.0,
                    end_ms: 0.0,
                    error: false,
                    error_origin: false,
                    name: op.name.to_string(),
                    kind: Kind::Client,
                    protocol: op.protocol,
                });
                nodes[p].children.push(idx);
                Some(idx)
            }
            other => other,
        };
        let idx = nodes.len();
        nodes.push(Node {
            service: op.service,
            parent,
            children: Vec::new(),
            self_ms: self.rng.lognormal(op.median_ms, op.sigma),
            start_ms: 0.0,
            end_ms: 0.0,
            error: false,
            error_origin: false,
            name: op.name.to_string(),
            kind: op.kind,
            protocol: op.protocol,
        });
        if let Some(p) = parent {
            nodes[p].children.push(idx);
        }
        if op_idx == 5 {
            self.build(16, Some(idx), nodes);
        }
        for call in op.calls {
            if !self.rng.chance(call.prob) {
                continue;
            }
            for _ in 0..self.rng.range(call.repeat.0, call.repeat.1) {
                self.build(call.op, Some(idx), nodes);
            }
        }
    }

    fn to_span(
        &self,
        id: TraceId,
        n: &Node,
        span_id: u64,
        parent: Option<u64>,
        start_ms: u64,
        root: bool,
    ) -> Span {
        let base = self.cfg.epoch_unix_nanos + start_ms * 1_000_000;
        let mut attrs: Vec<KeyValue> = Vec::new();
        match n.protocol {
            Protocol::Http { method, route } => {
                attrs.push(kv_str("http.request.method", method));
                attrs.push(kv_str("http.route", route));
                attrs.push(kv_int("http.response.status_code", if n.error { 500 } else { 200 }));
            }
            Protocol::Rpc { service, method } => {
                attrs.push(kv_str("rpc.system", "grpc"));
                attrs.push(kv_str("rpc.service", service));
                attrs.push(kv_str("rpc.method", method));
                attrs.push(kv_int("rpc.grpc.status_code", if n.error { 13 } else { 0 }));
            }
            Protocol::Db { system, statement } => {
                attrs.push(kv_str("db.system", system));
                attrs.push(kv_str("db.statement", statement));
            }
            Protocol::Internal => {}
        }
        if root {
            let tier = match span_id % 20 {
                0 => "enterprise",
                1..=5 => "pro",
                _ => "free",
            };
            attrs.push(kv_str("app.customer.tier", tier));
        }
        let mut events = Vec::new();
        if n.error_origin {
            events.push(Event {
                time_unix_nano: base + (n.end_ms * 1e6) as u64,
                name: "exception".into(),
                attributes: vec![
                    kv_str("exception.type", "UpstreamError"),
                    kv_str("exception.message", format!("{} failed", n.name)),
                ],
                dropped_attributes_count: 0,
            });
        }
        Span {
            trace_id: id.0.to_vec(),
            span_id: span_id.to_be_bytes().to_vec(),
            parent_span_id: parent.map(|p| p.to_be_bytes().to_vec()).unwrap_or_default(),
            name: n.name.clone(),
            kind: n.kind as i32,
            start_time_unix_nano: base + (n.start_ms * 1e6) as u64,
            end_time_unix_nano: base + (n.end_ms * 1e6) as u64,
            attributes: attrs,
            events,
            status: Some(Status {
                code: if n.error { 2 } else { 0 },
                message: if n.error { "upstream failure".into() } else { String::new() },
            }),
            ..Default::default()
        }
    }
}

/// Lay out children sequentially after a slice of the parent's self time;
/// a node ends after its own self time plus all children.
fn layout(nodes: &mut [Node], idx: usize, start: f64) {
    nodes[idx].start_ms = start;
    let lead = nodes[idx].self_ms * 0.3;
    let mut t = start + lead;
    let children = nodes[idx].children.clone();
    for c in children {
        layout(nodes, c, t);
        t = nodes[c].end_ms;
    }
    nodes[idx].end_ms = t + nodes[idx].self_ms * 0.7;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gen(cfg: GenConfig, n: usize) -> Vec<GenTrace> {
        let mut g = Generator::new(cfg);
        (0..n).map(|i| g.trace(i as u64 * 10)).collect()
    }

    #[test]
    fn traces_are_well_formed_trees() {
        for t in gen(GenConfig::default(), 300) {
            let ids: Vec<&Vec<u8>> = t.spans.iter().map(|s| &s.span.span_id).collect();
            let roots = t.spans.iter().filter(|s| s.span.parent_span_id.is_empty()).count();
            assert_eq!(roots, 1);
            for s in &t.spans {
                assert_eq!(s.span.trace_id, t.id.0.to_vec());
                assert!(s.span.end_time_unix_nano >= s.span.start_time_unix_nano);
                if !s.span.parent_span_id.is_empty() {
                    let parent = t
                        .spans
                        .iter()
                        .find(|p| p.span.span_id == s.span.parent_span_id)
                        .expect("parent present");
                    assert!(parent.span.start_time_unix_nano <= s.span.start_time_unix_nano);
                    assert!(parent.span.end_time_unix_nano >= s.span.end_time_unix_nano);
                }
            }
            let mut uniq = ids.clone();
            uniq.dedup();
            assert_eq!(uniq.len(), ids.len());
            assert_eq!(t.truth.spans, t.spans.len());
            if t.truth.health_check {
                assert_eq!(t.spans.len(), 1);
            }
        }
    }

    #[test]
    fn injected_errors_and_slowness_match_truth() {
        let traces = gen(
            GenConfig {
                error_rate: 0.3,
                slow_rate: 0.3,
                late_rate: 0.3,
                ..GenConfig::default()
            },
            1_000,
        );
        let errors = traces.iter().filter(|t| t.truth.error).count();
        assert!((200..400).contains(&errors), "{errors}");
        for t in &traces {
            let has_err = t
                .spans
                .iter()
                .any(|s| s.span.status.as_ref().unwrap().code == 2);
            assert_eq!(has_err, t.truth.error);
            if t.truth.slow {
                assert!(t.truth.duration_ms >= 1_200, "{:?}", t.truth);
            }
            let late = t.spans.iter().filter(|s| s.late).count();
            assert_eq!(late, usize::from(t.truth.has_late_span));
            if t.truth.error {
                let root = &t.spans[0].span;
                assert_eq!(root.status.as_ref().unwrap().code, 2);
            }
        }
        let normal_max = traces
            .iter()
            .filter(|t| !t.truth.slow)
            .map(|t| t.truth.duration_ms)
            .max()
            .unwrap();
        assert!(normal_max < 1_000, "normal traces stay under 1s: {normal_max}");
    }

    #[test]
    fn deterministic_for_a_seed() {
        let a = gen(GenConfig::default(), 50);
        let b = gen(GenConfig::default(), 50);
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(&b) {
            assert_eq!(x.id, y.id);
            assert_eq!(x.truth, y.truth);
        }
        let g = Generator::new(GenConfig::default());
        assert!(!g.resource(0).attributes.is_empty());
    }
}
