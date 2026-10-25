//! Load generator: realistic span trees over a small service graph, with
//! injected slow / error traces and late spans, delivered on a schedule that
//! models per-service export batching.
//!
//! [`Schedule`] is clock-agnostic: the simulator drives it with logical time
//! and [`run_live`] drives it with the wall clock against a real endpoint.

pub mod generator;
pub mod rng;
pub mod topology;

use std::cmp::Ordering as CmpOrdering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::Instant;

pub use generator::{GenConfig, GenSpan, GenTrace, Generator, Truth};

use crate::exporter::Exporter;
use crate::ids::TraceId;
use crate::ExportTraceServiceRequest;

struct Pending {
    arrival_ms: u64,
    seq: u64,
    span: GenSpan,
}

impl PartialEq for Pending {
    fn eq(&self, o: &Self) -> bool {
        (self.arrival_ms, self.seq) == (o.arrival_ms, o.seq)
    }
}
impl Eq for Pending {}
impl PartialOrd for Pending {
    fn partial_cmp(&self, o: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(o))
    }
}
impl Ord for Pending {
    // Reversed: BinaryHeap is a max-heap and we want the earliest arrival.
    fn cmp(&self, o: &Self) -> CmpOrdering {
        (o.arrival_ms, o.seq).cmp(&(self.arrival_ms, self.seq))
    }
}

/// Streaming delivery schedule.
pub struct Schedule {
    gen: Generator,
    interval_ms: f64,
    total: u64,
    generated: u64,
    next_start_ms: f64,
    heap: BinaryHeap<Pending>,
    seq: u64,
    max_spans_per_request: usize,
    truth: Option<HashMap<TraceId, Truth>>,
    scope: InstrumentationScope,
}

impl Schedule {
    /// `total` traces at `rate` traces per second; `record_truth` keeps a
    /// ground-truth map (costs memory proportional to `total`).
    pub fn new(gen: GenConfig, total: u64, rate: f64, record_truth: bool) -> Self {
        Schedule {
            gen: Generator::new(gen),
            interval_ms: 1_000.0 / rate.max(f64::MIN_POSITIVE),
            total,
            generated: 0,
            next_start_ms: 0.0,
            heap: BinaryHeap::new(),
            seq: 0,
            max_spans_per_request: 512,
            truth: record_truth.then(HashMap::new),
            scope: InstrumentationScope {
                name: "tailsample-loadgen".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                ..Default::default()
            },
        }
    }

    /// Cap spans per generated request (default 512).
    pub fn with_max_spans_per_request(mut self, n: usize) -> Self {
        self.max_spans_per_request = n.max(1);
        self
    }

    /// Ground truth of generated traces, if recorded.
    pub fn truth(&self) -> Option<&HashMap<TraceId, Truth>> {
        self.truth.as_ref()
    }

    /// Traces generated so far.
    pub fn generated(&self) -> u64 {
        self.generated
    }

    /// All traces generated and all spans delivered.
    pub fn done(&self) -> bool {
        self.generated >= self.total && self.heap.is_empty()
    }

    /// Earliest time something becomes due.
    pub fn next_event_ms(&self) -> Option<u64> {
        let gen_next = (self.generated < self.total).then_some(self.next_start_ms as u64);
        let span_next = self.heap.peek().map(|p| p.arrival_ms);
        match (gen_next, span_next) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// Generate traces started by `now_ms` and return every span that has
    /// arrived by then, grouped into one request per service (chunked).
    pub fn due(&mut self, now_ms: u64) -> Vec<ExportTraceServiceRequest> {
        while self.generated < self.total && self.next_start_ms as u64 <= now_ms {
            let t = self.gen.trace(self.next_start_ms as u64);
            if let Some(map) = self.truth.as_mut() {
                map.insert(t.id, t.truth);
            }
            for s in t.spans {
                self.seq += 1;
                self.heap.push(Pending {
                    arrival_ms: s.arrival_ms,
                    seq: self.seq,
                    span: s,
                });
            }
            self.generated += 1;
            self.next_start_ms += self.interval_ms;
        }
        let mut by_service: Vec<Vec<Span>> = vec![Vec::new(); topology::SERVICES.len()];
        while self.heap.peek().is_some_and(|p| p.arrival_ms <= now_ms) {
            if let Some(p) = self.heap.pop() {
                by_service[p.span.service].push(p.span.span);
            }
        }
        let mut out = Vec::new();
        for (svc, spans) in by_service.into_iter().enumerate() {
            let mut spans = spans;
            while !spans.is_empty() {
                let rest = spans.split_off(spans.len().min(self.max_spans_per_request));
                out.push(ExportTraceServiceRequest {
                    resource_spans: vec![ResourceSpans {
                        resource: Some((*self.gen.resource(svc)).clone()),
                        scope_spans: vec![ScopeSpans {
                            scope: Some(self.scope.clone()),
                            spans,
                            schema_url: String::new(),
                        }],
                        schema_url: String::new(),
                    }],
                });
                spans = rest;
            }
        }
        out
    }
}

/// Live-run options.
#[derive(Debug, Clone)]
pub struct LiveOptions {
    /// Maximum in-flight requests.
    pub concurrency: usize,
    /// Attempts per request when the sampler pushes back.
    pub max_attempts: u32,
    /// First retry delay (doubles per attempt).
    pub initial_backoff: Duration,
    /// Scheduler resolution.
    pub step: Duration,
}

impl Default for LiveOptions {
    fn default() -> Self {
        LiveOptions {
            concurrency: 8,
            max_attempts: 6,
            initial_backoff: Duration::from_millis(50),
            step: Duration::from_millis(20),
        }
    }
}

/// Live-run outcome.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LiveReport {
    /// Requests accepted by the target.
    pub requests_ok: u64,
    /// Spans accepted by the target.
    pub spans_ok: u64,
    /// Attempts refused with a retryable error (backpressure).
    pub retries: u64,
    /// Requests abandoned after all attempts or on a permanent error.
    pub requests_failed: u64,
    /// Spans in abandoned requests.
    pub spans_failed: u64,
    /// Wall time of the run.
    pub elapsed_ms: u64,
}

#[derive(Default)]
struct Outcome {
    ok: bool,
    spans: u64,
    retries: u64,
}

/// Drive `schedule` in real time against `target`, retrying refused
/// requests with exponential backoff.
pub async fn run_live(
    schedule: &mut Schedule,
    target: Arc<dyn Exporter>,
    opts: &LiveOptions,
) -> LiveReport {
    let start = Instant::now();
    let sem = Arc::new(Semaphore::new(opts.concurrency.max(1)));
    let mut tasks: JoinSet<Outcome> = JoinSet::new();
    let mut report = LiveReport::default();
    let absorb = |o: Outcome, report: &mut LiveReport| {
        report.retries += o.retries;
        if o.ok {
            report.requests_ok += 1;
            report.spans_ok += o.spans;
        } else {
            report.requests_failed += 1;
            report.spans_failed += o.spans;
        }
    };
    while !schedule.done() {
        let now = start.elapsed().as_millis() as u64;
        for req in schedule.due(now) {
            let Ok(permit) = Arc::clone(&sem).acquire_owned().await else {
                break;
            };
            let target = Arc::clone(&target);
            let opts = opts.clone();
            tasks.spawn(async move {
                let _permit = permit;
                send_with_retry(target.as_ref(), req, &opts).await
            });
        }
        while let Some(done) = tasks.try_join_next() {
            if let Ok(o) = done {
                absorb(o, &mut report);
            }
        }
        let next = schedule.next_event_ms().unwrap_or(now);
        let wait = Duration::from_millis(next.saturating_sub(now)).min(opts.step);
        tokio::time::sleep(wait.max(Duration::from_millis(1))).await;
    }
    while let Some(done) = tasks.join_next().await {
        if let Ok(o) = done {
            absorb(o, &mut report);
        }
    }
    report.elapsed_ms = start.elapsed().as_millis() as u64;
    report
}

async fn send_with_retry(
    target: &dyn Exporter,
    req: ExportTraceServiceRequest,
    opts: &LiveOptions,
) -> Outcome {
    let spans = crate::model::span_count(&req) as u64;
    let mut backoff = opts.initial_backoff;
    let mut out = Outcome {
        spans,
        ..Default::default()
    };
    for attempt in 1..=opts.max_attempts {
        match target.export(req.clone()).await {
            Ok(()) => {
                out.ok = true;
                return out;
            }
            Err(e) if e.retryable && attempt < opts.max_attempts => {
                out.retries += 1;
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(2));
            }
            Err(_) => return out,
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporter::ExportError;
    use crate::model::span_count;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn schedule_delivers_every_span_in_order() {
        let mut s =
            Schedule::new(GenConfig::default(), 200, 100.0, true).with_max_spans_per_request(16);
        let mut t = 0;
        let mut spans = 0;
        let mut last_now = 0;
        while !s.done() {
            for r in s.due(t) {
                assert!(span_count(&r) <= 16);
                assert_eq!(r.resource_spans.len(), 1);
                spans += span_count(&r);
            }
            last_now = t;
            t += 50;
        }
        let truth = s.truth().unwrap();
        assert_eq!(truth.len(), 200);
        assert_eq!(s.generated(), 200);
        assert_eq!(spans, truth.values().map(|t| t.spans).sum::<usize>());
        assert!(
            last_now >= 2_000,
            "rate of 100/s spreads 200 traces over 2s"
        );
        assert!(s.next_event_ms().is_none());
    }

    struct Flaky {
        calls: AtomicU64,
        refuse_every: u64,
    }

    #[tonic::async_trait]
    impl Exporter for Flaky {
        async fn export(&self, _r: ExportTraceServiceRequest) -> Result<(), ExportError> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if n % self.refuse_every == 0 {
                Err(ExportError::retryable("busy"))
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn live_run_retries_backpressure() {
        let mut s = Schedule::new(
            GenConfig {
                late_rate: 0.0,
                export_delay_ms: (0, 5),
                ..GenConfig::default()
            },
            40,
            2_000.0,
            false,
        );
        let target: Arc<dyn Exporter> = Arc::new(Flaky {
            calls: AtomicU64::new(0),
            refuse_every: 3,
        });
        let opts = LiveOptions {
            initial_backoff: Duration::from_millis(1),
            ..LiveOptions::default()
        };
        let r = run_live(&mut s, target, &opts).await;
        assert!(r.retries > 0);
        assert_eq!(r.requests_failed, 0);
        assert!(r.spans_ok > 40);
    }

    #[tokio::test]
    async fn permanent_errors_fail_requests() {
        struct Broken;
        #[tonic::async_trait]
        impl Exporter for Broken {
            async fn export(&self, _r: ExportTraceServiceRequest) -> Result<(), ExportError> {
                Err(ExportError::permanent("nope"))
            }
        }
        let mut s = Schedule::new(GenConfig::default(), 3, 1_000.0, false);
        let r = run_live(&mut s, Arc::new(Broken), &LiveOptions::default()).await;
        assert_eq!(r.requests_ok, 0);
        assert!(r.requests_failed > 0);
        assert!(r.spans_failed > 0);
    }
}
