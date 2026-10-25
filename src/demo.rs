//! Offline end-to-end demo over loopback sockets.
//!
//! 1. Start an OTLP/gRPC sink standing in for the downstream collector.
//! 2. Start the sampler (embedded `examples/demo.toml`) on ephemeral ports,
//!    exporting to the sink.
//! 3. Post the bundled JSON fixture trace over OTLP/HTTP+JSON.
//! 4. Replay generated traffic, alternating OTLP/gRPC and OTLP/HTTP+protobuf.
//! 5. Shut down (which decides every buffered trace) and score what reached
//!    the sink against the generator's ground truth.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::CONTENT_TYPE;
use hyper::Request;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

use crate::config::{Config, ExporterKind};
use crate::exporter::{ExportError, Exporter, GrpcExporter, HttpExporter};
use crate::ids::TraceId;
use crate::loadgen::{run_live, GenConfig, LiveOptions, LiveReport, Schedule};
use crate::server::Server;
use crate::sim::ClassReport;
use crate::sink::Sink;
use crate::telemetry::CoreSnapshot;
use crate::ExportTraceServiceRequest;

/// Embedded demo configuration.
pub const DEMO_CONFIG: &str = include_str!("../examples/demo.toml");
/// Embedded OTLP/JSON fixture (one error trace, two spans).
pub const FIXTURE_JSON: &str = include_str!("../fixtures/checkout-error.json");
/// Trace id of the fixture.
pub const FIXTURE_TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";

/// Demo knobs.
#[derive(Debug, Clone)]
pub struct DemoOptions {
    /// Traces to generate.
    pub traces: u64,
    /// Traces per second.
    pub rate: f64,
    /// Generator seed.
    pub seed: u64,
}

impl Default for DemoOptions {
    fn default() -> Self {
        DemoOptions {
            traces: 3_000,
            rate: 600.0,
            seed: 7,
        }
    }
}

/// Demo results.
#[derive(Debug, Clone, Default)]
pub struct DemoReport {
    /// Error traces.
    pub errors: ClassReport,
    /// Slow (non-error) traces.
    pub slow: ClassReport,
    /// Other traces.
    pub normal: ClassReport,
    /// Whether the JSON fixture trace reached the sink with both spans.
    pub fixture_kept: bool,
    /// Load generator outcome.
    pub load: LiveReport,
    /// Sampler counters at shutdown.
    pub core: CoreSnapshot,
    /// Spans the exporter delivered.
    pub exported_spans: u64,
    /// Requests seen by the sink.
    pub sink_requests: u64,
    /// Requests sent per protocol (gRPC, HTTP).
    pub per_protocol: (u64, u64),
    /// Lines of the Prometheus scrape mentioning outcomes.
    pub metrics_excerpt: Vec<String>,
    /// Wall time.
    pub elapsed: Duration,
}

/// Round-robins requests between the two receivers.
struct Alternate {
    grpc: GrpcExporter,
    http: HttpExporter,
    n: AtomicU64,
    grpc_sent: AtomicU64,
    http_sent: AtomicU64,
}

#[tonic::async_trait]
impl Exporter for Alternate {
    async fn export(&self, req: ExportTraceServiceRequest) -> Result<(), ExportError> {
        if self.n.fetch_add(1, Ordering::Relaxed) % 2 == 0 {
            self.grpc_sent.fetch_add(1, Ordering::Relaxed);
            self.grpc.export(req).await
        } else {
            self.http_sent.fetch_add(1, Ordering::Relaxed);
            self.http.export(req).await
        }
    }
}

async fn http_call(method: &str, url: &str, body: Option<(&str, String)>) -> anyhow::Result<(u16, String)> {
    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let mut b = Request::builder().method(method).uri(url);
    let payload = match body {
        Some((ctype, text)) => {
            b = b.header(CONTENT_TYPE, ctype);
            Full::new(Bytes::from(text))
        }
        None => Full::new(Bytes::new()),
    };
    let resp = client.request(b.body(payload)?).await?;
    let status = resp.status().as_u16();
    let bytes = resp.into_body().collect().await?.to_bytes();
    Ok((status, String::from_utf8_lossy(&bytes).into_owned()))
}

/// Run the demo.
pub async fn run(opts: &DemoOptions) -> anyhow::Result<DemoReport> {
    let started = Instant::now();
    let sink = Sink::start("127.0.0.1:0").await?;
    let mut cfg = Config::from_toml_str(DEMO_CONFIG)?;
    cfg.receiver.grpc_listen = Some("127.0.0.1:0".into());
    cfg.receiver.http_listen = Some("127.0.0.1:0".into());
    cfg.exporter.kind = ExporterKind::OtlpGrpc;
    cfg.exporter.endpoint = sink.endpoint();
    let server = Server::start(&cfg).await?;
    let grpc = server.grpc_addr().ok_or_else(|| anyhow::anyhow!("no gRPC receiver"))?;
    let http = server.http_addr().ok_or_else(|| anyhow::anyhow!("no HTTP receiver"))?;
    let traces_url = format!("http://{http}/v1/traces");

    let (status, _) = http_call("POST", &traces_url, Some(("application/json", FIXTURE_JSON.to_string()))).await?;
    anyhow::ensure!(status == 200, "fixture POST returned {status}");

    let gen = GenConfig {
        seed: opts.seed,
        slow_extra_ms: (1_200, 2_000),
        late_delay_ms: (3_500, 4_500),
        ..GenConfig::default()
    };
    let mut schedule = Schedule::new(gen, opts.traces, opts.rate, true).with_max_spans_per_request(256);
    let target = Arc::new(Alternate {
        grpc: GrpcExporter::new(&format!("http://{grpc}"))?,
        http: HttpExporter::new(&traces_url)?,
        n: AtomicU64::new(0),
        grpc_sent: AtomicU64::new(0),
        http_sent: AtomicU64::new(0),
    });
    let load = run_live(&mut schedule, Arc::clone(&target) as Arc<dyn Exporter>, &LiveOptions::default()).await;

    // Give the last traces their full decision wait before shutting down.
    tokio::time::sleep(Duration::from_millis(cfg.buffer.decision_wait_ms + 300)).await;
    let (_, metrics) = http_call("GET", &format!("http://{http}/metrics"), None).await?;
    let telemetry = server.telemetry();
    server.shutdown().await;
    let seen = sink.stop().await;

    let mut report = DemoReport {
        load,
        core: telemetry.core(),
        exported_spans: telemetry.export_spans_ok.get(),
        sink_requests: seen.requests,
        per_protocol: (
            target.grpc_sent.load(Ordering::Relaxed),
            target.http_sent.load(Ordering::Relaxed),
        ),
        metrics_excerpt: metrics
            .lines()
            .filter(|l| l.starts_with("tailsample_traces_total") || l.starts_with("tailsample_sampled_traces_by_policy_total{"))
            .map(str::to_string)
            .collect(),
        ..DemoReport::default()
    };
    let fixture_id = TraceId::from_slice(&hex(FIXTURE_TRACE_ID)).ok_or_else(|| anyhow::anyhow!("bad fixture id"))?;
    report.fixture_kept = seen.spans_by_trace.get(&fixture_id) == Some(&2);
    if let Some(truth) = schedule.truth() {
        for (id, t) in truth {
            let kept = seen.spans_by_trace.get(id).copied().unwrap_or(0);
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
    report.elapsed = started.elapsed();
    Ok(report)
}

fn hex(s: &str) -> Vec<u8> {
    (0..s.len() / 2)
        .filter_map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok())
        .collect()
}

impl fmt::Display for DemoReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let l = &self.load;
        writeln!(
            f,
            "load: {} requests ({} via OTLP/gRPC, {} via OTLP/HTTP), {} spans accepted, {} backpressure retries, {} failed",
            l.requests_ok, self.per_protocol.0, self.per_protocol.1, l.spans_ok, l.retries, l.requests_failed
        )?;
        writeln!(f)?;
        writeln!(
            f,
            "{:<8} {:>8} {:>8} {:>8} {:>9} {:>10} {:>10}",
            "class", "traces", "kept", "kept%", "complete", "spans_in", "spans_out"
        )?;
        for (name, c) in [("error", &self.errors), ("slow", &self.slow), ("normal", &self.normal)] {
            writeln!(
                f,
                "{:<8} {:>8} {:>8} {:>7.2}% {:>9} {:>10} {:>10}",
                name, c.generated, c.kept, c.kept_pct(), c.complete, c.spans_generated, c.spans_kept
            )?;
        }
        writeln!(f)?;
        writeln!(f, "fixture trace {FIXTURE_TRACE_ID} (OTLP/JSON, status ERROR) kept: {}", self.fixture_kept)?;
        let s = &self.core.stats;
        writeln!(
            f,
            "sampler: {} spans received, {} sampled, {} not sampled, {} late forwarded, {} late dropped",
            s.spans_received, s.spans_sampled, s.spans_not_sampled, s.spans_late_sampled, s.spans_late_dropped
        )?;
        writeln!(
            f,
            "exporter -> sink: {} spans in {} requests",
            self.exported_spans, self.sink_requests
        )?;
        writeln!(f, "metrics scraped from /metrics before shutdown:")?;
        for line in &self.metrics_excerpt {
            writeln!(f, "  {line}")?;
        }
        write!(f, "demo wall time: {:.1}s", self.elapsed.as_secs_f64())
    }
}
