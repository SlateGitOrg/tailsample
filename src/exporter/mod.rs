//! Export of sampled traces.
//!
//! The [`ExportWorker`] receives released traces from the sampler over a
//! bounded channel, batches them up to `max_batch_spans` (or until
//! `flush_interval_ms` passes), regroups spans into OTLP requests and sends
//! them through an [`Exporter`] with bounded exponential-backoff retries.
//! Requests are sent one at a time; when the downstream is slow the channel
//! fills and the sampler blocks on it, which in turn fills the ingest queue
//! and makes receivers refuse new data (see `docs/ARCHITECTURE.md`).

mod grpc;
mod http;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::{timeout, Instant};

pub use grpc::GrpcExporter;
pub use http::HttpExporter;

use crate::config::{ExporterConfig, ExporterKind};
use crate::model::{span_count, to_request, SpanRecord};
use crate::sampler::Released;
use crate::telemetry::Telemetry;
use crate::ExportTraceServiceRequest;

/// Export failure.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}")]
pub struct ExportError {
    /// Whether retrying may succeed.
    pub retryable: bool,
    /// Human-readable cause.
    pub message: String,
}

impl ExportError {
    /// A transient failure.
    pub fn retryable(message: impl Into<String>) -> Self {
        ExportError {
            retryable: true,
            message: message.into(),
        }
    }
    /// A permanent failure.
    pub fn permanent(message: impl Into<String>) -> Self {
        ExportError {
            retryable: false,
            message: message.into(),
        }
    }
}

/// A destination for OTLP export requests.
#[tonic::async_trait]
pub trait Exporter: Send + Sync {
    /// Send one request.
    async fn export(&self, req: ExportTraceServiceRequest) -> Result<(), ExportError>;
}

/// Discards everything (benchmarks, dry runs).
#[derive(Debug, Default)]
pub struct NullExporter;

#[tonic::async_trait]
impl Exporter for NullExporter {
    async fn export(&self, _req: ExportTraceServiceRequest) -> Result<(), ExportError> {
        Ok(())
    }
}

/// Build the exporter selected by configuration.
pub fn from_config(cfg: &ExporterConfig) -> Result<Box<dyn Exporter>, ExportError> {
    Ok(match cfg.kind {
        ExporterKind::OtlpGrpc => Box::new(GrpcExporter::new(&cfg.endpoint)?),
        ExporterKind::OtlpHttp => Box::new(HttpExporter::new(&cfg.endpoint)?),
        ExporterKind::None => Box::new(NullExporter),
    })
}

/// Batching, retrying export loop.
pub struct ExportWorker {
    exporter: Box<dyn Exporter>,
    cfg: ExporterConfig,
    telemetry: Arc<Telemetry>,
    pending: Vec<SpanRecord>,
}

impl ExportWorker {
    /// New worker.
    pub fn new(exporter: Box<dyn Exporter>, cfg: ExporterConfig, telemetry: Arc<Telemetry>) -> Self {
        ExportWorker {
            exporter,
            cfg,
            telemetry,
            pending: Vec::new(),
        }
    }

    /// Run until the channel closes, then flush what is pending.
    pub async fn run(mut self, mut rx: mpsc::Receiver<Vec<Released>>) {
        let flush_every = Duration::from_millis(self.cfg.flush_interval_ms.max(1));
        let mut deadline = Instant::now() + flush_every;
        loop {
            tokio::select! {
                msg = rx.recv() => match msg {
                    Some(batch) => {
                        for r in batch {
                            self.pending.extend(r.spans);
                        }
                        while self.pending.len() >= self.cfg.max_batch_spans {
                            let rest = self.pending.split_off(self.cfg.max_batch_spans);
                            let full = std::mem::replace(&mut self.pending, rest);
                            self.send(full).await;
                            deadline = Instant::now() + flush_every;
                        }
                    }
                    None => break,
                },
                _ = tokio::time::sleep_until(deadline) => {
                    if !self.pending.is_empty() {
                        let batch = std::mem::take(&mut self.pending);
                        self.send(batch).await;
                    }
                    deadline = Instant::now() + flush_every;
                }
            }
        }
        while !self.pending.is_empty() {
            let at = self.pending.len().min(self.cfg.max_batch_spans);
            let rest = self.pending.split_off(at);
            let batch = std::mem::replace(&mut self.pending, rest);
            self.send(batch).await;
        }
    }

    async fn send(&self, spans: Vec<SpanRecord>) {
        let req = to_request(spans);
        let n = span_count(&req) as u64;
        let per_attempt = Duration::from_millis(self.cfg.timeout_ms);
        let mut backoff = Duration::from_millis(self.cfg.initial_backoff_ms);
        for attempt in 1..=self.cfg.max_attempts {
            let result = match timeout(per_attempt, self.exporter.export(req.clone())).await {
                Ok(r) => r,
                Err(_) => Err(ExportError::retryable("export attempt timed out")),
            };
            match result {
                Ok(()) => {
                    self.telemetry.export_requests_ok.inc();
                    self.telemetry.export_spans_ok.add(n);
                    return;
                }
                Err(err) => {
                    self.telemetry.export_attempt_failures.inc();
                    let last = attempt == self.cfg.max_attempts;
                    if !err.retryable || last {
                        tracing::warn!(attempt, spans = n, error = %err, "dropping export batch");
                        self.telemetry.export_spans_dropped.add(n);
                        return;
                    }
                    tracing::debug!(attempt, error = %err, "export failed, retrying");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::TraceId;
    use crate::model::ScopeContext;
    use crate::sampler::ReleaseKind;
    use opentelemetry_proto::tonic::trace::v1::Span;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Mutex;

    #[derive(Default)]
    struct Scripted {
        failures_before_ok: u32,
        permanent: bool,
        calls: AtomicU32,
        got: Mutex<Vec<usize>>,
    }

    #[tonic::async_trait]
    impl Exporter for Arc<Scripted> {
        async fn export(&self, req: ExportTraceServiceRequest) -> Result<(), ExportError> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if self.permanent {
                return Err(ExportError::permanent("bad request"));
            }
            if n < self.failures_before_ok {
                return Err(ExportError::retryable("unavailable"));
            }
            self.got.lock().unwrap().push(span_count(&req));
            Ok(())
        }
    }

    fn released(n: usize) -> Released {
        let id = TraceId::from_u128(7);
        let ctx = Arc::new(ScopeContext::default());
        Released {
            trace_id: id,
            spans: (0..n)
                .map(|i| {
                    SpanRecord::new(
                        id,
                        Span {
                            trace_id: id.0.to_vec(),
                            span_id: (i as u64 + 1).to_be_bytes().to_vec(),
                            ..Default::default()
                        },
                        Arc::clone(&ctx),
                    )
                })
                .collect(),
            policy: Arc::from("p"),
            kind: ReleaseKind::Decided,
        }
    }

    fn cfg(attempts: u32) -> ExporterConfig {
        ExporterConfig {
            max_batch_spans: 4,
            flush_interval_ms: 10,
            max_attempts: attempts,
            initial_backoff_ms: 1,
            ..ExporterConfig::default()
        }
    }

    async fn run_with(s: Arc<Scripted>, attempts: u32, batches: Vec<Released>) -> Arc<Telemetry> {
        let t = Arc::new(Telemetry::default());
        let (tx, rx) = mpsc::channel(4);
        let w = ExportWorker::new(Box::new(s), cfg(attempts), Arc::clone(&t));
        let h = tokio::spawn(w.run(rx));
        tx.send(batches).await.unwrap();
        drop(tx);
        h.await.unwrap();
        t
    }

    #[tokio::test]
    async fn batches_by_size_and_flushes_on_close() {
        let s = Arc::new(Scripted::default());
        let t = run_with(Arc::clone(&s), 1, vec![released(6), released(3)]).await;
        assert_eq!(*s.got.lock().unwrap(), vec![4, 4, 1]);
        assert_eq!(t.export_spans_ok.get(), 9);
        assert_eq!(t.export_requests_ok.get(), 3);
    }

    #[tokio::test]
    async fn flushes_partial_batch_on_interval() {
        let s = Arc::new(Scripted::default());
        let t = Arc::new(Telemetry::default());
        let (tx, rx) = mpsc::channel(4);
        let w = ExportWorker::new(Box::new(Arc::clone(&s)), cfg(1), Arc::clone(&t));
        let h = tokio::spawn(w.run(rx));
        tx.send(vec![released(2)]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(*s.got.lock().unwrap(), vec![2]);
        drop(tx);
        h.await.unwrap();
    }

    #[tokio::test]
    async fn retries_transient_failures() {
        let s = Arc::new(Scripted {
            failures_before_ok: 2,
            ..Default::default()
        });
        let t = run_with(Arc::clone(&s), 3, vec![released(2)]).await;
        assert_eq!(t.export_attempt_failures.get(), 2);
        assert_eq!(t.export_spans_ok.get(), 2);
        assert_eq!(t.export_spans_dropped.get(), 0);
    }

    #[tokio::test]
    async fn drops_after_exhausting_retries() {
        let s = Arc::new(Scripted {
            failures_before_ok: 10,
            ..Default::default()
        });
        let t = run_with(Arc::clone(&s), 2, vec![released(2)]).await;
        assert_eq!(s.calls.load(Ordering::SeqCst), 2);
        assert_eq!(t.export_spans_dropped.get(), 2);
    }

    #[tokio::test]
    async fn permanent_errors_are_not_retried() {
        let s = Arc::new(Scripted {
            permanent: true,
            ..Default::default()
        });
        let t = run_with(Arc::clone(&s), 5, vec![released(1)]).await;
        assert_eq!(s.calls.load(Ordering::SeqCst), 1);
        assert_eq!(t.export_spans_dropped.get(), 1);
    }

    #[tokio::test]
    async fn null_exporter_and_factory() {
        assert!(NullExporter.export(ExportTraceServiceRequest::default()).await.is_ok());
        assert!(from_config(&ExporterConfig::default()).is_ok());
        let bad = ExporterConfig {
            kind: ExporterKind::OtlpHttp,
            endpoint: "not a url".into(),
            ..ExporterConfig::default()
        };
        assert!(from_config(&bad).is_err());
        let bad = ExporterConfig {
            kind: ExporterKind::OtlpGrpc,
            endpoint: "::::".into(),
            ..ExporterConfig::default()
        };
        assert!(from_config(&bad).is_err());
    }
}
