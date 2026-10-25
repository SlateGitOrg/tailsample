//! Async shell around the sampling core.
//!
//! ```text
//! receivers --(bounded ingest queue)--> sampler task --(bounded export queue)--> export worker
//!     ^                                      |
//!     +---------- admission flag ------------+
//! ```
//!
//! * Receivers flatten requests on their own tasks and enqueue them with
//!   [`IngestHandle::submit`]. A full queue is refused immediately (`reject`)
//!   or after a bounded wait (`block`).
//! * The sampler task is the only owner of [`Sampler`]; it never shares it
//!   behind a lock. It ticks on an interval and after every message.
//! * Sending to the export queue is awaited: a slow downstream stalls the
//!   sampler, which fills the ingest queue, which makes receivers refuse.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::config::{BackpressureMode, Config, ConfigError};
use crate::exporter::{ExportBatch, ExportWorker, Exporter};
use crate::model::{flatten, SpanRecord};
use crate::sampler::{Released, Sampler};
use crate::telemetry::{CoreSnapshot, Telemetry};
use crate::ExportTraceServiceRequest;

/// Why a request was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum IngestError {
    /// The ingest queue is full.
    #[error("ingest queue full")]
    QueueFull,
    /// The trace buffer is full and the eviction policy is `reject_new`.
    #[error("trace buffer full (admission closed)")]
    AdmissionClosed,
    /// The sampler is shutting down.
    #[error("shutting down")]
    ShuttingDown,
}

/// Successful submission summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Accepted {
    /// Spans enqueued.
    pub spans: usize,
    /// Spans rejected for invalid trace ids.
    pub invalid: u64,
}

struct IngestMsg {
    records: Vec<SpanRecord>,
    invalid: u64,
    /// Span permits, released when the sampler has consumed the message.
    _permit: OwnedSemaphorePermit,
}

/// Cloneable handle receivers use to submit data.
#[derive(Clone)]
pub struct IngestHandle {
    tx: mpsc::Sender<IngestMsg>,
    span_permits: Arc<Semaphore>,
    max_queued_spans: usize,
    admission: Arc<AtomicBool>,
    mode: BackpressureMode,
    block_timeout: Duration,
    retry_after_secs: u64,
    telemetry: Arc<Telemetry>,
}

impl IngestHandle {
    /// Flatten and enqueue a request.
    pub async fn submit(&self, req: ExportTraceServiceRequest) -> Result<Accepted, IngestError> {
        if !self.admission.load(Ordering::Acquire) {
            self.telemetry.rejected_admission.inc();
            return Err(IngestError::AdmissionClosed);
        }
        let flat = flatten(req);
        let accepted = Accepted {
            spans: flat.records.len(),
            invalid: flat.invalid,
        };
        if flat.records.is_empty() && flat.invalid == 0 {
            return Ok(accepted);
        }
        let sent = self.enqueue(flat.records, flat.invalid).await;
        match sent {
            Ok(()) => {
                let depth = (self.tx.max_capacity() - self.tx.capacity()) as u64;
                self.telemetry.observe_queue_depth(depth);
                self.telemetry.spans_enqueued.add(accepted.spans as u64);
                Ok(accepted)
            }
            Err(IngestError::QueueFull) => {
                self.telemetry.rejected_queue_full.inc();
                Err(IngestError::QueueFull)
            }
            Err(e) => {
                self.telemetry.rejected_shutdown.inc();
                Err(e)
            }
        }
    }

    async fn enqueue(&self, records: Vec<SpanRecord>, invalid: u64) -> Result<(), IngestError> {
        let want = records
            .len()
            .clamp(1, self.max_queued_spans)
            .min(u32::MAX as usize) as u32;
        match self.mode {
            BackpressureMode::Reject => {
                let permit = Arc::clone(&self.span_permits)
                    .try_acquire_many_owned(want)
                    .map_err(|e| match e {
                        TryAcquireError::NoPermits => IngestError::QueueFull,
                        TryAcquireError::Closed => IngestError::ShuttingDown,
                    })?;
                let msg = IngestMsg {
                    records,
                    invalid,
                    _permit: permit,
                };
                self.tx.try_send(msg).map_err(|e| match e {
                    mpsc::error::TrySendError::Full(_) => IngestError::QueueFull,
                    mpsc::error::TrySendError::Closed(_) => IngestError::ShuttingDown,
                })
            }
            BackpressureMode::Block => {
                let deadline = Instant::now() + self.block_timeout;
                let permit = match tokio::time::timeout_at(
                    deadline,
                    Arc::clone(&self.span_permits).acquire_many_owned(want),
                )
                .await
                {
                    Ok(Ok(p)) => p,
                    Ok(Err(_)) => return Err(IngestError::ShuttingDown),
                    Err(_) => return Err(IngestError::QueueFull),
                };
                let msg = IngestMsg {
                    records,
                    invalid,
                    _permit: permit,
                };
                match tokio::time::timeout_at(deadline, self.tx.send(msg)).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(_)) => Err(IngestError::ShuttingDown),
                    Err(_) => Err(IngestError::QueueFull),
                }
            }
        }
    }

    /// Retry hint for refused clients, in seconds.
    pub fn retry_after_secs(&self) -> u64 {
        self.retry_after_secs
    }

    /// Shared telemetry.
    pub fn telemetry(&self) -> &Arc<Telemetry> {
        &self.telemetry
    }
}

/// A running sampler + exporter pipeline.
pub struct Pipeline {
    handle: IngestHandle,
    shutdown: watch::Sender<bool>,
    sampler_task: JoinHandle<()>,
    export_task: JoinHandle<()>,
}

impl Pipeline {
    /// Spawn the sampler and export tasks on the current tokio runtime.
    pub fn spawn(cfg: &Config, exporter: Box<dyn Exporter>) -> Result<Self, ConfigError> {
        let sampler = Sampler::new(cfg)?;
        let telemetry = Arc::new(Telemetry::default());
        let (tx, rx) = mpsc::channel(cfg.backpressure.queue_capacity);
        let (export_tx, export_rx) = mpsc::channel(cfg.exporter.queue_capacity);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let admission = Arc::new(AtomicBool::new(true));
        let export_permits = Arc::new(Semaphore::new(cfg.exporter.max_queued_spans));
        let worker = ExportWorker::new(exporter, cfg.exporter.clone(), Arc::clone(&telemetry))
            .with_permits(Arc::clone(&export_permits));
        let export_task = tokio::spawn(worker.run(export_rx));
        let sampler_task = tokio::spawn(sampler_loop(
            sampler,
            rx,
            ExportSide {
                tx: export_tx,
                permits: export_permits,
                max_queued_spans: cfg.exporter.max_queued_spans,
            },
            shutdown_rx,
            Arc::clone(&admission),
            Arc::clone(&telemetry),
            Duration::from_millis(cfg.buffer.tick_interval_ms),
        ));
        Ok(Pipeline {
            handle: IngestHandle {
                tx,
                span_permits: Arc::new(Semaphore::new(cfg.backpressure.max_queued_spans)),
                max_queued_spans: cfg.backpressure.max_queued_spans,
                admission,
                mode: cfg.backpressure.mode,
                block_timeout: Duration::from_millis(cfg.backpressure.block_timeout_ms),
                retry_after_secs: cfg.backpressure.retry_after_secs,
                telemetry,
            },
            shutdown,
            sampler_task,
            export_task,
        })
    }

    /// Handle for receivers.
    pub fn handle(&self) -> IngestHandle {
        self.handle.clone()
    }

    /// Shared telemetry.
    pub fn telemetry(&self) -> Arc<Telemetry> {
        Arc::clone(&self.handle.telemetry)
    }

    /// Graceful shutdown: drain queued requests, decide every buffered
    /// trace, export, and wait for the exporter to finish.
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        drop(self.handle);
        let _ = self.sampler_task.await;
        let _ = self.export_task.await;
    }
}

struct ExportSide {
    tx: mpsc::Sender<ExportBatch>,
    permits: Arc<Semaphore>,
    max_queued_spans: usize,
}

impl ExportSide {
    /// Hand released traces to the exporter, waiting for span permits and
    /// channel space. This await is where downstream slowness turns into
    /// backpressure on the whole pipeline.
    async fn send(&self, released: Vec<Released>) -> bool {
        let spans: usize = released.iter().map(|r| r.spans.len()).sum();
        let want = spans.clamp(1, self.max_queued_spans).min(u32::MAX as usize);
        match Arc::clone(&self.permits)
            .acquire_many_owned(want as u32)
            .await
        {
            Ok(p) => p.forget(),
            Err(_) => return false,
        }
        self.tx
            .send(ExportBatch {
                released,
                permits: want,
            })
            .await
            .is_ok()
    }
}

async fn sampler_loop(
    mut sampler: Sampler,
    mut rx: mpsc::Receiver<IngestMsg>,
    export: ExportSide,
    mut shutdown: watch::Receiver<bool>,
    admission: Arc<AtomicBool>,
    telemetry: Arc<Telemetry>,
    tick_every: Duration,
) {
    let start = Instant::now();
    let now_ms = || start.elapsed().as_millis() as u64;
    let mut out: Vec<Released> = Vec::new();
    let mut ticker = tokio::time::interval(tick_every);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let mut publish = false;
        tokio::select! {
            msg = rx.recv() => match msg {
                Some(m) => {
                    let now = now_ms();
                    sampler.record_invalid(m.invalid);
                    sampler.ingest(m.records, now, &mut out);
                    sampler.tick(now, &mut out);
                }
                None => break,
            },
            _ = ticker.tick() => {
                sampler.tick(now_ms(), &mut out);
                publish = true;
            }
            _ = shutdown.changed() => break,
        }
        admission.store(sampler.admission_open(), Ordering::Release);
        if publish {
            telemetry.publish(snapshot(&sampler));
        }
        if !out.is_empty() && !export.send(std::mem::take(&mut out)).await {
            tracing::error!("export worker stopped; sampled spans lost");
        }
    }
    // Drain whatever receivers managed to enqueue, then decide everything.
    admission.store(false, Ordering::Release);
    rx.close();
    while let Some(m) = rx.recv().await {
        let now = now_ms();
        sampler.record_invalid(m.invalid);
        sampler.ingest(m.records, now, &mut out);
    }
    sampler.flush(now_ms(), &mut out);
    telemetry.publish(snapshot(&sampler));
    if !out.is_empty() {
        let _ = export.send(out).await;
    }
}

fn snapshot(s: &Sampler) -> CoreSnapshot {
    CoreSnapshot {
        stats: s.stats().clone(),
        gauges: s.gauges(),
        cache_evictions: s.cache_evictions(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporter::{ExportError, NullExporter};
    use crate::model::kv_str;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    fn req(trace: u8, spans: usize) -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: (0..spans)
                        .map(|i| Span {
                            trace_id: vec![trace; 16],
                            span_id: vec![i as u8 + 1; 8],
                            attributes: vec![kv_str("k", "v")],
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn cfg(extra: &str) -> Config {
        Config::from_toml_str(&format!(
            "[buffer]\ndecision_wait_ms = 50\ntick_interval_ms = 10\n{extra}"
        ))
        .unwrap()
    }

    #[tokio::test]
    async fn end_to_end_with_null_exporter() {
        let p = Pipeline::spawn(&cfg(""), Box::new(NullExporter)).unwrap();
        let h = p.handle();
        assert_eq!(
            h.submit(ExportTraceServiceRequest::default())
                .await
                .unwrap()
                .spans,
            0
        );
        let a = h.submit(req(1, 3)).await.unwrap();
        assert_eq!(a.spans, 3);
        let mut bad = req(0, 1);
        bad.resource_spans[0].scope_spans[0]
            .spans
            .extend(req(2, 1).resource_spans[0].scope_spans[0].spans.clone());
        assert_eq!(h.submit(bad).await.unwrap().invalid, 1);
        let t = p.telemetry();
        assert_eq!(h.retry_after_secs(), 1);
        assert!(Arc::ptr_eq(h.telemetry(), &t));
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(t.core().stats.traces_sampled >= 2);
        p.shutdown().await;
        assert_eq!(t.export_spans_ok.get(), 4);
        assert_eq!(t.core().stats.spans_invalid, 1);
        assert_eq!(h.submit(req(3, 1)).await, Err(IngestError::AdmissionClosed));
    }

    struct Stuck;
    #[tonic::async_trait]
    impl Exporter for Stuck {
        async fn export(&self, _r: ExportTraceServiceRequest) -> Result<(), ExportError> {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn stalled_exporter_propagates_backpressure_to_receivers() {
        let c = cfg(
            "[backpressure]\nqueue_capacity = 2\n[exporter]\nqueue_capacity = 1\nmax_batch_spans = 1\nflush_interval_ms = 1",
        );
        let p = Pipeline::spawn(&c, Box::new(Stuck)).unwrap();
        let h = p.handle();
        let mut refused = 0;
        for i in 1..=200u8 {
            if h.submit(req(i, 1)).await == Err(IngestError::QueueFull) {
                refused += 1;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert!(refused > 0, "expected the ingest queue to fill");
        assert!(p.telemetry().rejected_queue_full.get() > 0);
        drop(p); // tasks are detached; the stuck exporter never finishes
    }

    #[tokio::test]
    async fn block_mode_waits_then_refuses() {
        let c = cfg(
            "[backpressure]\nqueue_capacity = 1\nmode = \"block\"\nblock_timeout_ms = 5\n[exporter]\nqueue_capacity = 1\nmax_batch_spans = 1\nflush_interval_ms = 1",
        );
        let p = Pipeline::spawn(&c, Box::new(Stuck)).unwrap();
        let h = p.handle();
        let mut refused = 0;
        for i in 1..=100u8 {
            if h.submit(req(i, 1)).await.is_err() {
                refused += 1;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert!(refused > 0);
    }

    #[tokio::test]
    async fn span_permits_bound_the_ingest_queue() {
        // Plenty of message slots but only 5 queued spans allowed.
        let c = cfg(
            "[backpressure]\nqueue_capacity = 100\nmax_queued_spans = 5\n[exporter]\nqueue_capacity = 1\nmax_queued_spans = 1\nmax_batch_spans = 1\nflush_interval_ms = 1",
        );
        let p = Pipeline::spawn(&c, Box::new(Stuck)).unwrap();
        let h = p.handle();
        let mut refused = 0;
        for i in 1..=100u8 {
            if h.submit(req(i, 3)).await == Err(IngestError::QueueFull) {
                refused += 1;
            }
        }
        assert!(refused > 0);
        // A request larger than the whole queue still fits (permits are capped).
        let big = Pipeline::spawn(
            &cfg("[backpressure]\nmax_queued_spans = 2"),
            Box::new(NullExporter),
        )
        .unwrap();
        assert_eq!(big.handle().submit(req(1, 10)).await.unwrap().spans, 10);
        big.shutdown().await;
    }

    #[tokio::test]
    async fn reject_new_closes_admission() {
        let c = cfg("max_traces = 2\neviction = \"reject_new\"");
        let c = Config {
            buffer: crate::config::BufferConfig {
                decision_wait_ms: 60_000,
                ..c.buffer
            },
            ..c
        };
        let p = Pipeline::spawn(&c, Box::new(NullExporter)).unwrap();
        let h = p.handle();
        for i in 1..=3u8 {
            h.submit(req(i, 1)).await.unwrap();
        }
        let mut closed = false;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            if h.submit(req(9, 1)).await == Err(IngestError::AdmissionClosed) {
                closed = true;
                break;
            }
        }
        assert!(closed);
        assert!(p.telemetry().rejected_admission.get() > 0);
        p.shutdown().await;
    }
}
