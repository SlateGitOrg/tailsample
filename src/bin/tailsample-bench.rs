//! Throughput and memory benchmark.
//!
//! Heap usage is measured with a counting global allocator (live bytes and
//! the high-water mark), so "peak heap" numbers are real allocations made by
//! this process, not estimates. Run via `make bench` (or
//! `cargo run --release --bin tailsample-bench -- [--quick]`).

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use tailsample::config::{Config, EvictionPolicy};
use tailsample::exporter::{ExportError, Exporter, NullExporter};
use tailsample::loadgen::{GenConfig, Schedule};
use tailsample::model::{flatten, span_count};
use tailsample::sampler::Sampler;
use tailsample::server::Server;
use tailsample::ExportTraceServiceRequest;

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded unchanged to the system allocator.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let now = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` was allocated by `alloc` above with this layout.
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn reset_peak() -> usize {
    let live = LIVE.load(Ordering::Relaxed);
    PEAK.store(live, Ordering::Relaxed);
    live
}

fn mib(b: usize) -> f64 {
    b as f64 / (1024.0 * 1024.0)
}

const REFERENCE: &str = include_str!("../../examples/sampler.toml");

fn reference_config() -> Config {
    Config::from_toml_str(REFERENCE).expect("reference config")
}

fn workload(traces: u64, rate: f64, seed: u64) -> Vec<(u64, ExportTraceServiceRequest)> {
    let mut s = Schedule::new(
        GenConfig {
            seed,
            ..GenConfig::default()
        },
        traces,
        rate,
        false,
    );
    let mut out = Vec::new();
    let mut now = 0;
    while !s.done() {
        for r in s.due(now) {
            out.push((now, r));
        }
        now += 50;
    }
    out
}

/// Core: flatten + ingest + tick on a simulated clock, pre-generated input.
fn core_throughput(traces: u64, runs: usize) {
    println!("## Core sampler throughput (single thread, simulated clock)\n");
    println!("Workload: {traces} generated traces at 2000 traces/s of simulated time; reference policies (`examples/sampler.toml`).\n");
    println!("| run | spans | wall time | spans/s | ns/span |");
    println!("|---:|---:|---:|---:|---:|");
    let mut rates = Vec::new();
    for run in 1..=runs {
        let input = workload(traces, 2_000.0, 1_000 + run as u64);
        let spans: usize = input.iter().map(|(_, r)| span_count(r)).sum();
        let mut sampler = Sampler::new(&reference_config()).expect("sampler");
        let mut out = Vec::new();
        let mut last = 0;
        let start = Instant::now();
        for (t, req) in input {
            let flat = flatten(req);
            sampler.ingest(flat.records, t, &mut out);
            sampler.tick(t, &mut out);
            out.clear();
            last = t;
        }
        sampler.tick(last + 10_000, &mut out);
        let el = start.elapsed();
        let rate = spans as f64 / el.as_secs_f64();
        rates.push(rate);
        println!(
            "| {run} | {spans} | {:.3} s | {:.0} | {:.0} |",
            el.as_secs_f64(),
            rate,
            el.as_nanos() as f64 / spans as f64
        );
    }
    rates.sort_by(f64::total_cmp);
    println!("\nMedian: **{:.0} spans/s**.\n", rates[rates.len() / 2]);
}

/// Memory: streamed workload, heap high-water mark versus buffer limit.
fn memory_bound(traces: u64) {
    println!("## Memory bound under sustained overload\n");
    println!(
        "Workload: {traces} traces streamed at 4000 traces/s (~48k spans/s), 5 s decision wait,"
    );
    println!(
        "so an unbounded buffer would need ~240k spans in memory. `peak heap` is the counting"
    );
    println!("allocator's high-water mark during the run; it includes the load generator's own");
    println!(
        "in-flight spans, which the 64 KiB control row measures. `sampler heap` = peak heap minus"
    );
    println!(
        "that control, i.e. memory attributable to the buffer, decision cache and bookkeeping.\n"
    );
    println!("| max_bytes | eviction | peak est. buffer | peak heap | sampler heap | sampler heap / limit | decided early | evicted | rejected spans | error traces kept |");
    println!("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|");
    let mut control = 0usize;
    for (limit_kib, eviction) in [
        (64usize, EvictionPolicy::DecideEarly),
        (8 * 1024, EvictionPolicy::DecideEarly),
        (32 * 1024, EvictionPolicy::DecideEarly),
        (128 * 1024, EvictionPolicy::DecideEarly),
        (32 * 1024, EvictionPolicy::DropOldest),
        (32 * 1024, EvictionPolicy::RejectNew),
        (1024 * 1024, EvictionPolicy::DecideEarly),
    ] {
        let mut cfg = reference_config();
        cfg.buffer.max_bytes = limit_kib * 1024;
        cfg.buffer.max_traces = 1_000_000;
        cfg.buffer.eviction = eviction;
        let mut sampler = Sampler::new(&cfg).expect("sampler");
        let mut sched = Schedule::new(
            GenConfig {
                seed: 2_024,
                ..GenConfig::default()
            },
            traces,
            4_000.0,
            true,
        );
        let mut out = Vec::new();
        let mut kept_errors = std::collections::HashSet::new();
        let mut now = 0;
        let mut peak_est = 0;
        let base = reset_peak();
        while !sched.done() {
            for r in sched.due(now) {
                let flat = flatten(r);
                sampler.ingest(flat.records, now, &mut out);
            }
            sampler.tick(now, &mut out);
            for r in out.drain(..) {
                kept_errors.insert(r.trace_id);
            }
            peak_est = peak_est.max(sampler.gauges().bytes);
            now += 50;
        }
        let peak_heap = PEAK.load(Ordering::Relaxed).saturating_sub(base);
        while let Some(d) = sampler.next_deadline() {
            sampler.tick(d, &mut out);
            for r in out.drain(..) {
                kept_errors.insert(r.trace_id);
            }
        }
        let truth = sched.truth().expect("truth");
        let errors: Vec<_> = truth
            .iter()
            .filter(|(_, t)| t.error)
            .map(|(id, _)| *id)
            .collect();
        let kept = errors.iter().filter(|id| kept_errors.contains(id)).count();
        let s = sampler.stats();
        if limit_kib == 64 {
            control = peak_heap;
        }
        let attributable = peak_heap.saturating_sub(control);
        let label = if limit_kib < 1024 {
            format!("{limit_kib} KiB (control)")
        } else {
            format!("{} MiB", limit_kib / 1024)
        };
        println!(
            "| {label} | {eviction:?} | {:.1} MiB | {:.1} MiB | {:.1} MiB | {:.2} | {} | {} | {} | {}/{} ({:.1}%) |",
            mib(peak_est),
            mib(peak_heap),
            mib(attributable),
            attributable as f64 / cfg.buffer.max_bytes as f64,
            s.traces_decided_early,
            s.traces_evicted,
            s.spans_rejected_full,
            kept,
            errors.len(),
            100.0 * kept as f64 / errors.len().max(1) as f64
        );
        drop(sampler);
    }
    println!();
}

struct Slow(Duration);

#[tonic::async_trait]
impl Exporter for Slow {
    async fn export(&self, _req: ExportTraceServiceRequest) -> Result<(), ExportError> {
        tokio::time::sleep(self.0).await;
        Ok(())
    }
}

/// End-to-end over loopback gRPC with concurrent clients.
async fn e2e(
    label: &str,
    cfg: Config,
    exporter: Box<dyn Exporter>,
    secs: u64,
    clients: usize,
    input: Arc<Vec<ExportTraceServiceRequest>>,
) {
    let base = reset_peak();
    let server = Server::start_with_exporter(&cfg, exporter)
        .await
        .expect("server");
    let endpoint = format!("http://{}", server.grpc_addr().expect("grpc"));
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut tasks = Vec::new();
    for c in 0..clients {
        let endpoint = endpoint.clone();
        let input = Arc::clone(&input);
        tasks.push(tokio::spawn(async move {
            let mut client = TraceServiceClient::connect(endpoint)
                .await
                .expect("connect");
            let (mut ok, mut refused, mut i) = (0u64, 0u64, c);
            while Instant::now() < deadline {
                let mut req = input[i % input.len()].clone();
                // Every pass over the input gets fresh trace ids, so resent
                // spans are new traces rather than cheap decision-cache hits.
                let round = (i / input.len()) as u64;
                if round > 0 {
                    let salt = round.wrapping_mul(0x9e37_79b9_7f4a_7c15).to_be_bytes();
                    for span in req
                        .resource_spans
                        .iter_mut()
                        .flat_map(|rs| rs.scope_spans.iter_mut())
                        .flat_map(|ss| ss.spans.iter_mut())
                    {
                        for (b, s) in span.trace_id.iter_mut().zip(salt) {
                            *b ^= s;
                        }
                    }
                }
                let n = span_count(&req) as u64;
                match client.export(req).await {
                    Ok(_) => ok += n,
                    Err(_) => {
                        refused += 1;
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }
                i += clients;
            }
            (ok, refused)
        }));
    }
    let (mut ok, mut refused) = (0, 0);
    for t in tasks {
        let (a, b) = t.await.expect("client");
        ok += a;
        refused += b;
    }
    let peak = PEAK.load(Ordering::Relaxed).saturating_sub(base);
    let tel = server.telemetry();
    let core = tel.core();
    // Do not wait for a slow exporter to drain during the benchmark.
    drop(server);
    println!(
        "| {label} | {clients} | {secs} s | {:.0} | {refused} | {} | {:.1} MiB |",
        ok as f64 / secs as f64,
        core.stats.traces_sampled,
        mib(peak)
    );
}

fn main() {
    let quick = std::env::args().any(|a| a == "--quick");
    let (core_traces, runs, mem_traces, e2e_secs) = if quick {
        (5_000, 1, 8_000, 2)
    } else {
        (100_000, 3, 60_000, 10)
    };
    println!(
        "# tailsample benchmark{}\n",
        if quick { " (quick)" } else { "" }
    );
    core_throughput(core_traces, runs);
    memory_bound(mem_traces);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        println!("## End-to-end OTLP/gRPC over loopback\n");
        println!("Clients replay pre-generated requests (<=512 spans each) as fast as possible, rewriting");
        println!("trace ids on every pass so all traffic is new traces; a refused request is counted and");
        println!("retried after 5 ms. Server: 4 tokio worker threads, reference policies, 1 s decision wait.\n");
        println!("| scenario | clients | duration | accepted spans/s | refused requests | traces sampled | peak heap |");
        println!("|---|---:|---:|---:|---:|---:|---:|");
        let input: Arc<Vec<ExportTraceServiceRequest>> = Arc::new(
            workload(if quick { 3_000 } else { 20_000 }, 20_000.0, 77)
                .into_iter()
                .map(|(_, r)| r)
                .collect(),
        );
        let mut cfg = reference_config();
        cfg.receiver.grpc_listen = Some("127.0.0.1:0".into());
        cfg.receiver.http_listen = None;
        cfg.buffer.max_bytes = 256 * 1024 * 1024;
        cfg.buffer.decision_wait_ms = 1_000;
        e2e("null exporter", cfg.clone(), Box::new(NullExporter), e2e_secs, 8, Arc::clone(&input)).await;
        let mut slow = cfg.clone();
        slow.policies.clear(); // always sample, so every span must cross the slow exporter
        slow.backpressure.queue_capacity = 64;
        slow.exporter.queue_capacity = 4;
        e2e(
            "exporter capped at ~50k spans/s (always sample)",
            slow,
            Box::new(Slow(Duration::from_millis(20))),
            e2e_secs,
            8,
            input,
        )
        .await;
        println!();
    });
}
