//! Memory-bound regression tests for shared resource / scope context.
//!
//! Spans keep the resource and scope they arrived with alive through `Arc`s.
//! When every request carries one span plus its own copy of a large resource
//! (SDKs with a simple/per-span processor and big k8s resource attributes),
//! that context dominates memory, so the buffer must charge it against
//! `max_bytes`. Retained context is measured independently of the sampler's
//! own estimate, through `Weak` handles to every resource ever ingested.

use std::sync::{Arc, Weak};

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use tailsample::ids::TraceId;
use tailsample::model::{flatten, kv_str, ResourceContext};
use tailsample::sampler::Sampler;
use tailsample::Config;

const RESOURCE_ATTR_BYTES: usize = 4 * 1024;
const MAX_BYTES: usize = 1024 * 1024;

fn span(trace: u128, id: u64) -> Span {
    Span {
        trace_id: TraceId::from_u128(trace).0.to_vec(),
        span_id: id.to_be_bytes().to_vec(),
        name: "GET /checkout".into(),
        start_time_unix_nano: 1,
        end_time_unix_nano: 2,
        ..Default::default()
    }
}

/// One request: one resource with a `RESOURCE_ATTR_BYTES` attribute and the
/// given spans.
fn request(spans: Vec<Span>) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    kv_str("service.name", "checkout"),
                    kv_str("k8s.pod.labels", "x".repeat(RESOURCE_ATTR_BYTES)),
                ],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn sampler(eviction: &str) -> Sampler {
    // Decision wait far beyond the test, so only the memory limit removes
    // traces from the buffer.
    let cfg = Config::from_toml_str(&format!(
        r#"
        [buffer]
        decision_wait_ms = 600000
        max_traces = 1000000
        max_bytes = {MAX_BYTES}
        eviction = "{eviction}"

        [[policies]]
        name = "all"
        type = "always_sample"
        "#
    ))
    .expect("valid config");
    Sampler::new(&cfg).expect("sampler")
}

/// Bytes of resource attribute data still alive (held by the sampler, since
/// the test drops every released span immediately).
fn retained_resource_bytes(handles: &[Weak<ResourceContext>]) -> usize {
    handles.iter().filter(|w| w.strong_count() > 0).count() * RESOURCE_ATTR_BYTES
}

#[test]
fn many_single_span_requests_stay_within_max_bytes() {
    for eviction in ["decide_early", "drop_oldest", "reject_new"] {
        let mut s = sampler(eviction);
        let mut handles = Vec::new();
        let mut out = Vec::new();
        let mut peak_retained = 0;
        for i in 0..3_000u64 {
            let flat = flatten(request(vec![span(u128::from(i) + 1, 1)]));
            handles.push(Arc::downgrade(&flat.records[0].ctx.resource));
            s.ingest(flat.records, i, &mut out);
            out.clear();
            let g = s.gauges();
            assert!(
                g.bytes <= MAX_BYTES,
                "{eviction}: estimate {} > {MAX_BYTES}",
                g.bytes
            );
            peak_retained = peak_retained.max(retained_resource_bytes(&handles));
        }
        // Before shared context was charged, the buffer held all 3,000 spans
        // (~12 MiB of resource data) while estimating under 1 MiB.
        assert!(
            peak_retained <= MAX_BYTES,
            "{eviction}: retained {peak_retained} bytes of resource data > {MAX_BYTES}"
        );
        let buffered = s.gauges().spans;
        assert!(
            buffered < MAX_BYTES / RESOURCE_ATTR_BYTES,
            "{eviction}: {buffered} spans buffered"
        );
        assert!(buffered > 0, "{eviction}: buffer unexpectedly empty");
        assert_eq!(s.stats().spans_received, s.accounted_spans());
    }
}

#[test]
fn batched_spans_share_one_context_charge() {
    // Same resource size, but 200 spans per request: the context is charged
    // once per request, so far more spans fit than with one span per request.
    let mut s = sampler("decide_early");
    let mut out = Vec::new();
    let mut handles = Vec::new();
    let mut trace = 0u128;
    for i in 0..10u64 {
        let spans = (0..200)
            .map(|j| {
                trace += 1;
                span(trace, j)
            })
            .collect();
        let flat = flatten(request(spans));
        handles.push(Arc::downgrade(&flat.records[0].ctx.resource));
        s.ingest(flat.records, i, &mut out);
        out.clear();
        assert!(s.gauges().bytes <= MAX_BYTES);
        assert!(retained_resource_bytes(&handles) <= MAX_BYTES);
    }
    assert_eq!(
        s.stats().traces_decided_early,
        0,
        "2,000 batched spans should fit in 1 MiB"
    );
    // One span per request with the same resource fits fewer than 256.
    assert_eq!(s.gauges().spans, 2_000);
    assert_eq!(s.stats().spans_received, s.accounted_spans());
}
