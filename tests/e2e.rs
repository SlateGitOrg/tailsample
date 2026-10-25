//! End-to-end tests over loopback sockets: receivers, pipeline, exporter
//! and a downstream sink. No external services are needed.

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::Request;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;

use tailsample::config::ExporterKind;
use tailsample::exporter::{ExportError, Exporter};
use tailsample::model::kv_str;
use tailsample::server::Server;
use tailsample::sink::Sink;
use tailsample::{Config, ExportTraceServiceRequest, TraceId};

fn request(trace: u8, spans: usize, error: bool) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
                attributes: vec![kv_str("service.name", "e2e")],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: (0..spans)
                    .map(|i| Span {
                        trace_id: vec![trace; 16],
                        span_id: vec![i as u8 + 1; 8],
                        name: format!("op{i}"),
                        start_time_unix_nano: 1_000,
                        end_time_unix_nano: 2_000,
                        status: Some(Status {
                            code: if error && i == 0 { 2 } else { 0 },
                            message: String::new(),
                        }),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn config(extra: &str) -> Config {
    let mut cfg = Config::from_toml_str(&format!(
        r#"
        [buffer]
        decision_wait_ms = 200
        tick_interval_ms = 20
        {extra}
        [exporter]
        flush_interval_ms = 20
        [[policies]]
        name = "errors"
        type = "status_code"
        codes = ["ERROR"]
        "#
    ))
    .unwrap();
    cfg.receiver.grpc_listen = Some("127.0.0.1:0".into());
    cfg.receiver.http_listen = Some("127.0.0.1:0".into());
    cfg
}

async fn post(
    url: &str,
    ctype: &str,
    encoding: Option<&str>,
    body: Vec<u8>,
) -> (u16, hyper::HeaderMap, Vec<u8>) {
    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let mut b = Request::builder()
        .method("POST")
        .uri(url)
        .header("content-type", ctype);
    if let Some(e) = encoding {
        b = b.header("content-encoding", e);
    }
    let resp = client
        .request(b.body(Full::new(Bytes::from(body))).unwrap())
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let headers = resp.headers().clone();
    let body = resp
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    (status, headers, body)
}

async fn get(url: &str) -> (u16, String) {
    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let resp = client
        .request(Request::get(url).body(Full::new(Bytes::new())).unwrap())
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&body).into_owned())
}

#[tokio::test]
async fn grpc_and_http_ingest_reach_the_sink() {
    let sink = Sink::start("127.0.0.1:0").await.unwrap();
    let mut cfg = config("");
    cfg.exporter.kind = ExporterKind::OtlpGrpc;
    cfg.exporter.endpoint = sink.endpoint();
    let server = Server::start(&cfg).await.unwrap();
    let grpc = format!("http://{}", server.grpc_addr().unwrap());
    let http = format!("http://{}/v1/traces", server.http_addr().unwrap());

    // gRPC: error trace (kept) and OK trace (dropped).
    let mut client = TraceServiceClient::connect(grpc).await.unwrap();
    client.export(request(1, 3, true)).await.unwrap();
    client.export(request(2, 3, false)).await.unwrap();
    // HTTP protobuf, gzip-compressed.
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(&request(3, 2, true).encode_to_vec()).unwrap();
    let (status, headers, _) = post(
        &http,
        "application/x-protobuf",
        Some("gzip"),
        gz.finish().unwrap(),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(headers["content-type"], "application/x-protobuf");
    // HTTP JSON fixture.
    let (status, headers, body) = post(
        &http,
        "application/json",
        None,
        tailsample::demo::FIXTURE_JSON.as_bytes().to_vec(),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(headers["content-type"], "application/json");
    assert!(body.starts_with(b"{"));

    tokio::time::sleep(Duration::from_millis(600)).await;
    let telemetry = server.telemetry();
    server.shutdown().await;
    let seen = sink.stop().await;
    assert_eq!(seen.spans_by_trace.get(&TraceId([1; 16])), Some(&3));
    assert_eq!(seen.spans_by_trace.get(&TraceId([3; 16])), Some(&2));
    assert!(!seen.spans_by_trace.contains_key(&TraceId([2; 16])));
    let fixture = TraceId::from_u128(0x4bf92f3577b34da6a3ce929d0e0e4736);
    assert_eq!(seen.spans_by_trace.get(&fixture), Some(&2));
    assert_eq!(seen.services.get("e2e"), Some(&5));
    assert_eq!(telemetry.grpc_requests.get(), 2);
    assert_eq!(telemetry.http_requests.get(), 2);
    assert_eq!(telemetry.export_spans_ok.get(), 7);
}

#[tokio::test]
async fn malformed_requests_metrics_and_health() {
    let server =
        Server::start_with_exporter(&config(""), Box::new(tailsample::exporter::NullExporter))
            .await
            .unwrap();
    let base = format!("http://{}", server.http_addr().unwrap());
    let url = format!("{base}/v1/traces");
    assert_eq!(
        post(&url, "application/json", None, b"{nope".to_vec())
            .await
            .0,
        400
    );
    assert_eq!(post(&url, "text/plain", None, b"x".to_vec()).await.0, 415);
    assert_eq!(
        post(&url, "application/x-protobuf", Some("br"), vec![])
            .await
            .0,
        415
    );
    // A small gzip body that inflates past max_request_bytes (8 MiB).
    let mut bomb = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
    bomb.write_all(&vec![0u8; 9 * 1024 * 1024]).unwrap();
    let bomb = bomb.finish().unwrap();
    assert!(bomb.len() < 64 * 1024);
    assert_eq!(
        post(&url, "application/x-protobuf", Some("gzip"), bomb)
            .await
            .0,
        413
    );

    // Invalid trace ids are reported as partial success.
    let mut bad = request(0, 2, false);
    bad.resource_spans[0].scope_spans[0].spans[1].trace_id = vec![5; 16];
    let (status, _, body) = post(&url, "application/x-protobuf", None, bad.encode_to_vec()).await;
    assert_eq!(status, 200);
    let resp =
        opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceResponse::decode(
            &body[..],
        )
        .unwrap();
    assert_eq!(resp.partial_success.unwrap().rejected_spans, 1);

    assert_eq!(get(&format!("{base}/healthz")).await, (200, "ok".into()));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (status, text) = get(&format!("{base}/metrics")).await;
    assert_eq!(status, 200);
    assert!(
        text.contains("tailsample_receiver_rejected_malformed_total 4"),
        "{text}"
    );
    assert!(
        text.contains("tailsample_spans_total{outcome=\"invalid\"} 1"),
        "{text}"
    );
    server.shutdown().await;
}

struct Stuck;

#[tonic::async_trait]
impl Exporter for Stuck {
    async fn export(&self, _req: ExportTraceServiceRequest) -> Result<(), ExportError> {
        tokio::time::sleep(Duration::from_secs(3_600)).await;
        Ok(())
    }
}

#[tokio::test]
async fn slow_downstream_turns_into_retryable_refusals() {
    let mut cfg = config("[backpressure]\nqueue_capacity = 2\nretry_after_secs = 2");
    cfg.buffer.decision_wait_ms = 1;
    cfg.exporter.queue_capacity = 1;
    cfg.exporter.max_batch_spans = 1;
    cfg.policies.clear(); // always sample: every span heads for the stuck exporter
    let server = Server::start_with_exporter(&cfg, Box::new(Stuck))
        .await
        .unwrap();
    let url = format!("http://{}/v1/traces", server.http_addr().unwrap());
    let mut grpc = TraceServiceClient::connect(format!("http://{}", server.grpc_addr().unwrap()))
        .await
        .unwrap();

    let mut http_429 = None;
    let mut grpc_unavailable = None;
    for i in 1..=250u8 {
        let (status, headers, _) = post(
            &url,
            "application/x-protobuf",
            None,
            request(i, 1, false).encode_to_vec(),
        )
        .await;
        if status == 429 && http_429.is_none() {
            http_429 = Some(headers["retry-after"].to_str().unwrap().to_string());
        }
        if let Err(st) = grpc.export(request(i, 1, false)).await {
            grpc_unavailable.get_or_insert(st.code());
        }
        if http_429.is_some() && grpc_unavailable.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(http_429.as_deref(), Some("2"));
    assert_eq!(grpc_unavailable, Some(tonic::Code::Unavailable));
    assert!(server.telemetry().rejected_queue_full.get() >= 2);
    // The stuck exporter never finishes, so the server is dropped rather
    // than shut down gracefully; the test runtime tears the tasks down.
}

#[tokio::test]
async fn reject_new_answers_503_when_buffer_full() {
    let mut cfg = config("max_traces = 2\neviction = \"reject_new\"");
    cfg.buffer.decision_wait_ms = 60_000;
    let server = Server::start_with_exporter(&cfg, Box::new(tailsample::exporter::NullExporter))
        .await
        .unwrap();
    let url = format!("http://{}/v1/traces", server.http_addr().unwrap());
    let mut got_503 = false;
    for i in 1..=50u8 {
        let (status, headers, _) = post(
            &url,
            "application/x-protobuf",
            None,
            request(i, 1, false).encode_to_vec(),
        )
        .await;
        if status == 503 {
            assert!(headers.contains_key("retry-after"));
            got_503 = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(got_503);
    server.shutdown().await;
}

#[tokio::test]
async fn shutdown_flushes_buffered_traces() {
    let sink = Sink::start("127.0.0.1:0").await.unwrap();
    let mut cfg = config("");
    cfg.buffer.decision_wait_ms = 600_000; // would never decide on its own
    cfg.exporter.kind = ExporterKind::OtlpGrpc;
    cfg.exporter.endpoint = sink.endpoint();
    cfg.receiver.grpc_listen = None;
    let server = Server::start(&cfg).await.unwrap();
    assert!(server.grpc_addr().is_none());
    let url = format!("http://{}/v1/traces", server.http_addr().unwrap());
    for i in 1..=20u8 {
        let (status, _, _) = post(
            &url,
            "application/x-protobuf",
            None,
            request(i, 2, i % 2 == 0).encode_to_vec(),
        )
        .await;
        assert_eq!(status, 200);
    }
    server.shutdown().await;
    let seen = sink.stop().await;
    assert_eq!(seen.spans_by_trace.len(), 10, "even traces carry errors");
    assert_eq!(seen.spans(), 20);
}

#[tokio::test]
async fn http_exporter_delivers_to_another_sampler() {
    // Chain: sampler A (exports OTLP/HTTP) -> sampler B (HTTP receiver) -> sink.
    let sink = Sink::start("127.0.0.1:0").await.unwrap();
    let mut b_cfg = config("");
    b_cfg.exporter.kind = ExporterKind::OtlpGrpc;
    b_cfg.exporter.endpoint = sink.endpoint();
    b_cfg.policies.clear();
    let b = Server::start(&b_cfg).await.unwrap();
    let mut a_cfg = config("");
    a_cfg.exporter.kind = ExporterKind::OtlpHttp;
    a_cfg.exporter.endpoint = format!("http://{}/v1/traces", b.http_addr().unwrap());
    let a = Server::start(&a_cfg).await.unwrap();
    let mut client = TraceServiceClient::connect(format!("http://{}", a.grpc_addr().unwrap()))
        .await
        .unwrap();
    client.export(request(7, 4, true)).await.unwrap();
    client.export(request(8, 4, false)).await.unwrap();
    a.shutdown().await;
    b.shutdown().await;
    let seen = sink.stop().await;
    assert_eq!(seen.spans_by_trace.get(&TraceId([7; 16])), Some(&4));
    assert!(!seen.spans_by_trace.contains_key(&TraceId([8; 16])));
}

#[tokio::test]
async fn bind_conflict_is_reported() {
    let holder = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut cfg = config("");
    cfg.receiver.grpc_listen = Some(holder.local_addr().unwrap().to_string());
    let err = Server::start(&cfg).await.err().expect("bind must fail");
    assert!(err.to_string().contains("cannot bind"), "{err}");
    drop(Arc::new(holder));
}
