//! OTLP/HTTP receiver: `POST /v1/traces` with binary protobuf or JSON bodies
//! (optionally gzip-compressed), plus `GET /metrics` and `GET /healthz`.

use std::future::Future;
use std::io::Read;
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTracePartialSuccess, ExportTraceServiceResponse,
};
use prost::Message;
use tokio::net::TcpListener;

use crate::runtime::{IngestError, IngestHandle};
use crate::ExportTraceServiceRequest;

#[derive(Clone)]
struct AppState {
    handle: IngestHandle,
    max_request_bytes: usize,
}

/// Body encodings understood by the receiver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    /// `application/x-protobuf`
    Protobuf,
    /// `application/json`
    Json,
}

impl Encoding {
    fn content_type(self) -> &'static str {
        match self {
            Encoding::Protobuf => "application/x-protobuf",
            Encoding::Json => "application/json",
        }
    }
}

/// Decode failures, reported as HTTP 400 / 415 / 413.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DecodeError {
    /// Unsupported content type.
    #[error("unsupported content type {0:?}")]
    UnsupportedMediaType(String),
    /// Unsupported content encoding.
    #[error("unsupported content encoding {0:?}")]
    UnsupportedEncoding(String),
    /// Body exceeded the limit after decompression.
    #[error("request body exceeds {0} bytes")]
    TooLarge(usize),
    /// Body could not be parsed.
    #[error("malformed body: {0}")]
    Malformed(String),
}

/// Decode a request body according to its headers.
pub fn decode_body(
    headers: &HeaderMap,
    body: &[u8],
    limit: usize,
) -> Result<(ExportTraceServiceRequest, Encoding), DecodeError> {
    let ctype = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/x-protobuf");
    let base = ctype.split(';').next().unwrap_or("").trim();
    let enc = match base {
        "application/x-protobuf" | "application/protobuf" => Encoding::Protobuf,
        "application/json" => Encoding::Json,
        other => return Err(DecodeError::UnsupportedMediaType(other.to_string())),
    };
    let content_encoding = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("identity");
    let decompressed;
    let raw: &[u8] = match content_encoding {
        "identity" | "" => body,
        "gzip" => {
            let mut buf = Vec::new();
            let read = flate2::read::GzDecoder::new(body)
                .take(limit as u64 + 1)
                .read_to_end(&mut buf)
                .map_err(|e| DecodeError::Malformed(format!("gzip: {e}")))?;
            if read > limit {
                return Err(DecodeError::TooLarge(limit));
            }
            decompressed = buf;
            &decompressed
        }
        other => return Err(DecodeError::UnsupportedEncoding(other.to_string())),
    };
    let req = match enc {
        Encoding::Protobuf => ExportTraceServiceRequest::decode(raw)
            .map_err(|e| DecodeError::Malformed(e.to_string()))?,
        Encoding::Json => {
            serde_json::from_slice(raw).map_err(|e| DecodeError::Malformed(e.to_string()))?
        }
    };
    Ok((req, enc))
}

fn encode_response(resp: &ExportTraceServiceResponse, enc: Encoding) -> Vec<u8> {
    match enc {
        Encoding::Protobuf => resp.encode_to_vec(),
        Encoding::Json => serde_json::to_vec(resp).unwrap_or_else(|_| b"{}".to_vec()),
    }
}

async fn export(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let telemetry = Arc::clone(st.handle.telemetry());
    telemetry.http_requests.inc();
    let (req, enc) = match decode_body(&headers, &body, st.max_request_bytes) {
        Ok(v) => v,
        Err(e) => {
            telemetry.rejected_malformed.inc();
            let code = match e {
                DecodeError::UnsupportedMediaType(_) | DecodeError::UnsupportedEncoding(_) => {
                    StatusCode::UNSUPPORTED_MEDIA_TYPE
                }
                DecodeError::TooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
                DecodeError::Malformed(_) => StatusCode::BAD_REQUEST,
            };
            return (code, e.to_string()).into_response();
        }
    };
    match st.handle.submit(req).await {
        Ok(acc) => {
            let resp = ExportTraceServiceResponse {
                partial_success: (acc.invalid > 0).then(|| ExportTracePartialSuccess {
                    rejected_spans: acc.invalid as i64,
                    error_message: "spans with invalid trace ids were rejected".into(),
                }),
            };
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, enc.content_type())
                .body(Body::from(encode_response(&resp, enc)))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
        Err(e) => refusal(e, st.handle.retry_after_secs()),
    }
}

/// HTTP response for an ingest refusal.
pub fn refusal(err: IngestError, retry_after_secs: u64) -> Response {
    let code = match err {
        IngestError::QueueFull => StatusCode::TOO_MANY_REQUESTS,
        IngestError::AdmissionClosed | IngestError::ShuttingDown => StatusCode::SERVICE_UNAVAILABLE,
    };
    let mut r = (code, err.to_string()).into_response();
    if let Ok(v) = HeaderValue::from_str(&retry_after_secs.to_string()) {
        r.headers_mut().insert(header::RETRY_AFTER, v);
    }
    r
}

async fn metrics(State(st): State<AppState>) -> Response {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        st.handle.telemetry().render_prometheus(),
    )
        .into_response()
}

async fn healthz() -> &'static str {
    "ok"
}

/// Build the router (exposed for tests).
pub fn router(handle: IngestHandle, max_request_bytes: usize) -> Router {
    Router::new()
        .route("/v1/traces", post(export))
        .route("/metrics", get(metrics))
        .route("/healthz", get(healthz))
        .layer(DefaultBodyLimit::max(max_request_bytes))
        .with_state(AppState {
            handle,
            max_request_bytes,
        })
}

/// Serve on an already-bound listener until `shutdown` resolves.
pub async fn serve(
    listener: TcpListener,
    handle: IngestHandle,
    max_request_bytes: usize,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    axum::serve(listener, router(handle, max_request_bytes))
        .with_graceful_shutdown(shutdown)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn headers(ct: &str, ce: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(header::CONTENT_TYPE, HeaderValue::from_str(ct).unwrap());
        if let Some(ce) = ce {
            h.insert(header::CONTENT_ENCODING, HeaderValue::from_str(ce).unwrap());
        }
        h
    }

    const JSON: &str = r#"{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"svc"}}]},
      "scopeSpans":[{"scope":{"name":"lib"},"spans":[{"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174",
      "name":"GET /","kind":2,"startTimeUnixNano":"1544712660000000000","endTimeUnixNano":"1544712661000000000",
      "attributes":[{"key":"http.status_code","value":{"intValue":"500"}}],"status":{"code":2}}]}]}]}"#;

    #[test]
    fn decodes_otlp_json() {
        let (req, enc) = decode_body(&headers("application/json", None), JSON.as_bytes(), 1 << 20).unwrap();
        assert_eq!(enc, Encoding::Json);
        let span = &req.resource_spans[0].scope_spans[0].spans[0];
        assert_eq!(span.trace_id.len(), 16);
        assert_eq!(span.trace_id[0], 0x5b);
        assert_eq!(span.end_time_unix_nano - span.start_time_unix_nano, 1_000_000_000);
        assert_eq!(span.status.as_ref().unwrap().code, 2);
    }

    #[test]
    fn decodes_gzip_protobuf_and_enforces_limits() {
        let (req, _) = decode_body(&headers("application/json", None), JSON.as_bytes(), 1 << 20).unwrap();
        let pb = req.encode_to_vec();
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        gz.write_all(&pb).unwrap();
        let gz = gz.finish().unwrap();
        let h = headers("application/x-protobuf", Some("gzip"));
        let (back, enc) = decode_body(&h, &gz, 1 << 20).unwrap();
        assert_eq!(enc, Encoding::Protobuf);
        assert_eq!(back, req);
        assert_eq!(decode_body(&h, &gz, 10).unwrap_err(), DecodeError::TooLarge(10));
        assert!(matches!(decode_body(&h, b"not gzip", 100), Err(DecodeError::Malformed(_))));
    }

    #[test]
    fn rejects_unknown_types_and_garbage() {
        assert!(matches!(
            decode_body(&headers("text/plain", None), b"", 10),
            Err(DecodeError::UnsupportedMediaType(_))
        ));
        assert!(matches!(
            decode_body(&headers("application/json", Some("br")), b"", 10),
            Err(DecodeError::UnsupportedEncoding(_))
        ));
        assert!(matches!(
            decode_body(&headers("application/json; charset=utf-8", None), b"{", 10),
            Err(DecodeError::Malformed(_))
        ));
        assert!(matches!(
            decode_body(&headers("application/x-protobuf", None), &[0xff, 0xff, 0xff], 10),
            Err(DecodeError::Malformed(_))
        ));
        // Missing content type defaults to protobuf; empty body is a valid empty request.
        assert!(decode_body(&HeaderMap::new(), b"", 10).is_ok());
    }

    #[test]
    fn refusals_carry_retry_after() {
        let r = refusal(IngestError::QueueFull, 3);
        assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(r.headers()[header::RETRY_AFTER], "3");
        assert_eq!(refusal(IngestError::AdmissionClosed, 1).status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(refusal(IngestError::ShuttingDown, 1).status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn json_response_encoding() {
        let resp = ExportTraceServiceResponse::default();
        let json = encode_response(&resp, Encoding::Json);
        assert!(serde_json::from_slice::<serde_json::Value>(&json).unwrap().is_object());
        assert!(encode_response(&resp, Encoding::Protobuf).is_empty());
    }
}
