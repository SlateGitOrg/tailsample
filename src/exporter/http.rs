//! OTLP/HTTP exporter (binary protobuf bodies).

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::{CONTENT_TYPE, HeaderValue};
use hyper::{Method, Request, StatusCode, Uri};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use prost::Message;

use super::{ExportError, Exporter};
use crate::ExportTraceServiceRequest;

/// POSTs protobuf-encoded requests to an OTLP/HTTP endpoint
/// (e.g. `http://127.0.0.1:4318/v1/traces`).
#[derive(Debug, Clone)]
pub struct HttpExporter {
    uri: Uri,
    client: Client<HttpConnector, Full<Bytes>>,
}

impl HttpExporter {
    /// Validate the endpoint and build a pooled client.
    pub fn new(endpoint: &str) -> Result<Self, ExportError> {
        let uri: Uri = endpoint
            .parse()
            .map_err(|e| ExportError::permanent(format!("invalid HTTP endpoint {endpoint:?}: {e}")))?;
        if uri.scheme_str() != Some("http") || uri.host().is_none() {
            return Err(ExportError::permanent(format!(
                "HTTP endpoint must be an absolute http:// URL, got {endpoint:?}"
            )));
        }
        Ok(HttpExporter {
            uri,
            client: Client::builder(TokioExecutor::new()).build_http(),
        })
    }
}

/// Whether an HTTP status is worth retrying (per the OTLP specification).
pub fn retryable_status(s: StatusCode) -> bool {
    matches!(s.as_u16(), 429 | 502 | 503 | 504)
}

#[tonic::async_trait]
impl Exporter for HttpExporter {
    async fn export(&self, req: ExportTraceServiceRequest) -> Result<(), ExportError> {
        let body = Full::new(Bytes::from(req.encode_to_vec()));
        let request = Request::builder()
            .method(Method::POST)
            .uri(self.uri.clone())
            .header(CONTENT_TYPE, HeaderValue::from_static("application/x-protobuf"))
            .body(body)
            .map_err(|e| ExportError::permanent(format!("building request: {e}")))?;
        let resp = self
            .client
            .request(request)
            .await
            .map_err(|e| ExportError::retryable(format!("HTTP export failed: {e}")))?;
        let status = resp.status();
        // Drain the body so the connection can be reused.
        let _ = resp.into_body().collect().await;
        if status.is_success() {
            Ok(())
        } else if retryable_status(status) {
            Err(ExportError::retryable(format!("HTTP export got {status}")))
        } else {
            Err(ExportError::permanent(format!("HTTP export got {status}")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_endpoint_and_classifies() {
        assert!(HttpExporter::new("http://127.0.0.1:4318/v1/traces").is_ok());
        assert!(HttpExporter::new("https://x/v1/traces").is_err());
        assert!(HttpExporter::new("/v1/traces").is_err());
        assert!(retryable_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(retryable_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(!retryable_status(StatusCode::BAD_REQUEST));
    }

    #[tokio::test]
    async fn unreachable_endpoint_is_retryable() {
        let e = HttpExporter::new("http://127.0.0.1:9/v1/traces").unwrap();
        let err = e.export(ExportTraceServiceRequest::default()).await.unwrap_err();
        assert!(err.retryable, "{err}");
    }
}
