//! OTLP/gRPC exporter.

use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use tonic::transport::{Channel, Endpoint};
use tonic::Code;

use super::{ExportError, Exporter};
use crate::ExportTraceServiceRequest;

/// Sends requests to an OTLP/gRPC endpoint over a lazily connected channel.
#[derive(Debug, Clone)]
pub struct GrpcExporter {
    client: TraceServiceClient<Channel>,
}

impl GrpcExporter {
    /// `endpoint` like `http://127.0.0.1:4317`. Connection happens on first use.
    pub fn new(endpoint: &str) -> Result<Self, ExportError> {
        let ep = Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| ExportError::permanent(format!("invalid gRPC endpoint {endpoint:?}: {e}")))?;
        Ok(GrpcExporter {
            client: TraceServiceClient::new(ep.connect_lazy()),
        })
    }
}

/// Whether a gRPC status is worth retrying (per the OTLP specification).
pub fn retryable_code(code: Code) -> bool {
    matches!(
        code,
        Code::Cancelled
            | Code::DeadlineExceeded
            | Code::Aborted
            | Code::OutOfRange
            | Code::Unavailable
            | Code::DataLoss
            | Code::ResourceExhausted
    )
}

#[tonic::async_trait]
impl Exporter for GrpcExporter {
    async fn export(&self, req: ExportTraceServiceRequest) -> Result<(), ExportError> {
        let mut client = self.client.clone();
        match client.export(req).await {
            Ok(resp) => {
                if let Some(p) = resp.into_inner().partial_success {
                    if p.rejected_spans > 0 {
                        tracing::warn!(rejected = p.rejected_spans, msg = %p.error_message, "downstream rejected spans");
                    }
                }
                Ok(())
            }
            Err(status) => {
                let msg = format!("gRPC export failed: {:?}: {}", status.code(), status.message());
                if retryable_code(status.code()) {
                    Err(ExportError::retryable(msg))
                } else {
                    Err(ExportError::permanent(msg))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_codes() {
        assert!(retryable_code(Code::Unavailable));
        assert!(retryable_code(Code::ResourceExhausted));
        assert!(!retryable_code(Code::InvalidArgument));
        assert!(!retryable_code(Code::Unauthenticated));
    }

    #[tokio::test]
    async fn unreachable_endpoint_is_retryable() {
        // Port 9 (discard) on loopback is closed on CI and dev machines.
        let e = GrpcExporter::new("http://127.0.0.1:9").unwrap();
        let err = e.export(ExportTraceServiceRequest::default()).await.unwrap_err();
        assert!(err.retryable, "{err}");
    }
}
