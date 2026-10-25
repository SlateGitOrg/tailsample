//! OTLP/gRPC receiver (`opentelemetry.proto.collector.trace.v1.TraceService/Export`).

use std::net::SocketAddr;
use std::future::Future;

use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTracePartialSuccess, ExportTraceServiceResponse,
};
use tokio::net::TcpListener;
use tonic::codec::CompressionEncoding;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status};

use crate::runtime::{IngestError, IngestHandle};
use crate::ExportTraceServiceRequest;

/// gRPC service implementation.
pub struct GrpcReceiver {
    handle: IngestHandle,
}

impl GrpcReceiver {
    /// New receiver feeding `handle`.
    pub fn new(handle: IngestHandle) -> Self {
        GrpcReceiver { handle }
    }
}

/// Map an ingest refusal to a gRPC status. `UNAVAILABLE` is used for every
/// transient condition because OTLP clients retry it unconditionally.
pub fn status_for(err: IngestError, retry_after_secs: u64) -> Status {
    let mut st = Status::unavailable(format!("{err}; retry after {retry_after_secs}s"));
    if let Ok(v) = retry_after_secs.to_string().parse() {
        st.metadata_mut().insert("retry-after", v);
    }
    st
}

#[tonic::async_trait]
impl TraceService for GrpcReceiver {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        self.handle.telemetry().grpc_requests.inc();
        match self.handle.submit(request.into_inner()).await {
            Ok(acc) => Ok(Response::new(ExportTraceServiceResponse {
                partial_success: (acc.invalid > 0).then(|| ExportTracePartialSuccess {
                    rejected_spans: acc.invalid as i64,
                    error_message: "spans with invalid trace ids were rejected".into(),
                }),
            })),
            Err(e) => Err(status_for(e, self.handle.retry_after_secs())),
        }
    }
}

/// Serve the gRPC receiver on an already-bound listener until `shutdown`
/// resolves.
pub async fn serve(
    listener: TcpListener,
    handle: IngestHandle,
    max_request_bytes: usize,
    shutdown: impl Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    let svc = TraceServiceServer::new(GrpcReceiver::new(handle))
        .accept_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(max_request_bytes);
    tonic::transport::Server::builder()
        .add_service(svc)
        .serve_with_incoming_shutdown(TcpIncoming::from(listener), shutdown)
        .await
}

/// Bind helper returning the listener and its actual address.
pub async fn bind(addr: &str) -> std::io::Result<(TcpListener, SocketAddr)> {
    let l = TcpListener::bind(addr).await?;
    let a = l.local_addr()?;
    Ok((l, a))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_are_retryable_with_hint() {
        let s = status_for(IngestError::QueueFull, 2);
        assert_eq!(s.code(), tonic::Code::Unavailable);
        assert!(s.message().contains("queue full"));
        assert_eq!(s.metadata().get("retry-after").unwrap(), "2");
    }
}
