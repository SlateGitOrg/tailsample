//! A minimal OTLP/gRPC "downstream collector" that records what it receives.
//! Used by the demo, the integration tests and `tailsample sink`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceResponse;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::codec::CompressionEncoding;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status};

use crate::ids::TraceId;
use crate::ExportTraceServiceRequest;

/// What the sink has seen.
#[derive(Debug, Clone, Default)]
pub struct SinkState {
    /// Export requests received.
    pub requests: u64,
    /// Spans received per trace id.
    pub spans_by_trace: HashMap<TraceId, usize>,
    /// Distinct `service.name` values seen.
    pub services: HashMap<String, u64>,
}

impl SinkState {
    /// Total spans received.
    pub fn spans(&self) -> usize {
        self.spans_by_trace.values().sum()
    }
}

#[derive(Clone, Default)]
struct SinkService {
    state: Arc<Mutex<SinkState>>,
}

#[tonic::async_trait]
impl TraceService for SinkService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let req = request.into_inner();
        let mut st = self
            .state
            .lock()
            .map_err(|_| Status::internal("sink state poisoned"))?;
        st.requests += 1;
        for rs in &req.resource_spans {
            let service = rs
                .resource
                .as_ref()
                .and_then(|r| r.attributes.iter().find(|kv| kv.key == "service.name"))
                .and_then(|kv| kv.value.as_ref())
                .and_then(|v| match &v.value {
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => {
                        Some(s.clone())
                    }
                    _ => None,
                })
                .unwrap_or_else(|| "unknown".into());
            for ss in &rs.scope_spans {
                *st.services.entry(service.clone()).or_default() += ss.spans.len() as u64;
                for span in &ss.spans {
                    if let Some(id) = TraceId::from_slice(&span.trace_id) {
                        *st.spans_by_trace.entry(id).or_default() += 1;
                    }
                }
            }
        }
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

/// A running sink.
pub struct Sink {
    addr: SocketAddr,
    state: Arc<Mutex<SinkState>>,
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

impl Sink {
    /// Listen on `addr` (port 0 picks a free port).
    pub async fn start(addr: &str) -> std::io::Result<Sink> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let svc = SinkService::default();
        let state = Arc::clone(&svc.state);
        let (stop, stop_rx) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let server = TraceServiceServer::new(svc)
                .accept_compressed(CompressionEncoding::Gzip)
                .max_decoding_message_size(64 * 1024 * 1024);
            let res = tonic::transport::Server::builder()
                .add_service(server)
                .serve_with_incoming_shutdown(TcpIncoming::from(listener), async {
                    let _ = stop_rx.await;
                })
                .await;
            if let Err(e) = res {
                tracing::error!(error = %e, "sink failed");
            }
        });
        Ok(Sink {
            addr,
            state,
            stop,
            task,
        })
    }

    /// Bound address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// `http://` endpoint for an OTLP/gRPC exporter.
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Copy of the current state.
    pub fn snapshot(&self) -> SinkState {
        self.state.lock().map(|s| s.clone()).unwrap_or_default()
    }

    /// Stop the server.
    pub async fn stop(self) -> SinkState {
        let _ = self.stop.send(());
        let _ = self.task.await;
        self.state.lock().map(|s| s.clone()).unwrap_or_default()
    }
}
