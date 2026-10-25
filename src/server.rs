//! Process assembly: bind receivers, spawn the pipeline, shut down cleanly.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::config::{Config, ConfigError};
use crate::exporter::{self, ExportError, Exporter};
use crate::receiver;
use crate::runtime::Pipeline;
use crate::telemetry::Telemetry;

/// Startup failures.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// Invalid configuration.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// Exporter could not be built.
    #[error("exporter: {0}")]
    Exporter(#[from] ExportError),
    /// A listener could not be bound.
    #[error("cannot bind {addr}: {source}")]
    Bind {
        /// Requested address.
        addr: String,
        /// Underlying error.
        source: std::io::Error,
    },
}

/// A running sampler with its receivers.
pub struct Server {
    grpc_addr: Option<SocketAddr>,
    http_addr: Option<SocketAddr>,
    pipeline: Pipeline,
    stop: watch::Sender<bool>,
    receivers: Vec<JoinHandle<()>>,
}

async fn bind(addr: &str) -> Result<(tokio::net::TcpListener, SocketAddr), ServerError> {
    receiver::grpc::bind(addr)
        .await
        .map_err(|source| ServerError::Bind {
            addr: addr.to_string(),
            source,
        })
}

impl Server {
    /// Start with the exporter described by the configuration.
    pub async fn start(cfg: &Config) -> Result<Server, ServerError> {
        let exp = exporter::from_config(&cfg.exporter)?;
        Server::start_with_exporter(cfg, exp).await
    }

    /// Start with a caller-supplied exporter. Listen addresses may use port
    /// 0; the bound addresses are available from [`grpc_addr`](Self::grpc_addr)
    /// and [`http_addr`](Self::http_addr).
    pub async fn start_with_exporter(
        cfg: &Config,
        exporter: Box<dyn Exporter>,
    ) -> Result<Server, ServerError> {
        cfg.validate()?;
        // Bind first so a port conflict fails before any task is spawned.
        let grpc = match &cfg.receiver.grpc_listen {
            Some(a) => Some(bind(a).await?),
            None => None,
        };
        let http = match &cfg.receiver.http_listen {
            Some(a) => Some(bind(a).await?),
            None => None,
        };
        let pipeline = Pipeline::spawn(cfg, exporter)?;
        let (stop, stop_rx) = watch::channel(false);
        let max = cfg.receiver.max_request_bytes;
        let mut receivers = Vec::new();
        let wait = |mut rx: watch::Receiver<bool>| async move {
            let _ = rx.wait_for(|v| *v).await;
        };
        let grpc_addr = grpc.as_ref().map(|(_, a)| *a);
        let http_addr = http.as_ref().map(|(_, a)| *a);
        if let Some((listener, addr)) = grpc {
            let h = pipeline.handle();
            let signal = wait(stop_rx.clone());
            receivers.push(tokio::spawn(async move {
                if let Err(e) = receiver::grpc::serve(listener, h, max, signal).await {
                    tracing::error!(%addr, error = %e, "gRPC receiver failed");
                }
            }));
            tracing::info!(%addr, "OTLP/gRPC receiver listening");
        }
        if let Some((listener, addr)) = http {
            let h = pipeline.handle();
            let signal = wait(stop_rx.clone());
            receivers.push(tokio::spawn(async move {
                if let Err(e) = receiver::http::serve(listener, h, max, signal).await {
                    tracing::error!(%addr, error = %e, "HTTP receiver failed");
                }
            }));
            tracing::info!(%addr, "OTLP/HTTP receiver listening");
        }
        Ok(Server {
            grpc_addr,
            http_addr,
            pipeline,
            stop,
            receivers,
        })
    }

    /// Bound gRPC address.
    pub fn grpc_addr(&self) -> Option<SocketAddr> {
        self.grpc_addr
    }

    /// Bound HTTP address.
    pub fn http_addr(&self) -> Option<SocketAddr> {
        self.http_addr
    }

    /// Shared telemetry.
    pub fn telemetry(&self) -> Arc<Telemetry> {
        self.pipeline.telemetry()
    }

    /// Stop accepting connections, drain, decide all buffered traces and
    /// wait for the exporter to finish.
    pub async fn shutdown(self) {
        let _ = self.stop.send(true);
        for r in self.receivers {
            let _ = r.await;
        }
        self.pipeline.shutdown().await;
    }
}
