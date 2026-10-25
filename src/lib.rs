//! # tailsample
//!
//! A tail-based trace sampler. Spans arrive over OTLP (gRPC or HTTP), are
//! assembled into traces in a bounded in-memory buffer, held for a decision
//! wait, evaluated against an ordered set of policies, and the kept traces
//! are exported over OTLP. Memory is bounded by explicit trace / byte limits
//! with a configurable eviction policy, and a decision cache routes spans
//! that arrive after their trace was decided.
//!
//! The crate is split into a deterministic core ([`sampler::Sampler`],
//! driven by a logical clock) and an async shell ([`runtime`], [`receiver`],
//! [`exporter`]) that feeds it.

#![warn(missing_docs)]

pub mod buffer;
pub mod cache;
pub mod config;
pub mod exporter;
pub mod ids;
pub mod loadgen;
pub mod model;
pub mod policy;
pub mod receiver;
pub mod runtime;
pub mod sampler;
pub mod sim;
pub mod telemetry;

pub use config::Config;
pub use ids::TraceId;
pub use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
pub use sampler::{Released, Sampler, SamplerStats};
