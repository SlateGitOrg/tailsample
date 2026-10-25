//! OTLP receivers.
//!
//! Both receivers translate [`IngestError`](crate::runtime::IngestError)
//! into protocol-level "retry later" signals:
//!
//! | condition          | gRPC status   | HTTP status | Retry-After |
//! |--------------------|---------------|-------------|-------------|
//! | ingest queue full  | `UNAVAILABLE` | 429         | yes         |
//! | admission closed   | `UNAVAILABLE` | 503         | yes         |
//! | shutting down      | `UNAVAILABLE` | 503         | yes         |
//! | malformed request  | `INVALID_ARGUMENT` | 400    | no          |

pub mod grpc;
pub mod http;
