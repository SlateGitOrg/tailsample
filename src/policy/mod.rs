//! Sampling policies.
//!
//! A policy looks at a complete (buffered) trace and answers "sample?". The
//! answer is the label of the policy that decided to keep the trace, or
//! `None`. Stateful policies (rate limiting, composite budgets) receive the
//! logical clock so they are deterministic under simulated time.
//!
//! Top-level policies are evaluated in configuration order and evaluation
//! stops at the first policy that samples; budgets of later policies are
//! therefore only consumed by traces that earlier policies did not keep.

mod attribute;
mod composite;
mod leaf;
mod rate;

use std::sync::Arc;

pub use attribute::AttributePolicy;
pub use composite::{AndPolicy, CompositePolicy};
pub use leaf::{
    AlwaysSample, LatencyPolicy, ProbabilisticPolicy, SpanCountPolicy, StatusCodePolicy,
};
pub use rate::RateLimitingPolicy;

use crate::buffer::TraceStats;
use crate::config::{ConfigError, PolicyConfig, PolicyKind};
use crate::ids::TraceId;
use crate::model::SpanRecord;

/// Read-only view of a trace handed to policies.
#[derive(Debug, Clone, Copy)]
pub struct TraceView<'a> {
    /// Trace id.
    pub id: TraceId,
    /// Buffered spans.
    pub spans: &'a [SpanRecord],
    /// Incremental aggregates.
    pub stats: &'a TraceStats,
}

/// A sampling policy.
pub trait Policy: Send + std::fmt::Debug {
    /// Configured name.
    fn name(&self) -> &str;

    /// Decide. Returns the label to attribute the decision to when the trace
    /// is sampled. `now_ms` is the sampler's logical clock.
    fn evaluate(&mut self, trace: &TraceView<'_>, now_ms: u64) -> Option<Arc<str>>;
}

/// Build a policy from configuration. `prefix` is prepended to labels of
/// nested policies (e.g. `budget/errors`).
pub fn build(cfg: &PolicyConfig, prefix: &str) -> Result<Box<dyn Policy>, ConfigError> {
    let label: Arc<str> = if prefix.is_empty() {
        Arc::from(cfg.name.as_str())
    } else {
        Arc::from(format!("{prefix}/{}", cfg.name))
    };
    let p: Box<dyn Policy> = match &cfg.kind {
        PolicyKind::AlwaysSample => Box::new(AlwaysSample::new(label)),
        PolicyKind::Latency {
            threshold_ms,
            upper_threshold_ms,
        } => Box::new(LatencyPolicy::new(
            label,
            *threshold_ms,
            *upper_threshold_ms,
        )),
        PolicyKind::StatusCode { codes } => Box::new(StatusCodePolicy::new(label, codes)),
        PolicyKind::Attribute {
            key,
            values,
            regex,
            min,
            max,
            scope,
            invert,
        } => Box::new(AttributePolicy::new(
            label,
            key.clone(),
            values.clone(),
            regex.as_deref(),
            (*min, *max),
            *scope,
            *invert,
        )?),
        PolicyKind::Probabilistic {
            percentage,
            hash_seed,
        } => Box::new(ProbabilisticPolicy::new(label, *percentage, *hash_seed)),
        PolicyKind::RateLimiting {
            spans_per_second,
            burst_spans,
        } => Box::new(RateLimitingPolicy::new(
            label,
            *spans_per_second,
            burst_spans.unwrap_or(*spans_per_second),
        )),
        PolicyKind::SpanCount {
            min_spans,
            max_spans,
        } => Box::new(SpanCountPolicy::new(label, *min_spans, *max_spans)),
        PolicyKind::And { sub_policies } => {
            let subs = sub_policies
                .iter()
                .map(|s| build(s, &cfg.name))
                .collect::<Result<Vec<_>, _>>()?;
            Box::new(AndPolicy::new(label, subs))
        }
        PolicyKind::Composite {
            max_total_spans_per_second,
            sub_policies,
            rate_allocation,
        } => {
            let subs = sub_policies
                .iter()
                .map(|s| build(s, &cfg.name))
                .collect::<Result<Vec<_>, _>>()?;
            let names: Vec<&str> = sub_policies.iter().map(|s| s.name.as_str()).collect();
            let budgets = composite::allocate(*max_total_spans_per_second, &names, rate_allocation);
            Box::new(CompositePolicy::new(label, subs, budgets))
        }
    };
    Ok(p)
}

/// Ordered set of top-level policies.
#[derive(Debug)]
pub struct PolicyEngine {
    policies: Vec<Box<dyn Policy>>,
}

impl PolicyEngine {
    /// Build every top-level policy.
    pub fn from_config(policies: &[PolicyConfig]) -> Result<Self, ConfigError> {
        Ok(PolicyEngine {
            policies: policies
                .iter()
                .map(|p| build(p, ""))
                .collect::<Result<_, _>>()?,
        })
    }

    /// Evaluate in order; first policy that samples wins.
    pub fn evaluate(&mut self, trace: &TraceView<'_>, now_ms: u64) -> Option<Arc<str>> {
        self.policies
            .iter_mut()
            .find_map(|p| p.evaluate(trace, now_ms))
    }

    /// Names of the top-level policies.
    pub fn names(&self) -> Vec<&str> {
        self.policies.iter().map(|p| p.name()).collect()
    }
}

#[cfg(test)]
pub(crate) mod testutil {
    use super::*;
    use crate::model::{kv_str, ResourceContext, ScopeContext};
    use opentelemetry_proto::tonic::common::v1::KeyValue;
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{Span, Status};

    /// Owned trace for policy unit tests.
    pub struct OwnedTrace {
        pub id: TraceId,
        pub spans: Vec<SpanRecord>,
        pub stats: TraceStats,
    }

    impl OwnedTrace {
        pub fn view(&self) -> TraceView<'_> {
            TraceView {
                id: self.id,
                spans: &self.spans,
                stats: &self.stats,
            }
        }
    }

    /// Span spec: (duration_ms, status code, span attrs).
    pub fn trace(id: u128, service: &str, spans: &[(u64, i32, Vec<KeyValue>)]) -> OwnedTrace {
        let ctx = Arc::new(ScopeContext {
            resource: Arc::new(ResourceContext {
                resource: Some(Resource {
                    attributes: vec![kv_str("service.name", service)],
                    ..Default::default()
                }),
                schema_url: String::new(),
            }),
            ..Default::default()
        });
        let tid = TraceId::from_u128(id);
        let mut stats = TraceStats::default();
        let recs = spans
            .iter()
            .enumerate()
            .map(|(i, (dur, code, attrs))| {
                let start = 1_000_000_000 + i as u64 * 1_000;
                let span = Span {
                    trace_id: tid.0.to_vec(),
                    span_id: (i as u64 + 1).to_be_bytes().to_vec(),
                    start_time_unix_nano: start,
                    end_time_unix_nano: start + dur * 1_000_000,
                    attributes: attrs.clone(),
                    status: Some(Status {
                        code: *code,
                        message: String::new(),
                    }),
                    ..Default::default()
                };
                let r = SpanRecord::new(tid, span, Arc::clone(&ctx));
                stats.observe(&r);
                r
            })
            .collect();
        OwnedTrace {
            id: tid,
            spans: recs,
            stats,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testutil::trace;
    use super::*;
    use crate::config::Config;

    #[test]
    fn engine_first_match_wins_in_order() {
        let cfg = Config::from_toml_str(
            r#"
            [[policies]]
            name = "errors"
            type = "status_code"
            codes = ["ERROR"]
            [[policies]]
            name = "slow"
            type = "latency"
            threshold_ms = 100
            "#,
        )
        .unwrap();
        let mut e = PolicyEngine::from_config(&cfg.policies).unwrap();
        assert_eq!(e.names(), vec!["errors", "slow"]);
        let t = trace(1, "svc", &[(500, 2, vec![])]);
        assert_eq!(e.evaluate(&t.view(), 0).as_deref(), Some("errors"));
        let t = trace(2, "svc", &[(500, 0, vec![])]);
        assert_eq!(e.evaluate(&t.view(), 0).as_deref(), Some("slow"));
        let t = trace(3, "svc", &[(5, 1, vec![])]);
        assert_eq!(e.evaluate(&t.view(), 0), None);
    }

    #[test]
    fn builds_every_kind() {
        let cfg = Config::from_toml_str(
            r#"
            [[policies]]
            name = "a"
            type = "always_sample"
            [[policies]]
            name = "attr"
            type = "attribute"
            key = "k"
            regex = "^x"
            [[policies]]
            name = "p"
            type = "probabilistic"
            percentage = 10.0
            [[policies]]
            name = "r"
            type = "rate_limiting"
            spans_per_second = 10
            [[policies]]
            name = "sc"
            type = "span_count"
            min_spans = 2
            [[policies]]
            name = "and"
            type = "and"
            sub_policies = [{ name = "x", type = "always_sample" }]
            [[policies]]
            name = "comp"
            type = "composite"
            max_total_spans_per_second = 10
            sub_policies = [{ name = "y", type = "always_sample" }]
            "#,
        )
        .unwrap();
        let e = PolicyEngine::from_config(&cfg.policies).unwrap();
        assert_eq!(e.names().len(), 7);
    }
}
