//! Configuration model, defaults and validation.
//!
//! Configuration is read from TOML. Every section has defaults, so an empty
//! file is a valid configuration (receivers on the standard OTLP ports, an
//! "always sample" policy and the `none` exporter).

use std::collections::HashSet;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Errors raised while loading or validating configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The file could not be read.
    #[error("cannot read config file {path}: {source}")]
    Io {
        /// Path that failed.
        path: String,
        /// Underlying error.
        source: std::io::Error,
    },
    /// TOML syntax or schema error.
    #[error("invalid config: {0}")]
    Parse(#[from] toml::de::Error),
    /// Semantically invalid value.
    #[error("invalid config value: {0}")]
    Invalid(String),
}

fn invalid<T>(msg: impl Into<String>) -> Result<T, ConfigError> {
    Err(ConfigError::Invalid(msg.into()))
}

/// Root configuration.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// OTLP receivers.
    pub receiver: ReceiverConfig,
    /// Trace buffer and eviction.
    pub buffer: BufferConfig,
    /// Decision cache for late spans.
    pub decision_cache: DecisionCacheConfig,
    /// Ingest queue and backpressure behaviour.
    pub backpressure: BackpressureConfig,
    /// Downstream exporter.
    pub exporter: ExporterConfig,
    /// Top-level sampling policies. A trace is kept when any policy samples it.
    pub policies: Vec<PolicyConfig>,
}

/// Receiver listen addresses and request limits.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct ReceiverConfig {
    /// OTLP/gRPC listen address; `None` disables the gRPC receiver.
    pub grpc_listen: Option<String>,
    /// OTLP/HTTP listen address; `None` disables the HTTP receiver.
    pub http_listen: Option<String>,
    /// Maximum accepted request body (after decompression), in bytes.
    pub max_request_bytes: usize,
    /// Requests decoded concurrently across both receivers; further
    /// requests wait before their body is read. Bounds decode memory to
    /// roughly `max_concurrent_requests * max_request_bytes`.
    pub max_concurrent_requests: usize,
}

impl Default for ReceiverConfig {
    fn default() -> Self {
        ReceiverConfig {
            grpc_listen: Some("127.0.0.1:4317".into()),
            http_listen: Some("127.0.0.1:4318".into()),
            max_request_bytes: 8 * 1024 * 1024,
            max_concurrent_requests: 32,
        }
    }
}

/// What to do when admitting a span would exceed the buffer limits.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvictionPolicy {
    /// Evaluate policies on the oldest buffered traces ahead of their decision
    /// wait and release them (sampled ones are exported, others dropped).
    DecideEarly,
    /// Drop the oldest buffered traces without evaluating policies.
    DropOldest,
    /// Keep buffered traces; refuse the new spans and close admission so the
    /// receivers push back on clients until the buffer drains.
    RejectNew,
}

/// Trace buffer sizing and timing.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct BufferConfig {
    /// How long to wait after the first span of a trace before deciding.
    pub decision_wait_ms: u64,
    /// Maximum number of traces held at once.
    pub max_traces: usize,
    /// Maximum estimated bytes of span data held at once.
    pub max_bytes: usize,
    /// Spans beyond this count for a single trace are dropped.
    pub max_spans_per_trace: usize,
    /// Eviction policy when a limit is reached.
    pub eviction: EvictionPolicy,
    /// How often the runtime checks for traces whose decision wait elapsed.
    pub tick_interval_ms: u64,
    /// With `reject_new`, admission re-opens once usage falls below this
    /// fraction of both limits (hysteresis).
    pub admission_resume_ratio: f64,
}

impl Default for BufferConfig {
    fn default() -> Self {
        BufferConfig {
            decision_wait_ms: 5_000,
            max_traces: 50_000,
            max_bytes: 256 * 1024 * 1024,
            max_spans_per_trace: 2_000,
            eviction: EvictionPolicy::DecideEarly,
            tick_interval_ms: 100,
            admission_resume_ratio: 0.9,
        }
    }
}

/// Capacities of the post-decision cache.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct DecisionCacheConfig {
    /// Remembered "sampled" decisions.
    pub sampled_capacity: usize,
    /// Remembered "not sampled" decisions.
    pub not_sampled_capacity: usize,
}

impl Default for DecisionCacheConfig {
    fn default() -> Self {
        DecisionCacheConfig {
            sampled_capacity: 100_000,
            not_sampled_capacity: 200_000,
        }
    }
}

/// Behaviour when the ingest queue between receivers and sampler is full.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BackpressureMode {
    /// Fail the request immediately (gRPC `UNAVAILABLE`, HTTP 429).
    Reject,
    /// Wait up to `block_timeout_ms` for queue space, then reject.
    Block,
}

/// Ingest queue configuration.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct BackpressureConfig {
    /// Number of export requests that may wait for the sampler.
    pub queue_capacity: usize,
    /// Spans that may wait for the sampler (a request needs one permit per
    /// span, capped at this value).
    pub max_queued_spans: usize,
    /// Full-queue behaviour.
    pub mode: BackpressureMode,
    /// Maximum wait in `block` mode.
    pub block_timeout_ms: u64,
    /// Retry hint (seconds) returned to clients on rejection.
    pub retry_after_secs: u64,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        BackpressureConfig {
            queue_capacity: 1_024,
            max_queued_spans: 100_000,
            mode: BackpressureMode::Reject,
            block_timeout_ms: 200,
            retry_after_secs: 1,
        }
    }
}

/// Exporter protocol.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExporterKind {
    /// OTLP over gRPC.
    OtlpGrpc,
    /// OTLP over HTTP with protobuf bodies.
    OtlpHttp,
    /// Count and discard (useful for benchmarks and dry runs).
    None,
}

/// Exporter configuration.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct ExporterConfig {
    /// Protocol.
    pub kind: ExporterKind,
    /// Endpoint URL, e.g. `http://127.0.0.1:14317` (gRPC) or
    /// `http://127.0.0.1:14318/v1/traces` (HTTP).
    pub endpoint: String,
    /// Maximum spans per outgoing request.
    pub max_batch_spans: usize,
    /// Flush a partial batch after this long.
    pub flush_interval_ms: u64,
    /// Sampled-trace batches that may wait for the exporter.
    pub queue_capacity: usize,
    /// Sampled spans that may wait for the exporter; the sampler blocks
    /// when this is exhausted.
    pub max_queued_spans: usize,
    /// Attempts per request (1 = no retry).
    pub max_attempts: u32,
    /// First retry delay; doubles per attempt.
    pub initial_backoff_ms: u64,
    /// Per-attempt timeout.
    pub timeout_ms: u64,
}

impl Default for ExporterConfig {
    fn default() -> Self {
        ExporterConfig {
            kind: ExporterKind::None,
            endpoint: String::new(),
            max_batch_spans: 1_024,
            flush_interval_ms: 200,
            queue_capacity: 64,
            max_queued_spans: 100_000,
            max_attempts: 5,
            initial_backoff_ms: 100,
            timeout_ms: 5_000,
        }
    }
}

/// OTLP span status codes, as written in configuration.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "UPPERCASE")]
pub enum StatusCodeName {
    /// `STATUS_CODE_UNSET`
    Unset,
    /// `STATUS_CODE_OK`
    Ok,
    /// `STATUS_CODE_ERROR`
    Error,
}

impl StatusCodeName {
    /// Numeric OTLP value.
    pub fn as_i32(self) -> i32 {
        match self {
            StatusCodeName::Unset => 0,
            StatusCodeName::Ok => 1,
            StatusCodeName::Error => 2,
        }
    }
}

/// Where an attribute policy looks for its key.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttributeScope {
    /// Span attributes only.
    Span,
    /// Resource attributes only.
    Resource,
    /// Either.
    #[default]
    Any,
}

/// Share of a composite policy's span budget given to one sub-policy.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RateAllocation {
    /// Sub-policy name.
    pub policy: String,
    /// Percentage of `max_total_spans_per_second`.
    pub percent: f64,
}

/// A named policy.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct PolicyConfig {
    /// Unique name, reported in metrics and decisions.
    pub name: String,
    /// Policy kind and parameters.
    #[serde(flatten)]
    pub kind: PolicyKind,
}

/// Policy kinds. See `docs/ARCHITECTURE.md` for semantics.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PolicyKind {
    /// Sample every trace.
    AlwaysSample,
    /// Sample traces whose duration (latest span end minus earliest span
    /// start) is at least `threshold_ms` (and at most `upper_threshold_ms`).
    Latency {
        /// Lower bound, inclusive.
        threshold_ms: u64,
        /// Optional upper bound, inclusive.
        #[serde(default)]
        upper_threshold_ms: Option<u64>,
    },
    /// Sample traces with at least one span whose status is in `codes`.
    StatusCode {
        /// Matching status codes.
        codes: Vec<StatusCodeName>,
    },
    /// Sample traces with a span or resource attribute matching.
    Attribute {
        /// Attribute key.
        key: String,
        /// Exact string / bool / int values (compared as strings).
        #[serde(default)]
        values: Vec<String>,
        /// Regular expression matched against string values.
        #[serde(default)]
        regex: Option<String>,
        /// Inclusive numeric lower bound for int/double values.
        #[serde(default)]
        min: Option<f64>,
        /// Inclusive numeric upper bound for int/double values.
        #[serde(default)]
        max: Option<f64>,
        /// Where to look.
        #[serde(default)]
        scope: AttributeScope,
        /// Sample traces that do NOT match instead.
        #[serde(default)]
        invert: bool,
    },
    /// Sample a deterministic fraction of trace ids.
    Probabilistic {
        /// Percentage in `[0, 100]`.
        percentage: f64,
        /// Seed mixed into the trace-id hash; instances sharing a seed agree.
        #[serde(default)]
        hash_seed: u32,
    },
    /// Sample while a token bucket of spans has capacity.
    RateLimiting {
        /// Sustained span rate.
        spans_per_second: u64,
        /// Bucket size; defaults to one second of rate.
        #[serde(default)]
        burst_spans: Option<u64>,
    },
    /// Sample traces whose span count is within bounds.
    SpanCount {
        /// Lower bound, inclusive.
        min_spans: usize,
        /// Optional upper bound, inclusive.
        #[serde(default)]
        max_spans: Option<usize>,
    },
    /// Sample when every sub-policy samples.
    And {
        /// Sub-policies (non-composite).
        sub_policies: Vec<PolicyConfig>,
    },
    /// Evaluate sub-policies in order; each has its own per-second span
    /// budget carved out of `max_total_spans_per_second`.
    Composite {
        /// Total spans per second the composite may sample.
        max_total_spans_per_second: u64,
        /// Ordered sub-policies (non-composite).
        sub_policies: Vec<PolicyConfig>,
        /// Explicit budget shares. Sub-policies without an entry split the
        /// unallocated remainder equally.
        #[serde(default)]
        rate_allocation: Vec<RateAllocation>,
    },
}

impl Config {
    /// Parse and validate TOML text.
    pub fn from_toml_str(text: &str) -> Result<Config, ConfigError> {
        let cfg: Config = toml::from_str(text)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Read, parse and validate a TOML file.
    pub fn load(path: &Path) -> Result<Config, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.display().to_string(),
            source,
        })?;
        Config::from_toml_str(&text)
    }

    /// Policies to evaluate; an empty list means "always sample".
    pub fn effective_policies(&self) -> Vec<PolicyConfig> {
        if self.policies.is_empty() {
            vec![PolicyConfig {
                name: "always".into(),
                kind: PolicyKind::AlwaysSample,
            }]
        } else {
            self.policies.clone()
        }
    }

    /// Check semantic constraints that the type system cannot express.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let b = &self.buffer;
        if b.decision_wait_ms == 0 {
            return invalid("buffer.decision_wait_ms must be > 0");
        }
        if b.max_traces == 0 || b.max_bytes == 0 || b.max_spans_per_trace == 0 {
            return invalid("buffer.max_traces, max_bytes and max_spans_per_trace must be > 0");
        }
        if b.tick_interval_ms == 0 {
            return invalid("buffer.tick_interval_ms must be > 0");
        }
        if !(b.admission_resume_ratio > 0.0 && b.admission_resume_ratio <= 1.0) {
            return invalid("buffer.admission_resume_ratio must be in (0, 1]");
        }
        if self.backpressure.queue_capacity == 0 || self.backpressure.max_queued_spans == 0 {
            return invalid("backpressure.queue_capacity and max_queued_spans must be > 0");
        }
        if self.receiver.max_request_bytes == 0 || self.receiver.max_concurrent_requests == 0 {
            return invalid("receiver.max_request_bytes and max_concurrent_requests must be > 0");
        }
        let e = &self.exporter;
        if e.max_batch_spans == 0
            || e.queue_capacity == 0
            || e.max_queued_spans == 0
            || e.max_attempts == 0
        {
            return invalid(
                "exporter.max_batch_spans, queue_capacity, max_queued_spans and max_attempts must be > 0",
            );
        }
        if e.kind != ExporterKind::None && e.endpoint.is_empty() {
            return invalid("exporter.endpoint is required unless exporter.kind = \"none\"");
        }
        validate_policies(&self.policies)
    }
}

fn validate_policies(policies: &[PolicyConfig]) -> Result<(), ConfigError> {
    let mut names = HashSet::new();
    for p in policies {
        if p.name.trim().is_empty() {
            return invalid("policy name must not be empty");
        }
        if !names.insert(p.name.as_str()) {
            return invalid(format!("duplicate policy name {:?}", p.name));
        }
        validate_policy(p)?;
    }
    Ok(())
}

fn validate_policy(p: &PolicyConfig) -> Result<(), ConfigError> {
    let name = &p.name;
    match &p.kind {
        PolicyKind::AlwaysSample => {}
        PolicyKind::Latency {
            threshold_ms,
            upper_threshold_ms,
        } => {
            if let Some(up) = upper_threshold_ms {
                if up < threshold_ms {
                    return invalid(format!("{name}: upper_threshold_ms < threshold_ms"));
                }
            }
        }
        PolicyKind::StatusCode { codes } => {
            if codes.is_empty() {
                return invalid(format!("{name}: codes must not be empty"));
            }
        }
        PolicyKind::Attribute {
            key,
            values,
            regex,
            min,
            max,
            ..
        } => {
            if key.is_empty() {
                return invalid(format!("{name}: key must not be empty"));
            }
            if values.is_empty() && regex.is_none() && min.is_none() && max.is_none() {
                return invalid(format!(
                    "{name}: one of values, regex, min, max is required"
                ));
            }
            if let Some(r) = regex {
                if let Err(err) = regex::Regex::new(r) {
                    return invalid(format!("{name}: bad regex: {err}"));
                }
            }
            if let (Some(lo), Some(hi)) = (min, max) {
                if hi < lo {
                    return invalid(format!("{name}: max < min"));
                }
            }
        }
        PolicyKind::Probabilistic { percentage, .. } => {
            if !(0.0..=100.0).contains(percentage) {
                return invalid(format!("{name}: percentage must be in [0, 100]"));
            }
        }
        PolicyKind::RateLimiting {
            spans_per_second,
            burst_spans,
        } => {
            if *spans_per_second == 0 || *burst_spans == Some(0) {
                return invalid(format!("{name}: rates must be > 0"));
            }
        }
        PolicyKind::SpanCount {
            min_spans,
            max_spans,
        } => {
            if let Some(mx) = max_spans {
                if mx < min_spans {
                    return invalid(format!("{name}: max_spans < min_spans"));
                }
            }
        }
        PolicyKind::And { sub_policies } => {
            validate_sub_policies(name, sub_policies, false)?;
        }
        PolicyKind::Composite {
            max_total_spans_per_second,
            sub_policies,
            rate_allocation,
        } => {
            if *max_total_spans_per_second == 0 {
                return invalid(format!("{name}: max_total_spans_per_second must be > 0"));
            }
            validate_sub_policies(name, sub_policies, true)?;
            let mut total = 0.0;
            let mut seen = HashSet::new();
            for a in rate_allocation {
                if !sub_policies.iter().any(|s| s.name == a.policy) {
                    return invalid(format!(
                        "{name}: rate_allocation references unknown sub-policy {:?}",
                        a.policy
                    ));
                }
                if !seen.insert(a.policy.as_str()) {
                    return invalid(format!("{name}: duplicate allocation for {:?}", a.policy));
                }
                if !(a.percent > 0.0 && a.percent <= 100.0) {
                    return invalid(format!("{name}: allocation percent must be in (0, 100]"));
                }
                total += a.percent;
            }
            if total > 100.0 + 1e-9 {
                return invalid(format!("{name}: rate_allocation sums to {total} > 100"));
            }
        }
    }
    Ok(())
}

/// Composite policies may only appear at top level; `and` may appear at top
/// level or directly inside a composite (`allow_and`), never inside another
/// `and`. Nesting is therefore at most two levels deep.
fn validate_sub_policies(
    name: &str,
    subs: &[PolicyConfig],
    allow_and: bool,
) -> Result<(), ConfigError> {
    if subs.is_empty() {
        return invalid(format!("{name}: sub_policies must not be empty"));
    }
    for s in subs {
        let nested_and = matches!(s.kind, PolicyKind::And { .. });
        if matches!(s.kind, PolicyKind::Composite { .. }) || (nested_and && !allow_and) {
            return invalid(format!(
                "{name}: sub-policy {:?} cannot be a composite, or an and inside an and",
                s.name
            ));
        }
    }
    validate_policies(subs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_config_is_valid_with_defaults() {
        let cfg = Config::from_toml_str("").unwrap();
        assert_eq!(cfg.buffer.decision_wait_ms, 5_000);
        assert_eq!(cfg.buffer.eviction, EvictionPolicy::DecideEarly);
        assert_eq!(cfg.effective_policies()[0].kind, PolicyKind::AlwaysSample);
    }

    #[test]
    fn parses_full_policy_set() {
        let cfg = Config::from_toml_str(
            r#"
            [buffer]
            eviction = "reject_new"

            [[policies]]
            name = "errors"
            type = "status_code"
            codes = ["ERROR"]

            [[policies]]
            name = "budget"
            type = "composite"
            max_total_spans_per_second = 1000
            rate_allocation = [{ policy = "slow", percent = 50 }]

              [[policies.sub_policies]]
              name = "slow"
              type = "latency"
              threshold_ms = 500

              [[policies.sub_policies]]
              name = "rest"
              type = "always_sample"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.buffer.eviction, EvictionPolicy::RejectNew);
        assert_eq!(cfg.policies.len(), 2);
        match &cfg.policies[1].kind {
            PolicyKind::Composite { sub_policies, .. } => assert_eq!(sub_policies.len(), 2),
            other => panic!("unexpected {other:?}"),
        }
    }

    fn err_of(text: &str) -> String {
        Config::from_toml_str(text).unwrap_err().to_string()
    }

    #[test]
    fn rejects_invalid_values() {
        assert!(err_of("[buffer]\ndecision_wait_ms = 0").contains("decision_wait_ms"));
        assert!(err_of("[buffer]\nmax_traces = 0").contains("max_traces"));
        assert!(err_of("[buffer]\ntick_interval_ms = 0").contains("tick_interval_ms"));
        assert!(err_of("[buffer]\nadmission_resume_ratio = 1.5").contains("ratio"));
        assert!(err_of("[backpressure]\nqueue_capacity = 0").contains("queue_capacity"));
        assert!(err_of("[receiver]\nmax_request_bytes = 0").contains("max_request_bytes"));
        assert!(err_of("[exporter]\nkind = \"otlp_grpc\"").contains("endpoint"));
        assert!(err_of("[exporter]\nmax_attempts = 0").contains("max_attempts"));
        assert!(err_of("[exporter]\nmax_queued_spans = 0").contains("max_queued_spans"));
        assert!(err_of("[backpressure]\nmax_queued_spans = 0").contains("max_queued_spans"));
        assert!(err_of("[receiver]\nmax_concurrent_requests = 0").contains("max_concurrent"));
        assert!(err_of("[buffer]\nbogus = 1").contains("unknown field"));
    }

    #[test]
    fn rejects_invalid_policies() {
        let p = |body: &str| err_of(&format!("[[policies]]\nname = \"p\"\n{body}"));
        assert!(p("type = \"status_code\"\ncodes = []").contains("codes"));
        assert!(
            p("type = \"latency\"\nthreshold_ms = 5\nupper_threshold_ms = 1").contains("upper")
        );
        assert!(p("type = \"probabilistic\"\npercentage = 101.0").contains("percentage"));
        assert!(p("type = \"rate_limiting\"\nspans_per_second = 0").contains("rates"));
        assert!(p("type = \"attribute\"\nkey = \"k\"").contains("one of"));
        assert!(p("type = \"attribute\"\nkey = \"\"\nvalues=[\"a\"]").contains("key"));
        assert!(p("type = \"attribute\"\nkey = \"k\"\nregex = \"(\"").contains("regex"));
        assert!(p("type = \"attribute\"\nkey = \"k\"\nmin = 5.0\nmax = 1.0").contains("max < min"));
        assert!(p("type = \"span_count\"\nmin_spans = 5\nmax_spans = 1").contains("max_spans"));
        assert!(p("type = \"and\"\nsub_policies = []").contains("sub_policies"));
        assert!(
            p("type = \"composite\"\nmax_total_spans_per_second = 0\nsub_policies = []")
                .contains("max_total")
        );
        let unknown_alloc = p(r#"type = "composite"
max_total_spans_per_second = 10
rate_allocation = [{ policy = "nope", percent = 10 }]
sub_policies = [{ name = "a", type = "always_sample" }]"#);
        assert!(
            unknown_alloc.contains("unknown sub-policy"),
            "{unknown_alloc}"
        );
        let over = p(r#"type = "composite"
max_total_spans_per_second = 10
rate_allocation = [{ policy = "a", percent = 60 }, { policy = "b", percent = 60 }]
sub_policies = [{ name = "a", type = "always_sample" }, { name = "b", type = "always_sample" }]"#);
        assert!(over.contains("sums to"), "{over}");
        let nested = p(r#"type = "and"
sub_policies = [{ name = "a", type = "and", sub_policies = [{ name = "x", type = "always_sample" }] }]"#);
        assert!(nested.contains("cannot be"), "{nested}");
        let comp_in_comp = p(r#"type = "composite"
max_total_spans_per_second = 10
sub_policies = [{ name = "a", type = "composite", max_total_spans_per_second = 1, sub_policies = [{ name = "x", type = "always_sample" }] }]"#);
        assert!(comp_in_comp.contains("cannot be"), "{comp_in_comp}");
        Config::from_toml_str(
            r#"[[policies]]
name = "c"
type = "composite"
max_total_spans_per_second = 10
sub_policies = [{ name = "a", type = "and", sub_policies = [{ name = "x", type = "always_sample" }] }]"#,
        )
        .expect("and inside composite is allowed");
        let dup = err_of(
            "[[policies]]\nname = \"a\"\ntype = \"always_sample\"\n[[policies]]\nname = \"a\"\ntype = \"always_sample\"",
        );
        assert!(dup.contains("duplicate"));
    }

    #[test]
    fn status_code_values() {
        assert_eq!(StatusCodeName::Unset.as_i32(), 0);
        assert_eq!(StatusCodeName::Ok.as_i32(), 1);
        assert_eq!(StatusCodeName::Error.as_i32(), 2);
    }

    #[test]
    fn load_reports_missing_file() {
        let err = Config::load(Path::new("definitely/not/here.toml")).unwrap_err();
        assert!(err.to_string().contains("cannot read"));
    }
}
