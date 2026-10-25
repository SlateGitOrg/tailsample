//! Attribute-match policy.

use std::sync::Arc;

use opentelemetry_proto::tonic::common::v1::{any_value::Value, AnyValue};
use regex::Regex;

use super::{Policy, TraceView};
use crate::config::{AttributeScope, ConfigError};
use crate::model::SpanRecord;

/// Samples traces where some span (or its resource) carries attribute `key`
/// whose value matches any configured criterion: an exact value, the regex,
/// or the numeric range. With `invert`, samples traces where nothing matches.
#[derive(Debug)]
pub struct AttributePolicy {
    label: Arc<str>,
    key: String,
    values: Vec<String>,
    regex: Option<Regex>,
    range: (Option<f64>, Option<f64>),
    scope: AttributeScope,
    invert: bool,
}

impl AttributePolicy {
    /// New policy; fails if the regex does not compile.
    pub fn new(
        label: Arc<str>,
        key: String,
        values: Vec<String>,
        regex: Option<&str>,
        range: (Option<f64>, Option<f64>),
        scope: AttributeScope,
        invert: bool,
    ) -> Result<Self, ConfigError> {
        let regex = regex
            .map(Regex::new)
            .transpose()
            .map_err(|e| ConfigError::Invalid(format!("{label}: bad regex: {e}")))?;
        Ok(AttributePolicy {
            label,
            key,
            values,
            regex,
            range,
            scope,
            invert,
        })
    }

    fn value_matches(&self, v: &AnyValue) -> bool {
        let Some(v) = v.value.as_ref() else {
            return false;
        };
        let text: Option<String> = match v {
            Value::StringValue(s) => Some(s.clone()),
            Value::BoolValue(b) => Some(b.to_string()),
            Value::IntValue(i) => Some(i.to_string()),
            Value::DoubleValue(d) => Some(d.to_string()),
            _ => None,
        };
        if let Some(t) = &text {
            if self.values.iter().any(|x| x == t) {
                return true;
            }
            if let (Some(re), Value::StringValue(s)) = (&self.regex, v) {
                if re.is_match(s) {
                    return true;
                }
            }
        }
        let num = match v {
            Value::IntValue(i) => Some(*i as f64),
            Value::DoubleValue(d) => Some(*d),
            _ => None,
        };
        match (num, self.range) {
            (Some(n), (lo, hi)) if lo.is_some() || hi.is_some() => {
                lo.is_none_or(|l| n >= l) && hi.is_none_or(|h| n <= h)
            }
            _ => false,
        }
    }

    fn span_matches(&self, r: &SpanRecord) -> bool {
        let span_hit = || r.span_attr(&self.key).is_some_and(|v| self.value_matches(v));
        let res_hit = || {
            r.resource_attr(&self.key)
                .is_some_and(|v| self.value_matches(v))
        };
        match self.scope {
            AttributeScope::Span => span_hit(),
            AttributeScope::Resource => res_hit(),
            AttributeScope::Any => span_hit() || res_hit(),
        }
    }
}

impl Policy for AttributePolicy {
    fn name(&self) -> &str {
        &self.label
    }

    fn evaluate(&mut self, trace: &TraceView<'_>, _now_ms: u64) -> Option<Arc<str>> {
        let hit = trace.spans.iter().any(|r| self.span_matches(r));
        (hit != self.invert).then(|| Arc::clone(&self.label))
    }
}

#[cfg(test)]
mod tests {
    use super::super::testutil::trace;
    use super::*;
    use crate::model::{kv_int, kv_str};
    use opentelemetry_proto::tonic::common::v1::KeyValue;

    fn pol(
        values: &[&str],
        regex: Option<&str>,
        range: (Option<f64>, Option<f64>),
        scope: AttributeScope,
        invert: bool,
        key: &str,
    ) -> AttributePolicy {
        AttributePolicy::new(
            Arc::from("attr"),
            key.into(),
            values.iter().map(|s| s.to_string()).collect(),
            regex,
            range,
            scope,
            invert,
        )
        .unwrap()
    }

    fn eval(p: &mut AttributePolicy, attrs: Vec<KeyValue>, service: &str) -> bool {
        p.evaluate(&trace(1, service, &[(1, 0, attrs)]).view(), 0)
            .is_some()
    }

    #[test]
    fn exact_values_and_types() {
        let mut p = pol(&["premium", "true", "7"], None, (None, None), AttributeScope::Span, false, "tier");
        assert_eq!(p.name(), "attr");
        assert!(eval(&mut p, vec![kv_str("tier", "premium")], "s"));
        assert!(!eval(&mut p, vec![kv_str("tier", "free")], "s"));
        assert!(eval(&mut p, vec![kv_int("tier", 7)], "s"));
        let b = KeyValue {
            key: "tier".into(),
            value: Some(AnyValue {
                value: Some(Value::BoolValue(true)),
            }),
            ..Default::default()
        };
        assert!(eval(&mut p, vec![b], "s"));
        let empty = KeyValue {
            key: "tier".into(),
            value: Some(AnyValue { value: None }),
            ..Default::default()
        };
        assert!(!eval(&mut p, vec![empty], "s"));
        let bytes = KeyValue {
            key: "tier".into(),
            value: Some(AnyValue {
                value: Some(Value::BytesValue(vec![1])),
            }),
            ..Default::default()
        };
        assert!(!eval(&mut p, vec![bytes], "s"));
    }

    #[test]
    fn regex_and_numeric_range() {
        let mut re = pol(&[], Some("^/api/v[0-9]+/pay"), (None, None), AttributeScope::Span, false, "http.route");
        assert!(eval(&mut re, vec![kv_str("http.route", "/api/v2/pay")], "s"));
        assert!(!eval(&mut re, vec![kv_str("http.route", "/health")], "s"));
        let mut range = pol(&[], None, (Some(500.0), Some(599.0)), AttributeScope::Span, false, "http.status_code");
        assert!(eval(&mut range, vec![kv_int("http.status_code", 503)], "s"));
        assert!(!eval(&mut range, vec![kv_int("http.status_code", 404)], "s"));
        let d = KeyValue {
            key: "http.status_code".into(),
            value: Some(AnyValue {
                value: Some(Value::DoubleValue(550.5)),
            }),
            ..Default::default()
        };
        assert!(eval(&mut range, vec![d], "s"));
        assert!(!eval(&mut range, vec![kv_str("http.status_code", "503")], "s"));
    }

    #[test]
    fn scope_and_invert() {
        let mut res = pol(&["checkout"], None, (None, None), AttributeScope::Resource, false, "service.name");
        assert!(eval(&mut res, vec![], "checkout"));
        assert!(!eval(&mut res, vec![kv_str("service.name", "checkout")], "other"));
        let mut any = pol(&["checkout"], None, (None, None), AttributeScope::Any, false, "service.name");
        assert!(eval(&mut any, vec![kv_str("service.name", "checkout")], "other"));
        assert!(eval(&mut any, vec![], "checkout"));
        let mut inv = pol(&["health"], None, (None, None), AttributeScope::Span, true, "kind");
        assert!(!eval(&mut inv, vec![kv_str("kind", "health")], "s"));
        assert!(eval(&mut inv, vec![kv_str("kind", "user")], "s"));
    }

    #[test]
    fn bad_regex_is_an_error() {
        let r = AttributePolicy::new(
            Arc::from("x"),
            "k".into(),
            vec![],
            Some("("),
            (None, None),
            AttributeScope::Any,
            false,
        );
        assert!(r.is_err());
    }
}
