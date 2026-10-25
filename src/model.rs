//! Internal span representation: OTLP requests are flattened into
//! [`SpanRecord`]s that share their resource / scope context through `Arc`s,
//! and regrouped into OTLP requests on export.

use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

use crate::ids::TraceId;

/// Resource-level context shared by every span of one `ResourceSpans`.
#[derive(Debug, Default, PartialEq)]
pub struct ResourceContext {
    /// The resource, if the sender supplied one.
    pub resource: Option<Resource>,
    /// `ResourceSpans.schema_url`.
    pub schema_url: String,
}

/// Scope-level context shared by every span of one `ScopeSpans`.
#[derive(Debug, Default, PartialEq)]
pub struct ScopeContext {
    /// Owning resource context.
    pub resource: Arc<ResourceContext>,
    /// Instrumentation scope.
    pub scope: Option<InstrumentationScope>,
    /// `ScopeSpans.schema_url`.
    pub schema_url: String,
}

/// One span plus the context it arrived with.
#[derive(Debug, Clone)]
pub struct SpanRecord {
    /// Parsed trace id.
    pub trace_id: TraceId,
    /// The span itself, unmodified.
    pub span: Span,
    /// Shared resource / scope context.
    pub ctx: Arc<ScopeContext>,
    /// Estimated heap footprint in bytes, see [`estimate_span_bytes`].
    pub cost: usize,
}

impl SpanRecord {
    /// Build a record, computing its cost estimate.
    pub fn new(trace_id: TraceId, span: Span, ctx: Arc<ScopeContext>) -> Self {
        let cost = estimate_span_bytes(&span);
        SpanRecord {
            trace_id,
            span,
            ctx,
            cost,
        }
    }

    /// Look up a resource attribute.
    pub fn resource_attr(&self, key: &str) -> Option<&AnyValue> {
        let res = self.ctx.resource.resource.as_ref()?;
        find_attr(&res.attributes, key)
    }

    /// Look up a span attribute.
    pub fn span_attr(&self, key: &str) -> Option<&AnyValue> {
        find_attr(&self.span.attributes, key)
    }

    /// The `service.name` resource attribute, if present.
    pub fn service_name(&self) -> Option<&str> {
        match self.resource_attr("service.name")?.value.as_ref()? {
            any_value::Value::StringValue(s) => Some(s.as_str()),
            _ => None,
        }
    }
}

fn find_attr<'a>(attrs: &'a [KeyValue], key: &str) -> Option<&'a AnyValue> {
    attrs.iter().find(|kv| kv.key == key)?.value.as_ref()
}

/// Result of flattening a request.
#[derive(Debug, Default)]
pub struct Flattened {
    /// Valid spans.
    pub records: Vec<SpanRecord>,
    /// Spans dropped because their trace id was not 16 non-zero bytes.
    pub invalid: u64,
}

/// Flatten an OTLP export request into span records.
pub fn flatten(req: ExportTraceServiceRequest) -> Flattened {
    let mut out = Flattened::default();
    for rs in req.resource_spans {
        let res_ctx = Arc::new(ResourceContext {
            resource: rs.resource,
            schema_url: rs.schema_url,
        });
        for ss in rs.scope_spans {
            let ctx = Arc::new(ScopeContext {
                resource: Arc::clone(&res_ctx),
                scope: ss.scope,
                schema_url: ss.schema_url,
            });
            for span in ss.spans {
                match TraceId::from_slice(&span.trace_id) {
                    Some(id) => out
                        .records
                        .push(SpanRecord::new(id, span, Arc::clone(&ctx))),
                    None => out.invalid += 1,
                }
            }
        }
    }
    out
}

/// Regroup records into an OTLP request, rebuilding one `ResourceSpans` per
/// distinct resource context and one `ScopeSpans` per distinct scope context
/// (identity, not value, comparison; order of first appearance is kept).
pub fn to_request<I>(records: I) -> ExportTraceServiceRequest
where
    I: IntoIterator<Item = SpanRecord>,
{
    let mut resource_spans: Vec<ResourceSpans> = Vec::new();
    let mut res_index: HashMap<*const ResourceContext, usize> = HashMap::new();
    let mut scope_index: HashMap<*const ScopeContext, (usize, usize)> = HashMap::new();
    for rec in records {
        let scope_key = Arc::as_ptr(&rec.ctx);
        let (ri, si) = match scope_index.get(&scope_key) {
            Some(&pos) => pos,
            None => {
                let res_key = Arc::as_ptr(&rec.ctx.resource);
                let ri = *res_index.entry(res_key).or_insert_with(|| {
                    resource_spans.push(ResourceSpans {
                        resource: rec.ctx.resource.resource.clone(),
                        scope_spans: Vec::new(),
                        schema_url: rec.ctx.resource.schema_url.clone(),
                    });
                    resource_spans.len() - 1
                });
                let scopes = &mut resource_spans[ri].scope_spans;
                scopes.push(ScopeSpans {
                    scope: rec.ctx.scope.clone(),
                    spans: Vec::new(),
                    schema_url: rec.ctx.schema_url.clone(),
                });
                let pos = (ri, scopes.len() - 1);
                scope_index.insert(scope_key, pos);
                pos
            }
        };
        resource_spans[ri].scope_spans[si].spans.push(rec.span);
    }
    ExportTraceServiceRequest { resource_spans }
}

/// Number of spans in a request.
pub fn span_count(req: &ExportTraceServiceRequest) -> usize {
    req.resource_spans
        .iter()
        .flat_map(|rs| rs.scope_spans.iter())
        .map(|ss| ss.spans.len())
        .sum()
}

/// Estimate the heap bytes retained by a buffered span: the record itself
/// plus the capacity of every owned allocation reachable from the span.
/// Shared resource / scope context is not attributed to individual spans.
pub fn estimate_span_bytes(span: &Span) -> usize {
    size_of::<SpanRecord>()
        + span.trace_id.capacity()
        + span.span_id.capacity()
        + span.parent_span_id.capacity()
        + span.trace_state.capacity()
        + span.name.capacity()
        + attrs_bytes(&span.attributes)
        + span.events.capacity() * size_of::<opentelemetry_proto::tonic::trace::v1::span::Event>()
        + span
            .events
            .iter()
            .map(|e| e.name.capacity() + attrs_bytes(&e.attributes))
            .sum::<usize>()
        + span.links.capacity() * size_of::<opentelemetry_proto::tonic::trace::v1::span::Link>()
        + span
            .links
            .iter()
            .map(|l| {
                l.trace_id.capacity()
                    + l.span_id.capacity()
                    + l.trace_state.capacity()
                    + attrs_bytes(&l.attributes)
            })
            .sum::<usize>()
        + span.status.as_ref().map_or(0, |s| s.message.capacity())
}

fn attrs_bytes(attrs: &Vec<KeyValue>) -> usize {
    attrs.capacity() * size_of::<KeyValue>()
        + attrs
            .iter()
            .map(|kv| kv.key.capacity() + kv.value.as_ref().map_or(0, value_bytes))
            .sum::<usize>()
}

fn value_bytes(v: &AnyValue) -> usize {
    match &v.value {
        None => 0,
        Some(any_value::Value::StringValue(s)) => s.capacity(),
        Some(any_value::Value::BytesValue(b)) => b.capacity(),
        Some(any_value::Value::ArrayValue(a)) => {
            a.values.capacity() * size_of::<AnyValue>()
                + a.values.iter().map(value_bytes).sum::<usize>()
        }
        Some(any_value::Value::KvlistValue(l)) => attrs_bytes(&l.values),
        Some(_) => 0,
    }
}

/// Convenience constructor for a string attribute.
pub fn kv_str(key: &str, value: impl Into<String>) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }),
        ..Default::default()
    }
}

/// Convenience constructor for an integer attribute.
pub fn kv_int(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(value)),
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
    use opentelemetry_proto::tonic::trace::v1::Status;

    fn span(trace: u8, id: u8) -> Span {
        Span {
            trace_id: vec![trace; 16],
            span_id: vec![id; 8],
            name: format!("op-{id}"),
            attributes: vec![kv_str("http.route", "/x"), kv_int("n", 3)],
            ..Default::default()
        }
    }

    fn request() -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![
                ResourceSpans {
                    resource: Some(Resource {
                        attributes: vec![kv_str("service.name", "a")],
                        ..Default::default()
                    }),
                    scope_spans: vec![
                        ScopeSpans {
                            spans: vec![span(1, 1), span(2, 2), span(0, 9)],
                            ..Default::default()
                        },
                        ScopeSpans {
                            scope: Some(InstrumentationScope {
                                name: "lib".into(),
                                ..Default::default()
                            }),
                            spans: vec![span(1, 3)],
                            ..Default::default()
                        },
                    ],
                    schema_url: "s".into(),
                },
                ResourceSpans {
                    resource: Some(Resource {
                        attributes: vec![kv_str("service.name", "b")],
                        ..Default::default()
                    }),
                    scope_spans: vec![ScopeSpans {
                        spans: vec![span(1, 4)],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
        }
    }

    #[test]
    fn flatten_drops_invalid_ids_and_keeps_context() {
        let flat = flatten(request());
        assert_eq!(flat.invalid, 1);
        assert_eq!(flat.records.len(), 4);
        assert_eq!(flat.records[0].service_name(), Some("a"));
        assert_eq!(flat.records[3].service_name(), Some("b"));
        assert!(flat.records[0].span_attr("http.route").is_some());
        assert!(flat.records[0].span_attr("missing").is_none());
        assert!(flat.records[0].cost > size_of::<SpanRecord>());
    }

    #[test]
    fn regroup_roundtrips_structure() {
        let flat = flatten(request());
        let req = to_request(flat.records);
        assert_eq!(span_count(&req), 4);
        assert_eq!(req.resource_spans.len(), 2);
        assert_eq!(req.resource_spans[0].scope_spans.len(), 2);
        assert_eq!(req.resource_spans[0].schema_url, "s");
        assert_eq!(
            req.resource_spans[0].scope_spans[1]
                .scope
                .as_ref()
                .unwrap()
                .name,
            "lib"
        );
    }

    #[test]
    fn regroup_subset_by_trace_preserves_context() {
        let flat = flatten(request());
        let only_trace_1: Vec<_> = flat
            .records
            .into_iter()
            .filter(|r| r.trace_id == TraceId([1; 16]))
            .collect();
        let req = to_request(only_trace_1);
        assert_eq!(span_count(&req), 3);
        assert_eq!(req.resource_spans.len(), 2);
    }

    #[test]
    fn estimate_counts_nested_allocations() {
        let base = span(1, 1);
        let mut rich = base.clone();
        rich.events.push(Event {
            name: "exception".into(),
            attributes: vec![kv_str("exception.message", "x".repeat(500))],
            ..Default::default()
        });
        rich.links.push(Link {
            trace_id: vec![3; 16],
            span_id: vec![3; 8],
            ..Default::default()
        });
        rich.status = Some(Status {
            message: "boom".into(),
            code: 2,
        });
        rich.attributes.push(KeyValue {
            key: "arr".into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::ArrayValue(
                    opentelemetry_proto::tonic::common::v1::ArrayValue {
                        values: vec![AnyValue {
                            value: Some(any_value::Value::BytesValue(vec![0; 64])),
                        }],
                    },
                )),
            }),
            ..Default::default()
        });
        rich.attributes.push(KeyValue {
            key: "kv".into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::KvlistValue(
                    opentelemetry_proto::tonic::common::v1::KeyValueList {
                        values: vec![kv_str("inner", "v")],
                    },
                )),
            }),
            ..Default::default()
        });
        assert!(estimate_span_bytes(&rich) > estimate_span_bytes(&base) + 500 + 64);
    }
}
