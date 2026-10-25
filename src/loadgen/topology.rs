//! A small e-commerce service graph used to generate realistic span trees:
//! HTTP entry points on a frontend fanning out over gRPC to backend services
//! that in turn hit Redis and PostgreSQL.

/// Span kind, mirroring OTLP `SpanKind` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Incoming request handler.
    Server = 2,
    /// Outgoing call (RPC or database).
    Client = 3,
    /// In-process work.
    Internal = 1,
}

/// What protocol attributes an operation carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// HTTP server route.
    Http {
        /// HTTP method.
        method: &'static str,
        /// Route template.
        route: &'static str,
    },
    /// gRPC server method.
    Rpc {
        /// Service.
        service: &'static str,
        /// Method.
        method: &'static str,
    },
    /// Database client call.
    Db {
        /// `db.system` value.
        system: &'static str,
        /// Statement / command.
        statement: &'static str,
    },
    /// No protocol attributes.
    Internal,
}

/// A downstream call made by an operation.
#[derive(Debug, Clone, Copy)]
pub struct Call {
    /// Index of the callee operation in [`OPS`].
    pub op: usize,
    /// Probability that the call happens at all.
    pub prob: f64,
    /// Inclusive repeat range.
    pub repeat: (u64, u64),
}

/// One operation in the graph.
#[derive(Debug, Clone, Copy)]
pub struct Op {
    /// Owning service (index into [`SERVICES`]).
    pub service: usize,
    /// Span name.
    pub name: &'static str,
    /// Span kind.
    pub kind: Kind,
    /// Protocol attributes.
    pub protocol: Protocol,
    /// Median self time in ms.
    pub median_ms: f64,
    /// Log-normal shape.
    pub sigma: f64,
    /// Downstream calls, executed sequentially.
    pub calls: &'static [Call],
}

/// Services and their versions.
pub const SERVICES: &[(&str, &str)] = &[
    ("frontend", "1.14.2"),
    ("cart", "2.3.0"),
    ("product-catalog", "0.9.7"),
    ("checkout", "3.1.1"),
    ("payment", "1.0.4"),
    ("shipping", "1.2.0"),
    ("currency", "4.0.0"),
    ("email", "0.4.3"),
    ("recommendation", "2.0.0-rc1"),
];

const fn c(op: usize, prob: f64, lo: u64, hi: u64) -> Call {
    Call {
        op,
        prob,
        repeat: (lo, hi),
    }
}

const fn rpc(
    service: usize,
    svc: &'static str,
    method: &'static str,
    name: &'static str,
    median_ms: f64,
    calls: &'static [Call],
) -> Op {
    Op {
        service,
        name,
        kind: Kind::Server,
        protocol: Protocol::Rpc {
            service: svc,
            method,
        },
        median_ms,
        sigma: 0.5,
        calls,
    }
}

const fn db(service: usize, system: &'static str, statement: &'static str, median_ms: f64) -> Op {
    Op {
        service,
        name: statement,
        kind: Kind::Client,
        protocol: Protocol::Db { system, statement },
        median_ms,
        sigma: 0.6,
        calls: &[],
    }
}

const fn http(
    method: &'static str,
    route: &'static str,
    name: &'static str,
    median_ms: f64,
    calls: &'static [Call],
) -> Op {
    Op {
        service: 0,
        name,
        kind: Kind::Server,
        protocol: Protocol::Http { method, route },
        median_ms,
        sigma: 0.4,
        calls,
    }
}

/// Operation table. Indices are referenced by [`Call::op`] and [`ENTRY_POINTS`].
pub const OPS: &[Op] = &[
    /* 0 */
    http(
        "GET",
        "/product/{id}",
        "GET /product/{id}",
        6.0,
        &[c(4, 1.0, 1, 1), c(6, 0.8, 1, 1), c(7, 0.9, 1, 1)],
    ),
    /* 1 */
    http(
        "GET",
        "/cart",
        "GET /cart",
        4.0,
        &[c(2, 1.0, 1, 1), c(4, 1.0, 1, 4), c(7, 0.5, 1, 1)],
    ),
    /* 2 */
    rpc(
        1,
        "CartService",
        "GetCart",
        "CartService/GetCart",
        1.5,
        &[c(9, 1.0, 1, 1)],
    ),
    /* 3 */
    http(
        "POST",
        "/checkout",
        "POST /checkout",
        8.0,
        &[c(5, 1.0, 1, 1)],
    ),
    /* 4 */
    rpc(
        2,
        "ProductCatalogService",
        "GetProduct",
        "ProductCatalogService/GetProduct",
        2.0,
        &[c(10, 0.7, 1, 1)],
    ),
    /* 5 */
    rpc(
        3,
        "CheckoutService",
        "PlaceOrder",
        "CheckoutService/PlaceOrder",
        12.0,
        &[
            c(2, 1.0, 1, 1),
            c(4, 1.0, 1, 3),
            c(7, 1.0, 1, 1),
            c(8, 1.0, 1, 1),
            c(11, 1.0, 1, 1),
            c(12, 0.9, 1, 1),
            c(13, 1.0, 1, 1),
        ],
    ),
    /* 6 */
    rpc(
        8,
        "RecommendationService",
        "ListRecommendations",
        "RecommendationService/ListRecommendations",
        9.0,
        &[c(4, 1.0, 1, 2)],
    ),
    /* 7 */
    rpc(
        6,
        "CurrencyService",
        "Convert",
        "CurrencyService/Convert",
        0.8,
        &[],
    ),
    /* 8 */
    rpc(
        4,
        "PaymentService",
        "Charge",
        "PaymentService/Charge",
        30.0,
        &[c(14, 1.0, 1, 1)],
    ),
    /* 9 */ db(1, "redis", "HGETALL", 0.4),
    /* 10 */ db(2, "postgresql", "SELECT products", 1.5),
    /* 11 */
    rpc(
        5,
        "ShippingService",
        "ShipOrder",
        "ShippingService/ShipOrder",
        6.0,
        &[],
    ),
    /* 12 */
    rpc(
        7,
        "EmailService",
        "SendOrderConfirmation",
        "EmailService/SendOrderConfirmation",
        15.0,
        &[],
    ),
    /* 13 */
    rpc(
        1,
        "CartService",
        "EmptyCart",
        "CartService/EmptyCart",
        1.5,
        &[c(9, 1.0, 1, 1)],
    ),
    /* 14 */ db(4, "postgresql", "INSERT payments", 3.0),
    /* 15 */ http("GET", "/healthz", "GET /healthz", 0.2, &[]),
    /* 16 */
    Op {
        service: 3,
        name: "validate cart",
        kind: Kind::Internal,
        protocol: Protocol::Internal,
        median_ms: 0.5,
        sigma: 0.3,
        calls: &[],
    },
];

/// Root operations and their traffic weights.
pub const ENTRY_POINTS: &[(usize, f64)] = &[(0, 0.50), (1, 0.25), (3, 0.17), (15, 0.08)];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_is_well_formed_and_acyclic() {
        fn depth(op: usize, seen: &mut Vec<usize>) -> usize {
            assert!(!seen.contains(&op), "cycle through {op}");
            seen.push(op);
            let d = OPS[op]
                .calls
                .iter()
                .map(|c| depth(c.op, seen))
                .max()
                .unwrap_or(0);
            seen.pop();
            d + 1
        }
        for (i, op) in OPS.iter().enumerate() {
            assert!(op.service < SERVICES.len(), "op {i}");
            for c in op.calls {
                assert!(c.op < OPS.len());
                assert!(c.repeat.0 <= c.repeat.1);
            }
            assert!(depth(i, &mut Vec::new()) <= 6);
        }
        let w: f64 = ENTRY_POINTS.iter().map(|e| e.1).sum();
        assert!((w - 1.0).abs() < 1e-9);
        assert_eq!(Kind::Internal as i32, 1);
    }
}
