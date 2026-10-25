//! The bundled demo runs end to end at a small scale.

use tailsample::demo::{run, DemoOptions};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn demo_keeps_errors_and_the_fixture() {
    let r = run(&DemoOptions {
        traces: 300,
        rate: 600.0,
        seed: 3,
    })
    .await
    .expect("demo runs");
    assert!(r.fixture_kept, "{r}");
    assert_eq!(r.errors.kept, r.errors.generated, "{r}");
    assert_eq!(r.load.requests_failed, 0);
    assert!(r.per_protocol.0 > 0 && r.per_protocol.1 > 0);
    assert!(r.normal.kept < r.normal.generated);
    assert!(!r.metrics_excerpt.is_empty());
    let text = r.to_string();
    assert!(text.contains("fixture trace"));
    assert!(text.contains("OTLP/gRPC"));
}
