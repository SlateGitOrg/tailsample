//! Policy-budget guarantees, checked on the deterministic simulator against
//! generator ground truth.

use tailsample::loadgen::GenConfig;
use tailsample::sim::{simulate, SimOptions, SimReport};
use tailsample::Config;

const ERROR_BUDGET: u64 = 1_200; // 60% of 2000 spans/s

fn budget_config(extra_buffer: &str) -> Config {
    Config::from_toml_str(&format!(
        r#"
        [buffer]
        decision_wait_ms = 3000
        {extra_buffer}

        [[policies]]
        name = "budget"
        type = "composite"
        max_total_spans_per_second = 2000
        rate_allocation = [{{ policy = "errors", percent = 60 }}, {{ policy = "slow", percent = 25 }}]
          [[policies.sub_policies]]
          name = "errors"
          type = "status_code"
          codes = ["ERROR"]
          [[policies.sub_policies]]
          name = "slow"
          type = "latency"
          threshold_ms = 1000
          [[policies.sub_policies]]
          name = "baseline"
          type = "probabilistic"
          percentage = 2.0
        "#
    ))
    .expect("valid config")
}

fn run(cfg: &Config, traces: u64, rate: f64, gen: GenConfig) -> SimReport {
    simulate(
        cfg,
        &SimOptions {
            traces,
            rate,
            gen,
            step_ms: 50,
        },
    )
    .expect("simulation runs")
}

fn gen(seed: u64, error_rate: f64) -> GenConfig {
    GenConfig {
        seed,
        error_rate,
        slow_rate: 0.02,
        slow_extra_ms: (1_200, 2_000),
        late_rate: 0.05,
        late_delay_ms: (4_000, 6_000),
        ..GenConfig::default()
    }
}

#[test]
fn every_error_trace_is_kept_while_within_budget() {
    // ~400 traces/s * ~12 spans * error rate stays below 1200 error spans/s.
    for (seed, error_rate) in [(1, 0.01), (2, 0.05), (3, 0.10), (4, 0.15)] {
        let r = run(&budget_config(""), 12_000, 400.0, gen(seed, error_rate));
        assert!(r.errors.generated > 0);
        assert_eq!(
            r.errors.kept, r.errors.generated,
            "seed {seed}, error rate {error_rate}: {r}"
        );
        assert_eq!(
            r.errors.complete, r.errors.generated,
            "late spans of kept error traces must be forwarded: {r}"
        );
        let peak = r.max_window_spans.get("budget/errors").copied().unwrap_or(0);
        assert!(peak <= ERROR_BUDGET, "error budget exceeded: {peak}");
    }
}

#[test]
fn error_budget_is_a_hard_cap_during_an_error_storm() {
    // 60% of traces fail: error spans far exceed the 1200/s budget.
    let r = run(&budget_config(""), 12_000, 400.0, gen(9, 0.6));
    let peak = r.max_window_spans.get("budget/errors").copied().unwrap_or(0);
    assert!(peak <= ERROR_BUDGET, "error budget exceeded: {peak}\n{r}");
    assert!(r.errors.kept < r.errors.generated, "{r}");
    // Budget is actually used, not starved: at least 70% of the error
    // spans the budget allows over the run were exported.
    let seconds = (r.sim_ms / 1_000).max(1);
    let allowed = ERROR_BUDGET * seconds;
    assert!(
        r.errors.spans_kept * 10 >= allowed * 7,
        "kept {} error spans, budget allows {allowed}",
        r.errors.spans_kept
    );
    for (label, peak) in &r.max_window_spans {
        let cap = match label.as_str() {
            "budget/errors" => 1_200,
            "budget/slow" => 500,
            "budget/baseline" => 300,
            other => panic!("unexpected label {other}"),
        };
        assert!(*peak <= cap, "{label}: {peak} > {cap}");
    }
}

#[test]
fn decide_early_preserves_errors_under_memory_pressure() {
    // A 2 MiB buffer holds far fewer traces than 3s of traffic.
    let tight = "max_bytes = 2097152";
    let early = run(
        &budget_config(&format!("{tight}\neviction = \"decide_early\"")),
        8_000,
        800.0,
        gen(5, 0.03),
    );
    let dropping = run(
        &budget_config(&format!("{tight}\neviction = \"drop_oldest\"")),
        8_000,
        800.0,
        gen(5, 0.03),
    );
    assert!(early.peak_buffer_bytes <= 2 * 1024 * 1024);
    assert!(dropping.peak_buffer_bytes <= 2 * 1024 * 1024);
    assert!(early.stats.traces_decided_early > 0);
    assert!(dropping.stats.traces_evicted > 0);
    assert!(
        early.errors.kept > dropping.errors.kept,
        "decide_early {} vs drop_oldest {}",
        early.errors.kept,
        dropping.errors.kept
    );
    // Decide-early only loses error traces whose error span had not arrived
    // when the trace was forced out.
    assert!(
        early.errors.kept_pct() >= 90.0,
        "decide_early kept {:.1}% of errors\n{early}",
        early.errors.kept_pct()
    );
}

#[test]
fn reject_new_holds_limits_and_counts_refusals() {
    let r = run(
        &budget_config("max_traces = 300\neviction = \"reject_new\""),
        6_000,
        800.0,
        gen(6, 0.03),
    );
    assert!(r.peak_buffer_traces <= 300);
    assert!(r.stats.spans_rejected_full > 0);
    assert!(r.stats.admission_closures > 0);
    let s = &r.stats;
    let terminal = s.spans_invalid
        + s.spans_rejected_full
        + s.spans_rejected_oversize
        + s.spans_dropped_trace_limit
        + s.spans_late_sampled
        + s.spans_late_dropped
        + s.spans_sampled
        + s.spans_not_sampled
        + s.spans_evicted;
    assert_eq!(terminal, s.spans_received, "every span accounted for");
}

#[test]
fn top_level_rate_limit_caps_throughput() {
    let cfg = Config::from_toml_str(
        r#"
        [buffer]
        decision_wait_ms = 1000
        [[policies]]
        name = "limit"
        type = "rate_limiting"
        spans_per_second = 500
        "#,
    )
    .unwrap();
    let r = run(&cfg, 5_000, 500.0, gen(8, 0.02));
    let peak = r.max_window_spans["limit"];
    // Bucket of 500 plus up to 500 refilled within one window.
    assert!(peak <= 1_000, "{peak}");
    let seconds = r.sim_ms as f64 / 1_000.0;
    let mean = r.total().spans_kept as f64 / seconds;
    assert!(mean <= 500.0 * 1.15, "mean {mean:.0} spans/s over {seconds:.1}s");
}

#[test]
fn simulation_is_deterministic() {
    let cfg = budget_config("");
    let a = run(&cfg, 3_000, 500.0, gen(11, 0.05));
    let b = run(&cfg, 3_000, 500.0, gen(11, 0.05));
    assert_eq!(a.stats, b.stats);
    assert_eq!(a.errors, b.errors);
    assert_eq!(a.max_window_spans, b.max_window_spans);
}

