//! Policy-quality evaluation on the deterministic simulator.
//!
//! Seeds 5001+ are used here and nowhere else (tests use seeds < 100), and
//! the reference configuration was written before these runs, so the
//! numbers are not tuned to this data. Run with `make eval`.

use tailsample::config::EvictionPolicy;
use tailsample::loadgen::GenConfig;
use tailsample::sim::{simulate, SimOptions, SimReport};
use tailsample::Config;

const REFERENCE: &str = include_str!("sampler.toml");

fn reference() -> Config {
    Config::from_toml_str(REFERENCE).expect("reference config")
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
    .expect("simulate")
}

fn gen(seed: u64) -> GenConfig {
    GenConfig {
        seed,
        ..GenConfig::default()
    }
}

fn pct(n: u64, d: u64) -> String {
    format!("{:.1}%", 100.0 * n as f64 / d.max(1) as f64)
}

fn main() {
    println!("# tailsample policy evaluation\n");

    println!("## 1. Reference policy at nominal load\n");
    println!("30,000 traces at 500 traces/s (~6k spans/s), 2% injected errors, 2% injected latency spikes,");
    println!("1% of traces with a span delivered 6-9 s late; `examples/sampler.toml` (5 s decision wait).\n");
    println!("| seed | error traces kept | complete | slow kept | other kept | span reduction | peak error spans/s (budget 1800) |");
    println!("|---:|---:|---:|---:|---:|---:|---:|");
    for seed in [5001, 5002, 5003] {
        let r = run(&reference(), 30_000, 500.0, gen(seed));
        let t = r.total();
        println!(
            "| {seed} | {}/{} | {} | {} | {} | {} | {} |",
            r.errors.kept,
            r.errors.generated,
            pct(r.errors.complete, r.errors.kept),
            pct(r.slow.kept, r.slow.generated),
            pct(r.normal.kept, r.normal.generated),
            pct(t.spans_generated - t.spans_kept, t.spans_generated),
            r.max_window_spans
                .get("budget/errors")
                .copied()
                .unwrap_or(0)
        );
    }

    println!("\n## 2. Error storm: budgets are caps\n");
    println!(
        "Same policy, 20,000 traces at 500 traces/s, injected error rate varied. Error budget is"
    );
    println!("60% of 3000 = 1800 spans/s.\n");
    println!(
        "| error rate | error traces | kept | peak error spans/s | mean error spans/s exported |"
    );
    println!("|---:|---:|---:|---:|---:|");
    for (i, rate) in [0.05, 0.2, 0.4, 0.8].into_iter().enumerate() {
        let r = run(
            &reference(),
            20_000,
            500.0,
            GenConfig {
                error_rate: rate,
                ..gen(5101 + i as u64)
            },
        );
        println!(
            "| {:.0}% | {} | {} | {} | {:.0} |",
            rate * 100.0,
            r.errors.generated,
            pct(r.errors.kept, r.errors.generated),
            r.max_window_spans
                .get("budget/errors")
                .copied()
                .unwrap_or(0),
            r.errors.spans_kept as f64 / (r.sim_ms as f64 / 1000.0)
        );
    }

    println!("\n## 3. Eviction policy under memory pressure\n");
    println!(
        "12,000 traces at 1,000 traces/s with `max_bytes` = 4 MiB (about a third of a second of"
    );
    println!("traffic against a 5 s decision wait).\n");
    println!("| eviction | error traces kept | slow kept | traces decided early | traces dropped undecided | spans refused | peak est. bytes |");
    println!("|---|---:|---:|---:|---:|---:|---:|");
    for ev in [
        EvictionPolicy::DecideEarly,
        EvictionPolicy::DropOldest,
        EvictionPolicy::RejectNew,
    ] {
        let mut cfg = reference();
        cfg.buffer.max_bytes = 4 * 1024 * 1024;
        cfg.buffer.eviction = ev;
        let r = run(&cfg, 12_000, 1_000.0, gen(5201));
        println!(
            "| {ev:?} | {} | {} | {} | {} | {} | {:.1} MiB |",
            pct(r.errors.kept, r.errors.generated),
            pct(r.slow.kept, r.slow.generated),
            r.stats.traces_decided_early,
            r.stats.traces_evicted,
            r.stats.spans_rejected_full,
            r.peak_buffer_bytes as f64 / (1024.0 * 1024.0)
        );
    }

    println!("\n## 4. Decision wait versus completeness\n");
    println!(
        "Reference policy, 20,000 traces at 500 traces/s. Latency spikes add 1.2-3 s; late spans"
    );
    println!("arrive 6-9 s after their trace ends.\n");
    println!("| decision_wait_ms | slow traces kept | kept traces complete | spans after decision: forwarded / dropped | peak buffered traces |");
    println!("|---:|---:|---:|---:|---:|");
    for wait in [1_000u64, 2_000, 3_500, 5_000, 10_000] {
        let mut cfg = reference();
        cfg.buffer.decision_wait_ms = wait;
        let r = run(&cfg, 20_000, 500.0, gen(5301));
        let t = r.total();
        println!(
            "| {wait} | {} | {} | {} / {} | {} |",
            pct(r.slow.kept, r.slow.generated),
            pct(t.complete, t.kept),
            r.stats.spans_late_sampled,
            r.stats.spans_late_dropped,
            r.peak_buffer_traces
        );
    }
}
