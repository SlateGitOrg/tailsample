//! `tailsample` command-line interface.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use tailsample::config::Config;
use tailsample::exporter::{Exporter, GrpcExporter, HttpExporter};
use tailsample::loadgen::{run_live, GenConfig, LiveOptions, Schedule};
use tailsample::server::Server;
use tailsample::sim::{simulate, SimOptions};
use tailsample::sink::Sink;
use tailsample::{demo, policy};

#[derive(Parser)]
#[command(name = "tailsample", version, about = "Tail-based trace sampler for OTLP")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the sampler (OTLP gRPC/HTTP in, OTLP out).
    Serve {
        /// TOML configuration file.
        #[arg(long, short)]
        config: PathBuf,
    },
    /// Validate a configuration and print the resolved policies.
    CheckConfig {
        /// TOML configuration file.
        #[arg(long, short)]
        config: PathBuf,
    },
    /// Run the sampler core against generated traffic on a simulated clock.
    Simulate {
        /// TOML configuration file.
        #[arg(long, short)]
        config: PathBuf,
        #[command(flatten)]
        workload: Workload,
    },
    /// Send generated traffic to a running sampler or collector.
    Loadgen {
        /// OTLP/gRPC endpoint, e.g. http://127.0.0.1:4317.
        #[arg(long, conflicts_with = "http")]
        grpc: Option<String>,
        /// OTLP/HTTP endpoint, e.g. http://127.0.0.1:4318/v1/traces.
        #[arg(long)]
        http: Option<String>,
        /// Maximum in-flight requests.
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
        #[command(flatten)]
        workload: Workload,
    },
    /// Run a debug OTLP/gRPC sink that counts what it receives.
    Sink {
        /// Listen address.
        #[arg(long, default_value = "127.0.0.1:14317")]
        listen: String,
    },
    /// Offline end-to-end demo over loopback.
    Demo {
        /// Traces to generate.
        #[arg(long, default_value_t = 3_000)]
        traces: u64,
        /// Traces per second.
        #[arg(long, default_value_t = 600.0)]
        rate: f64,
    },
}

#[derive(Args, Clone)]
struct Workload {
    /// Traces to generate.
    #[arg(long, default_value_t = 20_000)]
    traces: u64,
    /// Traces per second.
    #[arg(long, default_value_t = 500.0)]
    rate: f64,
    /// Generator seed.
    #[arg(long, default_value_t = 42)]
    seed: u64,
    /// Fraction of traces with an injected error.
    #[arg(long, default_value_t = 0.02)]
    error_rate: f64,
    /// Fraction of traces with an injected latency spike.
    #[arg(long, default_value_t = 0.02)]
    slow_rate: f64,
    /// Fraction of traces with one span delivered late.
    #[arg(long, default_value_t = 0.01)]
    late_rate: f64,
}

impl Workload {
    fn gen(&self) -> GenConfig {
        GenConfig {
            seed: self.seed,
            error_rate: self.error_rate,
            slow_rate: self.slow_rate,
            late_rate: self.late_rate,
            ..GenConfig::default()
        }
    }
}

fn init_logging(default: &str) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).with_target(false).try_init();
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli.cmd).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cmd: Cmd) -> anyhow::Result<()> {
    match cmd {
        Cmd::Serve { config } => {
            init_logging("info");
            let cfg = Config::load(&config)?;
            let server = Server::start(&cfg).await?;
            tracing::info!(policies = ?policy::PolicyEngine::from_config(&cfg.effective_policies())?.names(), "sampler running; Ctrl-C to stop");
            tokio::signal::ctrl_c().await?;
            tracing::info!("shutting down: draining and deciding buffered traces");
            server.shutdown().await;
            Ok(())
        }
        Cmd::CheckConfig { config } => {
            let cfg = Config::load(&config)?;
            let engine = policy::PolicyEngine::from_config(&cfg.effective_policies())?;
            println!("config OK: {}", config.display());
            println!("policies (evaluated in order): {}", engine.names().join(", "));
            println!(
                "buffer: wait {} ms, {} traces / {} bytes max, eviction {:?}",
                cfg.buffer.decision_wait_ms, cfg.buffer.max_traces, cfg.buffer.max_bytes, cfg.buffer.eviction
            );
            Ok(())
        }
        Cmd::Simulate { config, workload } => {
            let cfg = Config::load(&config)?;
            let opts = SimOptions {
                traces: workload.traces,
                rate: workload.rate,
                gen: workload.gen(),
                ..SimOptions::default()
            };
            let started = std::time::Instant::now();
            let report = simulate(&cfg, &opts)?;
            println!("{report}");
            println!("(computed in {:.2}s wall time)", started.elapsed().as_secs_f64());
            Ok(())
        }
        Cmd::Loadgen {
            grpc,
            http,
            concurrency,
            workload,
        } => {
            init_logging("warn");
            let target: Arc<dyn Exporter> = match (grpc, http) {
                (Some(g), _) => Arc::new(GrpcExporter::new(&g)?),
                (None, Some(h)) => Arc::new(HttpExporter::new(&h)?),
                (None, None) => anyhow::bail!("one of --grpc or --http is required"),
            };
            let mut schedule = Schedule::new(workload.gen(), workload.traces, workload.rate, false);
            let opts = LiveOptions {
                concurrency,
                ..LiveOptions::default()
            };
            let r = run_live(&mut schedule, target, &opts).await;
            println!(
                "sent {} requests / {} spans in {:.1}s ({:.0} spans/s); {} retries after pushback; {} requests ({} spans) failed",
                r.requests_ok,
                r.spans_ok,
                r.elapsed_ms as f64 / 1_000.0,
                r.spans_ok as f64 / (r.elapsed_ms.max(1) as f64 / 1_000.0),
                r.retries,
                r.requests_failed,
                r.spans_failed
            );
            Ok(())
        }
        Cmd::Sink { listen } => {
            init_logging("info");
            let sink = Sink::start(&listen).await?;
            println!("sink listening on {}", sink.addr());
            let mut tick = tokio::time::interval(Duration::from_secs(2));
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        let s = sink.snapshot();
                        println!("{} requests, {} traces, {} spans", s.requests, s.spans_by_trace.len(), s.spans());
                    }
                    _ = tokio::signal::ctrl_c() => break,
                }
            }
            sink.stop().await;
            Ok(())
        }
        Cmd::Demo { traces, rate } => {
            init_logging("warn");
            println!("tailsample demo: sink + sampler on loopback, {traces} traces at {rate}/s\n");
            let r = demo::run(&demo::DemoOptions {
                traces,
                rate,
                ..demo::DemoOptions::default()
            })
            .await?;
            println!("{r}");
            anyhow::ensure!(r.fixture_kept, "fixture error trace was not exported");
            anyhow::ensure!(
                r.errors.kept == r.errors.generated,
                "not every error trace was kept"
            );
            Ok(())
        }
    }
}
