#!/usr/bin/env bash
# Run the throughput/memory benchmark and the policy evaluation, recording
# results (with machine details) under bench/.
set -euo pipefail
cd "$(dirname "$0")/.."

machine() {
  echo "- date: $(date -u +%Y-%m-%dT%H:%MZ)"
  echo "- os/arch: $(uname -s) $(uname -m)"
  if command -v sysctl >/dev/null 2>&1 && sysctl -n machdep.cpu.brand_string >/dev/null 2>&1; then
    echo "- cpu: $(sysctl -n machdep.cpu.brand_string), $(sysctl -n hw.ncpu) logical cores"
  elif [ -r /proc/cpuinfo ]; then
    echo "- cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //'), $(nproc) logical cores"
  fi
  echo "- rustc: $(rustc --version)"
  echo "- commit: $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
}

cargo build --release --locked --bin tailsample-bench --example eval
{
  machine
  echo
  cargo run --release --locked --quiet --bin tailsample-bench -- "$@"
} | tee bench/bench-results.md
{
  machine
  echo
  cargo run --release --locked --quiet --example eval
} | tee bench/eval-results.md
