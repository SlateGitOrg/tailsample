# One command gates everything: `make check`.
# Set CARGO_TARGET_DIR in your environment to share a build directory.

CARGO ?= cargo
COVERAGE_MIN ?= 80
# CLI glue and the benchmark harness are exercised by `make demo` / `make bench`,
# not by unit tests, so they are excluded from the coverage gate.
COVERAGE_IGNORE ?= (src/main\.rs|src/bin/|examples/)

.PHONY: check fmt fmt-check lint test coverage coverage-report demo bench bench-quick eval build clean

check: fmt-check lint test coverage

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

lint:
	$(CARGO) clippy --all-targets --locked -- -D warnings

test:
	$(CARGO) test --all-targets --locked

coverage:
	$(CARGO) llvm-cov --locked --lib --tests --summary-only \
		--ignore-filename-regex '$(COVERAGE_IGNORE)' \
		--fail-under-lines $(COVERAGE_MIN)

coverage-report:
	$(CARGO) llvm-cov --locked --lib --tests --html \
		--ignore-filename-regex '$(COVERAGE_IGNORE)'

build:
	$(CARGO) build --release --locked

demo:
	$(CARGO) run --release --locked --bin tailsample -- demo

eval:
	$(CARGO) run --release --locked --example eval

bench:
	./scripts/bench.sh

bench-quick:
	$(CARGO) run --release --locked --bin tailsample-bench -- --quick

clean:
	$(CARGO) clean
