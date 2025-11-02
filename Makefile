SHELL := /bin/bash

CARGO ?= cargo
PACKAGE_SCRIPT := packaging/debian/build.sh
DEB_OUTPUT_DIR := target/debian
SERVICE_NAME := nanocloud
VALIDATION_LOG_DIR ?= target/validation

.PHONY: all debug release openapi package install clean help validate-controllers

all: debug

# Build a debug profile binary
debug:
	$(CARGO) build

# Build an optimized release binary
release:
	$(CARGO) build --release

# Regenerate the OpenAPI specification JSON
openapi:
	$(CARGO) run --features openapi --bin openapi_gen > docs/openapi.json

# Run the tests with deterministic test directories
test:
	@set -euo pipefail; \
	root="$$(pwd)/target/test-output/default"; \
	secure_dir="$$root/secure"; \
	keyspace_dir="$$root/keyspace"; \
	lock_dir="$$root/lock"; \
	mkdir -p "$$secure_dir" "$$keyspace_dir" "$$lock_dir"; \
	touch "$$lock_dir/nanocloud.lock"; \
	export NANOCLOUD_SECURE_ASSETS="$$secure_dir"; \
	export NANOCLOUD_KEYSPACE="$$keyspace_dir"; \
	export NANOCLOUD_LOCK_FILE="$$lock_dir/nanocloud.lock"; \
	export RUST_TEST_THREADS="$${RUST_TEST_THREADS:-1}"; \
	$(CARGO) test --locked --workspace --all-targets

lint:
	$(CARGO) clippy --all-targets --all-features

# Produce a Debian package using the packaging script. Expect the release binary to already exist.
package:
	$(PACKAGE_SCRIPT)

# Install the latest Debian package and restart the service if it is running
install:
	set -euo pipefail; \
	DEB_FILE=$$(ls -1t $(DEB_OUTPUT_DIR)/nanocloud_*.deb | head -n1); \
	if [[ -z "$$DEB_FILE" ]]; then \
		echo "No Debian package found in $(DEB_OUTPUT_DIR)" >&2; \
		exit 1; \
	fi; \
	echo "Installing $$DEB_FILE"; \
	sudo dpkg -i "$$DEB_FILE"; \
	if systemctl is-active --quiet $(SERVICE_NAME); then \
		echo "Restarting $(SERVICE_NAME) service"; \
		sudo systemctl restart $(SERVICE_NAME); \
	else \
		echo "$(SERVICE_NAME) service not running; skipping restart"; \
	fi

clean:
	$(CARGO) clean

validate-controllers:
	./util/controller_validation.sh "$(VALIDATION_LOG_DIR)"

help:
	@echo "Available targets:"; \
	 echo "  debug    Build nanocloud in debug mode"; \
	 echo "  release  Build nanocloud in release mode"; \
	 echo "  openapi  Regenerate docs/openapi.json using the embedded generator"; \
	 echo "  lint     Run cargo clippy across all targets and features"; \
	 echo "  package  Build a Debian installer (requires release binary)"; \
	 echo "  install  Install the latest Debian package and restart the service if active"; \
	 echo "  clean    Remove build artifacts"; \
	 echo "  validate-controllers  Run controller validation pipeline"
