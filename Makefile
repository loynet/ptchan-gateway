BINARY ?= ptchan-gateway
IMAGE ?= ptchan-gateway:local
GATEWAY_ENV ?= dev
ENV_FILE ?= .env.$(GATEWAY_ENV)
CONFIG_FILE ?= config/$(GATEWAY_ENV).toml
CONTAINER ?= ptchan-gateway-$(GATEWAY_ENV)
VOLUME ?= ptchan-gateway-$(GATEWAY_ENV)-data
DOCKER_NETWORK ?=
DOCKER_RUN_EXTRA ?=

LOAD_ENV = set -a; . ./$(ENV_FILE); set +a; \
	CONFIG_FILE=$(CONFIG_FILE); \
	SQLITE_PATH=$${SQLITE_PATH:-data/$(GATEWAY_ENV).db}; \
	export CONFIG_FILE SQLITE_PATH

DOCKER_RUN_FLAGS = --env-file $(ENV_FILE) \
	-e CONFIG_FILE=/etc/ptchan-gateway/config.toml \
	-e SQLITE_PATH=/data/ptchan-gateway.db \
	--mount type=bind,source=$(abspath $(CONFIG_FILE)),target=/etc/ptchan-gateway/config.toml,readonly \
	--mount type=volume,source=$(VOLUME),target=/data \
	--read-only \
	--tmpfs /tmp:rw,noexec,nosuid,nodev,size=16m \
	--cap-drop ALL \
	--security-opt no-new-privileges

ifneq ($(strip $(DOCKER_NETWORK)),)
DOCKER_NETWORK_FLAGS = --network $(DOCKER_NETWORK)
endif

.PHONY: check run db-reset build tools doctor docker-build docker-run docker-deploy docker-logs clean

check:
	cargo fmt -- --check
	cargo clippy --all-targets --all-features -- -D warnings -W clippy::pedantic -W clippy::cargo -A clippy::cargo-common-metadata -A clippy::multiple-crate-versions
	cargo test
	@if cargo metadata --no-deps --format-version 1 | rg '"kind":\["lib"\]' >/dev/null; then cargo test --doc; fi
	$(LOAD_ENV); cargo run -- --check-config
	cargo machete
	cargo deny check
	cargo build --release --locked

run:
	$(LOAD_ENV); cargo run

db-reset:
	$(LOAD_ENV); rm -f "$$SQLITE_PATH" "$$SQLITE_PATH-shm" "$$SQLITE_PATH-wal"

build:
	cargo build --release --locked
	cp target/release/$(BINARY) ./$(BINARY)

tools:
	cargo install cargo-machete
	cargo install --locked cargo-deny

doctor:
	@printf 'rustc: '; rustc --version
	@printf 'cargo: '; cargo --version
	@printf 'cargo-machete: '; if command -v cargo-machete >/dev/null 2>&1; then cargo machete --version; else printf '%s\n' 'missing'; fi
	@printf 'cargo-deny: '; if command -v cargo-deny >/dev/null 2>&1; then cargo deny --version; else printf '%s\n' 'missing'; fi

docker-build:
	docker build --pull -t $(IMAGE) .

docker-run:
	docker run -d \
		--name $(CONTAINER) \
		--restart unless-stopped \
		$(DOCKER_RUN_FLAGS) \
		$(DOCKER_NETWORK_FLAGS) \
		$(DOCKER_RUN_EXTRA) \
		$(IMAGE)

docker-deploy: docker-build
	-docker rm -f $(CONTAINER)
	docker run -d \
		--name $(CONTAINER) \
		--restart unless-stopped \
		$(DOCKER_RUN_FLAGS) \
		$(DOCKER_NETWORK_FLAGS) \
		$(DOCKER_RUN_EXTRA) \
		$(IMAGE)

docker-logs:
	docker logs -f $(CONTAINER)

clean:
	rm -f $(BINARY) ptchan-gateway-*
	cargo clean
