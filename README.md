# Staskel

A high-performance **Layer 4 (TCP/UDP) load balancer** written in Rust, designed for reliability, observability, and clean architecture.

## Features

- **TCP & UDP** traffic forwarding
- **Round-robin** and **least-connections** routing algorithms
- **Active health checking** with configurable thresholds
- **Graceful shutdown** with connection draining on SIGINT/SIGTERM
- **Structured logging** via the `tracing` ecosystem
- **Lock-free state management** using atomics for zero-contention hot paths
- **YAML configuration** with validation and sensible defaults

## Architecture

```text
                    ┌──────────────────────────────────┐
                    │           Balancer               │
  Clients ────────► │  ┌──────────┐    ┌──────────┐    │
                    │  │ TCP Proxy│    │ UDP Proxy│    │
                    │  │ :8080    │    │ :5353    │    │
                    │  └────┬─────┘    └────┬─────┘    │
                    │       │               │          │
                    │       ▼               ▼          │
                    │  ┌──────────────────────────┐    │
                    │  │   Router (per frontend)  │    │
                    │  │  • Round Robin           │    │
                    │  │  • Least Connections     │    │
                    │  └────────────┬─────────────┘    │
                    │               │                  │
                    │               ▼                  │
                    │  ┌──────────────────────────┐    │
                    │  │  Backend Pool (atomics)  │    │
                    │  │   ┌────┐ ┌────┐ ┌────┐   │    │
                    │  │   │ B1 │ │ B2 │ │ B3 │   │    │
                    │  │   └────┘ └────┘ └────┘   │    │
                    │  └──────────────────────────┘    │
                    │               ▲                  │
                    │  ┌────────────┴─────────────┐    │
                    │  │    Health Checker        │    │
                    │  │  TCP probe @ interval    │    │
                    │  └──────────────────────────┘    │
                    └──────────────────────────────────┘
                                    │
                                    ▼
                    ┌────┐  ┌────┐  ┌────┐
                    │ S1 │  │ S2 │  │ S3 │  Backend Servers
                    └────┘  └────┘  └────┘
```

### How It Works

1. **Frontend listeners** bind to configured addresses and accept traffic (TCP connections or UDP datagrams).
2. A **Router** selects a backend from the pool using the configured algorithm (round-robin or least-connections).
3. **TCP Proxy** opens a connection to the backend and copies bytes bidirectionally using `tokio::io::copy_bidirectional`.
4. **UDP Proxy** maintains per-client sessions with dedicated sockets, relaying datagrams to/from backends.
5. **Health Checker** probes each backend with periodic TCP connects, removing unhealthy nodes from rotation.
6. All state is managed via **lock-free atomics** — no mutexes in the hot path.

## Prerequisites

- **Rust** 1.75+ (2021 edition)
- **Cargo** (included with Rust)

## Installation

```bash
# Clone the repository
git clone https://github.com/bagasdisini/staskel.git
cd staskel

# Build in release mode
cargo build --release

# The binary is at target/release/staskel
```

## Usage

### 1. Create a Configuration File

Copy the example and edit it:

```bash
cp config.example.yaml config.yaml
```

### 2. Configure Your Frontends and Backends

```yaml
frontends:
  - name: http
    listen: "0.0.0.0:8080"
    protocol: tcp
    backends: web_pool
    algorithm: round_robin

backend_pools:
  web_pool:
    backends:
      - "10.0.0.1:3000"
      - "10.0.0.2:3000"
      - "10.0.0.3:3000"

health_check:
  interval_secs: 10
  timeout_secs: 3
  unhealthy_threshold: 3
  healthy_threshold: 1
```

### 3. Start the Load Balancer

```bash
# With default config path (config.yaml)
./target/release/staskel

# With custom config path
./target/release/staskel --config /etc/staskel/config.yaml

# With debug logging
RUST_LOG=debug ./target/release/staskel
```

### 4. Graceful Shutdown

Send `SIGINT` (Ctrl-C) or `SIGTERM` to shut down gracefully. Active connections will be drained before the process exits.

## Configuration Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `frontends[].name` | string | required | Human-readable name for logging |
| `frontends[].listen` | string | required | Bind address (`host:port`) |
| `frontends[].protocol` | `tcp` / `udp` | required | Network protocol |
| `frontends[].backends` | string | required | Backend pool name |
| `frontends[].algorithm` | `round_robin` / `least_connections` | required | Routing algorithm |
| `backend_pools.<name>.backends[]` | string | required | Backend addresses |
| `health_check.interval_secs` | u64 | `10` | Probe interval |
| `health_check.timeout_secs` | u64 | `3` | Probe timeout |
| `health_check.unhealthy_threshold` | u32 | `3` | Failures before unhealthy |
| `health_check.healthy_threshold` | u32 | `1` | Successes before healthy |

## Running Tests

```bash
# Run all tests (unit + integration)
cargo test

# Run with output visible
cargo test -- --nocapture

# Run only unit tests
cargo test --lib

# Run only integration tests
cargo test --test integration

# Run clippy lints
cargo clippy -- -D warnings
```

## License

MIT — see [LICENSE](LICENSE) for details.
