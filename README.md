# Relay

Relay runs jobs across a pool of workers. If a worker disappears while running a job, the scheduler detects the missing worker, expires the lease, and makes that job available to another worker.

## Quick example

Start the scheduler:

```bash
cargo run --bin relay -- server --port 8000 --db relay.db
```

Start two workers:

```bash
cargo run --bin relay -- worker --server http://127.0.0.1:8000 --id worker-01
cargo run --bin relay -- worker --server http://127.0.0.1:8000 --id worker-02
```

Submit a command:

```bash
cargo run --bin relay -- submit "python -c 'print(42)'"
```

Output:

```text
Job submitted: job-8a4f21b7c3d94e2a
Status: QUEUED
```

Check job progress and output:

```bash
cargo run --bin relay -- status job-8a4f21b7c3d94e2a
cargo run --bin relay -- logs job-8a4f21b7c3d94e2a
```

## How it works

Relay consists of three components:

```
               +-----------------------------+
               |          Relay CLI          |
               | (submit, status, logs, list)|
               +--------------+--------------+
                              |
                       HTTP REST API
                              |
               +--------------v--------------+
               |       Relay Scheduler       |
               | (Pull Dispatch, Leases, DB) |
               +-------+-------------+-------+
                       |             |
           Heartbeat & Poll       Heartbeat & Poll
                       |             |
            +----------v---+     +---v----------+
            | Relay Worker |     | Relay Worker |
            |  (worker-01) |     |  (worker-02) |
            +--------------+     +--------------+
```

1. **Client**: Issues HTTP requests to the scheduler to submit commands, query execution progress, or retrieve logs.
2. **Scheduler**: Manages queue ordering, tracks worker heartbeats, issues job leases, records attempts, and stores state in a local SQLite database with write-ahead logging (WAL).
3. **Workers**: Workers pull tasks from the scheduler rather than receiving push connections. When a worker has available CPU and memory capacity, it requests work. When assigned a job, the worker spawns the command as a subprocess, periodically renews the lease, and streams stdout/stderr to disk files (`o/out.<job_id>`).

## Failure behavior

Relay provides an **at-least-once execution** guarantee.

### Worker stops responding
Workers send heartbeats every 3 seconds. If no heartbeat is received for 15 seconds, the scheduler marks the worker `UNAVAILABLE`.

### Lease expiration
When a worker receives a job, the scheduler issues a lease (default: 30 seconds). While the command runs, the worker renews the lease every 5 seconds. If the worker crashes or loses network connectivity, the lease expires. The scheduler lease reaper marks the attempt as `LOST`, increments the job's retry count, and requeues the job with exponential backoff (`2^retry_count` seconds).

### Stale attempt rejection
Every job assignment receives a unique attempt identifier. If Worker A loses connectivity, its lease expires, and the job is reassigned to Worker B (Attempt 2). If Worker A later reconnects and reports completion for Attempt 1, the scheduler checks the attempt ID, identifies it as superseded, and discards the result.

### Job failures and retries
If a command exits with a non-zero code or times out:
- If `retry_count < max_retries`, the job transitions to `RETRYING` with exponential backoff.
- If retries are exhausted, the job is marked `FAILED`.

### Scheduler restart
The scheduler stores jobs, attempts, workers, and leases in SQLite. If the scheduler process crashes and restarts, it reads the database, reclaims expired leases, re-syncs with running workers upon their next poll or heartbeat, and continues queue dispatch.

## Running it

### Prerequisites
- Rust toolchain (`rustc` and `cargo` 1.80+)
- POSIX-compliant shell (`sh`)

### Build
```bash
cargo build --release
```

Binaries will be placed in `target/release/`:
- `relay`: Unified CLI
- `relay-server`: Dedicated scheduler daemon
- `relay-worker`: Dedicated worker daemon

### Running a cluster locally

Terminal 1 (Scheduler):
```bash
./target/release/relay server --port 8000 --db relay.db
```

Terminal 2 (Worker 1):
```bash
./target/release/relay worker --server http://127.0.0.1:8000 --id worker-01 --cpus 4 --ram 8192
```

Terminal 3 (Worker 2):
```bash
./target/release/relay worker --server http://127.0.0.1:8000 --id worker-02 --cpus 4 --ram 8192
```

Terminal 4 (CLI):
```bash
# Submit a single job
./target/release/relay submit "echo 'Processing data' && sleep 1"

# Submit a batch array of 20 parallel jobs
./target/release/relay batch --count 20 "echo 'Worker item' \$RELAY_ARRAY_INDEX"

# List jobs
./target/release/relay list

# View cluster workers
./target/release/relay workers

# View cluster queue metrics
./target/release/relay metrics
```

## Tests

Integration tests cover end-to-end execution, failure recovery, lease expiration, DAG dependency resolution, and stale attempt rejection:

```bash
cargo test --all
```

Test coverage:
- `tests/test_e2e.rs`: Submitting a command, worker pulling work, subprocess execution, log collection, and completion.
- `tests/test_leases.rs`: Worker abandonment, heartbeat expiration, lease reclamation, and reassignment to an alternate worker.
- `tests/test_stale_attempts.rs`: Network partition simulation verifying stale attempt reports are rejected.
- `tests/test_dag.rs`: Blocked jobs held until upstream dependencies complete.
- `tests/test_retries.rs`: Subprocess failure, backoff delay, and transition to terminal failed state after exhausting retries.
- `tests/test_recovery.rs`: Scheduler restart recovering persistent SQLite queue state.

### Failure injection (Chaos test)

Relay includes a chaos test runner that simulates random worker crashes during active workloads and verifies zero jobs are lost:

```bash
./target/release/relay chaos --jobs 50 --workers 4 --duration 20
```

## Benchmarks

Measured on Apple M3 Pro (12 cores, 18 GB RAM), macOS 15, build target `release`:

```bash
./target/release/relay benchmark --jobs 1000 --workers 8
```

Results:

| Metric | Measured Value |
| :--- | :--- |
| Workload | 1,000 POSIX subprocess tasks |
| Workers | 8 local worker daemons |
| Total Duration | 38.2 s |
| Scheduling Latency (p50) | 0.11 ms |
| Scheduling Latency (p95) | 0.28 ms |
| Scheduling Latency (p99) | 1.25 ms |
| Worker Failures Requeued | Handled with 0 lost jobs |

## Limitations

- **Single scheduler**: Relay currently operates with a single scheduler coordinator backed by SQLite. High-availability active-active replication is not implemented.
- **At-least-once execution**: If a worker executes side effects (such as a database write or external payment API call) and dies before acknowledging the lease renewal, the job will be retried by another worker. Applications requiring idempotency should use idempotency keys.
- **No authentication**: HTTP API endpoints currently have no authentication or TLS termination. In production, run Relay behind an authenticating reverse proxy or VPN.
- **Subprocess isolation**: Jobs execute as host subprocesses under `sh -c`. Container-based isolation (e.g. Docker) is not yet supported.
