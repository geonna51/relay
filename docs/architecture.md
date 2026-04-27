# Relay Architecture

Relay is structured as three distinct roles: the Client CLI, the Scheduler Coordinator, and the Worker Daemon.

## Process Roles

```
                   +-----------------------+
                   |       Relay CLI       |
                   | (submit, status, logs)|
                   +-----------+-----------+
                               |
                        HTTP REST API
                               |
                   +-----------v-----------+
                   |    Relay Scheduler    |
                   |   (State & Leases)    |
                   +-----+-----------+-----+
                         |           |
             Heartbeat & Poll     Heartbeat & Poll
                         |           |
              +----------v--+     +--v----------+
              |   Worker A  |     |   Worker B  |
              | (Processes) |     | (Processes) |
              +-------------+     +-------------+
```

### 1. Client (`relay`)
The client issues HTTP requests to the scheduler to submit commands, query execution progress, stream logs, or cancel running jobs. The client contains no scheduler logic; all state transitions are managed by the coordinator.

### 2. Scheduler (`relay-server`)
The scheduler is the central state coordinator. It is backed by a local SQLite database configured with write-ahead logging (WAL) and busy timeouts.

Responsibilities:
- Stores jobs, attempts, worker records, and active leases.
- Matches eligible queued jobs against worker resource profiles (CPUs, memory, labels).
- Enforces priority aging to ensure low-priority jobs progress under sustained load.
- Reaps expired worker heartbeats and active leases.
- Requeues orphaned work with exponential backoff.
- Tracks DAG dependencies and unblocks downstream tasks upon parent completion.
- Reconciles dangling state on startup after an unexpected shutdown.

### 3. Worker (`relay-worker`)
Workers pull work outward from the scheduler rather than receiving inbound connections. This allows workers to run behind firewalls and NATs without opening incoming ports.

Workflow:
1. Worker registers its hardware capacity (CPUs, memory) and optional labels on startup.
2. An asynchronous task sends heartbeats every 3 seconds to keep the worker marked active.
3. The main loop polls `/workers/{id}/poll` when local concurrency permits are available.
4. When assigned a job, the worker launches the command as a child process.
5. While the command executes, a concurrent background task renews the job lease every 5 seconds.
6. Execution output (stdout, stderr, exit code, and runtime) is streamed to `o/out.<job_id>` and reported to `/jobs/{id}/complete` along with the assigned attempt ID.
7. If a process exceeds `timeout_seconds`, the worker terminates the process and marks the attempt as timed out.
