# Execution Semantics

Relay provides **at-least-once execution**. This document details the invariants, failure scenarios, and edge cases handled by the system.

## Invariants

1. **Attempt ID Uniqueness**: Every assignment of a job to a worker generates a unique attempt identifier (`att-<uuid>`) with an incremented `attempt_number`.
2. **Lease Exclusivity**: A job can have at most one active lease at any given moment.
3. **Stale Completion Rejection**: The scheduler verifies that the `attempt_id` reported on completion matches `current_attempt_id` and that the job is not in a terminal state (`CANCELLED`, `SUCCEEDED`, `FAILED`). Results from previous or expired attempts are rejected.
4. **Dynamic Resource Budgeting**: Workers request tasks using only unallocated capacity. The scheduler decrements capacity per candidate claimed in a batch.
5. **Real-time Log Persistence**: Child process stdout and stderr are piped and flushed line-by-line to disk in real time.

## State Machine

```
              +---------+
              | BLOCKED | (waiting on upstream dependencies)
              +----+----+
                   | parents succeed
                   v
              +---------+
       +----->| QUEUED  |<-------------+
       |      +----+----+              |
       |           | worker pull       |
       |           v                   |
       |      +----------+             |
       |      | ASSIGNED |             |
       |      +----+-----+             |
       |           | worker renews     |
       |           v                   |
       |      +---------+              |
       |      | RUNNING |              |
       |      +----+----+              |
       |           |                   |
       |   +-------+-------+           |
       |   |               |           |
       | exit 0         failure /      |
       |   |         lease expired     |
       |   v               |           |
       | +-----------+     v           |
       | | SUCCEEDED | +----------+    |
       | +-----------+ | RETRYING +----+
       |               +----+-----+ backoff elapsed
       |                    |
       |            retries exhausted
       |                    v
       |               +----------+
       |               |  FAILED  |
       |               +----------+
       |
       | User abort
       +---------------------> CANCELLED
```

## Failure Scenarios

### Worker Disconnection / Crash
- Worker A is executing job 123 (Attempt 1).
- Worker A stops sending heartbeats and lease renewals.
- After 15 seconds, the scheduler marks Worker A as `UNAVAILABLE`.
- After the lease expires (30 seconds), the scheduler lease reaper:
  1. Marks Attempt 1 as `LOST`.
  2. Increments `retry_count` on job 123.
  3. Transitions job 123 to `RETRYING` with exponential backoff delay `2^retry_count` seconds.
- Once the delay elapses, job 123 becomes eligible for claiming.
- Worker B claims job 123 (Attempt 2).

### Late Completion (Network Partition)
- Worker A loses network connectivity while running job 123 (Attempt 1).
- The scheduler marks the lease expired and reassigns job 123 to Worker B (Attempt 2).
- Worker A reconnects and reports successful execution of Attempt 1.
- The scheduler detects that `attempt_id` does not match `job.current_attempt_id`.
- The scheduler ignores Worker A's report and returns `{ "status": "stale_ignored" }`.
- Worker B finishes Attempt 2 and records the official completion.

### Cancellation Race Condition
- Worker A is actively running job 123 (Attempt 1).
- The user issues `relay cancel 123`.
- The scheduler:
  1. Transitions job 123 to `CANCELLED`.
  2. Removes the active lease.
  3. Sets `current_attempt_id = NULL` and `worker_id = NULL`.
  4. Marks Attempt 1 as `CANCELLED`.
- If Worker A's subprocess finishes later and posts a completion request to `/jobs/123/complete`:
  1. The scheduler checks `job.status.is_terminal()` and `current_attempt_id`.
  2. Because the job is already `CANCELLED` and `current_attempt_id` is cleared, the scheduler returns `{ "status": "stale_ignored" }`.
  3. The job remains `CANCELLED` and is never overwritten with `SUCCEEDED` or `FAILED`.

### Non-Zero Exit Code and Retries
- If a command terminates with a non-zero exit code and `retry_count < max_retries`, the job transitions to `RETRYING`.
- Backoff increases exponentially:
  - Attempt 1 failure: retry after 2 seconds
  - Attempt 2 failure: retry after 4 seconds
  - Attempt 3 failure: retry after 8 seconds
- If `retry_count >= max_retries`, the job transitions to `FAILED`.

### Scheduler Restart
- If the scheduler process crashes, jobs in progress continue running on workers.
- When the scheduler restarts:
  1. Re-reads all state from SQLite.
  2. Cleans up leases that expired during the downtime.
  3. Reconnects with workers on their subsequent heartbeats.
  4. Accepts completions for valid in-flight attempts.
