# ForgeDB

ForgeDB is a Go implementation and test laboratory for the Raft consensus
protocol. It combines a deterministic fault-injection simulator, a pure Raft
state machine, a crash-recoverable write-ahead log, a replicated key/value
state machine, and a Porcupine linearizability test.

The project is intentionally an engineering prototype: it demonstrates and
tests consensus, durability primitives, and state-machine behavior. It is not
yet a production database server and does not expose a network client API.

## Requirements

- Go 1.24 or newer
- Dependencies are vendored; tests can run without downloading modules.

## Quick start

```sh
go test -mod=vendor ./...
go test -mod=vendor -race ./...
go vet -mod=vendor ./...
```

Replay one deterministic milestone test:

```sh
go test -mod=vendor ./test -seed=48213 -run M5 -v
```

Generate a simulation trace:

```sh
go run -mod=vendor ./cmd/trace -seed=42 -proposals=10 -out=trace.json
```

Open `web/trace.html` in a browser and load the generated `trace.json` file.
The viewer can filter events by type or search the event text. No web server,
JavaScript package manager, or external library is required.

The trace generator accepts `-nodes`, `-proposals`, `-duration`, `-drop`,
`-dup`, `-seed`, and `-out`. The seed makes the simulated schedule
reproducible.

## Architecture

- `raft/` — pure Raft state machine. It has no clock, network, disk, goroutine,
  or global-randomness dependency. The driver supplies ticks and messages and
  consumes `Ready` output.
- `sim/` — deterministic discrete-event simulator with seeded message delay,
  loss, duplication, partitions, crashes, restarts, and safety checks. Each
  simulated node persists to the real `wal/` package on an in-memory file
  (`wal.MemFS`, where unsynced bytes are lost on a crash), so every restart in
  the milestone and fuzz tests runs the real recovery code. `Config.FakeDisk`
  selects the older in-memory fake disk; a test checks that both give identical
  traces.
- `wal/` — append-only, checksummed WAL for Raft hard state and log suffixes.
  `Save` queues changes; `Sync` makes them durable; `Open` recovers synced data.
  Every record has its own header checksum (verified before the length is
  trusted) and a payload checksum. Recovery truncates only a torn tail (partial
  header, payload cut short, a damaged final record, or a zero-filled tail) and
  refuses to start, without modifying the file, on any damage that is followed
  by more data. Accepted risk: bit rot in the very last record looks like a torn
  write and is truncated; the other replicas hold that entry. Files written with
  the earlier 9-byte record header are rejected as corrupt.
  A `Save` larger than one record (16 MiB) is split across records between
  entries, hard state first, and recovery applies it all-or-nothing: a split
  `Save` cut short by a crash is discarded back to where it began. Only the first
  record of a split `Save` may carry the hard state; recovery treats any other
  placement as corruption and refuses to start. A single log entry is never
  split; `Save` returns `ErrEntryTooLarge` for one that cannot fit in a record
  by itself (the hard state never counts against it), so callers must bound
  command size below that.
- `kv/` — deterministic Put/Get state machine with JSON command encoding and
  per-client request deduplication.
- `test/` — multi-seed Raft milestone tests and a concurrent KV history checked
  for linearizability by Porcupine.
- `cmd/trace/` and `web/` — trace export command and dependency-free browser
  viewer.

## Raft driver contract

For each `Ready` batch, the driver must:

1. Persist the changed hard state and log entries.
2. Sync them to durable storage.
3. Send the outbound messages.
4. Apply committed entries to the state machine, in order.
5. Call `Advance`.

Sending messages before the corresponding state is durable can violate Raft
safety. The simulator's `SkipSync` fault switch deliberately violates this
ordering so the safety checker can prove it detects the failure.

## Milestones

- M1 — deterministic simulator and safety checker
- M2 — leader election and failover
- M3 — log replication and Figure 8 safety
- M4 — persistence behavior and crash/restart testing
- M5 — PreVote and CheckQuorum disruption protection
- M6 — KV state machine, per-client request deduplication, and Porcupine linearizability validation
- M7 — reproducible trace export and browser trace viewer

## Scope and limitations

- The simulator is not a real network transport.
- The WAL is a standalone persistence component; it is not currently wired
  into a production server process.
- The KV state machine is an in-memory component applied to committed Raft
  commands. There is no external client/server protocol or snapshot/compaction
  layer in this repository.
- The trace viewer displays simulator log events; it is not a live cluster
  dashboard.

These boundaries are deliberate. The repository demonstrates the core
consensus and testing work without claiming to be a production-ready database.
