# Idempotent Job Processing (Sequence 1)

The ingestion pipeline must be safe under retries, duplicate events, worker
restarts, and operational replays: repeated inputs must never create duplicate
jobs, duplicate tiles, conflicting layer versions, or wasted processing. For
CRE data this is a correctness requirement, not just an efficiency one —
duplicate or conflicting parcel/risk publications distort valuation, comps,
and risk overlays.

This document is the normative reference for the identity model, state
machine, lease protocol, replay rules, and telemetry. Error codes referenced
here are defined in [`ERRORS.md`](ERRORS.md).

## Guarantees

1. **One job per logical upload.** Duplicate upload requests with the same
   `Idempotency-Key` and equivalent payload return the existing job.
2. **One processing run per event.** Duplicate content events (at-least-once
   redelivery) are acknowledged and counted, never re-executed.
3. **One active worker per job.** A lease with TTL ensures a single processor
   owns a job; crashed workers are superseded only after expiry.
4. **Atomic version publication.** Every run mints a fresh `tileVersion`;
   `manifest.json` / `latest.json` swap atomically, so consumers never see
   partial tiles and replays never leave two live versions.
5. **Auditable recovery.** Replays carry operator identity + reason and emit
   `vector.tile.job.replay_requested`.

## Job identity model

| Field (`JobRecord`) | Purpose |
|---|---|
| `jobId` | Server-generated unique id created at upload request time |
| `idempotencyKey` | `sha256:` + SHA-256(tenantId + layerId + client token + processing profile) |
| `requestFingerprint` | SHA-256 over the upload payload (fileName, contentType, sourceFormat, zoom range, profile) — mismatch detection |
| `eventDedupeFingerprint` | SHA-256(tenantId + layerId + objectKey + etag + jobId) — redelivery dedupe |
| `stateVersion` | Optimistic-concurrency version; bumped by every state write |
| `leaseToken` / `lockedBy` / `leaseExpiresAt` | Active worker lease |
| `duplicateEventCount` | Duplicate events acknowledged for this job |
| `requestedTileVersion` | Tile version requested at upload time (each run still mints its own published version) |
| `replayAudit` | `{ requestedBy, reason, createNewVersion, occurredAt }` of the most recent replay |

**Client-token rule (CRE-specific).** Intentional repeat uploads — e.g. a
county parcel refresh — must receive a *new* job. Requests without an
explicit `Idempotency-Key` therefore get a server-minted unique token, so
only clients that deliberately reuse a token converge on one job.

## US-01/US-02 — Idempotent upload flow

```http
POST /api/v1/ingest/uploads
Header: Idempotency-Key: <optional client token>
```

1. Validate request (`INVALID_REQUEST` / `UNSUPPORTED_FORMAT` /
   `INVALID_FILE_TYPE`).
2. Compute `idempotencyKey` (client token, or server-minted) and
   `requestFingerprint`.
3. If a job exists for the key:
   - fingerprints match → **HTTP 200** with the existing job (metric
     `idempotent_replays`);
   - fingerprints differ → **HTTP 409** `IDEMPOTENCY_KEY_PAYLOAD_MISMATCH`
     (metric `idempotency_key_conflicts`).
4. Otherwise create the job with a **conditional create** — the local analog
   of DynamoDB `attribute_not_exists(jobId)`. Losing the creation race to a
   concurrent same-key request converges on the winner's record (step 3).
5. Response: `202` with `jobId`, `idempotencyKey`, `uploadUrl`, `expiresIn`
   (15 min), `status: UPLOAD_PENDING`.

The staged object path embeds the job id —
`staging/{tenantId}/{jobId}/input/{fileName}` — so every downstream event is
traceable to a known job (production additionally sets `x-job-id` /
`x-tenant-id` / `x-layer-id` object metadata).

## US-03 — Duplicate event suppression

The content PUT is the local event boundary (production: S3 event → SQS).
Each event is classified by `classify_ingest_event`:

| Job state (on event) | Fingerprint seen? | Decision | Effect |
|---|---|---|---|
| no job resolves | — | `Orphan` | recorded in `data/orphans/`, alerted, `404 JOB_NOT_FOUND`; **never** silently creates an untracked job |
| `UPLOAD_PENDING` | no | `StartRun` | processing starts, exactly once |
| `UPLOAD_PENDING` | yes | `DuplicateSuppressed` | ack, `duplicateEventCount++`, metric |
| `QUEUED`…`PUBLISHING` (active) | — | `DuplicateSuppressed` | ack, no new run |
| `COMPLETED` / `FAILED` / `CANCELLED` | — | `TerminalAck` | ack, no new run; a new run requires explicit replay |

Accepted events persist a `DedupeRecord` under `data/dedupe/`
(`{ dedupeKey, jobId, seenAt, sourceEventType }`).

**CRE edge case:** a large parcel Shapefile uploaded via multipart can emit
several S3 notifications; the fingerprint + status rules collapse them into
one run. (Locally the "etag" is the SHA-256 of the payload; production should
hash the object too, since multipart S3 ETags are not content hashes.)

## US-04 — Optimistic state machine and worker lease

### State machine

```text
UPLOAD_PENDING → QUEUED → VALIDATING → NORMALIZING → TILING → PUBLISHING → COMPLETED
{QUEUED..PUBLISHING} → FAILED          (failure from any active stage)
{UPLOAD_PENDING..PUBLISHING} → CANCELLED
FAILED | COMPLETED → QUEUED            (replay re-entry; COMPLETED guarded)
```

`JobStatus::can_transition_to` is the single legal-edge table. Every
transition is a **conditional update**: it asserts the stored status equals
the expected status, asserts the edge is legal, optionally asserts lease
ownership, and bumps `stateVersion`. Violations return `StateConflict` /
`LeaseConflict` (metric `job_state_transition_conflict`).

### Leases

- `run_job` acquires a lease before processing
  (`acquire_lease(jobId, workerId, 900s)` — matching the TRD 15-minute
  Fargate hard limit) and checkpoints every stage through it.
- A second worker on the same job gets `LeaseConflict` and backs off without
  touching job state or emitting a failure event (the winner owns the job).
- Expired leases may be taken over (crashed-worker recovery; metric
  `lease_expired_count`).
- The lease is released when the run settles (final record clears
  `leaseToken`/`lockedBy`/`leaseExpiresAt`).
- **Versioned output isolation:** tiles land under
  `tiles/{tenantId}/{layerId}/{tileVersion}/` and only a completed run swaps
  the manifest — a crashed run can never expose partial tiles (TRD §14).

Local note: `FileJobStore` performs read-modify-write, so the *mechanism* is
best-effort within one process; the *semantics* (version checks, lease
validation, conflict errors) mirror the DynamoDB conditional writes that
production uses, and the tests exercise them directly.

## US-05 — Replay guardrails and audit

| Job status | `createNewVersion` | Outcome |
|---|---|---|
| `FAILED` | any | allowed (canonical DLQ redrive, original `jobId`) |
| `COMPLETED` | `true` | allowed — authorized re-publication, fresh `tileVersion` |
| `COMPLETED` | `false` | rejected: "requires explicit createNewVersion intent" |
| active (`QUEUED`…`PUBLISHING`) | any | rejected: `JOB_ALREADY_ACTIVE` |
| `CANCELLED` | any | rejected |

Source bytes resolve from the **quarantine** first (DLQ redrive); when no
quarantine entry exists (`COMPLETED` re-publication), they fall back to the
staged upload, which TRD §6 retains for 30 days.

Every replay persists a `ReplayAudit` on the job and emits
`vector.tile.job.replay_requested` with `requestedBy`, `reason`,
`createNewVersion` — the audit trail for who replayed and why. Tenant
isolation is enforced (a job can only be replayed by its own tenant), and the
`run_job` idempotency guard + atomic version swap guarantee replay never
duplicates published tiles.

```bash
make replay-job TENANT=tenant-acme JOB_ID=job_... ASSUME_WGS84=1
vtile replay --data-dir ./data --tenant tenant-acme --job-id job_... \
    --requested-by sre-user --reason "Transient Fargate timeout" \
    [--create-new-version] [--assume-wgs84]
```

## US-06 — Telemetry

`IdempotencyMetrics` (process-wide atomics; the local analog of the CloudWatch
custom metrics) is served at **`GET /internal/metrics`**:

| Metric | Meaning |
|---|---|
| `duplicate_events_suppressed` | Events acked without starting new work |
| `idempotency_key_conflicts` | Key reused with a different payload (409s) |
| `idempotent_replays` | Duplicate uploads resolved to an existing job |
| `orphan_events_detected` | Events with no resolvable job |
| `lease_acquisition_success` | Leases acquired |
| `lease_acquisition_conflict` | Workers rejected by an active foreign lease |
| `lease_expired_count` | Takeovers of expired leases |
| `replay_requested_count` | Replays attempted |
| `replay_rejected_count` | Replays rejected by guardrails |
| `job_state_transition_conflict` | Conditional transitions rejected |

Structured logs carry `jobId`, `tenantId`, `layerId`, `idempotencyKey`,
`stateVersion`, lease token, and decision/`duplicateEvent` flags — never
CRE client data values.

## Test matrix (`tests/idempotency.rs`, `tests/end_to_end.rs`)

- Idempotency key stability + tenant scoping (US-01)
- Conditional create rejects duplicate job ids (US-01)
- Key lookup resolves the registered job (US-01)
- Request fingerprint detects payload change (US-02)
- Event classification decision table, incl. orphans (US-03)
- Dedupe store record/detect; orphan store writes (US-03)
- Lease: acquire → conflict → expired takeover (US-04)
- Transition: expected-status + legal-edge enforcement, version bumps,
  foreign-lease rejection (US-04)
- `run_job` acquires/releases lease and bumps version (US-04)
- Replay of FAILED records audit and completes (US-05)
- Replay rejection: COMPLETED w/o `createNewVersion`, active →
  `JOB_ALREADY_ACTIVE` (US-05)
- Metrics snapshot tracks counters (US-06)
- End-to-end: rerun of a COMPLETED job rejected by the idempotency guard

## Local → production mapping

| Local (this repo) | Production (TRD §2/§11) |
|---|---|
| `FileJobStore::create` existence check | DynamoDB `PutItem` + `attribute_not_exists(jobId)` |
| `find_by_idempotency_key` file scan | DynamoDB GSI on `idempotencyKey` |
| `transition` read-check-write | DynamoDB conditional `UpdateItem` on `stateVersion`/status |
| `FileDedupeStore` | DynamoDB TTL table keyed by `dedupeKey` |
| `FileOrphanStore` + warn log | DLQ + CloudWatch alarm + on-call page |
| `IdempotencyMetrics` + `/internal/metrics` | CloudWatch custom metrics + dashboard |
| In-process lease | Fargate task lease via job record |
| `vector.tile.job.replay_requested` log line | EventBridge event → audit archive |
