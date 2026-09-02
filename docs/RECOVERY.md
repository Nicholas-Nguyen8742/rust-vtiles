# DLQ and Replay (Sequence 3)

The recoverable failure path for CRE geospatial ingestion: failed jobs are
**classified**, **captured with full context**, **quarantined with
remediation guidance**, and **replayed only when safe** — under the original
job identity, through the atomic publishing flow (Sequence 2), so recovery
never produces duplicate tiles or partially published layers.

## Error classes and replay eligibility (US-03)

Every failure carries its taxonomy code (`docs/ERRORS.md`) **and** a class
that drives retry/replay decisions. Classification is deterministic
(`recovery::classify_code`), persisted on the job record (`errorClass`,
`replayEligible`), and recorded in the DLQ and quarantine report.

| Class | Codes | Retry? | Replay? |
|---|---|---|---|
| `TRANSIENT` | `PROCESSING_TIMEOUT`, `S3_THROTTLED`, `ECS_TASK_TIMEOUT`, `TEMPORARY_INTERNAL_ERROR`, `INTERNAL_ERROR`, `STORE_ERROR`, `PROMOTION_CONFLICT` | yes, with backoff | yes |
| `PERMANENT_VALIDATION` | `INVALID_FILE_TYPE`, `INVALID_SHAPEFILE`, `MISSING_SHAPEFILE_COMPONENTS`, `INVALID_GEOJSON`, `EMPTY_DATASET`, `UNSUPPORTED_CRS`, `GEOMETRY_ERRORS`, `ENCODING_ERROR`, `PAYLOAD_TOO_LARGE`, `FILE_TOO_LARGE`, `TILE_SIZE_EXCEEDED`, `TILE_GENERATION_FAILED`, `PUBLISH_VALIDATION_FAILED` | no | **no** — fix the source and submit a new upload |
| `PERMANENT_VALIDATION` (special) | `UNKNOWN_CRS` | no | yes — replay with `--assume-wgs84` is the TRD §10 user confirmation |
| `MANUAL_REVIEW` | `PIPELINE_ERROR`, `INGEST_FAILED`, unknown codes | no | no — investigate first |

`replay_eligible = class ∈ {TRANSIENT} ∪ {UNKNOWN_CRS}` — enforced
server-side in `replay::replay_job` and in the replay API route. Replaying a
non-eligible failure returns `REPLAY_NOT_ALLOWED` with remediation guidance,
emits `vector.tile.job.replay.denied`, and increments `replay_rejected_count`.

## Retry policy and DLQ capture (US-01)

Production mirrors an SQS redrive policy; locally each run/replay is one
delivery attempt and the wrapper applies the same decision logic:

```text
maxReceives: 4
backoff:     0s, 30s, 60s, 120s (exponential)
```

On each failed attempt:

1. The failure is classified. **Transient** failures with attempts remaining
   reset the job to `QUEUED` and emit `vector.tile.job.retry_scheduled`.
2. When retries are exhausted — or the failure is not transient — the job is
   **dead-lettered**: a `DlqRecord` is written and
   `vector.tile.job.dead-lettered` emitted.

DLQ layout and record schema (`data/dlq/{tenantId}/{jobId}.json`):

```json
{
  "jobId": "job_...",
  "tenantId": "tenant_acme",
  "layerId": "nyc_parcels",
  "sourceUri": ".../staging/tenant_acme/job_.../input/parcels.zip",
  "errorCode": "PROCESSING_TIMEOUT",
  "errorClass": "TRANSIENT",
  "failedStage": "TILING",
  "errorMessage": "...",
  "retryCount": 4,
  "maxReceives": 4,
  "replayEligible": true,
  "failedAt": "2026-06-17T16:10:00Z"
}
```

A **successful replay removes the DLQ entry** (the redrive consumed the
message); a failed replay is captured again with an updated `retryCount`.

Inspect locally:

```bash
make dlq                          # all tenants
make dlq DLQ_TENANT=tenant-acme   # one tenant
# or: vtile dlq list --data-dir ./data [--tenant tenant-acme]
# or: GET /api/v1/ops/dlq[?tenantId=...]
```

## Quarantine and failure reports (US-02)

Ingest failures quarantine the source bytes plus an enriched,
machine-readable report (`quarantine/{tenantId}/{jobId}/`):

```json
{
  "jobId": "job_...",
  "tenantId": "tenant_acme",
  "layerId": "nyc_parcels",
  "sourceUri": "...",
  "errorCode": "MISSING_SHAPEFILE_COMPONENTS",
  "errorMessage": "Missing required .dbf file.",
  "failedStage": "NORMALIZING",
  "errorClass": "PERMANENT_VALIDATION",
  "replayEligible": false,
  "remediation": "Upload a zipped Shapefile containing .shp, .shx, .dbf, and .prj.",
  "quarantineUri": "data/quarantine/tenant_acme/job_.../input.bin",
  "quarantinedAt": "2026-06-17T16:12:00Z"
}
```

Every code has operator-facing remediation text (`recovery::remediation_for`)
so CRE data engineers can fix vendor/county source problems without log
archaeology. Quarantine writes emit `vector.tile.job.quarantined`.

## Replay workflow (US-04/US-05)

Replay re-runs a job under its **original `jobId`, idempotency key, and
source bytes** — the local analog of an SQS DLQ redrive that preserves the
original message identity (TRD §14).

```bash
# CLI
make replay-job TENANT=tenant-acme JOB_ID=job_... ASSUME_WGS84=1 \
  REASON="Transient Fargate timeout"
vtile replay --data-dir ./data --tenant tenant-acme --job-id job_... \
  --assume-wgs84 --requested-by sre-user --reason "..."

# API (Sequence 3 US-04)
curl -s -X POST localhost:8080/api/v1/ops/jobs/{jobId}/replay \
  -H 'Content-Type: application/json' \
  -d '{"reason":"Retry after increasing Fargate memory",
       "requestedBy":"sre-user@company.com",
       "assumeCrsWgs84":true}'
```

Guardrails (all enforced server-side, all audited):

| Condition | Outcome |
|---|---|
| `FAILED` + replay-eligible class | replay accepted; job re-enters at `QUEUED` |
| `FAILED` + permanent class | `422 REPLAY_NOT_ALLOWED` + remediation |
| Replay limit exhausted (`MAX_MANUAL_REPLAYS` = 3) | `422 REPLAY_NOT_ALLOWED` — submit a new upload |
| Active job (`QUEUED`…`PUBLISHING`) | `409 JOB_ALREADY_ACTIVE` |
| `CANCELLED` | `422 REPLAY_NOT_ALLOWED` |
| `COMPLETED` without `createNewVersion` | `REPLAY_NO_OP` — nothing to do, no state change |
| Cross-tenant | denied (403 API / error CLI), never replayed |

Every accepted replay records `replayAudit` on the job (`requestedBy`,
`reason`, `createNewVersion`, timestamp), increments `replayCount`, and emits
`vector.tile.job.replay_requested`; completion/failure emit
`vector.tile.job.replay.completed` / `.failed`.

Because replay goes through `run_job`, publication uses the same atomic
candidate → validate → conditional-promote flow (Sequence 2): a replayed
parcel layer can never expose partial tiles or duplicate a published version,
and the outcome remains rollback-safe.

## Observability and audit (US-06)

Metrics (merged into `GET /internal/metrics`; local analog of the CloudWatch
dashboard): `dlq_message_count`, `quarantine_object_count`,
`replay_success_count`, `replay_failure_count`, plus the idempotency suite's
`replay_requested_count` / `replay_rejected_count`.

Events: `vector.tile.job.retry_scheduled`, `vector.tile.job.dead-lettered`,
`vector.tile.job.quarantined`, `vector.tile.job.replay_requested`,
`vector.tile.job.replay.completed`, `vector.tile.job.replay.failed`,
`vector.tile.job.replay.denied`.

Production alert thresholds (configure on the CloudWatch metrics):

| Alert | Threshold | Severity |
|---|---|---|
| DLQ depth > 0 | any dead-lettered job | P2 |
| DLQ depth > 10 in 15 min | burst of failures | P1 |
| Replay failure rate > 20% | recovery itself failing | P2 |
| Quarantine access denied | cross-tenant attempt | P1 security review |

Audit trail: job records keep the full failure taxonomy; the DLQ keeps
attempt history; `replayAudit` records who replayed and why; the publish
`audit.jsonl` (Sequence 2) shows the resulting version changes. All are
tenant-scoped — cross-tenant access is denied by construction (tenant-prefixed
paths + API tenant checks).

## Test matrix (`tests/recovery.rs`)

- Classification matrix + replay eligibility rules (US-03)
- Retry policy bounds and backoff schedule (US-01)
- Permanent failure → DLQ without retries; enriched quarantine report
  (US-01/US-02)
- Transient failure → retry → success, no DLQ (US-01)
- Transient failure → retries exhausted → DLQ with `retryCount` (US-01)
- Replay denied for permanent failures + denied event (US-03)
- `UNKNOWN_CRS` replay with confirmation succeeds and clears the DLQ (US-05)
- Replay limit enforcement (US-04)
- Completed-job replay is a no-op (US-05)

## Local → production mapping

| Local | Production (TRD §2/§11) |
|---|---|
| `run_job_with_retries` backoff loop | SQS redrive policy (maxReceiveCount 4, exponential backoff) |
| `FileDlqStore` (`data/dlq/`) | SQS dead-letter queue (14-day minimum, 30-day recommended retention, SSE-KMS) |
| `FileQuarantineStore` | S3 quarantine prefix, SSE-KMS, lifecycle expiration unless flagged |
| `LoggingEventEmitter` events | EventBridge rules → ops channel alerts |
| Replay API actor from token | RBAC via API Gateway + Cognito groups |
| `/internal/metrics` counters | CloudWatch custom metrics + dashboard |
