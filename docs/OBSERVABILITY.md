# Observability and Operational Trust (Sequence 4)

Production-grade observability across ingestion, processing, publishing, tile
delivery, tenant access, and data-quality outcomes — so failures are
**detectable, diagnosable, auditable, and recoverable** for CRE workflows
(parity with TRD §15).

Local surfaces:

| Surface | Local | Production |
|---|---|---|
| Structured logs | `tracing` JSON subscriber | CloudWatch Logs (queryable < 1 min) |
| Metrics | `GET /internal/metrics` | CloudWatch custom metrics |
| Dashboards | `GET /internal/dashboard` | CloudWatch dashboards |
| Alerts | `GET /internal/alerts` (rule evaluation) | CloudWatch alarms → on-call |
| Tracing | `traceId`/`spanId` correlation (W3C-shaped) | X-Ray / OpenTelemetry |
| Audit | `data/audit/audit.jsonl` + per-layer `audit.jsonl` | CloudTrail + S3 access logs |

## US-OBS-01 — Structured logging and correlation

Every lifecycle boundary emits a [`StageLog`](../crates/vtile-pipeline/src/obs.rs)
document through `tracing` with a fixed camelCase schema:

```json
{
  "service": "vector-tile-processor",
  "environment": "local",
  "event": "TILE_GENERATION_COMPLETED",
  "tenantId": "tenant_acme",
  "jobId": "job_123",
  "layerId": "nyc_parcels",
  "stage": "TILING",
  "sourceFormat": "SHAPEFILE",
  "crs": "EPSG:4326",
  "featureCount": 850000,
  "durationMs": 180000,
  "traceId": "…32 hex…",
  "spanId": "…16 hex…"
}
```

Required stages (all emitted): `UPLOAD_REQUESTED`, `UPLOAD_COMPLETED`,
`JOB_SUBMITTED`, `VALIDATION_STARTED/COMPLETED`,
`NORMALIZATION_STARTED/COMPLETED`, `TILE_GENERATION_STARTED/COMPLETED`,
`PUBLISH_STARTED/COMPLETED`, `JOB_FAILED`, `JOB_RETRIED`, `JOB_SENT_TO_DLQ`.

Correlation:

- `traceId` — minted at upload (`JobRecord.traceId`), carried through every
  stage log for the job's lifetime: search logs by `jobId`, `tenantId`,
  `layerId`, or `traceId`.
- `spanId` — one per stage event (16 hex chars).
- `environment` — `VTILE_ENVIRONMENT` env var (default `local`).

**Redaction:** logs carry only operational context (counts, sizes, durations,
taxonomy codes). Feature property values — including owner names and financial
fields — never appear in logs; the property denylist strips them from tiles
during normalization (`vtile-ingest`).

## US-OBS-02 — Core pipeline metrics

Dimensioned registry (`ObsMetrics`) served under `pipeline` at
`GET /internal/metrics`, alongside the `idempotency`, `publishing`, and
`recovery` families from Sequences 1–3.

| Metric | Type | Notes |
|---|---|---|
| `ingest_uploads_requested_total` | counter | per tenant + sourceFormat |
| `ingest_uploads_completed_total` | counter | content accepted |
| `ingest_jobs_submitted_total` | counter | |
| `ingest_jobs_started_total` | counter | lease acquired |
| `ingest_jobs_completed_total` | counter | |
| `ingest_jobs_failed_total` | counter | + `errorCode` dimension |
| `ingest_job_duration_seconds` | histogram | count/sum/avg/min/max/p50/p95 |
| `ingest_validation_failures_total` | counter | permanent-validation class |
| `ingest_retry_total` | counter | per errorCode |
| `ingest_dlq_messages_total` | counter | per errorCode |
| `geospatial_features_processed_total` | counter | |
| `geospatial_tiles_published_total` | counter | |
| `geospatial_output_bytes` | counter | gzipped bytes |
| `geospatial_tile_size_bytes` | histogram | per-run max tile size |
| `layers_published_total` | counter | per layerCategory |
| `layer_publish_duration_seconds` | histogram | |
| `layer_max_tile_size_bytes` | histogram | per publish |
| `tile_requests_total` | counter | per tenant + status class |
| `tile_request_duration_seconds` | histogram | |
| `tile_cache_hits_total` / `tile_cache_misses_total` | counter | `304` vs. origin serve |
| `tile_4xx_total` / `tile_5xx_total` | counter | |
| `tile_empty_responses_total` | counter | `204` voids |
| `tile_payload_bytes` | counter | |
| `tenant_authorization_denied_total` | counter | security-review signal |
| `cross_tenant_access_attempt_total` | counter | |
| `authorization_failure_count` | counter | any authorization failure (Sequence 5) |
| `privileged_access_count` | counter | break-glass events (Sequence 5) |
| `replay_operation_total` | counter | |

**Dimensions (bounded):** `environment`, `tenantId`, `sourceFormat`,
`layerCategory`, `status`, `errorCode`. Cardinality guardrail: raw file
names, asset IDs, and other unbounded values are never used as labels.
Per-category publish splits (`parcel_layers_published_total`, …) are covered
by the `layerCategory` dimension on `layers_published_total`.

Histograms keep a capped reservoir (4096 samples) for percentile estimates —
the local mirror of CloudWatch histogram metrics; emit paths never block job
processing.

## US-OBS-03 — Dashboards and alerting

`GET /internal/dashboard` returns one JSON view: job state counts
(uploadPending/queued/active/completed/failed/cancelled), `dlqDepth`,
per-layer quality (`tileVersion`, `tileCount`, `generatedAt`,
`stalenessSeconds`), currently triggered alerts, and the full metrics
snapshot.

`GET /internal/alerts` evaluates the rule catalog below against current
telemetry. Rules referencing production-only metrics (CloudFront, windowed
queue stalls) report `currentValue: null` locally.

| Rule | Severity | Condition |
|---|---|---|
| `Tile5xxRateHigh` | **P1** | tile 5xx / requests > 1% |
| `OriginFailureRateHigh` | **P1** | CloudFront origin failure > 1% (prod metric) |
| `DlqMessageReceived` | P2 | DLQ depth > 0 |
| `JobFailureRateHigh` | P2 | failed / (completed+failed) > 5% |
| `NoCompletedJobsWithBacklog` | P2 | windowed; evaluate on SQS/CloudWatch |
| `JobDurationHigh` | P3 | job duration p95 > 2× baseline |
| `TileSizeP95High` | P3 | tile size p95 > 500 KB |
| `ReplayFailureRateHigh` | P2 | replay failures / requests > 20% |
| `TenantAuthorizationFailureSpike` | P2 | any cross-tenant denial |
| `LayerStalenessHigh` | P2 | max layer staleness > 7 days |
| `RollbackOccurred` | P2 | any rollback |
| `CrossTenantDenialSpike` | P2 | > 5 cross-tenant denials (Sequence 5 TI-06) |
| `PrivilegedAccessOccurred` | P2 | any break-glass / privileged access (Sequence 5 TI-06) |

Alert payloads carry severity, environment, rule name, current value, and a
runbook link (`runbook` field). Production wiring: CloudWatch alarms on the
same metric names → SNS → on-call channel; see
`terraform/cloudwatch.tf` for the alarm + dashboard definitions.

## US-OBS-04 — Distributed tracing and failure forensics

Local model: the `traceId` minted at upload propagates through every stage
log and event for the job, so the full path (upload → validation →
normalization → tiling → publish, or the failing stage) is reconstructable by
searching one id — the log-to-trace linkage the epic requires. Stage logs
carry `durationMs`, so the slowest stage of a slow job is directly visible.

Sampling policy (production): 100% of errors and staging traffic; lower
baseline sampling in healthy prod with force-capture on failure. Trace
retention ≥ 30 days. Span attributes never contain sensitive data (same
redaction rules as logs).

## US-OBS-05 — Tenant-scoped audit and compliance

Append-only central trail: `data/audit/audit.jsonl`
(`FileAuditTrail`), plus the per-layer publish/rollback trail
(`manifests/{tenant}/{layer}/audit.jsonl`, Sequence 2).

Audit event catalog:

| Event | Emitted by |
|---|---|
| `upload.initiated` | `POST /api/v1/ingest/uploads` |
| `upload.completed` | content PUT accepted |
| `layer.published` | atomic promotion (`run_job`) |
| `tile.version.rolled_back` | `rollback_layer_version` |
| `job.replayed` | `POST /api/v1/ops/jobs/:job_id/replay` (operator + reason) |
| `tenant.access.denied` | any cross-tenant 403 (tiles, jobs, replay) |
| `manifest.updated` | covered by `layer.published` (manifest swap) |

Every record answers who/what/when/succeeded:

```json
{ "eventType": "layer.published", "eventId": "evt_…", "tenantId": "tenant_acme",
  "layerId": "nyc_parcels", "jobId": "job_123",
  "tileVersion": "2026-06-17T17-00-00Z-…", "actor": "pipeline:vtile-publisher",
  "succeeded": true, "occurredAt": "2026-06-17T17:05:00Z" }
```

Query tenant-scoped (auth pins callers to their own tenant when enabled):

```bash
curl -s 'localhost:8080/api/v1/ops/audit?tenantId=tenant-acme&eventType=job.replayed&limit=50'
```

Sequence 5 extends the trail with `tenant.access.decision` records (ALLOW and
DENY on control-plane operations, incl. `resourceType`/`resourceId`/`action`/
`decision` fields) and `tenant.break_glass` events for privileged access.

SOC2 alignment: append-only writes, retention per TRD §6 (audit ≥ 7 years in
production), restricted access via API auth, and no cross-tenant exposure.

## US-OBS-06 — Tile delivery and CRE data-quality telemetry

Delivery metrics are recorded on every tile request (both unversioned and
explicit-version routes): request counts by status class, latency histogram,
`204` empty counts, `304` cache hits (conditional requests via
`If-None-Match`), and payload bytes. Targets: P95 < 50 ms US / 150 ms EMEA
(MVP), 5xx < 0.1% sustained.

Layer quality (dashboard + metrics): `feature_count`, `tile_count`,
`max_tile_size_bytes`, publish duration, `generatedAt` →
`stalenessSeconds`. Quality checks available from state:

- metadata vs. manifest agreement (catalog `tileVersion` == registry current)
- high-zoom parcel geometry preservation (no simplification ≥ z14, TRD §4)
- feature-count deviation vs. previous version (candidate manifests keep
  `featureCount` per version for comparison)
- max tile size vs. the 250 KB preferred / 750 KB hard caps (TRD §5)

## Test matrix (`tests/observability.rs`)

- Trace/span id shape; stage-log camelCase schema (US-OBS-01)
- Metric key canonicalization; counter accumulation per dimension; histogram
  percentiles; bounded category labels (US-OBS-02)
- Alert catalog completeness; DLQ + ratio-rule evaluation; null-data safety
  (US-OBS-03)
- Audit trail append-only behavior + tenant/layer/event scoping (US-OBS-05)
- Layer freshness, DLQ depth, dashboard aggregation (US-OBS-06)
